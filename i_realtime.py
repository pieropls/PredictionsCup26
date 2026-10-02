"""I. REALTIME: event triggers; REST stays the truth (plan_review A7, A14; night plan §6).

  SigUserFeed   SIG's user channel (Supabase realtime, `user:<profileId>`): `account_batch` broadcasts with our fills and
                order updates, revision-numbered. A duplicate revision is ignored. A gap, a (re)connect or a token refresh
                means "resync": the engine re-reads REST before acting. The token is refreshed every 3 h.
                (Market channels deliver nothing (RECAP §11, 9.1): SIG books stay on REST /exchanges/prices.)
  PolyFeed      Polymarket's CLOB market websocket for the mapped YES tokens (clobTokenIds[0], only where outcomes[0] is
                "Yes"): best bid/ask per token, summed per SIG market like c_polymarket.fairs(); the engine feeds them to its
                FairTracker between REST polls and replans a market when its Polymarket mid moves >= 1.5c.

Both run in daemon threads, never raise into the engine, and only call on_event(kind, exchange ids, received_at):
mark markets dirty and wake the planner.
"""
import asyncio
import json
import threading
import time

SIG_HEARTBEAT_S, TOKEN_REFRESH_S = 25, 3 * 3600
PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class SigUserFeed:
    def __init__(self, mint_token, on_event, clock=time.time, connect=None, log=print):
        """mint_token() -> {token, supabaseUrl, anonKey, channels: {user: topic}}; connect(url) -> async websocket."""
        self.mint_token, self.on_event, self.clock, self.log = mint_token, on_event, clock, log
        self.connect = connect
        self.last_rev, self.last_msg_t, self.connected_t = None, None, None
        self.stats = dict(batches=0, duplicates=0, gaps=0, reconnects=0, fills=0, order_updates=0, errors=0)
        self.running, self.thread, self.ws = False, None, None

    # ---- message handling (pure: tested without a socket)
    def handle(self, msg):
        """One websocket message -> what it means: None, 'resync' (gap) or 'batch' (new fills / order updates)."""
        if msg.get("event") != "broadcast":
            return None
        p = msg.get("payload") or {}
        if p.get("event") != "account_batch":
            return None
        b = p.get("payload") or {}
        d = b.get("delivery") or {}
        rev, prev = d.get("revision"), d.get("previousRevision")
        now = self.clock()
        self.last_msg_t = now
        if rev is not None and self.last_rev is not None and rev <= self.last_rev:
            self.stats["duplicates"] += 1
            return None
        fills, updates = b.get("fills") or [], b.get("orderUpdates") or []
        exs = {str(f.get("exchangeId")) for f in fills if f.get("exchangeId") is not None}
        exs |= {str(u.get("exchangeId")) for u in updates if u.get("exchangeId") is not None}
        gap = rev is not None and self.last_rev is not None and prev is not None and prev != self.last_rev
        self.last_rev = rev if rev is not None else self.last_rev
        self.stats["batches"] += 1
        self.stats["fills"] += len(fills)
        self.stats["order_updates"] += len(updates)
        if gap:
            self.stats["gaps"] += 1
            self.on_event("resync", exs, now)
            return "resync"
        if exs:
            self.on_event("fills" if fills else "orders", exs, now)
        return "batch"

    def connection_lost(self):
        """A reconnect or token refresh: whatever happened meanwhile is unknown -> REST resync before acting."""
        self.stats["reconnects"] += 1
        self.last_rev = None
        self.on_event("resync", set(), self.clock())

    # ---- socket loop
    async def _run_once(self):
        import websockets
        tok = self.mint_token()
        url = tok["supabaseUrl"].replace("https://", "wss://") + f"/realtime/v1/websocket?apikey={tok['anonKey']}&vsn=1.0.0"
        topic = (tok.get("channels") or {}).get("user")
        connect = self.connect or (lambda u: websockets.connect(u, max_size=2 ** 23))
        async with connect(url) as ws:
            self.ws = ws
            await ws.send(json.dumps({"topic": f"realtime:{topic}", "event": "phx_join", "ref": "1",
                                      "payload": {"config": {"broadcast": {"self": False}, "presence": {"key": ""}, "private": True},
                                                  "access_token": tok["token"]}}))
            self.connected_t = self.clock()
            self.connection_lost()                          # (re)connected: resync from REST before acting
            beat = self.clock()
            while self.running and self.clock() - self.connected_t < TOKEN_REFRESH_S:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    self.handle(json.loads(raw))
                except asyncio.TimeoutError:
                    pass
                if self.clock() - beat >= SIG_HEARTBEAT_S:
                    beat = self.clock()
                    await ws.send(json.dumps({"topic": "phoenix", "event": "heartbeat", "payload": {}, "ref": "hb"}))

    def _thread(self):
        backoff = 1
        while self.running:
            try:
                asyncio.run(self._run_once())
                backoff = 1
            except Exception as e:                          # never raises into the engine: reconnect with backoff
                self.stats["errors"] += 1
                self.log("RT", f"SIG user feed: {str(e)[:120]}; reconnecting in {backoff} s")
                time.sleep(backoff)
                backoff = min(60, backoff * 2)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._thread, daemon=True, name="sig-user-feed")
        self.thread.start()

    def stop(self):
        self.running = False


class PolyFeed:
    """Best bid/ask of the YES token(s) of every mapped SIG market from Polymarket's market websocket."""

    def __init__(self, tokens, on_event, clock=time.time, log=print):
        """tokens: {ex: [yes token id, ...]} (a candidate-sum market lists several)."""
        self.tokens, self.on_event, self.clock, self.log = tokens, on_event, clock, log
        self.by_token = {t: ex for ex, ts in tokens.items() for t in ts}
        self.books, self.quotes, self.last_mid = {}, {}, {}
        self.last_msg_t, self.running, self.thread = None, False, None
        self.stats = dict(messages=0, moves=0, errors=0, reconnects=0)

    def _apply(self, token, bid, ask):
        if bid is None and ask is None:
            return
        self.books[token] = (bid, ask)
        ex = self.by_token.get(token)
        legs = [self.books.get(t) for t in self.tokens.get(ex, [])]
        if ex is None or any(l is None for l in legs):
            return
        if len(legs) == 1 and None in legs[0]:
            return
        b = sum(float(l[0] or 0) for l in legs)
        a = sum(float(l[1] if l[1] is not None else (l[0] or 0)) for l in legs)
        now = self.clock()
        self.quotes[ex] = (b, a, now)
        mid, old = (a + b) / 2, self.last_mid.get(ex)
        if old is None or abs(mid - old) >= 0.015 - 1e-12:
            self.last_mid[ex] = mid
            if old is not None:
                self.stats["moves"] += 1
                self.on_event("pm_move", {ex}, now)

    def handle(self, msg):
        """One websocket payload (an event or a list of events)."""
        events = msg if isinstance(msg, list) else [msg]
        self.last_msg_t = self.clock()
        for e in events:
            if not isinstance(e, dict):
                continue
            self.stats["messages"] += 1
            kind = e.get("event_type")
            if kind == "book":
                bids = [float(x["price"]) for x in e.get("bids") or []]
                asks = [float(x["price"]) for x in e.get("asks") or []]
                self._apply(e.get("asset_id"), max(bids) if bids else None, min(asks) if asks else None)
            elif kind == "price_change":
                for c in e.get("price_changes") or e.get("changes") or []:
                    tok = c.get("asset_id") or e.get("asset_id")
                    bb, ba = c.get("best_bid"), c.get("best_ask")
                    if bb is not None or ba is not None:
                        old = self.books.get(tok, (None, None))
                        self._apply(tok, float(bb) if bb not in (None, "") else old[0], float(ba) if ba not in (None, "") else old[1])
            elif kind == "best_bid_ask":
                self._apply(e.get("asset_id"), float(e["best_bid"]) if e.get("best_bid") else None,
                            float(e["best_ask"]) if e.get("best_ask") else None)

    async def _run_once(self):
        import websockets
        async with websockets.connect(PM_WS, max_size=2 ** 24, ping_interval=20) as ws:
            await ws.send(json.dumps({"assets_ids": sorted(self.by_token), "type": "market"}))
            while self.running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    continue
                if raw in ("PONG", "PING"):
                    continue
                try:
                    self.handle(json.loads(raw))
                except ValueError:
                    continue

    def _thread(self):
        backoff = 1
        while self.running:
            try:
                asyncio.run(self._run_once())
            except Exception as e:
                self.stats["errors"] += 1
                self.log("RT", f"Polymarket feed: {str(e)[:120]}; reconnecting in {backoff} s")
            self.stats["reconnects"] += 1
            time.sleep(backoff)
            backoff = min(60, backoff * 2)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._thread, daemon=True, name="pm-feed")
        self.thread.start()

    def stop(self):
        self.running = False
