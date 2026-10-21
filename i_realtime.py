"""I. REALTIME: event triggers; REST stays the truth (plan_review A7, A14; night plan §6).

  SigUserFeed   SIG's user channel (Supabase realtime, `user:<profileId>`): `account_batch` broadcasts with our fills and
                order updates, revision-numbered, and `notification` broadcasts (order_filled, ...) that carry no market. A
                duplicate revision is ignored; a gap (previousRevision > last accepted), `resyncRequired`, a (re)connect or
                a token refresh means "resync": the engine re-reads REST before acting. The token is refreshed every 3 h.
                (Market channels deliver nothing (RECAP §11, 9.1): SIG books stay on REST /exchanges/prices.)
                Day 2 §6 probe (13:51-13:56 PT, join + setAuth + private, the token's only channel): 5 `notification`
                order_filled broadcasts for 4 fills, 0 `account_batch`. SIG sends no account batches now, so a
                notification wakes the loop and REST /portfolio/fills (read every loop, <= 10 s apart) is the fill feed.
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
        if p.get("event") == "notification":                # e.g. order_filled: arrives even when account_batch lags
            typ = str((p.get("payload") or {}).get("type") or "")
            if typ.startswith("order_"):
                self.stats["notifications"] = self.stats.get("notifications", 0) + 1
                self.last_msg_t = self.clock()
                self.on_event("fills" if "fill" in typ else "orders", set(), self.last_msg_t)   # market unknown: REST tells
                return "notification"
            return None
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
        gap = (rev is not None and self.last_rev is not None and prev is not None and prev > self.last_rev) or bool(b.get("resyncRequired"))
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
            await ws.send(json.dumps({"topic": f"realtime:{topic}", "event": "phx_join", "ref": "1", "join_ref": "1",
                                      "payload": {"config": {"broadcast": {"ack": False, "self": False}, "presence": {"key": ""},
                                                             "postgres_changes": [], "private": True},
                                                  "access_token": tok["token"]}}))
            await ws.send(json.dumps({"topic": f"realtime:{topic}", "event": "access_token", "ref": "2", "join_ref": "1",
                                      "payload": {"access_token": tok["token"]}}))     # supabase-js setAuth()
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


class SigMarketFeed:
    """C86 (Piero's OK, Oct 8 15:35): SIG's tournament markets topic `tournament:{tid}:markets` — one private broadcast channel, a
    `markets_batch` about every second with every trade (id, exchangeId, executedAt, price, quantity, sequence) and the FULL versioned
    order book (asOf {at, sequence}) of each exchange that changed. Books and sweeps only: fills stay on the user channel + REST.
    Revisions are per market (delivery.revision / previousRevision): a gap counts and flags the market's trades as incomplete (the books
    in a batch are full snapshots, so they stay usable); a reconnect / token refresh marks every book stale until a fresh one arrives."""

    def __init__(self, mint_token, tournament_id, on_sweep=None, clock=time.time, connect=None, log=print,
                 sweep_ticks=3, sweep_shares=5_000, sweep_window_s=2.0, trade_keep_s=600):
        self.mint_token, self.tid, self.on_sweep, self.clock, self.connect, self.log = mint_token, tournament_id, on_sweep, clock, connect, log
        self.sweep_ticks, self.sweep_shares, self.sweep_window_s, self.trade_keep_s = sweep_ticks, sweep_shares, sweep_window_s, trade_keep_s
        self.books, self.trades, self.last_rev, self.incomplete = {}, {}, {}, {}
        self.epoch = 0                                      # bumps on every (re)connect: books from an older epoch are stale
        self.sweeps, self.lags, self.last_msg_t, self.connected_t = [], [], None, None
        self.stats = dict(messages=0, batches=0, markets=0, books=0, trades=0, gaps=0, duplicates=0, reconnects=0, errors=0, sweeps=0)
        self.running, self.thread, self.ws = False, None, None
        self.lock = threading.Lock()

    @staticmethod
    def _ts(iso):
        """ISO-8601 with a UTC offset -> epoch seconds (None when absent / unparseable)."""
        if not iso:
            return None
        try:
            from datetime import datetime
            return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    # ---- message handling (pure: tested without a socket)
    def handle(self, msg):
        if msg.get("event") != "broadcast":
            return None
        p = msg.get("payload") or {}
        if p.get("event") not in ("markets_batch", "market_batch"):
            return None
        inner = p.get("payload") or {}
        entries = inner.get("markets") if p.get("event") == "markets_batch" else [dict(marketId=inner.get("marketId"), batch=inner)]
        now = self.clock()
        self.last_msg_t = now
        self.stats["messages"] += 1
        out = []
        with self.lock:
            for e in entries or []:
                b = e.get("batch") or {}
                mid = str(e.get("marketId") or (b.get("delivery") or {}).get("marketId") or "")
                d = b.get("delivery") or {}
                rev, prev = d.get("revision"), d.get("previousRevision")
                last = self.last_rev.get(mid)
                if rev is not None and last is not None and rev <= last:
                    self.stats["duplicates"] += 1
                    continue
                gap = (rev is not None and last is not None and prev is not None and prev > last) or bool(b.get("resyncRequired"))
                if rev is not None:
                    self.last_rev[mid] = rev
                self.stats["batches"] += 1
                for bk in b.get("books") or []:
                    ex = str(bk.get("exchangeId"))
                    a = bk.get("asOf") or {}
                    seq, at = a.get("sequence"), self._ts(a.get("at"))
                    cur = self.books.get(ex)
                    if cur and seq is not None and cur["seq"] is not None and seq <= cur["seq"] and cur["epoch"] == self.epoch:
                        continue                            # an older or equal version
                    self.books[ex] = dict(seq=seq, at=at, recv=now, epoch=self.epoch,
                                          bids=[(float(x["price"]), float(x["quantity"])) for x in bk.get("bids") or []],
                                          asks=[(float(x["price"]), float(x["quantity"])) for x in bk.get("asks") or []])
                    self.stats["books"] += 1
                    if at is not None:
                        self.lags.append(now - at)
                        if len(self.lags) > 5000:
                            self.lags = self.lags[-2000:]
                exs = set()
                for t in b.get("trades") or []:
                    ex = str(t.get("exchangeId"))
                    q = self.trades.setdefault(ex, [])
                    q.append((self._ts(t.get("executedAt")) or now, now, float(t.get("price") or 0), float(t.get("quantity") or 0), str(t.get("id"))))
                    exs.add(ex)
                    self.stats["trades"] += 1
                for ex in exs:
                    self.trades[ex] = [x for x in self.trades[ex] if x[1] >= now - self.trade_keep_s]
                    sw = self._sweep(ex, now)
                    if sw:
                        out.append(sw)
                if gap:
                    self.stats["gaps"] += 1
                    for bk in b.get("books") or []:
                        self.incomplete[str(bk.get("exchangeId"))] = now
        for sw in out:
            self.sweeps.append(sw)
            self.stats["sweeps"] += 1
            if self.on_sweep:
                self.on_sweep(sw)
        self.sweeps = self.sweeps[-500:]
        return "batch"

    def _sweep(self, ex, now):
        """A sweep = >= sweep_shares printing through >= sweep_ticks of price on one exchange within sweep_window_s (executedAt)."""
        q = self.trades.get(ex) or []
        if not q:
            return None
        t_last = q[-1][0]
        win = [x for x in q if x[0] >= t_last - self.sweep_window_s]
        shares = sum(x[3] for x in win)
        lo, hi = min(x[2] for x in win), max(x[2] for x in win)
        if shares < self.sweep_shares or (hi - lo) < self.sweep_ticks * 0.005 - 1e-9:
            return None
        first, last_p = win[0][2], win[-1][2]
        key = (ex, round(win[0][0], 1))
        if getattr(self, "_last_sweep_key", {}).get(ex) == key:
            return None                                     # the same burst, reported once
        self._last_sweep_key = getattr(self, "_last_sweep_key", {}); self._last_sweep_key[ex] = key
        return dict(ex=ex, t=t_last, recv=now, shares=shares, lo=lo, hi=hi, direction="up" if last_p >= first else "down", n=len(win))

    def book(self, ex, max_age_s=10.0):
        """(bids, asks) best first from the feed, or None when absent, from an older connection, or older than max_age_s (asOf)."""
        with self.lock:
            b = self.books.get(str(ex))
            if not b or b["epoch"] != self.epoch or not self.connected():
                return None
            ref = b["at"] if b["at"] is not None else b["recv"]
            if self.clock() - ref > max_age_s and self.clock() - b["recv"] > max_age_s:
                return None
            return list(b["bids"]), list(b["asks"])

    def connected(self):
        return self.connected_t is not None and self.last_msg_t is not None and self.clock() - self.last_msg_t < 30

    def connection_lost(self):
        """A reconnect or token refresh: every book is stale until the next batch names it; revisions restart."""
        with self.lock:
            self.stats["reconnects"] += 1
            self.epoch += 1
            self.last_rev = {}
            self.connected_t = None

    def lag_pct(self, f):
        xs = sorted(self.lags[-2000:])
        return round(xs[min(len(xs) - 1, int(f * (len(xs) - 1)))], 2) if xs else None

    # ---- socket loop
    async def _run_once(self):
        import websockets
        tok = self.mint_token()
        url = tok["supabaseUrl"].replace("https://", "wss://") + f"/realtime/v1/websocket?apikey={tok['anonKey']}&vsn=1.0.0"
        topic = f"tournament:{self.tid}:markets"
        connect = self.connect or (lambda u: websockets.connect(u, max_size=2 ** 24))
        async with connect(url) as ws:
            self.ws = ws
            await ws.send(json.dumps({"topic": f"realtime:{topic}", "event": "phx_join", "ref": "1", "join_ref": "1",
                                      "payload": {"config": {"broadcast": {"ack": False, "self": False}, "presence": {"key": ""},
                                                             "postgres_changes": [], "private": True}, "access_token": tok["token"]}}))
            await ws.send(json.dumps({"topic": f"realtime:{topic}", "event": "access_token", "ref": "2", "join_ref": "1",
                                      "payload": {"access_token": tok["token"]}}))
            self.connection_lost()                          # a (re)connect: books from before are stale
            self.connected_t = self.clock()
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
            except Exception as e:
                self.stats["errors"] += 1
                self.log("RT", f"SIG market feed: {str(e)[:120]}; reconnecting in {backoff} s")
                self.connection_lost()
                time.sleep(backoff)
                backoff = min(60, backoff * 2)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._thread, daemon=True, name="sig-market-feed")
        self.thread.start()

    def stop(self):
        self.running = False


class PolyFeed:
    """Best bid/ask of the YES token(s) of every mapped SIG market from Polymarket's market websocket."""

    def __init__(self, tokens, on_event, clock=time.time, log=print):
        """tokens: {ex: [yes token id, ...]} (a candidate-sum market lists several)."""
        self.tokens, self.on_event, self.clock, self.log = tokens, on_event, clock, log
        self.by_token = {t: ex for ex, ts in tokens.items() for t in ts}
        self.on_quote = None                                # C102 (night item 1): per-quote hook -> the election call detector (feed thread)
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
        if self.on_quote is not None:
            try:
                self.on_quote(ex, b, a, now)
            except Exception as e:                          # the detector never breaks the feed
                self.stats["quote_hook_errors"] = self.stats.get("quote_hook_errors", 0) + 1
                if self.stats["quote_hook_errors"] <= 3:
                    self.log("ERR", f"PM quote hook: {e!r}")
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
