"""In-memory SIG for tests: the parts of b_sig the engine and executor use, with SIG's real failure modes.

  netting       positions are net YES per exchange; fills on opposite sides cancel out
  oversell      a sell larger than the shares it is backed by is turned into the opposite buy (SIG's canonicalisation);
                every such case is recorded in .canonicalized, which tests require to stay empty (I2)
  processing    each order takes 1-10 s (virtual clock); a batch is cut off at 60 s: the rest never lands and the client
                times out (Unknown)
  faults        queued per write: "503" (nothing lands, Unknown), ("partial", k) (k orders land, then Unknown),
                "timeout_ok" (everything lands, the client hears nothing: Unknown), ("429", s) (RateLimited),
                "cancel_timeout_ok" / "cancel_timeout_fail" for DELETEs (in .cancel_faults)
  timeline      (kind, start, end) of every write in real seconds: what was in flight when
  idempotency   a replayed key returns the stored result
  realtime      account_batches(): revision-numbered batches with duplicates and gaps (for P2)
"""
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone


class VClock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def time(self):
        return self.t

    def advance(self, dt):
        self.t += dt

    sleep = advance


class FakeSIG:
    class Unknown(Exception):
        pass

    class RateLimited(Exception):
        def __init__(self, retry_after):
            super().__init__(f"rate limited, retry after {retry_after}s")
            self.retry_after = retry_after

    def __init__(self, markets=(), prices=None, positions=None, seed=1, clock=None, real_delay=0.0, writes_per_min=25):
        self.T, self.DRY_RUN, self.WRITES_PER_MIN = {"slug": "s", "id": "t"}, False, writes_per_min
        self._writes = deque()
        self.clock = clock
        self.now = clock.time if clock else time.time
        self.rng = random.Random(seed)
        self.markets, self.px = list(markets), dict(prices or {})
        self.pos = {str(k): v for k, v in (positions or {}).items()}
        self.book, self.next_id, self.idem, self.fill_log = {}, 7_000_000, {}, []
        self.faults, self.cancel_faults, self.calls, self.canonicalized, self.timeline = [], [], [], [], []
        self.writing = {}                                  # writes on the wire right now: token -> (kind, start)
        self.processing, self.function_limit, self.real_delay = (1.0, 10.0), 60.0, real_delay
        self.lock = threading.RLock()

    # ---------------------------------------------------------------------------------------- budget
    def writes_left(self):
        with self.lock:
            while self._writes and self._writes[0] < self.now() - 60:
                self._writes.popleft()
            return self.WRITES_PER_MIN - len(self._writes)

    def _write(self, kind, detail):
        t0, token = time.time(), object()
        with self.lock:
            if self.writes_left() <= 0:
                raise RuntimeError("write budget exhausted")
            self._writes.append(self.now())
            self.calls.append((kind, detail))
            self.writing[token] = (kind, t0)
        delay = self.real_delay.get(kind, 0.0) if isinstance(self.real_delay, dict) else self.real_delay
        if delay:                                          # the request is on the wire (real seconds; per kind if a dict)
            time.sleep(delay)
        with self.lock:
            del self.writing[token]
            self.timeline.append((kind, t0, time.time()))

    def write_intervals(self):
        """(kind, start, end) of every write so far; writes still on the wire end now."""
        with self.lock:
            return list(self.timeline) + [(k, t0, time.time()) for k, t0 in self.writing.values()]

    # ---------------------------------------------------------------------------------------- reads
    def init(self):
        return dict(id="t", slug="s")

    def tournament_markets(self):
        return self.markets

    def positions(self):
        return {ex: q for ex, q in self.pos.items() if q}

    def positions_raw(self):
        return [dict(exchangeId=ex, settled=False, avgCost=0.5, unrealizedPnl=0.0, marketValue=abs(q) * 0.5, currentPrice=0.5,
                     lots=[dict(side="YES" if q > 0 else "NO", quantity=abs(q))]) for ex, q in self.pos.items() if q]

    @staticmethod
    def net(p):
        return sum(l["quantity"] * (1 if l["side"].lower() == "yes" else -1) for l in p["lots"])

    def open_orders(self):
        with self.lock:
            self._expire()
            return [dict(id=str(i), ex=o["ex"], side=o["side"], action=o["action"], price=o["price"], qty=float(o["qty"]))
                    for i, o in self.book.items()]

    def prices(self, exs):
        return {str(e): self.px.get(str(e), (None, None, None)) for e in exs}

    spare = True                                          # the read governor (b_sig.spare_reads): tests can tighten it

    def stall_fills(self):
        """From now on the fills projection returns what it had (SIG stopped publishing fills at 15:25:55 on Oct 2)."""
        with self.lock:
            self._fills_seen_at_stall = list(reversed(self.fill_log))[:500]
        self.fill_stalled = True
    tape = {}                                             # {ex: [trade, ...]} for /exchanges/{ex}/trades (intel)

    def _req(self, method, path, params=None, **kw):
        assert method == "GET" and path.startswith("/exchanges/") and path.endswith("/trades"), path
        self.tape_reads = getattr(self, "tape_reads", 0) + 1
        ex = path.split("/")[2]
        rows = sorted((r for r in self.tape.get(ex, []) if r["createdAt"] >= (params or {}).get("from", "")), key=lambda r: -int(r["id"]))
        return dict(data=rows[:200], pagination=dict(hasMore=False, nextCursor=None))

    def spare_reads(self, keep=30):
        return self.spare

    def orderbook(self, ex, depth=10):
        self.book_reads = getattr(self, "book_reads", 0) + 1
        b, a, _ = self.px.get(str(ex), (None, None, None))
        return ([(b, 1000.0)] if b is not None else [], [(a, 1000.0)] if a is not None else [])

    FILL_COVERAGE = {}                                    # evening §1.2: the projection's sequence advances on every read
    fill_stalled = False                                  # tests: SIG's fills projection stops (no new fills, frozen sequence)

    def fills(self, known=(), max_pages=5):
        if self.fill_stalled:
            return [f for f in self._fills_seen_at_stall]
        self.FILL_COVERAGE.update(t=time.time(), seq=self.FILL_COVERAGE.get("seq", 0) + 1, newest=None)
        with self.lock:
            return list(reversed(self.fill_log))[:100 * max_pages]

    def health(self, window_s=600):
        return dict(read=dict(n=0, p50=None, p90=None, bad_rate=0.0), write=dict(n=len(self.timeline), p50=None, p90=None, bad_rate=0.0),
                    reads_last_min=0, writes_last_min=self.WRITES_PER_MIN - self.writes_left(), read_limit=100, write_limit=30,
                    last_ok=time.time())

    def leaderboard(self, limit=20, offset=0):
        rows = [dict(rank=i + 1, username=f"leader-{i + 1}", profileId=str(i), pnl=500_000.0 - 10_000 * i, tradesCount=100 + i,
                     volume=1.0, roi=1.0) for i in range(30)]
        if offset >= len(rows):                            # our own row, far down the board
            rows = rows + [dict(rank=offset + 1, username="us", profileId="me", pnl=-7700.0, tradesCount=900, volume=1.0, roi=-7.7)]
            return dict(leaderboard=rows[-1:], total=700, myRank=585)
        return dict(leaderboard=rows[offset:offset + limit], total=700, myRank=585)

    def recent_markets(self, limit=20):
        return list(reversed(self.markets))[:limit]

    def pnl(self):
        return dict(period="all", totalAccountValue=93000.0, totalHoldingsValue=9000.0, unrealizedPnl=-6000.0, roi=-7.0)

    # ---------------------------------------------------------------------------------------- writes
    def _expire(self):
        t = self.now()
        for i in [i for i, o in self.book.items() if o["expires"] and o["expires"] <= t]:
            del self.book[i]

    def _land(self, o, ttl_s):
        ex, side, action, price, qty = o
        if action == "sell":                               # SIG: an unbacked sell becomes the complement buy
            held = self.pos.get(ex, 0) if side == "yes" else -self.pos.get(ex, 0)
            selling = sum(b["qty"] for b in self.book.values() if b["ex"] == ex and b["side"] == side and b["action"] == "sell")
            if qty > max(0, held) - selling:
                self.canonicalized.append(o)
                side, action, price = ("no" if side == "yes" else "yes"), "buy", round(1 - price, 3)
        self.next_id += 1
        self.book[self.next_id] = dict(ex=ex, side=side, action=action, price=price, qty=qty,
                                       expires=self.now() + ttl_s if ttl_s else None)
        return dict(orderId=self.next_id, open=True, remainingQuantity=qty)

    def _place(self, orders, ttl_s, key, atomic):
        self._write("multileg" if atomic else "batch", (len(orders), ttl_s))
        with self.lock:
            if key and key in self.idem:
                return self.idem[key]
            fault = self.faults.pop(0) if self.faults and not str(self.faults[0]).startswith("cancel_") else None
            if isinstance(fault, tuple) and fault[0] == "429":
                raise self.RateLimited(fault[1])
            if fault == "503":
                raise self.Unknown("503 twice: nothing landed")
            out, t = [], 0.0
            for i, o in enumerate(orders):
                t += self.rng.uniform(*self.processing)
                if isinstance(fault, tuple) and fault[0] == "partial" and i >= fault[1] and not atomic:
                    break
                if t > self.function_limit and not atomic:
                    break
                out.append((o, True, self._land(o, ttl_s)))
            if self.clock:
                self.clock.advance(min(t, 75.0))
            if len(out) < len(orders):
                raise self.Unknown(f"cut off after {len(out)} of {len(orders)} orders")
            if key:
                self.idem[key] = out
            if fault == "timeout_ok":
                raise self.Unknown("timed out after landing")
            return out

    def place_batch(self, orders, ttl_s=None, key=None):
        assert len(orders) <= 50
        return self._place(list(orders), ttl_s, key, atomic=False)

    def place_multileg(self, legs, ttl_s=None, key=None):
        assert 1 <= len(legs) <= 10
        return self._place(list(legs), ttl_s, key, atomic=True)

    def cancel(self, oid):
        self._write("cancel", str(oid))
        with self.lock:
            fault = (self.cancel_faults.pop(0) if self.cancel_faults else
                     self.faults.pop(0) if self.faults and str(self.faults[0]).startswith("cancel_") else None)
            if fault == "cancel_timeout_fail":
                raise TimeoutError("DELETE timed out (not cancelled)")
            if int(oid) not in self.book:
                raise RuntimeError(f"DELETE /orders/{oid} -> 409 order is already closed")
            del self.book[int(oid)]
            if fault == "cancel_timeout_ok":
                raise TimeoutError("DELETE timed out (but it was cancelled)")

    def cancel_all(self, ex=None):
        if not ex:
            raise ValueError("tournament-wide cancel-all is forbidden")
        self._write("cancel_all", str(ex))
        with self.lock:
            for i in [i for i, o in self.book.items() if o["ex"] == str(ex)]:
                del self.book[i]

    # ---------------------------------------------------------------------------------------- test helpers
    def manual_order(self, ex, side, action, price, qty):
        """An order placed by hand on the website: not in the engine's ownership record."""
        with self.lock:
            return self._land((str(ex), side, action, price, qty), None)["orderId"]

    def fill(self, oid, qty, price=None):
        """Someone trades against our resting order: netting moves the position, a fill is recorded."""
        with self.lock:
            o = self.book[int(oid)]
            q = min(qty, o["qty"])
            sign = (1 if o["side"] == "yes" else -1) * (1 if o["action"] == "buy" else -1)
            self.pos[o["ex"]] = self.pos.get(o["ex"], 0) + sign * q
            self.fill_log.append(dict(id=len(self.fill_log) + 1, orderId=int(oid), exchangeId=o["ex"], price=price or o["price"],
                                      quantity=q if o["side"] == "yes" else -q, side=o["side"].upper(),
                                      filledAt=datetime.fromtimestamp(self.now(), timezone.utc).isoformat().replace("+00:00", "Z")))
            o["qty"] -= q
            if o["qty"] <= 0:
                del self.book[int(oid)]
            return q

    def account_batches(self, fills, duplicate_every=3, drop=()):
        """Realtime account_batch messages for P2 tests: revision-numbered, some re-sent, some missing."""
        out, rev = [], 0
        for i, f in enumerate(fills):
            rev += 1
            msg = dict(event="account_batch", payload=dict(fills=[f], orderUpdates=[],
                                                           delivery=dict(revision=rev, previousRevision=rev - 1)))
            if i in drop:
                continue
            out.append(msg)
            if duplicate_every and i % duplicate_every == duplicate_every - 1:
                out.append(msg)
        return out
