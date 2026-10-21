"""Election-night rehearsal, the rig (docs/night_sim.md): n_election.Election wired the way f_engine.el_build / el_place / el_levels /
el_quote / el_step wire it, orders through the real j_executor.Executor on a FakeSIG that fills against tests/night_world.World's lagging
SIG book, on a virtual clock (SIG processing ~0.3 s an order + 0.25 s a write, as measured in docs/night_tapes.md §3.1).

Engine timing modelled ("engine" mode): the PM quote runs on_pm in the feed thread with defer=True; the snipe goes out at the loop's next
fast-step window (the loop is busy ~8.3 s of every 10 s, docs/latency.md §1) or at the next loop's el_step, whichever is first; el_place
= executor.place + step_placements (the fast_l0 pattern); the executor's two lanes are drained after every event. "lane" mode is the
latency.md §3 lane: on_pm with defer=False, the order goes from the feed thread at once."""
import inspect
import math
import os
import random
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))
if not os.environ.get("SIG_DATA"):                       # never the live data/ (tests/__init__ does the same for `-m tests.…`)
    os.environ["SIG_DATA"] = tempfile.mkdtemp(prefix="sig_night_sim_")
import a_config                                           # noqa: E402
import d_ledger                                           # noqa: E402
import j_executor as jx                                   # noqa: E402
import n_election as el                                   # noqa: E402
from fake_sig import FakeSIG, VClock                      # noqa: E402
import night_world as nw                                  # noqa: E402

LOOP_S, BUSY_S = 10.0, 8.3


class NightSIG(FakeSIG):
    """FakeSIG + the world's book: a buy that lands walks the SIG levels at once (taker, each level's price); SIG's TTL rule (expiries
    < 30 s ahead are refused, RECAP §11 9.3) raises a 400 for the whole batch; every write is logged with its orders."""

    def __init__(self, world, clock, on_fill, writes_per_min, rtt=0.25, seed=1):
        super().__init__(list(world.legs), {}, {}, seed=seed, clock=clock, writes_per_min=writes_per_min)
        self.world, self.on_fill_cb, self.rtt = world, on_fill, rtt
        self.processing = (0.2, 0.4)
        self.write_log = []                                # (t start, kind, orders or detail)
        self._cur = None
        self.ttl_rejects = 0

    def _write(self, kind, detail):
        t = self.now()
        super()._write(kind, detail)
        self.write_log.append((t, kind, self._cur if kind in ("batch", "multileg") else detail))
        self._cur = None
        self.clock.advance(self.rtt)

    def place_batch(self, orders, ttl_s=None, key=None):
        self._cur = list(orders)
        if ttl_s is not None and ttl_s < 30:
            self._write("batch", (len(orders), ttl_s))
            self.ttl_rejects += 1
            raise RuntimeError("-> 400 VALIDATION_ERROR: expirationDate must be in the future (>= 30 s)")
        return super().place_batch(orders, ttl_s, key)

    def _land(self, o, ttl_s):
        res = super()._land(o, ttl_s)
        oid = res["orderId"]
        b = self.book.get(oid)
        if b and b["action"] == "buy":
            for p, q in self.world.take(b["ex"], b["side"], b["price"], b["qty"], self.now()):
                self.fill(oid, q, price=p, how="taker")
        return res

    def fill(self, oid, qty, price=None, how="maker"):
        with self.lock:
            o = self.book.get(int(oid))
            if not o:
                return 0
            ex, side, action = o["ex"], o["side"], o["action"]
            before = self.pos.get(ex, 0)
            q = super().fill(oid, qty, price=price)
            self.on_fill_cb(str(oid), ex, side, action, q, price if price is not None else o["price"], before, how)
            return q


class Rig:
    def __init__(self, world, name="normal", mode="engine", cash0=67_000.0, n_entry=30, faults=(), kill_at=None, feed_down=None,
                 bg_writes=0.0, book_depth=False, writes_per_min=None, seed=7, taker_p=0.05, verbose=False):
        self.W, self.name, self.mode, self.cash0 = world, name, mode, float(cash0)
        self.n_entry, self.faults, self.kill_at, self.feed_down = n_entry, sorted(faults), kill_at, feed_down
        self.bg_writes, self.book_depth, self.seed, self.taker_p, self.verbose = bg_writes, book_depth, seed, taker_p, verbose
        self.wpm = writes_per_min or a_config.WRITES_PER_MIN
        self.rng = random.Random(f"{seed}:rig")

    # ---------------------------------------------------------------------------------------- the engine's callbacks (f_engine C102)
    def el_place(self, items, layer, ttl):
        if self.paused:
            return None
        out = [(f"MKT {self.W.meta[ex]['race']} {self.W.meta[ex]['party']}", f"{layer}:{ex}:{side}", layer,
                (ex, side, action, round(px, 3), int(q))) for ex, side, action, px, q in items if ex in self.W.legs]
        key = self.ex.place(out, ttl_s=ttl) if out else None
        if key:
            for ex, side, action, px, q in items:
                self.placed.append((self.clock.time(), ex, side, layer, px, q))
            self.ex.step_placements()
        else:
            self.place_none += 1
        return key

    def el_cancel(self, o, reason=""):
        oo = next((x for x in self.open if x["id"] == o.get("id")), None)
        return bool(oo and self.ex.cancel(oo, reason=reason))

    def el_book(self, ex):
        b, a = self.W.sig_touch(ex, self.clock.time())
        if not self.book_depth:
            return (b, a, 0)                               # f_engine.el_book: the REST touch, depth 0
        lv = self.W.side_levels(ex, "yes", self.clock.time())
        return (b, a, lv[0][1] if lv else 0)

    def el_levels(self, ex, side):
        return self.W.side_levels(ex, side, self.clock.time())

    def layer_of(self, o):
        return (self.ledger.orders.get(o["id"]) or {}).get("layer") if self.ex.owned(o["id"]) else None

    def on_fill(self, oid, ex, side, action, q, p, before, how):
        sign = (1 if side == "yes" else -1) * (1 if action == "buy" else -1)
        netted = min(q, max(0.0, -sign * before))
        self.cash += -q * p + netted
        layer = (self.ledger.orders.get(oid) or {}).get("layer") if self.ledger else None
        self.fills.append((self.clock.time(), oid, ex, side, action, q, p, layer, how))

    def xlog(self, s):
        s = s.strip()
        k = s.split(" ", 1)[0] if s else ""
        self.xcount[k] = self.xcount.get(k, 0) + 1
        if self.verbose:
            print(nw.hm(self.clock.time()), "EXEC", s)

    def elog(self, k, t):
        if self.verbose:
            print(nw.hm(self.clock.time()), f"{k:9}", t)

    # ---------------------------------------------------------------------------------------- the run
    def build(self):
        W = self.W
        self.clock = VClock(nw.T_START)
        self.cash, self.fills, self.placed, self.paused, self.place_none = self.cash0, [], [], False, 0
        self.xcount, self.open = {}, []
        self.ledger = None
        self.fake = NightSIG(W, self.clock, self.on_fill, self.wpm, seed=self.seed)
        self.ledger = d_ledger.Ledger(persist=False)
        self.jdir = tempfile.mkdtemp(prefix="night_sim_journal_")
        self.journal = jx.Journal(self.jdir, clock=self.clock.time)
        self.ex = jx.Executor(self.fake, self.ledger, self.journal, batch_size=a_config.BATCH_SIZE, min_qty=a_config.MIN_QTY,
                              clock=self.clock.time, sleep=self.clock.advance, log=self.xlog)
        sig = inspect.signature(el.Election.__init__).parameters
        kw = dict(manual=tuple(sorted(W.manual)))
        if "cash_floor" in sig:
            kw["cash_floor"] = getattr(el, "CASH_FLOOR", 7_000)
        if "first_close" in sig:
            kw["first_close"] = dict(W.first)
        if "levels" in sig:
            kw["levels"] = self.el_levels
        self.E = el.Election(W.meta, dict(W.last), self.clock.time, self.el_place, self.el_cancel, self.el_book, self.elog, **kw)
        pm_sig = inspect.signature(self.E.on_pm).parameters
        self.pm_kw = "pm_bid" in pm_sig
        self.defer = self.mode == "engine" and "defer" in pm_sig and hasattr(self.E, "run_queue")
        # leftover entry bids that the poll-close pulls must cancel (cash_calendar: most layers are off by Nov 3; these are the rest)
        layers = [l for l in ("fmm", "carry", "rot") if l in getattr(el, "PULL_LAYERS", getattr(el, "ENTRY_LAYERS", ()))] or ["mm"]
        exs = sorted(e for e in W.legs if e not in W.manual)
        self.rng.shuffle(exs)
        for i, ex in enumerate(exs[:self.n_entry]):
            b, a = W.sig_touch(ex, nw.T_START)
            side = "yes" if W.legs[ex].fair >= 0.5 else "no"
            px = round(max(0.01, (b if side == "yes" else 1 - a) - 0.02), 3)
            q = self.rng.randint(500, 2000)
            res = FakeSIG._land(self.fake, (ex, side, "buy", px, q), None)
            self.ledger.placed(f"MKT {W.meta[ex]['race']}", (ex, side, "buy", px, q), True, res, layers[i % len(layers)],
                               sub=f"{layers[i % len(layers)]}:{ex}:{side}")
        self.seeded = min(self.n_entry, len(exs))

    def snapshot(self, now):
        self.fake._expire()
        self.open = self.fake.open_orders()
        self.pos = {k: float(v) for k, v in self.fake.pos.items()}
        self.cash_snap = self.cash

    def drain(self):
        n = 0
        for _ in range(200):
            k = 0
            if self.ex.cancels:
                k += self.ex.step_cancels()
            if self.ex.queue:
                k += self.ex.step_placements()
            n += k
            if not k:
                break
        return n

    def resting_fills(self, now):
        """Our resting buys: SIG's asks that fall to our price fill us at our price; a resting bid at the top of its side gets hit by a
        taker 5% of the 10-s loops (100-1,000 shares)."""
        for oid, o in list(self.fake.book.items()):
            if o["action"] != "buy" or not self.ex.owned(str(oid)):
                continue
            ex, side, P = o["ex"], o["side"], o["price"]
            got = self.W.take(ex, side, P, o["qty"], now)
            for _, q in got:
                if int(oid) in self.fake.book:
                    self.fake.fill(oid, q, price=P, how="maker")
            if int(oid) not in self.fake.book:
                continue
            b, a = self.W.sig_touch(ex, now)
            best = b if side == "yes" else round(1 - a, 3)
            if P >= best - 1e-9 and self.rng.random() < self.taker_p:
                self.fake.fill(oid, self.rng.randint(100, 1000), price=P, how="taker_hit")

    def tick(self, now):
        """One engine loop (el_step + the executor snapshot)."""
        self.snapshot(now)
        oo = [dict(id=o["id"], ex=str(o["ex"]), side=o["side"], action=o["action"], price=o["price"], qty=o["qty"]) for o in self.open]
        if self.kill_at is not None and self.kill is None and now >= self.kill_at:
            w0 = len(self.fake.write_log)
            n = self.E.kill(now, oo, self.pos, self.layer_of)
            self.paused = True
            self.kill = dict(t=now, n=n, w0=w0, done=None)
        self.E.on_tick(now, self.cash_snap, self.pos, oo, self.layer_of)
        if self.defer and self.E.queue and not self.paused:
            self.E.run_queue(now, self.cash_snap, self.pos)
        if nw.SETTLE_START <= now < nw.SETTLE_END and hasattr(self.E, "settlement"):
            pm_now = {ex: self.W.pm_mid(ex, now) for ex in self.W.legs}
            self.E.settlement(now, self.cash_snap, self.pos, pm_now, nw.SETTLE_START, nw.SETTLE_END)
        self.ex.update(self.pos, self.open, as_of=now)
        if self.ex.frozen or self.journal.unknown():
            self.ex.resolve_unknown(self.fake.open_orders(), self.fake.fills())
        self.resting_fills(now)
        self.cash_path.append((now, self.cash))

    def run(self):
        t_wall = time.time()
        self.build()
        W = self.W
        msgs = W.messages(self.feed_down)
        self.n_msgs = len(msgs)
        import heapq
        ev = []
        seq = 0

        def push(t, kind, data=None):
            nonlocal seq
            seq += 1
            heapq.heappush(ev, (t, seq, kind, data))
        push(nw.T_START, "loop")
        for t, f in self.faults:
            push(t, "fault", f)
        if self.bg_writes:
            push(nw.T_START, "bg")
        self.kill, self.cash_path = None, []
        self.snapshot(nw.T_START)
        wake_at, beat_at = None, None
        i, n = 0, len(msgs)
        while True:
            tm = msgs[i][0] if i < n else math.inf
            te = ev[0][0] if ev else math.inf
            t = min(tm, te)
            if t > nw.T_END:
                break
            if self.clock.time() < t:
                self.clock.t = t
            if te <= tm:
                _, _, kind, data = heapq.heappop(ev)
                now = self.clock.time()
                if kind == "loop":
                    self.tick(now)
                    push(t + LOOP_S, "loop")
                elif kind == "wake":
                    wake_at = None
                    if self.E.queue and not self.paused:
                        self.E.run_queue(now, self.cash_snap, self.pos)
                elif kind == "fault":
                    for f in data:
                        (self.fake.cancel_faults if str(f).startswith("cancel_") else self.fake.faults).append(f)
                elif kind == "bg":
                    with self.fake.lock:
                        if self.fake.writes_left() > 0:
                            self.fake._writes.append(now)
                            self.fake.write_log.append((now, "other", None))
                    push(t + 60.0 / self.bg_writes, "bg")
                elif kind == "beat":
                    beat_at = None
            else:
                _, ex, b, a = msgs[i]
                i += 1
                kw = dict(pm_bid=b, pm_ask=a) if self.pm_kw else {}
                if self.defer:
                    kw["defer"] = True
                r = self.E.on_pm(ex, (b + a) / 2, t, self.cash_snap, self.pos, **kw)
                if r == "queued" and wake_at is None:
                    ph = (t - nw.T_START) % LOOP_S
                    wake_at = (t - ph + BUSY_S + 0.05) if ph < BUSY_S else t + 0.05
                    push(wake_at, "wake")
            if self.ex.queue or self.ex.cancels:
                self.drain()
            if self.kill and self.kill["done"] is None and not self.ex.cancels and not self.ex.cancels_on_wire:
                self.kill["done"] = self.clock.time()
            if (self.ex.queue or self.ex.cancels) and beat_at is None:
                beat_at = self.clock.time() + 1.0
                push(beat_at, "beat")
        self.snapshot(self.clock.time())
        self.wall = time.time() - t_wall
        shutil.rmtree(self.jdir, ignore_errors=True)
        return self
