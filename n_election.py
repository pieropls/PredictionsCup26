"""Election night v2 (docs/night_reconcile.md: §3 C1-C21 as constants — Piero 01:05 Oct 18 accepted every Rec except C8, no post-call
bids — and §4 items 1-5, 9): the call detector, the call sniper, the poll-close pulls, the reversal stop, the settlement-morning loop and
the kill switch, as one object with injected I/O so tests/sim_election.py drives it on the fake exchange with a scripted clock.

The engine (f_engine, Piero 15:15 Oct 21) runs it in mode off / shadow / live: `on_pm` from the Polymarket feed thread (detection only
when `defer`: the snipes go out from the loop's fast step, where the executor lives), `on_tick` from the loop. In shadow, the engine's
place / cancel callbacks write would-be orders to data/election/shadow.jsonl and send nothing.

Invariants kept: placements only through the caller's executor (I4 self-cross and I2 are its business); cash floor + Piero's reserve
(5k + 2k, C21) is a hard stop here; manual markets are never touched; nothing is placed to move a price; rule 0 holds before a call
(a reversal stops the race and holds, C16)."""
import collections
import statistics
import threading
import time

# ---- C1 the call rules (the sniper): the PM side BID (not the mid), after the state's LAST poll close
CALL_HOLD, CALL_HOLD_S = 0.95, 20.0     # A: >= 0.95 held 20 s
CALL_STRICT = 0.98                      # B: >= 0.98 once (strict: the only call that may trigger RULE0-CALLED and wave recycling)
GUARD_SPREAD = 0.03                     # PM spread <= 3c
GUARD_COMPLEMENT = 0.04                 # the race's other books' PM <= 0.04 when a candidate is called to win
PRECALL_EVERY_S = 1800                  # a rule firing before the close is logged (PRECALL) once per book per 30 min
# ---- C2 price, C3 / C4 size, C5 TTL
PRICE_CAP, PRICE_UNDER_PM = 0.97, 0.02  # <= min(0.97, PM side bid - 2c), walking the SIG book
RACE_SHARES = 15_000                    # <= 15k shares a race (both books)
STATE_CAP = 20_000.0                    # <= 20k at cost a state (its Senate, Governor and House races are called off the same count)
WAVE_MULT = 2.0                         # the wave share: 2 x usable cash / the uncalled races of this wave and the next (v2 §4.3)
TTL_S = 120                             # SIG rejects 10-20 s expiries (RECAP §11 9.3)
WRITES_FAIL_STOP = 3                    # failed writes on a race -> the sniper stops there
MIN_QTY = 10
# ---- C14 pulls at the FIRST close - 10 min; C15 no entries until called or close + 3 h (the night drift is the one exception)
PULL_BEFORE_S, NO_ENTRY_AFTER_S = 600, 3 * 3600
PULL_LAYERS = ("fmm", "fmmtake", "fmmhedge", "carry", "rot", "dump", "fatfinger", "longshot", "scalp", "ladder", "set", "take", "touch", "mm",
               "wall", "sleeve", "swing", "fm")  # TM / SM keep their own glide (fm_poll_block)
SNIPE_LAYERS = ("callsnipe", "settle", "drift")
# ---- C16 reversal: the side bid back under 0.90 within 10 min of a call -> the race stops and HOLDS (rule 0); re-armed by control
REVERSAL_PM, REVERSAL_WINDOW_S = 0.90, 600
# ---- C10 settlement morning: a strict call + PM side >= 0.98 held 2 h -> buy < 0.99 at the ask (06:00-08:30 PT); no at-market exit
SETTLE_PM, SETTLE_HOLD_S, SETTLE_MAX = 0.98, 2 * 3600, 0.99
# ---- C21 the cash floor: 5k + Piero's 2k reserve
CASH_FLOOR = 7_000
PENDING_S = 60                          # an order with no answer from the executor after 60 s was dropped / never sent: its budget is released
FEED_GAP_S = 30                         # no Polymarket quote on any book for 30 s = a feed gap: every 0.95 run restarts (sim: a 3-min gap)

# v1 names kept for the old simulator scenarios
CALL_FOR, CALL_AGAINST, CONFIRM_S, MAX_BUY, SNIPER_TTL_S = CALL_HOLD, 1 - CALL_HOLD, CALL_HOLD_S, PRICE_CAP, TTL_S
ENTRY_LAYERS = PULL_LAYERS


def side_price(pm_yes, side):
    return pm_yes if side == "yes" else 1 - pm_yes


class Election:
    """meta: {ex: {race, party, title, state}}; close: {race: LAST poll-close epoch}; first_close: {race: FIRST poll-close epoch}
    (default: close); place(items, layer, ttl_s) -> job key or None, items = [(ex, side, action, price, qty)]; cancel(order, reason)
    -> bool; book(ex) -> (bid_yes, ask_yes, depth_at_ask) or None; levels(ex, side) -> [(ask price on that side, qty)] ascending or None;
    log(kind, text). `clock` returns the current epoch (the simulator's clock in tests). place_dry(items, layer, ttl, why): shadow logging
    of the PRECALL would-be snipes (never sends)."""

    def __init__(self, meta, close, clock, place, cancel, book, log, manual=(), cash_floor=CASH_FLOOR, first_close=None, levels=None,
                 place_dry=None, precall_dry=False):
        self.meta, self.close, self.clock = meta, dict(close), clock
        self.first = dict(first_close or close)
        self.place, self.cancel, self.book, self.log, self.levels = place, cancel, book, log, levels
        self.place_dry, self.precall_dry = place_dry, precall_dry
        self.manual, self.floor = set(manual), cash_floor
        self.lock = threading.RLock()
        self.pm = {}                                        # ex -> deque of (t, bid_yes, ask_yes)
        self.calls = {}                                     # ex -> dict(side, t, signal_t, rule, strict, bought, cost, fails, stopped, reversed)
        self.pulled = set()                                 # races whose entries were pulled before the first close
        self.stopped, self.stop_t = False, None
        self.race_stop = set()                              # races stopped by a reversal or 3 failed writes (re-armed by control)
        self.sent = []                                      # (signal t, order sent t) per sniper placement: the latency series
        self.events, self.records = [], []                  # records: the calls.jsonl rows (the engine drains them)
        self.settle_hold = {}
        self.cash_path = []
        self.committed = []                                 # (t sent, cost) of sniper orders that may still fill (TTL)
        self.queue = collections.OrderedDict()              # books whose snipe waits for the loop (defer=True)
        self.precall_t = {}
        self.run0 = {}                                      # (ex, side) -> start of the unbroken run of quotes past 0.95
        self.pending = {}                                   # order key -> dict(ex, qty, cost, t): sent to the executor, not yet answered
        self.settle_t = {}                                  # ex -> last settlement order t (one order per book per TTL)
        self.last_quote_t = None
        self._placing = None
        self.lane_lat = []
        self.post_bids = {}                                 # C8: none (kept for the old reports)
        waves = sorted(set(v for v in self.close.values() if v is not None))
        self.waves = waves

    # ---------------------------------------------------------------------------------------- helpers
    def race(self, ex):
        return (self.meta.get(ex) or {}).get("race")

    def state(self, ex):
        m = self.meta.get(ex) or {}
        return m.get("state") or str(m.get("race") or "").split(" ")[0]

    def called(self, ex):
        c = self.calls.get(ex)
        return c["side"] if c and not c.get("reversed") else None

    def strict_against(self, ex, held_side):
        """C17: a strict call (the 0.98 rule, after the last close) says `held_side` of book ex loses."""
        c = self.calls.get(ex)
        return bool(c and c.get("strict") and not c.get("reversed") and c["side"] != held_side)

    def strict_for(self, ex, held_side):
        """C7: a strict call says `held_side` of book ex wins (wave recycling may sell it into bids >= 0.98)."""
        c = self.calls.get(ex)
        return bool(c and c.get("strict") and not c.get("reversed") and c["side"] == held_side)

    def entries_allowed(self, ex, now):
        """C14 / C15: no new entries on a book from 10 min before its race's FIRST close until it is called or 3 h have passed."""
        t = self.first.get(self.race(ex))
        if self.stopped or t is None:
            return not self.stopped
        if now < t - PULL_BEFORE_S:
            return True
        return self.called(ex) is not None or now >= t + NO_ENTRY_AFTER_S

    def drift_ok(self, ex, now):
        """§4 item 6: the night drift may trade a book from its race's first close until it is called (not after a reversal / stop)."""
        t = self.first.get(self.race(ex))
        return (not self.stopped and t is not None and now >= t and self.race(ex) not in self.race_stop and self.called(ex) is None
                and ex not in self.manual)

    def _log(self, kind, text, now):
        self.events.append((now, kind, text))
        self.events = self.events[-2000:]
        self.log(kind, text)

    def _side_bid(self, ex, side):
        h = self.pm.get(ex)
        if not h:
            return None
        _, b, a = h[-1]
        return b if side == "yes" else (1 - a if a is not None else None)

    # ---------------------------------------------------------------------------------------- C1 the call detector (feed thread)
    def on_pm(self, ex, pm_yes, now, free_cash, pos, pm_bid=None, pm_ask=None, defer=False):
        """A Polymarket quote for one book. Rule A: the side bid >= 0.95 held 20 s; rule B (strict): >= 0.98 once; both need the PM spread
        <= 3c, the race's other books <= 0.04 (a win call) and the state's last poll close passed (before it: PRECALL, logged). On a call
        the snipe goes out at once (defer=False: the simulator) or waits for the loop (defer=True: the engine's feed thread)."""
        with self.lock:
            if ex in self.manual or self.stopped or (pm_yes is None and pm_bid is None):
                return None
            bid = pm_bid if pm_bid is not None else pm_yes
            ask = pm_ask if pm_ask is not None else pm_yes
            if bid is None or ask is None:
                return None
            if self.last_quote_t is not None and now - self.last_quote_t > FEED_GAP_S:
                self.run0.clear()                           # sim: after a feed gap the first quote back must not complete rule A
                self._log("FEEDGAP", f"no Polymarket quote for {now - self.last_quote_t:.0f} s: every 0.95 run restarts", now)
            self.last_quote_t = now
            h = self.pm.setdefault(ex, collections.deque(maxlen=400))
            h.append((now, float(bid), float(ask)))
            c = self.calls.get(ex)
            if c and not c.get("reversed"):
                sb = self._side_bid(ex, c["side"])
                if sb is not None and now - c["t"] <= REVERSAL_WINDOW_S and sb < REVERSAL_PM:
                    return self._reverse(ex, c, now)
                if sb is not None and sb >= CALL_STRICT - 1e-9 and not c.get("strict"):
                    c["strict"] = True
                    self._record("STRICT", ex, c, now, sb)
                return self._go(ex, c, now, free_cash, pos, defer)
            if self.race(ex) in self.race_stop:
                return None
            spread = ask - bid
            for side in ("yes", "no"):
                sb = bid if side == "yes" else 1 - ask
                if sb < CALL_HOLD - 1e-9:
                    self.run0.pop((ex, side), None)         # the run past 0.95 is broken
                    continue
                run_t = self.run0.setdefault((ex, side), now)
                strict = sb >= CALL_STRICT - 1e-9
                held = now - run_t >= CALL_HOLD_S - 1e-9
                if not (strict or held):
                    continue
                last = self.close.get(self.race(ex))
                if (last is None or now < last) and now - self.precall_t.get(ex, -1e18) < PRECALL_EVERY_S:
                    continue                                # before the close and logged lately: nothing to do (the common case, O(1))
                why = []
                if spread > GUARD_SPREAD + 1e-9:
                    why.append(f"PM spread {spread * 100:.1f}c > 3c")
                if side == "yes":
                    others = [self.pm[e][-1] for e, m in self.meta.items() if e != ex and m.get("race") == self.race(ex) and self.pm.get(e)]
                    hi = max([(b_ + a_) / 2 for _, b_, a_ in others], default=0.0)
                    if hi > GUARD_COMPLEMENT + 1e-9:
                        why.append(f"another book of the race at {hi:.3f} > 0.04")
                if last is None or now < last:
                    if now - self.precall_t.get(ex, -1e18) >= PRECALL_EVERY_S:
                        self.precall_t[ex] = now
                        rule = "B 0.98 once" if strict else "A 0.95 for 20 s"
                        self._log("PRECALL", f"{self.race(ex)}: {self.meta.get(ex, {}).get('party')} {side.upper()} PM bid {sb:.3f} fires rule {rule} before the "
                                             f"last poll close{' (' + '; '.join(why) + ')' if why else ''}: no call", now)
                        self.records.append(dict(t=now, kind="PRECALL", ex=ex, race=self.race(ex), side=side, pm_bid=round(sb, 4), rule=rule, guards=why))
                        if self.precall_dry and self.place_dry and not why:
                            self._snipe(ex, dict(side=side, t=now, signal_t=run_t or now, strict=strict, bought=0.0, cost=0.0, fails=0, stopped=False,
                                                 reversed=False), now, free_cash, pos, dry=True)
                    continue
                if why:
                    if now - self.precall_t.get(("nocall", ex), -1e18) >= 300:
                        self.precall_t[("nocall", ex)] = now
                        self._log("NOCALL", f"{self.race(ex)}: {side.upper()} PM bid {sb:.3f} after the close, but {'; '.join(why)}", now)
                        self.records.append(dict(t=now, kind="NOCALL", ex=ex, race=self.race(ex), side=side, pm_bid=round(sb, 4), guards=why))
                    continue
                c = self.calls[ex] = dict(side=side, t=now, signal_t=run_t or now, rule="B" if strict else "A", strict=strict, bought=0.0, cost=0.0,
                                          fails=0, stopped=False, reversed=False)
                self._log("CALL", f"{self.race(ex)}: {self.meta.get(ex, {}).get('party')} {side.upper()} called (PM bid {sb:.3f}, rule "
                                  f"{'B 0.98 once' if strict else 'A 0.95 for 20 s'}{', strict' if strict else ''})", now)
                self._record("CALL", ex, c, now, sb)
                return self._go(ex, c, now, free_cash, pos, defer)
            return None

    def _record(self, kind, ex, c, now, sb):
        self.records.append(dict(t=now, kind=kind, ex=ex, race=self.race(ex), side=c["side"], rule=c.get("rule"), strict=bool(c.get("strict")),
                                 pm_bid=round(sb, 4) if sb is not None else None, signal_t=c.get("signal_t")))

    def _go(self, ex, c, now, free_cash, pos, defer):
        if defer:
            self.queue[ex] = now
            return "queued"
        return self._snipe(ex, c, now, free_cash, pos)

    def run_queue(self, now, free_cash, pos):
        """The loop side of a deferred call: the queued snipes, oldest first. -> number of orders placed."""
        with self.lock:
            n = 0
            for ex in list(self.queue):
                qt = self.queue.pop(ex, None)
                c = self.calls.get(ex)
                if c and not c.get("reversed") and self._snipe(ex, c, now, free_cash, pos):
                    n += 1
                    if qt is not None:
                        self.lane_lat.append(self.clock() - qt)   # C106: the PM quote that queued the call -> the order handed to the executor
                        self.lane_lat = self.lane_lat[-500:]
            return n

    # ---------------------------------------------------------------------------------------- C2-C5 the call sniper
    def _wave_races(self, ex):
        last = self.close.get(self.race(ex))
        if last is None or not self.waves:
            return 1
        i = self.waves.index(last) if last in self.waves else 0
        times = set(self.waves[i:])                          # sim: "the next wave" after 17:00 was the 2-race 17:30 one; count every later wave
        races = {r for r, t in self.close.items() if t in times}
        called = {self.race(e) for e, c in self.calls.items() if not c.get("reversed")}
        return max(1, len((races - called) | {self.race(ex)}))

    def _snipe(self, ex, c, now, free_cash, pos, dry=False):
        """Buy the called side at <= min(0.97, PM side bid - 2c), walking the SIG book, sized min(15k shares a race, the wave share), <= 20k
        at cost a state, usable cash = free - floor (7k) - the unanswered / unfilled orders; 120-s orders; stop after 3 failed writes."""
        if c["stopped"] or c.get("reversed") or self.race(ex) in self.race_stop:
            return None
        sb = self._side_bid(ex, c["side"])
        if sb is None:
            return None
        return self._buy(ex, c, round(min(PRICE_CAP, sb - PRICE_UNDER_PM), 3), now, free_cash, pos, "callsnipe", dry=dry, sb=sb, wave=True)

    def _buy(self, ex, c, cap, now, free_cash, pos, layer, dry=False, sb=None, wave=True):
        side = c["side"]
        if (pos.get(ex, 0) < 0) if side == "yes" else (pos.get(ex, 0) > 0):
            k = ("held_other", ex)
            if not dry and k not in self.precall_t:         # (a PRECALL dry snipe says nothing: 06:37 Oct 22 it read like a real one)
                self.precall_t[k] = now
                self._log("SNIPE", f"{self.race(ex)}: we hold the other side of this book: no {layer} here (the executor's I2 trim would cut it and pause; "
                                   f"a strict call sells our lot by RULE0-CALLED; the race's other book can still be bought)", now)
            return None                                     # v2 §4.4 b: never a buy that nets our own losing lot
        lv = self.levels(ex, side) if self.levels else None
        if not lv:
            bk = self.book(ex)
            if not bk:
                return None
            bid_y, ask_y, depth = bk[:3]
            ask = ask_y if side == "yes" else (1 - bid_y if bid_y is not None else None)
            lv = [] if ask is None else [(round(ask, 3), depth or 0)]
        take = [(p, q) for p, q in lv if p <= cap + 1e-9 and q > 0]
        if not take:
            return None
        limit = max(p for p, _ in take)
        avail = sum(q for _, q in take)
        race, st = self.race(ex), self.state(ex)
        sh_race = sum(v["bought"] for e, v in self.calls.items() if self.race(e) == race)
        cost_race = sum(v["cost"] for e, v in self.calls.items() if self.race(e) == race)
        cost_state = sum(v["cost"] for e, v in self.calls.items() if self.state(e) == st)
        self.expire(now)
        usable = free_cash - self.floor - sum(e[1] for e in self.committed)
        share = WAVE_MULT * max(0.0, usable) / self._wave_races(ex) if wave else usable
        cash = min(share - cost_race, STATE_CAP - cost_state, usable)
        qty = int(min(avail, RACE_SHARES - sh_race, max(0.0, cash) / limit))
        if qty < MIN_QTY:
            return None
        items = [(ex, side, "buy", round(limit, 3), qty)]
        if dry:
            return self.place_dry(items, layer, TTL_S, "precall") if self.place_dry else None
        self._placing = dict(done=None)                     # the executor may answer inside place() (it sends at once): confirm() lands here
        try:
            key = self.place(items, layer, TTL_S)
        finally:
            pl, self._placing = self._placing, None
        if key is None:
            c["fails"] += 1
            if c["fails"] >= WRITES_FAIL_STOP:
                c["stopped"] = True
                self.race_stop.add(race)
                self._log("CALL", f"{race}: {WRITES_FAIL_STOP} failed writes: the sniper stops on this race (re-arm by control)", now)
            return None
        if layer == "callsnipe":
            self.sent.append((c.get("signal_t", now), self.clock()))
        self.committed.append([now, qty * limit, key, ex])
        self.pending[key] = dict(ex=ex, qty=qty, cost=qty * limit, t=now)
        c["bought"] += qty
        c["cost"] += qty * limit
        if pl and pl.get("done") is not None:               # answered already (sim: the answer came before the record, and 60 s later the budget was re-spent)
            if pl["done"] > 0:
                self._release(key, pl["done"])
            self.pending.pop(key, None)
        self._log("SNIPE" if layer == "callsnipe" else "SETTLE", f"{race}: buy {side.upper()} {qty:,} at <= {limit:.3f}" + (f" (PM bid {sb:.3f}" if sb is not None else " (")
                  + f"; {avail:,.0f} on the book under the cap)", now)
        return key

    # ---------------------------------------------------------------------------------------- what the executor did with an order
    def _release(self, key, frac=1.0):
        p = self.pending.get(key)
        if not p:
            return
        c = self.calls.get(p["ex"])
        if c:
            c["bought"] = max(0.0, c["bought"] - p["qty"] * frac)
            c["cost"] = max(0.0, c["cost"] - p["cost"] * frac)
        for e in self.committed:
            if e[2] == key:
                e[1] *= (1 - frac)
        self.committed = [e for e in self.committed if e[1] > 1e-6]

    def confirm(self, key, part):
        """The executor's on_done: [(bot, order, ok, data)] per order of the write. A rejected order releases its budget; an accepted one
        keeps it (until it fills or its 120 s run out)."""
        with self.lock:
            p = self.pending.get(key)
            if not p and getattr(self, "_placing", None) is not None and self._placing.get("done") is None and part:
                self._placing["done"] = sum(1 for _, _, ok, data in part if not ok or (data or {}).get("orderId") is None) / len(part)
                return                                      # the order being placed right now: _buy applies it
            if not p or not part:
                return
            bad = sum(1 for _, _, ok, data in part if not ok or (data or {}).get("orderId") is None)
            if bad:
                self._release(key, bad / len(part))
            self.pending.pop(key, None)

    def expire(self, now):
        """Orders the executor never answered in 60 s (dropped as stale, 429-bounced, never sent): their budget comes back."""
        for key in [k for k, p in self.pending.items() if now - p["t"] > PENDING_S]:
            p = self.pending.pop(key)
            self.pending[key] = p
            self._release(key)
            self.pending.pop(key, None)
            self._log("EXPIRE", f"{self.race(p['ex'])}: an order of {p['qty']:,} had no answer from the executor in {PENDING_S} s (dropped / not sent): its budget is released", now)
        self.committed = [e for e in self.committed if now - e[0] <= TTL_S]

    def on_fill(self, ex, qty, price):
        """A fill of our order on ex: the cash it takes is in the next cash read; the 120-s hold on that cash goes (no double count)."""
        with self.lock:
            left = qty * price
            for e in self.committed:
                if e[3] == ex and left > 0:
                    take = min(e[1], left); e[1] -= take; left -= take
            self.committed = [e for e in self.committed if e[1] > 1e-6]

    def _reverse(self, ex, c, now):
        """C16: the side bid back under 0.90 within 10 min of a call -> the race stops and holds (rule 0); `rearm` lifts it."""
        c["reversed"], c["stopped"] = True, True
        self.race_stop.add(self.race(ex))
        self.queue.pop(ex, None)
        self._log("REVERSAL", f"{self.race(ex)}: PM back under {REVERSAL_PM:.2f} within 10 min of the call: race stopped, lots held (rule 0); "
                              f"re-arm by control", now)
        self.records.append(dict(t=now, kind="REVERSAL", ex=ex, race=self.race(ex), side=c["side"]))
        return "reversed"

    def rearm(self, race=None):
        """`election_rearm`: lift a race stop (or every stop and the kill switch)."""
        with self.lock:
            if race is None:
                self.race_stop.clear(); self.stopped = False
            else:
                self.race_stop.discard(race)
            for ex, c in list(self.calls.items()):
                if race is None or self.race(ex) == race:
                    if c.get("reversed"):
                        self.calls.pop(ex, None)
                    else:
                        c["stopped"], c["fails"] = False, 0

    # ---------------------------------------------------------------------------------------- the loop side
    def on_tick(self, now, free_cash, pos, open_orders, layer_of):
        """Every loop: the poll-close pulls (C14: the race's first close - 10 min; every entry of PULL_LAYERS on its books; exits and foreign
        orders stay), the cash path. No post-call bids (C8). open_orders: [{id, ex, side, action, price, qty}]; layer_of(order) -> our layer
        name or None (foreign)."""
        with self.lock:
            self.cash_path.append((now, free_cash))
            self.cash_path = self.cash_path[-5000:]
            self.expire(now)
            out = dict(pulled=0, post_bids=0, settle=0)
            if self.stopped:
                return out
            for ex, m in self.meta.items():
                t = self.first.get(m.get("race"))
                if t is None or ex in self.manual or m.get("race") in self.pulled or now < t - PULL_BEFORE_S:
                    continue
                self.pulled.add(m["race"])
                n = 0
                for o in open_orders:
                    if self.race(o["ex"]) == m["race"] and layer_of(o) in PULL_LAYERS and not self._reduces(o, pos):
                        n += bool(self.cancel(o, reason=f"POLLS {m['race']}"))
                out["pulled"] += n
                self._log("POLLS", f"{m['race']}: first poll close {time.strftime('%H:%M', time.localtime(t))}: {n} entry bids pulled; no entries until "
                                   f"called or close + 3 h (the night drift excepted)", now)
            return out

    def settlement(self, now, free_cash, pos, pm_now, start, end):
        """C10, from 06:00 PT: a strict call with the PM side >= 0.98 for >= 2 h -> buy the winner below 0.99 at the ask, until 08:30 PT;
        PM back under 0.98 -> the hold clock restarts (no at-market exit: rule 0)."""
        with self.lock:
            if self.stopped or not (start <= now < end):
                return 0
            n = 0
            for ex, pm_yes in pm_now.items():
                c = self.calls.get(ex)
                if not c or c.get("reversed") or not c.get("strict") or ex in self.manual:
                    continue
                side = c["side"]
                if side_price(pm_yes, side) >= SETTLE_PM - 1e-9:
                    self.settle_hold.setdefault(ex, now)
                else:
                    self.settle_hold.pop(ex, None)
                    continue
                if now - self.settle_hold[ex] < SETTLE_HOLD_S or now - self.settle_t.get(ex, -1e18) < TTL_S:
                    continue                                # sim: one order per book per 120 s (it re-placed every loop)
                if self._buy(ex, c, round(SETTLE_MAX - 0.005, 3), now, free_cash, pos, "settle", wave=False):
                    self.settle_t[ex] = now                 # walks the book below 0.99; the race / state caps and the cash floor hold
                    n += 1
            return n

    # ---------------------------------------------------------------------------------------- the kill switch
    def kill(self, now, open_orders, pos, layer_of):
        """`election_stop`: pull every entry, call-sniper, drift and settle order, keep exits and foreign orders; nothing more goes out
        until `rearm`."""
        with self.lock:
            self.stopped, self.stop_t = True, now
            self.queue.clear()
            n = 0
            for o in open_orders:
                l = layer_of(o)
                if l is None:
                    continue                                # foreign / manual: never
                if (l in PULL_LAYERS or l in SNIPE_LAYERS) and not self._reduces(o, pos):
                    n += bool(self.cancel(o, reason="election_stop"))
            self._log("STOP", f"election_stop: {n} orders pulled, exits kept; the sniper and the drift stay off until election_rearm", now)
            return n

    @staticmethod
    def _reduces(o, pos):
        p = pos.get(o["ex"], 0)
        buys_yes = (o["side"] == "yes") == (o["action"] == "buy")
        return (p < 0 and buys_yes) or (p > 0 and not buys_yes)

    # ---------------------------------------------------------------------------------------- metrics
    def latency(self):
        d = sorted(b - a for a, b in self.sent)
        if not d:
            return dict(n=0, p50=None, p95=None)
        return dict(n=len(d), p50=round(statistics.median(d), 3), p95=round(d[min(len(d) - 1, int(0.95 * len(d)))], 3))
