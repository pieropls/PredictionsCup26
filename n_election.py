"""Election night (docs/ELECTION_NIGHT.md, day 5 §5): the poll-close pulls, the call sniper, the post-call bids, the
settlement-morning loop and the kill switch, as one object with injected I/O so tests/sim_election.py can drive it on the
fake exchange with a scripted clock. NOT wired into the live engine (build Oct 10-20, rehearse Oct 25): the engine will
call `on_pm` from the Polymarket feed thread (the fast path) and `on_tick` from its loop.

Invariants kept: `/api/v1` placements only through the caller's executor (I4 self-cross and I2 are its business), the
cash floor + 5k is a hard stop here, manual markets are never touched, nothing is placed to move a price."""
import collections
import statistics
import time

# ---- thresholds (ELECTION_NIGHT.md §1-§4)
CALL_FOR, CALL_AGAINST = 0.97, 0.03     # Polymarket side price that means "called" (two messages >= 1 s apart)
CONFIRM_S = 1.0
MAX_BUY = 0.95                          # never pay more than this for a called side (5c margin for a reversal + SIG's settlement delay)
RACE_SHARE = 0.25                       # of free cash per race
SNIPER_TTL_S = 20
WRITES_FAIL_STOP = 3                    # failed writes per race -> stop that race
REVERSAL_PM, REVERSAL_WINDOW_S, REVERSAL_MAX_LOSS = 0.90, 600, 0.03
PULL_BEFORE_S, NO_ENTRY_AFTER_S = 600, 3 * 3600
ENTRY_LAYERS = ("dump", "fatfinger", "longshot", "mm", "scalp", "ladder", "set", "take", "touch")
POST_CALL_PRICES = (0.96, 0.97, 0.98, 0.99)
POST_CALL_DIV, POST_CALL_CONC = 8, 12_000
SETTLE_PM, SETTLE_HOLD_S, SETTLE_MAX = 0.995, 2 * 3600, 0.99
FLOOR_MARGIN = 5_000                    # free cash must stay >= floor + 5k (hard)


def side_price(pm_yes, side):
    return pm_yes if side == "yes" else 1 - pm_yes


class Election:
    """meta: {ex: {race, party, title}}; close: {race: poll-close epoch}; place(items, layer, ttl_s) -> job key or None,
    items = [(ex, side, action, price, qty)]; cancel(order, reason) -> bool; book(ex) -> (bid_yes, ask_yes, depth_at_ask)
    or None; log(kind, text). `clock` returns the current epoch (the simulator's clock in tests)."""

    def __init__(self, meta, close, clock, place, cancel, book, log, manual=(), cash_floor=5_000):
        self.meta, self.close, self.clock = meta, dict(close), clock
        self.place, self.cancel, self.book, self.log = place, cancel, book, log
        self.manual, self.floor = set(manual), cash_floor
        self.pm = {}                                        # ex -> deque of (t, pm_yes) (last 20 min)
        self.calls = {}                                     # ex -> dict(side, t, bought, cost, fails, stopped, reversed)
        self.pulled = set()                                 # races whose entries were pulled before the close
        self.stopped, self.stop_t = False, None
        self.sent = []                                      # (pm event t, order sent t) per sniper placement: the latency series
        self.events = []
        self.settle_hold = {}                               # ex -> first t with pm >= 0.995
        self.post_bids = {}                                 # ex -> {price: qty} we want resting after a call
        self.cash_path = []                                 # (t, free cash) at each tick
        self.committed = []                                 # (t sent, cost) of sniper orders that may still fill (20-s TTL)

    # ---------------------------------------------------------------------------------------- helpers
    def race(self, ex):
        return (self.meta.get(ex) or {}).get("race")

    def called(self, ex):
        c = self.calls.get(ex)
        return c["side"] if c and not c.get("reversed") else None

    def entries_allowed(self, ex, now):
        """§1: no new entries on a market from 10 min before its poll close until it is called or 3 h have passed."""
        t = self.close.get(self.race(ex))
        if self.stopped or t is None:
            return not self.stopped
        if now < t - PULL_BEFORE_S:
            return True
        return self.called(ex) is not None or now >= t + NO_ENTRY_AFTER_S

    def _log(self, kind, text, now):
        self.events.append((now, kind, text))
        self.log(kind, text)

    # ---------------------------------------------------------------------------------------- §2 the call sniper (fast path)
    def on_pm(self, ex, pm_yes, now, free_cash, pos):
        """A Polymarket quote for one market from the feed thread. Two messages >= 1 s apart past the threshold = a call;
        the order goes out from here (no reads: the last book read, cached 20 s)."""
        if ex in self.manual or self.stopped or pm_yes is None:
            return None
        h = self.pm.setdefault(ex, collections.deque(maxlen=200))
        h.append((now, pm_yes))
        c = self.calls.get(ex)
        if c and not c.get("reversed"):
            if now - c["t"] <= REVERSAL_WINDOW_S and side_price(pm_yes, c["side"]) < REVERSAL_PM and c["bought"] > 0:
                return self._reverse(ex, c, now, pos)
            return self._snipe(ex, c, now, free_cash, pos)
        for side in ("yes", "no"):
            p = side_price(pm_yes, side)
            if p < CALL_FOR:
                continue
            run_t = None                                    # the unbroken run of messages past the threshold, newest first
            for t, v in reversed(h):
                if side_price(v, side) < CALL_FOR:
                    break
                run_t = t
            if run_t is None or now - run_t < CONFIRM_S:
                continue                                    # a single print on a thin book is not a call
            c = self.calls[ex] = dict(side=side, t=now, signal_t=run_t, bought=0.0, cost=0.0, fails=0, stopped=False, reversed=False)
            self._log("CALL", f"{self.race(ex)}: {self.meta[ex].get('party')} {side.upper()} called (PM {p:.3f})", now)
            return self._snipe(ex, c, now, free_cash, pos)
        return None

    def _snipe(self, ex, c, now, free_cash, pos):
        """Buy the called side while its best ask <= 0.95: a 20-s order at the ask for the visible depth, up to 25% of
        free cash per race, free cash >= floor + 5k always, stop after 3 failed writes."""
        if c["stopped"] or c.get("reversed"):
            return None
        bk = self.book(ex)
        if not bk:
            return None
        bid_y, ask_y, depth = bk
        side = c["side"]
        ask = ask_y if side == "yes" else (1 - bid_y if bid_y is not None else None)
        if ask is None or ask > MAX_BUY + 1e-9:
            return None
        race = self.race(ex)
        spent = sum(v["cost"] for e, v in self.calls.items() if self.race(e) == race)   # the 25% is per race, both markets
        self.committed = [(t, x) for t, x in self.committed if now - t <= SNIPER_TTL_S]
        pending = sum(x for _, x in self.committed)         # orders out but not yet filled: the cash they will take
        free = free_cash - pending
        budget = min(RACE_SHARE * free - spent, free - self.floor - FLOOR_MARGIN)
        if budget <= 0:
            return None
        held_other = (pos.get(ex, 0) < 0) if side == "yes" else (pos.get(ex, 0) > 0)
        qty = int(min(depth or 0, budget / ask))
        if qty < 10:
            return None
        key = self.place([(ex, side, "buy", round(ask, 3), qty)], "callsnipe", SNIPER_TTL_S)
        if key is None:
            c["fails"] += 1
            if c["fails"] >= WRITES_FAIL_STOP:
                c["stopped"] = True
                self._log("CALL", f"{self.race(ex)}: {WRITES_FAIL_STOP} failed writes: sniper stopped on this race", now)
            return None
        self.sent.append((c.get("signal_t", now), self.clock()))
        self.committed.append((now, qty * ask))
        c["bought"] += qty
        c["cost"] += qty * ask
        if held_other:
            self._log("CALL", f"{self.race(ex)}: we held the losing side; the buy nets it out first", now)
        return key

    def _reverse(self, ex, c, now, pos):
        """§2 reversal guard: PM back under 0.90 within 10 min of a call we bought -> exit at market up to a 3c loss, stop."""
        c["reversed"], c["stopped"] = True, True
        bk = self.book(ex)
        q = abs(pos.get(ex, 0))
        if bk and q >= 10:
            bid_y, ask_y, _ = bk
            side = c["side"]
            bid = bid_y if side == "yes" else 1 - ask_y
            avg = c["cost"] / max(c["bought"], 1)
            px = max(bid, avg - REVERSAL_MAX_LOSS)
            close = "no" if side == "yes" else "yes"
            self.place([(ex, close, "buy", round(1 - px, 3), int(q))], "callexit", SNIPER_TTL_S)
        self._log("REVERSAL", f"{self.race(ex)}: PM back under {REVERSAL_PM:.2f} within 10 min of the call: exit at market (max 3c loss), race stopped", now)
        return "reversed"

    # ---------------------------------------------------------------------------------------- the loop side
    def on_tick(self, now, free_cash, pos, open_orders, layer_of):
        """Every loop: poll-close pulls (§1), post-call bids (§3), the settlement-morning loop (§4), the cash path.
        open_orders: [{id, ex, side, action, price, qty}]; layer_of(order) -> our layer name or None (foreign)."""
        self.cash_path.append((now, free_cash))
        out = dict(pulled=0, post_bids=0, settle=0)
        if self.stopped:
            return out
        # §1 poll-close pulls: 10 min before a race's close cancel every ENTRY order on its markets; exits stay
        for ex, m in self.meta.items():
            t = self.close.get(m.get("race"))
            if t is None or ex in self.manual or m.get("race") in self.pulled or now < t - PULL_BEFORE_S:
                continue
            self.pulled.add(m["race"])
            n = 0
            for o in open_orders:
                if self.race(o["ex"]) == m["race"] and layer_of(o) in ENTRY_LAYERS and not self._reduces(o, pos):
                    n += bool(self.cancel(o, reason=f"POLLS {m['race']}"))
            out["pulled"] += n
            self._log("POLLS", f"{m['race']}: polls close at {time.strftime('%H:%M', time.localtime(t))}: {n} entry bids pulled", now)
        # §3 post-call bids 0.96-0.99 on called winners (held to settlement), §4 settlement morning
        for ex, c in self.calls.items():
            if c.get("reversed") or ex in self.manual:
                continue
            side = c["side"]
            held = pos.get(ex, 0) * (1 if side == "yes" else -1)
            room = min(POST_CALL_CONC - max(held, 0) * 0.97, free_cash - self.floor - FLOOR_MARGIN)
            want = {}
            bk = self.book(ex)
            best_bid = None if not bk else (bk[0] if side == "yes" else (1 - bk[1] if bk[1] is not None else None))
            if room > 0 and best_bid is not None:
                per = max(0.0, (free_cash - self.floor - FLOOR_MARGIN) / POST_CALL_DIV) / len(POST_CALL_PRICES)
                for p in POST_CALL_PRICES:
                    if p > best_bid + 0.005 + 1e-9:        # resting bids only: never lift the asks (that is the sniper's job, capped at 0.95)
                        continue
                    q = int(min(per, room / len(POST_CALL_PRICES)) / p)
                    if q >= 10:
                        want[p] = q
            have = {o["price"]: o for o in open_orders if o["ex"] == ex and o["side"] == side and o["action"] == "buy" and layer_of(o) == "postcall"}
            new = [(ex, side, "buy", p, q) for p, q in want.items() if p not in have]
            if new and self.place(new, "postcall", None) is not None:
                out["post_bids"] += len(new)
            self.post_bids[ex] = want
        return out

    def settlement(self, now, free_cash, pos, pm_now, start, end):
        """§4 from 06:00 PT: a race with PM side >= 0.995 for >= 2 h and called -> buy the winner below 0.99 at the ask,
        20-s orders, until 08:30 PT; PM back under 0.99 after we bought -> exit at market."""
        if self.stopped or not (start <= now < end):
            return 0
        n = 0
        for ex, pm_yes in pm_now.items():
            side = self.called(ex)
            if side is None or ex in self.manual:
                continue
            p = side_price(pm_yes, side)
            if p >= SETTLE_PM:
                self.settle_hold.setdefault(ex, now)
            else:
                self.settle_hold.pop(ex, None)
                if p < 0.99 and pos.get(ex, 0) * (1 if side == "yes" else -1) >= 10:
                    bk = self.book(ex)
                    if bk:
                        bid = bk[0] if side == "yes" else 1 - bk[1]
                        q = abs(pos.get(ex, 0))
                        self.place([(ex, "no" if side == "yes" else "yes", "buy", round(1 - bid, 3), int(q))], "callexit", SNIPER_TTL_S)
                        self._log("SETTLE", f"{self.race(ex)}: PM back under 0.99 after we bought: exit at market", now)
                continue
            if now - self.settle_hold[ex] < SETTLE_HOLD_S:
                continue
            bk = self.book(ex)
            if not bk:
                continue
            bid_y, ask_y, depth = bk
            ask = ask_y if side == "yes" else 1 - bid_y
            budget = free_cash - self.floor - FLOOR_MARGIN
            if ask is None or ask >= SETTLE_MAX or budget <= 0:
                continue
            q = int(min(depth or 0, budget / ask))
            if q >= 10 and self.place([(ex, side, "buy", round(ask, 3), q)], "settle", SNIPER_TTL_S) is not None:
                n += 1
                free_cash -= q * ask
        return n

    # ---------------------------------------------------------------------------------------- the kill switch
    def kill(self, now, open_orders, pos, layer_of):
        """`election_stop`: pull every entry, call-sniper and post-call order, keep exits, pause new risk (manual pause)."""
        self.stopped, self.stop_t = True, now
        n = 0
        for o in open_orders:
            l = layer_of(o)
            if l is None:
                continue                                    # foreign / manual: never
            if (l in ENTRY_LAYERS or l in ("callsnipe", "postcall", "settle")) and not self._reduces(o, pos):
                n += bool(self.cancel(o, reason="election_stop"))
        self._log("STOP", f"election_stop: {n} orders pulled, exits kept, new risk paused (manual: waits for Piero)", now)
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
