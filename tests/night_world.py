"""Election-night rehearsal, the world (docs/night_sim.md): the 2026 races, their Polymarket quote stream built from the 2024 tapes, and a
SIG book that lags PM. Read by tests/night_rig.py; no I/O beyond the two fixtures (research/night/night_sim_build.py) and
data/election/poll_close.json.

Mapping (one 2024 template per 2026 race, seeded): the 2026 favourite level p = F / (F + O) from the PM fairs of the race's two top books
is matched to the 2024 races' pre-close favourite level by |logit difference| + 0.4 for a type mismatch (House vs Senate / Governor /
control), choosing at random among the 3 nearest. The template's favourite path drives the 2026 favourite book and its other token the
other book; the 2026 level is matched by a logit shift that holds before the close and fades with a 60-min time constant after it. Time
is measured from each race's LAST poll close (2024 state -> 2026 state), so a race closing at 17:00 PT replays its template's evening
from that moment. The 2026 race is won by the favourite iff the template's favourite won. Templates whose 2024 book was one-sided at the
close (a mid far from the pre-close level, or > 60 emptied-book minutes in the first 18 h) are left out unless thin=True."""
import calendar
import gzip
import json
import math
import os
import random
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX = os.path.join(ROOT, "tests", "fixtures")
TITLE = re.compile(r"Will the (\w+) Party win the (.+?)\??$")
MIN = 60.0


def pt(day, hm):
    """'2026-11-03', '17:00' -> epoch (PT = UTC-8 on Nov 3-4 2026: DST ended Nov 1)."""
    y, m, d = (int(x) for x in day.split("-"))
    h, mi = (int(x) for x in hm.split(":"))
    return calendar.timegm((y, m, d, h, mi, 0)) + 8 * 3600


DAY = "2026-11-03"
T_START = pt(DAY, "14:30")
T_END = pt("2026-11-04", "09:00")          # SIG settles Nov 4 09:00 PT
SETTLE_START, SETTLE_END = pt("2026-11-04", "06:00"), pt("2026-11-04", "08:30")


def hm(t):
    """epoch -> 'HH:MM:SS PT' (sim clock)."""
    s = int(round(t - (T_START - (14 * 3600 + 30 * 60))))
    d, s = divmod(s, 86400)
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}" + (" +1" if d else "")


def logit(p):
    p = min(0.9995, max(0.0005, p))
    return math.log(p / (1 - p))


def expit(x):
    return 1 / (1 + math.exp(-x))


def tick(p, up=False, t=0.005):
    k = p / t
    k = math.ceil(k - 1e-9) if up else math.floor(k + 1e-9)
    return round(k * t, 3)


class Template:
    def __init__(self, d):
        self.__dict__.update(d)
        self.fav = [v / 1e4 for v in d["fav"]]
        self.oth = [v / 1e4 for v in d["oth"]]
        self.i0 = (self.last_close - self.t0) // self.step       # index of the last close
        pre = sorted(self.fav[self.i0 - 120:self.i0])
        self.pre_fav = pre[len(pre) // 2]
        pre_o = sorted(self.oth[self.i0 - 120:self.i0])
        self.pre_oth = pre_o[len(pre_o) // 2]
        self.p_fav = max(self.p_pre_winner, 1 - self.p_pre_winner)
        art = sum(self.fav_art[self.i0:self.i0 + 18 * 60]) + sum(self.oth_art[self.i0:self.i0 + 18 * 60])
        self.thin = art > 60 or abs(self.fav[self.i0] - self.p_fav) > 0.15
        self.group = "House" if self.kind == "House" else "Top"

    def at(self, tau, leg):
        """Price and emptied-book flag at tau seconds after the 2024 last close."""
        i = int(self.i0 + math.floor(tau / self.step))
        i = min(max(i, 0), len(self.fav) - 1)
        if leg == "fav":
            return self.fav[i], self.fav_art[i]
        return self.oth[i], self.oth_art[i]


def load_templates():
    with gzip.open(os.path.join(FIX, "night24_paths.json.gz"), "rt") as fh:
        return [Template(r) for r in json.load(fh)["races"]]


def load_2026():
    with open(os.path.join(FIX, "night26_levels.json")) as fh:
        lv = json.load(fh)
    pc_path = os.path.join(ROOT, "data", "election", "poll_close.json")
    pc = json.load(open(pc_path)) if os.path.exists(pc_path) else lv["poll_close"]
    return lv, pc


class Leg:
    __slots__ = ("ex", "race", "party", "role", "fair", "g0", "lvl", "art", "quote", "sig_t", "sig_m", "tau", "depth", "spread",
                 "winner")


class World:
    """One scripted night. Knobs: seed; thin (allow one-sided-book templates); stretch (contested races' post-close clock x k, k drawn
    in the given range); gap (share of today's SIG - PM gap that survives to Nov 3); overlays (reversal / stress40 lists)."""

    def __init__(self, seed=7, thin=False, stretch=None, gap=0.5, reversals=0, strict_reversals=0, stress40=False):
        self.seed, self.rng = seed, random.Random(seed)
        lv, pc = load_2026()
        self.snapshot = lv.get("snapshot")
        books = lv["books"]
        self.templates = load_templates()
        self.meta, self.first, self.last, self.manual = {}, {}, {}, set()
        races = {}
        for m in pc["markets"]:
            ex = str(m["ex"])
            mm = TITLE.match(m["title"])
            if not mm or not m.get("first_close_pt") or not m.get("last_close_pt"):
                continue
            race = mm.group(2)
            # the engine (f_engine.el_build) takes `state` from poll_close.json as is: the control markets come in as "Alaska"
            self.meta[ex] = dict(race=race, party=m.get("party") or mm.group(1)[0], title=m["title"], state=m.get("state"))
            f_, l_ = pt(DAY, m["first_close_pt"]), pt(DAY, m["last_close_pt"])
            self.first[race] = min(self.first.get(race, f_), f_)
            self.last[race] = max(self.last.get(race, l_), l_)
            races.setdefault(race, []).append(ex)
            if (books.get(ex) or {}).get("manual"):
                self.manual.add(ex)
        self.races = sorted(races, key=lambda r: (self.last[r], r))
        self.by_race = races
        self.state_of = {r: self.meta[races[r][0]]["state"] for r in races}
        self.minutes = int((T_END - T_START) // MIN) + 2
        self.legs, self.winner, self.template, self.kind, self.stretch = {}, {}, {}, {}, {}
        self.p26 = {}
        for r in self.races:
            exs = races[r]
            fair = {}
            for ex in exs:
                b = books.get(ex) or {}
                f = b.get("fair")
                if f is None and b.get("sig_bid") is not None and b.get("sig_ask") is not None:
                    f = (b["sig_bid"] + b["sig_ask"]) / 2
                fair[ex] = min(0.995, max(0.003, f if f is not None else 0.5))
            order = sorted(exs, key=lambda e: -fair[e])
            F, O = order[0], order[1]
            p = fair[F] / (fair[F] + fair[O])
            self.p26[r] = p
            kind = "House" if "House race" in r else ("Control" if r.startswith("U.S.") else "Top")
            self.kind[r] = kind
            tpl = self.pick(r, p, kind, thin)
            self.template[r] = tpl
            k = 1.0
            if stretch and p < 0.95:
                k = self.rng.uniform(*stretch)
            self.stretch[r] = k
            for i, ex in enumerate(order):
                L = Leg()
                L.ex, L.race, L.party, L.fair = ex, r, self.meta[ex]["party"], fair[ex]
                L.role = "fav" if i == 0 else ("oth" if i == 1 else "x")
                b = books.get(ex) or {}
                sig_mid = (b["sig_bid"] + b["sig_ask"]) / 2 if b.get("sig_bid") is not None and b.get("sig_ask") is not None else fair[ex]
                L.g0 = max(-0.06, min(0.04, (sig_mid - fair[ex]) * gap))
                L.tau = self.rng.uniform(5, 20) * 60          # SIG's lag behind PM (exponential time constant)
                L.depth = [self.rng.randint(1500, 6000)] + [self.rng.randint(1000, 4000) for _ in range(4)]
                L.spread = 0.01 if tpl.vol >= 250_000 else (0.015 if tpl.vol >= 60_000 else 0.02)
                self.legs[ex] = L
            self.winner[r] = F if tpl.fav_won else O
            self.build_paths(r, order, tpl, k)
        self.overlays = []
        if reversals or strict_reversals:
            self.add_reversals(reversals, strict_reversals)
        if stress40:
            self.add_stress40()
        for L in self.legs.values():
            self.build_quotes(L)
            self.build_sig(L)

    # ------------------------------------------------------------------------------------------- mapping
    def pick(self, race, p, kind, thin):
        pool = [t for t in self.templates if thin or not t.thin]
        want = "House" if kind == "House" else "Top"

        def score(t):
            return abs(logit(p) - logit(t.p_fav)) + (0.4 if t.group != want else 0.0)
        best = sorted(pool, key=score)[:3]
        rr = random.Random(f"{self.seed}:{race}")
        return rr.choice(best)

    def build_paths(self, r, order, tpl, k):
        L26 = self.last[r]
        for i, ex in enumerate(order):
            L = self.legs[ex]
            leg = "fav" if i == 0 else "oth"
            base_pre = tpl.pre_fav if leg == "fav" else tpl.pre_oth
            if i < 2:
                d = max(-2.5, min(2.5, logit(L.fair) - logit(base_pre)))
            lvl, art = [], []
            for m in range(self.minutes):
                t = T_START + m * MIN
                tau = t - L26
                tau24 = tau if tau <= 0 else tau / k
                if i < 2:
                    v, a = tpl.at(tau24, leg)
                    w = 1.0 if tau <= 0 else math.exp(-tau / 3600)
                    if not a:
                        v = expit(logit(v) + d * w)
                else:                                          # a third book: the second book's path, scaled to its own level
                    v, a = self.legs[order[1]].lvl[m] * L.fair / max(self.legs[order[1]].fair, 1e-3), 0
                    v = min(0.5, max(0.001, v))
                lvl.append(v)
                art.append(a)
            # an emptied-book minute keeps the last good level (the quote builder makes it one-sided)
            good = None
            for m in range(self.minutes):
                if art[m]:
                    lvl[m] = good if good is not None else L.fair
                else:
                    good = lvl[m]
            L.lvl, L.art = lvl, art

    # ------------------------------------------------------------------------------------------- overlays
    def _contested(self, max_last=None):
        out = [r for r in self.races if 0.55 <= self.p26[r] <= 0.93 and (max_last is None or self.last[r] <= max_last)]
        return out

    def add_reversals(self, n_a, n_b):
        """2024-style wrong calls (MI Senate: Rogers 0.975 at 03:22 ET, then Slotkin): the eventual loser's PM climbs to a call, holds,
        then collapses; half of them fall back under 0.90 within 10 min of the call (the guard fires), half fade over 30 min."""
        cand = self._contested(max_last=pt(DAY, "20:00"))
        self.rng.shuffle(cand)
        pick = cand[:n_a + n_b]
        for j, r in enumerate(pick):
            strict = j >= n_a
            W = self.winner[r]
            Lx = next(e for e in self.by_race[r] if e != W and self.legs[e].role in ("fav", "oth"))
            m1 = int((self.last[r] - T_START) / MIN + self.rng.uniform(40, 150))
            fast = j % 2 == 0
            peak = 0.989 if strict else self.rng.uniform(0.966, 0.976)
            hold = 3 if strict else self.rng.randint(6, 12)
            fall = 4 if fast else 30
            shape = []
            Lo = self.legs[Lx]
            v0 = Lo.lvl[m1]
            for s in range(10):
                shape.append(v0 + (peak - v0) * (s + 1) / 10)
            shape += [peak] * hold
            for s in range(fall):
                shape.append(peak + (0.55 - peak) * (s + 1) / fall)
            for s, v in enumerate(shape):
                m = m1 + s
                if m >= self.minutes:
                    break
                Lo.lvl[m], Lo.art[m] = v, 0
                self.legs[W].lvl[m], self.legs[W].art[m] = max(0.002, 1 - v - 0.004), 0
            for m in range(m1 + len(shape), min(self.minutes, m1 + len(shape) + 90)):      # the loser drifts back under 0.55
                Lo.lvl[m] = min(Lo.lvl[m], 0.55)
                self.legs[W].lvl[m] = max(self.legs[W].lvl[m], 0.45)
            self.overlays.append(dict(kind="reversal", race=r, loser=Lx, t=T_START + m1 * MIN, peak=round(peak, 3), strict=strict,
                                      fast=fast))

    def add_stress40(self):
        """40 calls in 20 min from 19:00 PT: 40 races (contested first, then safe; last close <= 18:00 PT) are held under 0.90 until their
        slot, then the winner jumps to 0.975 within a minute (the old sim's cliff-call, on the 2026 universe)."""
        lim = pt(DAY, "18:00")
        cont = [r for r in self._contested(max_last=lim)]
        safe = [r for r in self.races if r not in cont and self.last[r] <= lim and self.kind[r] != "Control"]
        self.rng.shuffle(cont)
        self.rng.shuffle(safe)
        pick = (cont + safe)[:40]
        t0 = pt(DAY, "19:00")
        for i, r in enumerate(pick):
            slot = t0 + i * 30
            ms = int((slot - T_START) // MIN)
            W = self.winner[r]
            for ex in self.by_race[r]:
                L = self.legs[ex]
                for m in range(int((self.last[r] - T_START) // MIN) - 30, self.minutes):
                    if m < ms:
                        L.lvl[m] = min(L.lvl[m], 0.88) if ex == W else max(L.lvl[m], 0.10 if L.role != "x" else L.lvl[m])
                    else:
                        L.lvl[m] = 0.975 if ex == W else 0.012
                    L.art[m] = 0
            self.overlays.append(dict(kind="stress40", race=r, t=slot))

    # ------------------------------------------------------------------------------------------- PM quotes
    def quote_at(self, L, m):
        """(bid, ask) of the PM YES book at minute m: the level +- half the template's spread; an emptied-book minute loses the side
        facing the move (a favourite's bids, an outsider's asks); a one-minute spike of >= 8c that reverts is a thin book (spread 2x)."""
        v, s = L.lvl[m], L.spread
        if L.art[m]:
            return (0.01, round(min(0.999, v + s / 2), 3)) if v >= 0.5 else (round(max(0.001, v - s / 2), 3), 0.99)
        p = L.lvl[m - 1] if m > 0 else v
        n = L.lvl[m + 1] if m + 1 < self.minutes else v
        dev = min(abs(v - p), abs(v - n))
        if (v - p) * (v - n) > 0 and dev >= 0.08:
            s = max(s, 2 * dev)
        b = max(0.001, math.floor((v - s / 2) * 1000 + 1e-9) / 1000)
        a = min(0.999, math.ceil((v + s / 2) * 1000 - 1e-9) / 1000)
        if a <= b:
            a = min(0.999, b + 0.001)
        return round(b, 3), round(a, 3)

    def build_quotes(self, L):
        L.quote = [self.quote_at(L, m) for m in range(self.minutes)]

    def messages(self, feed_down=None):
        """The PM feed: a quote at a random second of each minute it changes; 10-s heartbeats while a side bid is >= 0.93 after the
        race's last close - 1 min (the first 60 such minutes, then every 60 s). feed_down=(t0, t1): nothing in between, then a snapshot
        of every book at t1 (the resubscribe). -> sorted [(t, ex, bid, ask)]"""
        out = []
        for ex, L in self.legs.items():
            rng = random.Random(f"{self.seed}:msg:{ex}")
            last_q, hot = None, 0
            L26 = self.last[L.race]
            for m in range(self.minutes):
                t0 = T_START + m * MIN
                q = L.quote[m]
                off = rng.uniform(0, 59)
                b, a = q
                is_hot = t0 >= L26 - 60 and (b >= 0.93 or 1 - a >= 0.93 or (last_q and (last_q[0] >= 0.93 or 1 - last_q[1] >= 0.93)))
                if is_hot:
                    hot += 1
                    grid = range(0, 60, 10) if hot <= 60 else (0,)
                    for s in grid:
                        tt = t0 + s
                        cur = q if tt >= t0 + off else (last_q or q)
                        out.append((tt, ex, cur[0], cur[1]))
                    if q != last_q:
                        out.append((t0 + off, ex, q[0], q[1]))
                elif q != last_q:
                    out.append((t0 + off, ex, q[0], q[1]))
                last_q = q
        if feed_down:
            a0, a1 = feed_down
            out = [x for x in out if not (a0 <= x[0] < a1)]
            for ex, L in self.legs.items():
                m = int((a1 - T_START) // MIN)
                out.append((a1, ex, L.quote[m][0], L.quote[m][1]))
        out.sort(key=lambda x: (x[0], x[1]))
        return out

    # ------------------------------------------------------------------------------------------- the SIG book
    def build_sig(self, L):
        """SIG's YES mid: an exponential lag (time constant L.tau) of PM's level one minute earlier plus today's SIG - PM gap x `gap`,
        the gap fading with a 90-min constant after the race's last close; clipped to [0.025, 0.975] (SIG has shown no bids >= 0.98 to
        speak of, night_reconcile §0)."""
        L26 = self.last[L.race]
        m_, cur = [], None
        dec = math.exp(-MIN / L.tau)
        for m in range(self.minutes):
            t = T_START + m * MIN
            pm_prev = L.lvl[m - 1] if m > 0 else L.lvl[0]
            g = L.g0 * (1.0 if t <= L26 else math.exp(-(t - L26) / 5400))
            tgt = min(0.975, max(0.025, pm_prev + g))
            if cur is None:
                cur = tgt
            m_.append((cur, tgt))                          # (SIG mid at the minute's start, the target during the minute)
            cur = tgt + (cur - tgt) * dec
        L.sig_m = m_

    def sig_mid(self, ex, t):
        L = self.legs[ex]
        m = min(max(int((t - T_START) // MIN), 0), self.minutes - 1)
        start, tgt = L.sig_m[m]
        dt = max(0.0, t - (T_START + m * MIN))
        return tgt + (start - tgt) * math.exp(-dt / L.tau)

    def sig_touch(self, ex, t):
        L = self.legs[ex]
        mid = self.sig_mid(ex, t)
        wide = t >= self.last[L.race]
        bid = max(0.005, tick(mid - (0.005 if wide else 0.0025)))
        ask = min(0.995, max(bid + 0.005, tick(mid + (0.005 if wide else 0.0025), up=True)))
        return round(bid, 3), round(ask, 3)

    consumed = None

    def _used(self, key, t):
        if self.consumed is None:
            self.consumed = {}
        q, t0 = self.consumed.get(key, (0.0, t))
        return q * math.exp(-(t - t0) / 600)                 # our takes refill with a 10-min constant

    def _use(self, key, q, t):
        self.consumed[key] = (self._used(key, t) + q, t)

    def book_levels(self, ex, t):
        """(bids [(yes price, qty)] descending, asks [(yes price, qty)] ascending): 5 levels 0.5c apart, less what we took."""
        L = self.legs[ex]
        bid, ask = self.sig_touch(ex, t)
        asks, bids = [], []
        for k in range(5):
            pa, pb = round(ask + 0.005 * k, 3), round(bid - 0.005 * k, 3)
            if pa <= 0.995:
                qa = max(0.0, L.depth[k] - self._used((ex, "a", pa), t))
                asks.append((pa, int(qa)))
            if pb >= 0.005:
                qb = max(0.0, L.depth[k] - self._used((ex, "b", pb), t))
                bids.append((pb, int(qb)))
        return bids, asks

    def side_levels(self, ex, side, t):
        bids, asks = self.book_levels(ex, t)
        return list(asks) if side == "yes" else [(round(1 - p, 3), q) for p, q in bids]

    def take(self, ex, side, limit, qty, t):
        """A buy of `side` at <= limit: walks the levels (the taker pays each level's price). -> [(price, qty)]"""
        out, left = [], qty
        for p, q in self.side_levels(ex, side, t):
            if left <= 0 or p > limit + 1e-9:
                break
            x = min(left, q)
            if x >= 1:
                yes_p = p if side == "yes" else round(1 - p, 3)
                self._use((ex, "a" if side == "yes" else "b", yes_p), x, t)
                out.append((p, x))
                left -= x
        return out

    def pm_mid(self, ex, t):
        L = self.legs[ex]
        m = min(max(int((t - T_START) // MIN), 0), self.minutes - 1)
        b, a = L.quote[m]
        return (b + a) / 2

    def payout(self, ex):
        return 1.0 if self.winner[self.meta[ex]["race"]] == ex else 0.0

    def wave_of(self, race):
        return self.last[race]
