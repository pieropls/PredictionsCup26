#!/usr/bin/env python3
"""Election-night dress rehearsal, the plan (Piero 11:40 Oct 22 item 5; docs/night_reconcile.md C19; engine C111 control
`election_rehearsal`). Sun Oct 25 15:00-21:00 PT on the live engine in SHADOW: 2024 Polymarket paths injected into a sample of
every wave's books, the nine Nov 3 waves (last close 16:00 ... 22:00 PT) moved onto a 15:00-21:00 clock, each path compressed
x5-x12 so a contested race resolves in ~20-45 min.

What it does (reads only; the one write is the plan on the server):
  1. reads data/election/poll_close.json and data/state.json from the server (read-only `cat` over ssh; --local: the Mac mirror),
     tests/fixtures/night24_paths.json.gz (the 68 2024 races, 1-min PM paths of the pre-close favourite's token);
  2. builds one injection per book of every planned race (all books of a race, complementary paths: the 2024 favourite's path on the
     2026 favourite's book, 1 - it on the other book(s)), quotes every 2-10 s plus one at each 2024 price change, YES bid / ask with a
     1-2c spread (5c on the spread case), the race closing `close_at` s after the injection's start (3-5 min of PRECALL first);
  3. replays every injection through the engine's own detector (n_election.Election, imported, not edited) on an ideal clock -> the
     expected CALL / STRICT / NOCALL / REVERSAL rows and would-be orders, then again with the engine's loop stepping (period 4-20 s,
     the injected quotes are delivered once per loop) to flag any expectation that depends on the loop's phase;
  4. writes /opt/research/night/rehearsal_plan.json (the engine's format) and rehearsal_plan_expected.json (the human summary the
     evaluator reads) on the server, readable by the engine user, plus research/night/rehearsal_plan_expected.json here.

Usage (from the repo root):
  python3 research/night/rehearsal_plan.py              # build, replay, print the per-wave counts, upload
  python3 research/night/rehearsal_plan.py --dry-run    # the same, nothing written anywhere
  python3 research/night/rehearsal_plan.py --local      # inputs from the Mac mirror (data/ is only read)
Then Piero: control election_rehearsal, file=/opt/research/night/rehearsal_plan.json, start=<the epoch printed below>.
"""
import argparse
import collections
import gzip
import json
import math
import os
import random
import re
import shlex
import subprocess
import sys
import time

sys.dont_write_bytecode = True                              # nothing new on disk besides the plan files
os.environ["TZ"] = "America/Los_Angeles"
time.tzset()

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, ROOT)
import n_election as el  # noqa: E402  (the engine's detector, read-only use)

SERVER = os.environ.get("SIG_SERVER", "user@your-server")
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", SERVER]
ENGINE = "/opt/sig-engine"
REMOTE_DIR = "/opt/research/night"
REMOTE_PLAN = f"{REMOTE_DIR}/rehearsal_plan.json"
REMOTE_EXPECTED = f"{REMOTE_DIR}/rehearsal_plan_expected.json"
LOCAL_EXPECTED = os.path.join(HERE, "rehearsal_plan_expected.json")
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "night24_paths.json.gz")
START_PT = "2026-10-25 15:00:00"
WINDOW_H = 6.0
SIG_TITLE = re.compile(r"Will the (\w+) Party win the (.+?)\??$")   # c_polymarket.race_party
SENATE_EXS = ("842", "843")                                 # a_config ROTATION['senate_exs'] -> f_engine.rot_race 'U.S. Senate control'

# Nov 3 wave (last poll close PT) -> its synthetic close on the rehearsal clock, minutes after 15:00 PT Oct 25
WAVES = [("16:00", 15), ("16:30", 40), ("17:00", 70), ("17:30", 115), ("18:00", 145), ("19:00", 200), ("20:00", 260), ("21:00", 295),
         ("22:00", 320)]

# The sample. tpl = the 2024 race (fixture key) whose favourite path drives the 2026 favourite's book; k = 2024 seconds per
# rehearsal second; post = seconds of path after the synthetic close (every path starts PRE_S = 7 min before it); spread = the PM YES spread (default by the 2024
# volume: 1c >= 250k, 1.5c >= 60k, else 2c); mode: None, "spread" (>= 4.5c on every book: must not call), "other" (the other
# book's PM held at 0.06-0.12: the complement guard must block the YES call).
SCEN = [
    # W1 16:00 (KY, FL, GA, SC, VA, VT ...)
    dict(wave="16:00", race="Virginia Senate", tpl="Senate Connecticut", k=6, post=480, kind="safe"),
    dict(wave="16:00", race="Kentucky Senate", tpl="Senate West Virginia", k=6, post=480, kind="safe", spread=0.01),
    dict(wave="16:00", race="South Carolina Senate", tpl="Senate Texas", k=6, post=900, kind="lean"),
    dict(wave="16:00", race="Georgia Governor", tpl="Senate Arizona", k=6, post=1980, kind="contested"),
    # W2 16:30 (NC, WV)
    dict(wave="16:30", race="West Virginia Senate", tpl="Governor West Virginia", k=6, post=480, kind="safe"),
    dict(wave="16:30", race="North Carolina Senate", tpl="Senate New York", k=6, post=600, kind="spread", mode="spread"),
    # W3 17:00 (the big one)
    dict(wave="17:00", race="Massachusetts Senate", tpl="Senate Massachusetts", k=6, post=480, kind="safe"),
    dict(wave="17:00", race="Rhode Island Senate", tpl="Senate Rhode Island", k=6, post=480, kind="safe"),
    dict(wave="17:00", race="Delaware Senate", tpl="Senate Delaware", k=6, post=480, kind="safe"),
    dict(wave="17:00", race="Florida Senate", tpl="Governor New Hampshire", k=6, post=1200, kind="lean"),
    dict(wave="17:00", race="Mississippi Senate", tpl="Senate Ohio", k=7, post=2100, kind="contested"),
    dict(wave="17:00", race="Maine Senate", tpl="Senate Pennsylvania", k=12, post=2520, kind="upset"),
    dict(wave="17:00", race="PA-07 House race", tpl="House PA-07", k=8, post=2280, kind="upset"),
    # W4 17:30 (AR)
    dict(wave="17:30", race="Arkansas Governor", tpl="Governor Missouri", k=6, post=480, kind="safe"),
    dict(wave="17:30", race="Arkansas Senate", tpl="Senate Tennessee", k=6, post=600, kind="other", mode="other"),
    # W5 18:00 (the second big one)
    dict(wave="18:00", race="Colorado Senate", tpl="Senate Minnesota", k=6, post=480, kind="safe"),
    dict(wave="18:00", race="Wyoming Senate", tpl="Senate Wyoming", k=6, post=480, kind="safe", spread=0.01),
    dict(wave="18:00", race="Minnesota Governor", tpl="Governor Washington", k=6, post=720, kind="lean"),
    dict(wave="18:00", race="Nebraska Senate", tpl="Senate Nebraska", k=10, post=1500, kind="reversal"),
    dict(wave="18:00", race="Michigan Senate", tpl="Senate Michigan", k=8, post=2700, kind="slow"),
    dict(wave="18:00", race="Iowa Senate", tpl="Senate Ohio", k=7, post=2100, kind="contested"),
    dict(wave="18:00", race="Texas Senate", tpl="Senate Wisconsin", k=12, post=2400, kind="slow"),
    dict(wave="18:00", race="Arizona Governor", tpl="Senate Arizona", k=6, post=1980, kind="contested"),
    # W6 19:00 (ID / MT / NV)
    dict(wave="19:00", race="Montana Senate", tpl="Senate Montana", k=8, post=2760, kind="reversal"),
    dict(wave="19:00", race="Nevada Governor", tpl="Senate Nevada", k=8, post=1800, kind="slow"),
    # W7 20:00 (CA, OR, WA, ID)
    dict(wave="20:00", race="Oregon Senate", tpl="Senate Washington", k=6, post=480, kind="safe", spread=0.01),
    dict(wave="20:00", race="Idaho Senate", tpl="Governor North Carolina", k=6, post=600, kind="safe"),
    dict(wave="20:00", race="CA-22 House race", tpl="House FL-13", k=6, post=1200, kind="slow"),
    # W8 21:00 (HI)
    dict(wave="21:00", race="Hawaii Governor", tpl="Senate Hawaii", k=6, post=480, kind="safe"),
    # W9 22:00 (AK + control)
    dict(wave="22:00", race="U.S. Senate control", tpl="Control Senate", k=6, post=600, kind="safe"),
    dict(wave="22:00", race="U.S. House", tpl="Control House", k=6, post=840, kind="slow"),
    dict(wave="22:00", race="Alaska Senate", tpl="House AK-01", k=6, post=1500, kind="slow"),
]

# Rehearsal 2 (Piero spec Oct 27 item 4): --all adds every other armed race, in its own wave, with the same kind mix as the 32 above
# (about 28% no-call: slow / upset / spread / other; 6% reversal), each kind on the 2024 templates the hand-picked races use.
KIND_MIX = ["safe", "safe", "lean", "safe", "contested", "slow", "safe", "lean", "slow", "safe", "reversal", "contested", "safe", "upset",
            "slow", "safe", "lean", "spread", "safe", "slow", "contested", "safe", "other", "safe", "slow", "lean", "safe", "slow", "contested",
            "safe", "reversal", "upset"]
KIND_TPL = {"safe": [("Senate Connecticut", 6, 480), ("Senate West Virginia", 6, 480), ("Senate Massachusetts", 6, 480), ("Senate Delaware", 6, 480),
                     ("Governor Missouri", 6, 480), ("Senate Minnesota", 6, 480), ("Senate Washington", 6, 480), ("Senate Hawaii", 6, 480)],
            "lean": [("Senate Texas", 6, 900), ("Governor New Hampshire", 6, 1200), ("Governor Washington", 6, 720)],
            "contested": [("Senate Arizona", 6, 1980), ("Senate Ohio", 7, 2100)],
            "upset": [("Senate Pennsylvania", 12, 2520), ("House PA-07", 8, 2280)],
            "reversal": [("Senate Nebraska", 10, 1500), ("Senate Montana", 8, 2760)],
            "slow": [("Senate Michigan", 8, 2700), ("Senate Wisconsin", 12, 2400), ("Senate Nevada", 8, 1800), ("House FL-13", 6, 1200)],
            "spread": [("Senate New York", 6, 600)], "other": [("Senate Tennessee", 6, 600)]}
ALL = False


def extra_scen(by_race):
    """Every armed race not in SCEN (and without a manual book), in its real wave, cycling KIND_MIX."""
    have = {sc["race"] for sc in SCEN}
    out, i = [], 0
    for race in sorted(by_race):
        books = by_race[race]
        waves = {a["wave"] for a in books.values()}
        if race in have or len(waves) != 1 or any(a["manual"] for a in books.values()) or next(iter(waves)) not in dict(WAVES):
            continue
        kind = KIND_MIX[i % len(KIND_MIX)]
        tpl, k, post = KIND_TPL[kind][i % len(KIND_TPL[kind])]
        sc = dict(wave=next(iter(waves)), race=race, tpl=tpl, k=k, post=post, kind=kind)
        if kind in ("spread", "other"):
            sc["mode"] = kind
        out.append(sc)
        i += 1
    return out


VARIANTS = [(j, L, ph) for j in (0.05, 0.5, 2.0) for L in (5.0, 10.0, 16.0) for ph in (0.0, 0.5)]   # (thread jitter s, loop s, loop phase)
PRE_S = 420                                                 # C112: every injection starts 7 min before its close (the pulls fire at its start)


# --------------------------------------------------------------------------------------------- clock / I/O
def epoch_pt(s):
    return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))


def hms(t):
    return time.strftime("%H:%M:%S", time.localtime(t))


def ssh(cmd, data=None, timeout=120):
    r = subprocess.run(SSH + [cmd], input=data, capture_output=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"ssh {cmd[:80]!r}: rc {r.returncode}: {r.stderr.decode(errors='replace')[:300]}")
    return r.stdout


def read_server(path):
    """One file from the server, read-only (`cat`)."""
    return ssh(f"cat {shlex.quote(path)}")


def load_inputs(local):
    if local:
        pc = json.load(open(os.path.join(ROOT, "data", "election", "poll_close.json")))
        st = json.load(open(os.path.join(ROOT, "data", "state.json")))
        src = "Mac mirror data/ (read-only)"
    else:
        pc = json.loads(read_server(f"{ENGINE}/data/election/poll_close.json"))
        st = json.loads(read_server(f"{ENGINE}/data/state.json"))
        src = f"{SERVER}:{ENGINE}/data (read-only)"
    return pc, st, src


# --------------------------------------------------------------------------------------------- the 2026 books (as f_engine.el_build arms them)
def armed_books(pc, st):
    """{ex: dict(race, party, title, state, first, last, fair, bid, ask, pos, manual)} for every poll_close.json book the engine owns.
    race = f_engine.rot_race (c_polymarket.race_party; 842 / 843 -> 'U.S. Senate control'); first / last = Nov 3 closes (epoch)."""
    bots = {str(b["ex"]): b for b in st.get("bots") or []}
    day = pc.get("election_day") or "2026-11-03"

    def ep(hm):
        hm = str(hm); plus = hm.endswith("+1")
        return time.mktime(time.strptime(f"{day} {hm.replace('+1', '')}", "%Y-%m-%d %H:%M")) + (86400 if plus else 0)
    out = {}
    for r in pc["markets"]:
        ex = str(r["ex"])
        b = bots.get(ex)
        if b is None or not r.get("first_close_pt") or not r.get("last_close_pt"):
            continue
        m = SIG_TITLE.match(str(b.get("market") or r.get("title") or "").strip())
        race = "U.S. Senate control" if ex in SENATE_EXS else (m.group(2) if m else str(r.get("title")))
        state = race if "control" in race.lower() else r.get("state")
        out[ex] = dict(race=race, party=r.get("party") or (m.group(1)[0] if m else "?"), title=r.get("title"), state=state,
                       first=ep(r["first_close_pt"]), last=ep(r["last_close_pt"]), wave=r["last_close_pt"],
                       fair=b.get("fair"), bid=b.get("bid"), ask=b.get("ask"), pos=float(b.get("pos") or 0),
                       manual=b.get("kind") == "position")
    return out


def cash_of(st):
    a = st.get("account") or {}
    try:
        return float(a["totalAccountValue"]) - float(a["totalHoldingsValue"])
    except (KeyError, TypeError, ValueError):
        return 20_000.0


# --------------------------------------------------------------------------------------------- 2024 templates
class Tpl:
    def __init__(self, d):
        self.key, self.kind, self.vol, self.fav_won = d["key"], d["kind"], d["vol"], d["fav_won"]
        self.step = d["step"]
        self.i0 = (d["last_close"] - d["t0"]) // self.step
        raw = [v / 1e4 for v in d["fav"]]
        art = d["fav_art"]
        clean, good = [], None                               # an emptied-book minute keeps the last good level (tests/night_world.py)
        for v, a in zip(raw, art):
            if a and good is not None:
                clean.append(good)
            else:
                clean.append(v); good = v
        self.lvl = clean
        self.spread = 0.01 if self.vol >= 250_000 else (0.015 if self.vol >= 60_000 else 0.02)

    def idx(self, tau24):
        return min(max(int(self.i0 + math.floor(tau24 / self.step)), 0), len(self.lvl) - 1)

    def at(self, tau24):
        return self.lvl[self.idx(tau24)]


def load_templates():
    with gzip.open(FIXTURE, "rt") as fh:
        return {r["key"]: Tpl(r) for r in json.load(fh)["races"]}


# --------------------------------------------------------------------------------------------- paths
def q_round(v, s):
    b = math.floor((v - s / 2) * 1000 + 1e-9) / 1000
    a = math.ceil((v + s / 2) * 1000 - 1e-9) / 1000
    b = max(0.001, min(0.997, b))
    a = min(0.999, max(b + 0.001, a))
    return round(b, 3), round(a, 3)


def q_wide(v):
    """The spread case: >= 4.5c between the PM bid and ask whatever the level (the 3c guard must hold every quote)."""
    b = math.floor((v - 0.025) * 1000 + 1e-9) / 1000
    a = math.ceil((v + 0.025) * 1000 - 1e-9) / 1000
    if a > 0.999:
        a = 0.999; b = min(b, a - 0.045)
    if b < 0.001:
        b = 0.001; a = max(a, b + 0.045)
    return round(b, 3), round(a, 3)


def quote_times(tpl, k, pre, post, rng):
    """Path seconds 0 .. pre + post: a heartbeat every 2-10 s, plus a quote 0-1.5 s after each 2024 price change (PM pushes
    price_change events); heartbeats within 1 s of a change point are dropped so a one-minute spike is never lost."""
    total = pre + post
    chg = []
    prev = None
    for i in range(tpl.idx(-pre * k), tpl.idx(post * k) + 1):
        v = tpl.lvl[i]
        if prev is not None and v != prev:
            p = pre + ((i - tpl.i0) * tpl.step) / k + rng.uniform(0.0, 1.5)
            if 0 < p < total:
                chg.append(round(p, 1))
        prev = v
    hb, t = [], 0.0
    while t <= total:
        hb.append(round(t, 1)); t += rng.uniform(2.0, 10.0)
    chg_set = sorted(set(chg))
    out = set(chg_set)
    j = 0
    for t in hb:
        while j < len(chg_set) and chg_set[j] < t - 1.0:
            j += 1
        if j < len(chg_set) and abs(chg_set[j] - t) < 1.0:
            continue
        out.add(t)
    out.add(0.0)
    pts = sorted(out)
    while len(pts) > 1900:                                   # the engine takes <= 2000 points a path
        pts = pts[::2] if not chg_set else sorted(set(chg_set) | set(pts[::2]))
        if len(pts) > 1900:
            pts = pts[::2]
    return pts


def build_race(sc, books, tpl, rng):
    """-> [dict(ex, party, role, path)] for every book of the race; the favourite path = the 2024 favourite's (time x k)."""
    exs = sorted(books, key=lambda e: -(books[e]["fair"] or 0))
    F, others = exs[0], exs[1:]
    wsum = sum(max(0.001, books[e]["fair"] or 0.001) for e in others) or 1.0
    w = {e: max(0.001, books[e]["fair"] or 0.001) / wsum for e in others}
    times = quote_times(tpl, sc["k"], sc["pre"], sc["post"], rng)
    mode = sc.get("mode")
    out = {F: [], **{e: [] for e in others}}
    for p in times:
        tau24 = (p - sc["pre"]) * sc["k"]
        v = tpl.at(tau24)
        for ex in [F] + others:
            if ex == F:
                lv = v
            elif mode == "other" and ex == others[0]:
                lv = min(0.12, max(0.06, 1 - v + 0.05))     # the complement guard case: the other book never under 0.06
            else:
                lv = max(0.0005, (1 - v) * w[ex])
            s = sc.get("spread") or tpl.spread
            b, a = q_wide(lv) if mode == "spread" else q_round(lv, s)
            out[ex].append([p, b, a])
    return [dict(ex=ex, party=books[ex]["party"], role="fav" if ex == F else ("other" if ex == others[0] else "third"), path=out[ex])
            for ex in [F] + others]


# --------------------------------------------------------------------------------------------- the replay (the engine's detector)
class Replay:
    """The engine's election object (n_election.Election, as f_engine.el_build arms it) driven by the plan the way C112 does it:
      * the injector thread (f_engine.el_inject_loop): wakes at the next due quote (>= 50 ms after the last wake, + `jitter` s of
        GIL / scheduling delay, uniform), starts every injection whose t0 has passed (el_inject_begin: the race's FIRST close := t0 +
        close_at, the book's 0.95 runs reset), sets the LAST close at t0 + close_at and feeds every due quote stamped at that moment
        (el_inject_feed), injections in plan order; the call lane snipes at once (ms);
      * the main loop (period `loop_s`, phase): el_inject_begin too, the restore 120 s after an injection's last quote
        (el_inject_step), the poll-close pulls (Election.on_tick: once per race, at first close - 10 min).
    jitter 0 = the ideal clock (each quote at its own second). Shadow would-be orders as C112's el_place: a key per order, confirmed
    at once as accepted, and the depth taken off the book for 120 s (el_levels / el_taken)."""

    def __init__(self, armed, injs, start, cash, pos, jitter=0.0, loop_s=10.0, phase=0.0, seed=0):
        self.armed, self.injs, self.start, self.cash, self.pos = armed, injs, start, cash, dict(pos)
        self.jitter, self.loop_s, self.phase, self.rng = jitter, loop_s, phase, random.Random(seed)
        meta = {ex: dict(race=a["race"], party=a["party"], title=a["title"], state=a["state"]) for ex, a in armed.items()}
        close, first = {}, {}
        for a in armed.values():
            close[a["race"]] = max(close.get(a["race"], a["last"]), a["last"])
            first[a["race"]] = min(first.get(a["race"], a["first"]), a["first"])
        self.now = start
        self.orders, self.dry, self.logs, self.taken, self.n_key = [], [], [], {}, 0
        self.E = el.Election(meta, close, lambda: self.now, self._place, lambda o, reason="": True, self._book, self._log,
                             manual=tuple(ex for ex, a in armed.items() if a["manual"]), cash_floor=el.CASH_FLOOR, first_close=first,
                             levels=self._levels, place_dry=self._dry, precall_dry=True)

    def _log(self, kind, text):
        self.logs.append((self.now, kind, text))

    def _place(self, items, layer, ttl):
        self.orders.append(dict(t=self.now, items=[list(i) for i in items], layer=layer))
        for ex_, side_, _, _, q_ in items:
            self.taken.setdefault((ex_, side_), []).append((self.now, float(q_)))
        self.n_key += 1
        key = f"shadow-{self.n_key}"
        self.E.confirm(key, [("shadow", tuple(items[0]), True, {"orderId": "shadow"})])
        return key

    def _dry(self, items, layer, ttl, why):
        self.dry.append(dict(t=self.now, items=[list(i) for i in items]))
        return "dry"

    def _book(self, ex):
        a = self.armed.get(ex) or {}
        return None if a.get("bid") is None or a.get("ask") is None else (a["bid"], a["ask"], 2000)

    def _levels(self, ex, side):
        """Today's SIG touch (state.json) and 8 levels 0.5c apart, 2,000 each, less what our would-be orders took in the last 120 s
        (only for 'would an order go': the real book decides on the day)."""
        a = self.armed.get(ex) or {}
        if a.get("bid") is None or a.get("ask") is None:
            return None
        top = a["ask"] if side == "yes" else round(1 - a["bid"], 3)
        lv = [(round(top + 0.005 * i, 3), 2000) for i in range(8) if top + 0.005 * i <= 0.995]
        tk = self.taken[(ex, side)] = [(t, q) for t, q in self.taken.get((ex, side), []) if self.now - t <= el.TTL_S]
        left, out = sum(q for _, q in tk), []
        for p_, q_ in lv:
            use = min(q_, left); left -= use
            if q_ - use > 0:
                out.append((p_, q_ - use))
        return out

    def _begin(self, j):
        if j["started"]:
            return
        j["started"] = True
        self.E.first[j["race"]] = j["t0"] + j["close_at"]
        for side in ("yes", "no"):
            self.E.run0.pop((j["ex"], side), None)

    def run(self):
        E = self.E
        st = [dict(j, i=0, closed=False, gone=False, started=False) for j in self.injs]
        end = max(j["t0"] + j["path"][-1][0] for j in st) + el.TTL_S + 60
        t_thr = min(j["t0"] for j in st)
        t_loop = self.start - 900 + self.phase * self.loop_s
        while min(t_thr, t_loop) <= end:
            if t_thr <= t_loop:                              # the injector thread wakes
                self.now = T = t_thr
                for j in st:
                    if j["gone"] or T < j["t0"]:
                        continue
                    self._begin(j)
                    if not j["closed"] and T >= j["t0"] + j["close_at"]:
                        E.close[j["race"]] = self.now
                        j["closed"] = True
                    P = j["path"]
                    while j["i"] < len(P) and j["t0"] + P[j["i"]][0] <= T + 1e-9:
                        _, b, a = P[j["i"]]
                        j["i"] += 1
                        self.now += 0.0002
                        E.last_quote_t = self.now             # the real feed keeps PolyFeed busy: no 30-s feed gap
                        E.on_pm(j["ex"], (b + a) / 2, self.now, self.cash, self.pos, pm_bid=b, pm_ask=a, defer=True)
                        if E.queue:
                            E.run_queue(self.now, self.cash, self.pos)
                due = [j["t0"] + j["path"][j["i"]][0] for j in st if not j["gone"] and j["i"] < len(j["path"])]
                due += [j["t0"] + j["close_at"] for j in st if not j["gone"] and not j["closed"]]
                nxt = max(T + 0.05, min([T + 1.0] + due))
                t_thr = nxt + (self.rng.uniform(0, self.jitter) if self.jitter else 0.0)
            else:                                            # the main loop
                self.now = T = t_loop
                for j in st:
                    if j["gone"] or T < j["t0"]:
                        continue
                    self._begin(j)
                    P = j["path"]
                    if j["i"] >= len(P) and T - (j["t0"] + P[-1][0]) >= el.TTL_S:
                        j["gone"] = True
                        race = j["race"]
                        if any(k["race"] == race and not k["gone"] for k in st):
                            continue
                        E.close[race], E.first[race] = j["orig"]
                        for e in [e for e in list(E.calls) if E.race(e) == race]:
                            E.calls.pop(e, None)
                        E.race_stop.discard(race); E.pulled.discard(race)
                        for kk in [kk for kk in list(E.run0) if E.race(kk[0]) == race]:
                            E.run0.pop(kk, None)
                E.on_tick(self.now, self.cash, self.pos, [], lambda o: None)
                t_loop += self.loop_s
        return self


def guard_kind(g):
    return "spread" if "spread" in g else "other book"


def signature(rep, exs, nocall=False):
    """What must not depend on the loop's phase, for one race's books: the CALL / REVERSAL rows (book, side; the rule separately)
    and, for the must-not-call cases, the NOCALL guard kinds (a NOCALL before a CALL is a passing guard flicker, not an outcome)."""
    sig = set()
    for r in rep.E.records:
        if r["ex"] not in exs:
            continue
        if r["kind"] in ("CALL", "REVERSAL"):
            sig.add((r["kind"], r["ex"], r["side"]))
        elif r["kind"] == "NOCALL" and nocall:
            for g in r.get("guards") or []:
                sig.add(("NOCALL", r["ex"], r["side"], guard_kind(g)))
    return sig


def call_times(rep, exs, start):
    """{(kind, ex, side): (t - start, rule)} of the first CALL / REVERSAL per book side."""
    out = {}
    for r in rep.E.records:
        if r["ex"] in exs and r["kind"] in ("CALL", "REVERSAL"):
            out.setdefault((r["kind"], r["ex"], r["side"]), (r["t"] - start, r.get("rule")))
    return out


# --------------------------------------------------------------------------------------------- build
def build(pc, st, start):
    armed = armed_books(pc, st)
    tpls = load_templates()
    by_race = collections.defaultdict(dict)
    for ex, a in armed.items():
        by_race[a["race"]][ex] = a
    wave_close = {w: start + m * 60 for w, m in WAVES}
    injs, plan_races = [], []
    for sc in SCEN + (extra_scen(by_race) if ALL else []):
        sc = dict(sc, pre=PRE_S)
        books = by_race.get(sc["race"])
        if not books:
            raise SystemExit(f"race {sc['race']!r} is not armed (poll_close.json x the engine's books)")
        if any(a["manual"] for a in books.values()):
            raise SystemExit(f"race {sc['race']!r} has a manual book (the detector skips it): pick another")
        waves = {a["wave"] for a in books.values()}
        if waves != {sc["wave"]}:
            raise SystemExit(f"race {sc['race']!r} closes at {waves}, not in wave {sc['wave']}")
        tpl = tpls[sc["tpl"]]
        rng = random.Random(f"rehearsal-2026-10-25:{sc['race']}")
        legs = build_race(sc, books, tpl, rng)
        close_t = wave_close[sc["wave"]]
        t0 = close_t - sc["pre"]
        tag_kind = sc["kind"] + (f" ({sc['mode']})" if sc.get("mode") else "")
        for lg in legs:
            injs.append(dict(ex=lg["ex"], race=sc["race"], party=lg["party"], role=lg["role"], wave=sc["wave"], kind=sc["kind"],
                             mode=sc.get("mode"), tpl=sc["tpl"], k=sc["k"], t0=t0, close_at=float(sc["pre"]), path=lg["path"],
                             tag=f"W{[w for w, _ in WAVES].index(sc['wave']) + 1} {sc['wave']} {sc['race']} {lg['party']} {tag_kind}"))
        plan_races.append(dict(sc, close_t=close_t, t0=t0, exs=[lg["ex"] for lg in legs]))
    for j in injs:                                           # the race's real closes (restored when its injection ends)
        races_b = by_race[j["race"]]
        j["orig"] = (max(a["last"] for a in races_b.values()), min(a["first"] for a in races_b.values()))
    return armed, injs, plan_races


def expectations(armed, injs, plan_races, start, cash, pos):
    inj_exs = {j["ex"] for j in injs}
    ideal = Replay(armed, injs, start, cash, pos).run()
    loops = [((jit, L, ph), Replay(armed, injs, start, cash, pos, jitter=jit, loop_s=L, phase=ph, seed=i).run())
             for i, (jit, L, ph) in enumerate(VARIANTS)]
    diffs = collections.defaultdict(list)
    env = {}                                                 # (kind, ex, side) -> [t_lo, t_hi, {rules}] over the ideal + variant replays
    pulls = {}                                               # race -> [t_lo, t_hi] of its POLLS line (the loop's on_tick)
    for _, rt in [(None, ideal)] + loops:
        for t, kind, text in rt.logs:
            m = re.match(r"(.+?): first poll close", text) if kind == "POLLS" else None
            if m:
                e = pulls.setdefault(m.group(1), [t, t])
                e[0], e[1] = min(e[0], t), max(e[1], t)
    for pr in plan_races:
        exs = set(pr["exs"])
        base = signature(ideal, exs, nocall=bool(pr.get("mode")))
        for (k_, rt) in [(None, ideal)] + loops:
            if k_ is not None:
                s = signature(rt, exs, nocall=bool(pr.get("mode")))
                for x in sorted(s ^ base):
                    diffs[pr["race"]].append(dict(jitter_s=k_[0], loop_s=k_[1], phase=k_[2], row=list(x), only_in="variant" if x in s else "ideal"))
            for key, (t, rule) in call_times(rt, exs, start).items():
                e = env.setdefault(key, [t, t, set()])
                e[0], e[1] = min(e[0], t), max(e[1], t)
                if rule:
                    e[2].add(rule)
    recs = [r for r in ideal.E.records if r["ex"] in inj_exs]
    j_of = {j["ex"]: j for j in injs}
    out = []
    for pr in plan_races:
        race_rows, books = [], []
        for ex in pr["exs"]:
            j = j_of[ex]
            mine = [r for r in recs if r["ex"] == ex]
            calls = [r for r in mine if r["kind"] == "CALL"]
            ev = []
            for r in mine:
                if r["kind"] not in ("CALL", "STRICT", "REVERSAL"):
                    continue
                e = dict(kind=r["kind"], ex=ex, side=r["side"], t=round(r["t"] - start, 2), t_pt=hms(r["t"]),
                         after_close_s=round(r["t"] - (j["t0"] + j["close_at"]), 1))
                ek = env.get((r["kind"], ex, r["side"]))
                if ek and r["kind"] in ("CALL", "REVERSAL"):
                    e.update(t_lo=round(ek[0], 2), t_hi=round(ek[1], 2))
                    if r["kind"] == "CALL":
                        e["rules_seen"] = sorted(ek[2])
                if r["kind"] in ("CALL", "STRICT"):
                    e.update(rule=r.get("rule"), pm_bid=r.get("pm_bid"))
                    if r["kind"] == "CALL":
                        sig_t = r.get("signal_t") or r["t"]
                        hold = el.CALL_HOLD_S if r.get("rule") == "A" else 0.0
                        e["t_first"] = round(max(sig_t, r["t"] - hold) - start, 2)   # the first qualifying quote (rule A: the run, or due - 20 s)
                ev.append(e)
            for side in ("yes", "no"):
                nc = [r for r in mine if r["kind"] == "NOCALL" and r["side"] == side]
                if nc:
                    ev.append(dict(kind="NOCALL", ex=ex, side=side, t=round(nc[0]["t"] - start, 2), t_pt=hms(nc[0]["t"]),
                                   guards=nc[0].get("guards"), n=len(nc)))
            pre = [r for r in mine if r["kind"] == "PRECALL"]
            orders = [o for o in ideal.orders if o["items"][0][0] == ex]
            a = armed[ex]
            likely = []
            for c in calls:
                side = c["side"]
                held_other = (a["pos"] < 0) if side == "yes" else (a["pos"] > 0)
                cap = round(min(el.PRICE_CAP, (c.get("pm_bid") or 0) - el.PRICE_UNDER_PM), 3)
                top = a["ask"] if side == "yes" else (round(1 - a["bid"], 3) if a.get("bid") is not None else None)
                likely.append(dict(side=side, cap=cap, sig_ask_now=top, pos_now=a["pos"],
                                   order="no: we hold the other side (C107)" if held_other else
                                   ("likely" if top is not None and top <= cap + 1e-9 else f"only if the SIG ask falls to <= {cap:.3f} (now {top})")))
            books.append(dict(ex=ex, party=j["party"], role=j["role"], n_points=len(j["path"]), path_s=j["path"][-1][0],
                              precall=dict(t=round(pre[0]["t"] - start, 2), rule=pre[0].get("rule"), guards=pre[0].get("guards")) if pre else None,
                              would_be_orders=len(orders), first_order=(dict(t=round(orders[0]["t"] - start, 2), item=orders[0]["items"][0]) if orders else None),
                              order_if_called=likely, pos_now=a["pos"], sig_now=[a.get("bid"), a.get("ask")], pm_fair_now=a.get("fair")))
            race_rows += ev
        n_call = sum(1 for e in race_rows if e["kind"] == "CALL")
        n_rev = sum(1 for e in race_rows if e["kind"] == "REVERSAL")
        n_nc = sum(1 for e in race_rows if e["kind"] == "NOCALL")
        verdict = ("REVERSAL" if n_rev else "CALL" if n_call else "NOCALL" if n_nc else "no call (uncalled in the window)")
        end_t = max(pr["t0"] + j_of[ex]["path"][-1][0] for ex in pr["exs"])
        out.append(dict(race=pr["race"], wave=pr["wave"], kind=pr["kind"], mode=pr.get("mode"), template_2024=pr["tpl"], k=pr["k"],
                        start_pt=hms(pr["t0"]), close_pt=hms(pr["close_t"]), last_quote_pt=hms(end_t), restored_pt=hms(end_t + el.TTL_S),
                        at=round(pr["t0"] - start, 1), close_at=float(pr["pre"]), expect=verdict, events=sorted(race_rows, key=lambda e: e["t"]),
                        books=books, robust=not diffs.get(pr["race"]), loop_sensitive=diffs.get(pr["race"], [])[:12],
                        pull=(dict(t_lo=round(pulls[pr["race"]][0] - start, 1), t_hi=round(pulls[pr["race"]][1] - start, 1),
                                   pt=f"{hms(pulls[pr['race']][0])}-{hms(pulls[pr['race']][1])}") if pr["race"] in pulls else None)))
    waves = []
    for w, _ in WAVES:                                       # the C14 check per wave: its races' pulls vs its first CALL
        rs = [r for r in out if r["wave"] == w]
        fc = [e.get("t_lo", e["t"]) for r in rs for e in r["events"] if e["kind"] == "CALL"]
        pl = [r["pull"]["t_hi"] for r in rs if r.get("pull")]
        waves.append(dict(wave=w, races=len(rs), first_call=round(min(fc), 1) if fc else None, first_call_pt=hms(start + min(fc)) if fc else None,
                          last_pull=max(pl) if pl else None, last_pull_pt=hms(start + max(pl)) if pl else None,
                          min_lead_s=round(min(fc) - max(pl), 1) if fc and pl else None, pulls=len(pl)))
    return out, ideal, waves


def peak_books(injs):
    ev = []
    for j in injs:
        ev.append((j["t0"], 1)); ev.append((j["t0"] + j["path"][-1][0] + el.TTL_S, -1))
    cur = peak = 0; at = None
    for t, d in sorted(ev):
        cur += d
        if cur > peak:
            peak, at = cur, t
    return peak, at


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=START_PT, help="plan start, PT (default %(default)s)")
    ap.add_argument("--local", action="store_true", help="inputs from the Mac mirror instead of the server")
    ap.add_argument("--dry-run", action="store_true", help="build and replay only; write nothing")
    ap.add_argument("--all", action="store_true", help="rehearsal 2: every armed race in all 9 waves (the 32 above + the rest, same kind mix)")
    ap.add_argument("--after", metavar="HH:MM", help="contingency (the engine restarted mid-run: its injections are in memory only): "
                    "write only the races whose injection starts at / after HH:MM PT, same start epoch, to rehearsal_plan_after_HHMM.json; "
                    "the main plan and the expected list stay as they are")
    args = ap.parse_args()
    global ALL
    ALL = args.all
    start = epoch_pt(args.start)
    pc, st, src = load_inputs(args.local)
    cash = cash_of(st)
    armed, injs, plan_races = build(pc, st, start)
    pos = {ex: a["pos"] for ex, a in armed.items()}
    exp, ideal, waves = expectations(armed, injs, plan_races, start, cash, pos)
    peak, peak_t = peak_books(injs)
    last_end = max(j["t0"] + j["path"][-1][0] + el.TTL_S for j in injs)
    plan = dict(built=time.strftime("%Y-%m-%d %H:%M:%S PT"), start_pt=args.start, start_epoch=start,
                note="research/night/rehearsal_plan.py; engine format: f_engine.el_rehearsal_load (C111). at = s after the plan start; "
                     "close_at = s after the injection's own start; path = [[s from its start, PM YES bid, PM YES ask], ...]",
                injections=[dict(ex=j["ex"], at=round(j["t0"] - start, 1), close_at=j["close_at"], tag=j["tag"], path=j["path"]) for j in injs])
    calls = [e for r in exp for e in r["events"] if e["kind"] == "CALL"]
    revs = [e for r in exp for e in r["events"] if e["kind"] == "REVERSAL"]
    must_not = [r["race"] for r in exp if r["expect"] in ("NOCALL", "no call (uncalled in the window)")]
    summary = dict(start_pt=args.start, start_epoch=start, built=plan["built"], inputs=src, cash_at_build=round(cash), races=len(exp),
                   injections=len(injs), expected_calls=len(calls), expected_reversals=len(revs), no_call_races=must_not,
                   peak_books=peak, peak_at_pt=hms(peak_t) if peak_t else None, last_restore_pt=hms(last_end),
                   loop_sensitive_races=[r["race"] for r in exp if not r["robust"]], waves=waves, pre_s=PRE_S,
                   engine_release="C112-1247 (f_engine el_inject_loop / el_inject_begin / el_inject_feed, el_taken)",
                   engine=dict(CALL_HOLD=el.CALL_HOLD, CALL_HOLD_S=el.CALL_HOLD_S, CALL_STRICT=el.CALL_STRICT, GUARD_SPREAD=el.GUARD_SPREAD,
                               GUARD_COMPLEMENT=el.GUARD_COMPLEMENT, PRICE_CAP=el.PRICE_CAP, PRICE_UNDER_PM=el.PRICE_UNDER_PM,
                               RACE_SHARES=el.RACE_SHARES, STATE_CAP=el.STATE_CAP, WAVE_MULT=el.WAVE_MULT, TTL_S=el.TTL_S,
                               REVERSAL_PM=el.REVERSAL_PM, REVERSAL_WINDOW_S=el.REVERSAL_WINDOW_S, MIN_QTY=el.MIN_QTY))
    probe = pick_probe(armed, {j["ex"] for j in injs})
    summary["probe_suggestion"] = probe
    expected = dict(summary=summary, races=exp, injections=[dict(ex=j["ex"], race=j["race"], party=j["party"], role=j["role"], wave=j["wave"],
                                                                     kind=j["kind"], mode=j["mode"], tag=j["tag"], at=round(j["t0"] - start, 1),
                                                                     close_at=j["close_at"], path_s=j["path"][-1][0], n_points=len(j["path"]))
                                                                for j in injs])
    if args.after:
        cut = epoch_pt(time.strftime("%Y-%m-%d", time.localtime(start)) + f" {args.after}:00")
        rest = [i for i in plan["injections"] if start + i["at"] >= cut]
        path = f"{REMOTE_DIR}/rehearsal_plan_after_{args.after.replace(':', '')}.json"
        print(f"contingency plan: {len(rest)} of {len(plan['injections'])} injections start at / after {args.after} -> {SERVER}:{path}")
        if not args.dry_run:
            ssh(f"cat > {path}.tmp && mv {path}.tmp {path} && chmod a+r {path}", data=json.dumps(dict(plan, injections=rest), separators=(",", ":")).encode())
            print(f"written; control election_rehearsal file={path} start={start:.0f}")
        return
    print_summary(exp, injs, summary, start)
    if args.dry_run:
        print("\n--dry-run: nothing written")
        return
    blob_plan = json.dumps(plan, separators=(",", ":")).encode()
    blob_exp = json.dumps(expected, indent=1, default=str).encode()
    with open(LOCAL_EXPECTED, "wb") as fh:
        fh.write(blob_exp)
    ssh(f"mkdir -p {REMOTE_DIR} && chmod 755 {REMOTE_DIR}")   # never /opt/research itself: the required 750 root:sig (00:25 Oct 26 PERMS FAIL)
    for path, blob in ((REMOTE_PLAN, blob_plan), (REMOTE_EXPECTED, blob_exp)):
        ssh(f"cat > {path}.tmp && mv {path}.tmp {path} && chmod a+r {path}", data=blob)
    chk = ssh(f"sudo -u sig {ENGINE}/.venv/bin/python3 -I -c 'import json; p=json.load(open(\"{REMOTE_PLAN}\")); "
              f"print(len(p[\"injections\"]), \"injections readable by sig\")' && ls -la {REMOTE_DIR} && sha256sum {REMOTE_PLAN}").decode()
    print(f"\nwritten: {LOCAL_EXPECTED}\n         {SERVER}:{REMOTE_PLAN} ({len(blob_plan):,} B), {REMOTE_EXPECTED} ({len(blob_exp):,} B)\n"
          f"read back as the engine user sig: {chk.strip()}")


def pick_probe(armed, planned):
    """The probe's book: not manual, not in the plan, we hold nothing on its other side, a YES bid we would be glad to own
    (PM fair >= SIG bid + 3c), the biggest such edge first."""
    best = []
    for ex, a in armed.items():
        if a["manual"] or ex in planned or a.get("bid") is None or a.get("fair") is None or a["pos"] < 0:
            continue
        edge = a["fair"] - a["bid"]
        if 0.5 <= a["bid"] <= 0.95 and edge >= 0.03:
            best.append((edge, ex, a))
    if not best:
        return None
    edge, ex, a = max(best, key=lambda x: x[0])
    return dict(ex=ex, side="yes", name=f"{a['race']} {a['party']}", sig_bid_now=a["bid"], pm_fair_now=round(a["fair"], 4), pos_now=a["pos"],
                qty=el.MIN_QTY, note="one 10-share YES bid at the touch, 120 s (C111 el_probe_run); trigger it before 15:07 (no injection running)")


def print_summary(exp, injs, s, start):
    print(f"Rehearsal plan, start {s['start_pt']} PT (epoch {start:.0f}); inputs {s['inputs']}; cash at build {s['cash_at_build']:,}")
    print(f"{s['races']} races, {s['injections']} injections (every book of each race), expected CALL rows {s['expected_calls']}, "
          f"REVERSAL rows {s['expected_reversals']}; peak simultaneous books {s['peak_books']} at {s['peak_at_pt']}; last restore {s['last_restore_pt']}\n")
    by_w = collections.OrderedDict((w, []) for w, _ in WAVES)
    for r in exp:
        by_w[r["wave"]].append(r)
    nb = collections.Counter(j["wave"] for j in injs)
    wv = {x["wave"]: x for x in s.get("waves") or []}
    print(f"{'wave':6} {'close':8} {'races':>5} {'books':>5} {'CALLs':>5} {'REV':>4} {'pull lead':>9}  races (expected)")
    for w, m in WAVES:
        rs = by_w[w]
        nc = sum(1 for r in rs for e in r["events"] if e["kind"] == "CALL")
        nr = sum(1 for r in rs for e in r["events"] if e["kind"] == "REVERSAL")
        ld = (wv.get(w) or {}).get("min_lead_s")
        print(f"{w:6} {hms(start + m * 60):8} {len(rs):5} {nb[w]:5} {nc:5} {nr:4} {('%.0f s' % ld) if ld is not None else 'n/a':>9}  "
              + "; ".join(f"{r['race']} [{r['kind']}: {r['expect']}]" for r in rs))
    print()
    for r in exp:
        ev = ", ".join(f"{e['kind']} {e['ex']} {e['side'].upper()}{(' ' + '/'.join(e.get('rules_seen') or [e['rule']])) if e.get('rule') else ''} {e['t_pt']}"
                       + f" (+{e['after_close_s']:.0f} s" + (f", variants +{e['t_hi'] - e['t']:.0f} s" if e.get('t_hi') is not None and e['t_hi'] - e['t'] >= 1 else "") + ")"
                       for e in r["events"] if e["kind"] in ("CALL", "REVERSAL"))
        nc = sorted({g.split(' ')[0] + ' ' + g.split(' ')[1] for e in r["events"] if e["kind"] == "NOCALL" for g in (e.get("guards") or [])})
        orders = sum(b["would_be_orders"] for b in r["books"])
        print(f"  {r['wave']} {r['race']:24} {r['kind']:9} tpl {r['template_2024']:22} x{r['k']:<2} {r['start_pt']}-{r['last_quote_pt']} close {r['close_pt']}: "
              f"{ev or 'no CALL'}{'; NOCALL ' + ', '.join(nc) if nc else ''}; would-be orders (today's SIG book) {orders}"
              + ("" if r["robust"] else f"  ** TIMING-SENSITIVE: {r['loop_sensitive'][:2]}"))
    if s.get("probe_suggestion"):
        p = s["probe_suggestion"]
        print(f"\nprobe suggestion: ex {p['ex']} {p['name']} YES, SIG bid {p['sig_bid_now']}, PM fair {p['pm_fair_now']}, pos {p['pos_now']:+.0f}")


if __name__ == "__main__":
    main()
