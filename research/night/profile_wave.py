"""Night CPU budget (docs/night_oct27_spec.md item 3): cProfile of the live engine loop through the 17:00 wave of the night sim.

What runs: the release that ran the Oct 26 rehearsal (releases/C117-0037, copied to --out/code: the tree's f_engine.py / n_election.py are
being edited) on a copy of the data/ mirror (--out/run/data; data/ itself is only read), with
  - SIG: tests/fake_sig.FakeSIG seeded from data/state.json (positions, avg costs, the 247 resting orders under their real ids), its
    prices / order books / takes from tests/night_world.World's lagging SIG book (normal night, seed 7), cash set to 67k (60k usable);
  - Polymarket: REST fairs from the World's PM quote of the minute; the websocket is the real i_realtime.PolyFeed fed the World's
    quotes (PolyFeed._apply -> Engine.el_quote -> n_election.on_pm, pm_move -> rt_event), the SIG markets feed a stand-in returning
    the World's book;
  - a virtual clock: time.time / localtime / gmtime / strftime / ctime and f_engine.datetime.now shifted onto Nov 3 PT (it runs at the
    real rate inside a step, so every timeout and duration the engine measures is real; the idle waits between loops are skipped);
  - f_engine.main's cadence: step(); wake on an rt event or after max(1, 10 - took) s; fast_step(); loop_gap(); the feed-thread work and
    the call lane (el_lane_once, as el_lane_loop does on el_lane_ev) run inline between loops, the executor's lanes are drained inline;
    the ThreadPoolExecutor of step()'s reads is synchronous, so their CPU lands in the loop's profile.
  - the election in "live" mode (the night), every other control as in data/overrides.json.
Profiled windows (separate cProfile.Profile per window and per thread role: loop / feed / lane / executor): --rest (default 14:33-14:48
PT, before the first pulls) and --wave (default 16:30-17:30 PT). Between them the loop runs every --ff-loop s (default 10, no wake replay).

Limits: FakeSIG / no HTTP: no JSON decoding of SIG / PM responses, no network or I/O wait; CPU only, on this Mac (not the droplet).

    python3 -I research/night/profile_wave.py --out <scratch>/profile [--until 17:35] [--smoke 5]
"""
import argparse
import collections
import cProfile
import datetime as _dt
import io
import json
import os
import pstats
import random
import shutil
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REL = os.path.join(ROOT, "releases", "C117-0037")
DATA_SRC = os.path.join(ROOT, "data")
BIG_DIRS = ("intel/trades", "intel/books", "intel/pm_30s", "intel/orders")   # daily jsonl: only the newest 3 files are copied
SKIP = ("engine.log",)
sys.dont_write_bytecode = True                             # nothing written into the repo (tests/__pycache__)


def _no_net(*a, **k):
    raise OSError("profile_wave: offline (no network)")


import socket                                              # noqa: E402  offline: no connection of any kind (SIG, PM, DO metadata)
socket.create_connection = socket.getaddrinfo = _no_net
socket.socket.connect = socket.socket.connect_ex = _no_net

# ------------------------------------------------------------------------------------------------------------- virtual clock
_RT, _RLT, _RGM, _RSF, _RCT = time.time, time.localtime, time.gmtime, time.strftime, time.ctime


class VC:
    off = 0.0


def vtime():
    return _RT() + VC.off


def set_clock(t):
    """Move the virtual clock forward to t (never back)."""
    VC.off = max(VC.off, t - _RT())


def install_clock(t0):
    VC.off = t0 - _RT()
    time.time = vtime
    time.localtime = lambda s=None: _RLT(vtime() if s is None else s)
    time.gmtime = lambda s=None: _RGM(vtime() if s is None else s)
    time.strftime = lambda fmt, tt=None: _RSF(fmt, _RLT(vtime()) if tt is None else tt)
    time.ctime = lambda s=None: _RCT(vtime() if s is None else s)


class VDatetime(_dt.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromtimestamp(vtime(), tz)


class SyncPool:
    """concurrent.futures.ThreadPoolExecutor stand-in: runs each job at submit, in this thread (so the profile sees it)."""

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def submit(self, fn, *a, **k):
        import concurrent.futures as cf
        f = cf.Future()
        try:
            f.set_result(fn(*a, **k))
        except BaseException as e:                         # noqa: BLE001 (re-raised by .result(), as the pool would)
            f.set_exception(e)
        return f


# ------------------------------------------------------------------------------------------------------------- setup
def copy_code(out):
    dst = os.path.join(out, "code")
    if os.path.exists(dst):
        shutil.rmtree(dst)
    os.makedirs(dst)
    for f in os.listdir(REL):
        if f.endswith(".py"):
            shutil.copy2(os.path.join(REL, f), dst)
    if os.path.isdir(os.path.join(REL, "assets")):
        shutil.copytree(os.path.join(REL, "assets"), os.path.join(dst, "assets"))
    return dst


def copy_data(out, mode="live"):
    """data/ -> out/data0 once (read only on data/), then a fresh out/run/data per run."""
    d0 = os.path.join(out, "data0")
    if not os.path.exists(os.path.join(d0, ".done")):
        if os.path.exists(d0):
            shutil.rmtree(d0)
        big = {os.path.join(DATA_SRC, b) for b in BIG_DIRS}

        def ign(dirpath, names):
            if dirpath in big:
                keep = set(sorted(n for n in names if n.endswith(".jsonl"))[-3:])
                return [n for n in names if n not in keep]
            return [n for n in names if n in SKIP or n.endswith(".pid") or n == "engine.lock"]
        shutil.copytree(DATA_SRC, d0, ignore=ign, symlinks=False)
        open(os.path.join(d0, ".done"), "w").write(time.strftime("%Y-%m-%d %H:%M:%S"))
    run = os.path.join(out, "run")
    if os.path.exists(run):
        shutil.rmtree(run)
    shutil.copytree(d0, os.path.join(run, "data"))
    os.makedirs(os.path.join(run, "docs"), exist_ok=True)
    ov_p = os.path.join(run, "data", "overrides.json")
    ov = json.load(open(ov_p))
    ov["election"] = dict(ov.get("election") or {}, mode=mode)      # live = the night
    json.dump(ov, open(ov_p, "w"))
    return os.path.join(run, "data")


def pt(day, hm):
    return time.mktime(time.strptime(f"{day} {hm}", "%Y-%m-%d %H:%M"))


class ShiftedWorld:
    """night_world.World on another day: every time argument shifted by off (engine time + off = World time)."""

    def __init__(self, W, off):
        self.W, self.off = W, off

    def __getattr__(self, k):
        return getattr(self.W, k)

    def sig_touch(self, ex, t):
        return self.W.sig_touch(ex, t + self.off)

    def book_levels(self, ex, t):
        return self.W.book_levels(ex, t + self.off)

    def side_levels(self, ex, side, t):
        return self.W.side_levels(ex, side, t + self.off)

    def take(self, ex, side, limit, qty, t):
        return self.W.take(ex, side, limit, qty, t + self.off)


def install_cache(d_ledger):
    """--cache what-if: the same answers, computed once. strptime / mktime of a repeated string are pure (TZ fixed for the run)."""
    import functools
    time.strptime = functools.lru_cache(maxsize=500_000)(time.strptime)
    time.mktime = functools.lru_cache(maxsize=500_000)(time.mktime)
    real = d_ledger.Ledger.tranche_avg

    def tranche_avg(self, sub, layer="ladder", ex=None, side=None, since=None):
        sig = (len(self.fills), self.fills[-1].get("key") if self.fills else None, len(self.orders))
        c = getattr(self, "_ta_cache", None)
        if c is None or c[0] != sig:
            c = self._ta_cache = (sig, {})
        k = (sub, layer, ex, side, since)
        if k not in c[1]:
            c[1][k] = real(self, sub, layer, ex, side, since)
        return c[1][k]
    d_ledger.Ledger.tranche_avg = tranche_avg


# ------------------------------------------------------------------------------------------------------------- the fakes
def build_fakes(FakeSIG, W, state, markets, cash0):
    class NightFake(FakeSIG):
        """FakeSIG on the World's book: prices / orderbook from it, a buy that lands walks its asks (taker), cash tracked."""

        def __init__(self):
            pos = {str(b["ex"]): float(b["pos"]) for b in state["bots"] if b.get("pos")}
            super().__init__(markets, {}, pos, seed=7, writes_per_min=25)
            self.processing = (0.01, 0.05)
            self.cash = float(cash0)
            self.static = {str(b["ex"]): (b.get("bid"), b.get("ask"), None) for b in state["bots"]}
            self.avg_cost = {str(b["ex"]): float(b["avg"]) for b in state["bots"] if b.get("avg")}
            self.next_id = 90_000_000
            for o in state.get("open_orders") or []:          # the engine's resting orders, under their real ids
                self.book[int(o["id"])] = dict(ex=str(o["ex"]), side=o["side"], action=o["action"], price=float(o["price"]),
                                               qty=float(o["qty"]), expires=None)
            self.fill_id = 800_000_000

        def touch(self, ex, now=None):
            ex = str(ex)
            if ex in W.legs:
                b, a = W.sig_touch(ex, now or self.now())
                return (b, a, round((b + a) / 2, 3))
            return self.static.get(ex, (None, None, None))

        def prices(self, exs):
            self._count("prices")
            now = self.now()
            return {str(e): self.touch(e, now) for e in exs}

        def orderbook(self, ex, depth=10):
            self.book_reads = getattr(self, "book_reads", 0) + 1
            ex = str(ex)
            if ex in W.legs:
                bids, asks = W.book_levels(ex, self.now())
                return [(p, float(q)) for p, q in bids[:depth]], [(p, float(q)) for p, q in asks[:depth]]
            b, a, _ = self.touch(ex)
            return ([(b, 1000.0)] if b is not None else [], [(a, 1000.0)] if a is not None else [])

        def holdings(self):
            h = 0.0
            for ex, q in self.pos.items():
                b, a, _ = self.touch(ex)
                if b is None or a is None or not q:
                    continue
                m = (b + a) / 2
                h += abs(q) * (m if q > 0 else 1 - m)
            return h

        def pnl(self):
            self._count("pnl")
            h = self.holdings()
            return dict(period="all", totalAccountValue=round(self.cash + h, 2), totalHoldingsValue=round(h, 2), unrealizedPnl=0.0, roi=0.0)

        def _land(self, o, ttl_s):
            res = super()._land(o, ttl_s)
            oid = res["orderId"]
            b = self.book.get(oid)
            if b and b["action"] == "buy" and b["ex"] in W.legs:
                for p, q in W.take(b["ex"], b["side"], b["price"], b["qty"], self.now()):
                    self.fill(oid, q, price=p)
            return res

        def fill(self, oid, qty, price=None):
            with self.lock:
                o = self.book.get(int(oid))
                if not o:
                    return 0
                before = self.pos.get(o["ex"], 0)
                sign = (1 if o["side"] == "yes" else -1) * (1 if o["action"] == "buy" else -1)
                q = min(qty, o["qty"])
                p = price if price is not None else o["price"]
                netted = min(q, max(0.0, -sign * before))
                self.cash += (-q * p + netted) if o["action"] == "buy" else q * p
                self.fill_id += 1
                r = super().fill(oid, q, price=p)
                self.fill_log[-1]["id"] = self.fill_id      # ids clear of the ledger's real SIG fill ids
                return r

        def resting_fills(self, now, rng):
            """Our resting buys on World books: SIG's asks falling to the price fill them there; a bid at the top of its side is hit
            by a taker 5% of the loops (100-1,000 shares), as tests/night_rig.Rig.resting_fills."""
            for oid, o in list(self.book.items()):
                if o["action"] != "buy" or o["ex"] not in W.legs:
                    continue
                for _, q in W.take(o["ex"], o["side"], o["price"], o["qty"], now):
                    if oid in self.book:
                        self.fill(oid, q, price=o["price"])
                if oid not in self.book:
                    continue
                b, a = W.sig_touch(o["ex"], now)
                best = b if o["side"] == "yes" else round(1 - a, 3)
                if o["price"] >= best - 1e-9 and rng.random() < 0.05:
                    self.fill(oid, rng.randint(100, 1000), price=o["price"])

    class FakeMarketFeed:
        """i_realtime.SigMarketFeed stand-in: book(ex, max_age) = the World's 5 levels (cached per 5 s), stats for feed_report."""

        def __init__(self, sig):
            self.sig, self.cache = sig, {}
            self.stats = dict(messages=0, batches=0, markets=0, books=0, trades=0, gaps=0, duplicates=0, reconnects=0, errors=0, sweeps=0)
            self.on_book = None

        def book(self, ex, max_age_s=10.0):
            ex = str(ex)
            now = vtime()
            k = (ex, int(now // 5))
            got = self.cache.get(k)
            if got is None:
                if ex in W.legs:
                    bids, asks = W.book_levels(ex, now)
                    got = ([(p, float(q)) for p, q in bids], [(p, float(q)) for p, q in asks])
                else:
                    b, a, _ = self.sig.touch(ex)
                    got = ([(b, 2000.0)] if b is not None else [], [(a, 2000.0)] if a is not None else [])
                if len(self.cache) > 20_000:
                    self.cache.clear()
                self.cache[k] = got
            return list(got[0]), list(got[1])

        def start(self):
            pass

    return NightFake, FakeMarketFeed


# ------------------------------------------------------------------------------------------------------------- the report
SUB = [   # step()'s / fast_step()'s direct callees (inclusive time when called from there) -> subsystem
    ("dashboard + state.json (write_state)", ("write_state",)),
    ("ledger.save (5 MB json.dumps each loop)", ("d_ledger:save",)),
    ("reads (FakeSIG / PM stand-ins; HTTP + JSON decode in production)", ("submit", "take_rt", "reload_if_changed", "health")),
    ("ledger sync / fills / audits", ("sync", "order_truth", "audit_subbooks", "init_subbooks", "markouts", "foreign_fill_actions", "manual_book",
                                      "az06_audit", "archive_append", "_fill_events", "rule0_fill_check", "apply_subbook_fix", "backfill_fills",
                                      "audit_skip")),
    ("election: el_step (pulls, snapshot, queue, drift, call exits, recycle)", ("el_step",)),
    ("executor update / resolve", ("jx:update", "resolve_unknown", "pending", "admit@jx", "stats")),
    ("release_plan (sale loop)", ("release_plan",)),
    ("rotation_plan (rotation sales)", ("rotation_plan",)),
    ("FMM planner (fmm_plan / fmm_gate)", ("fmm_plan", "fmm_gate")),
    ("carry planner", ("carry_plan",)),
    ("rotation bids (rot_plan)", ("rot_plan",)),
    ("catcher (dump_plan)", ("dump_plan",)),
    ("other planners (longshot / wall / sleeve / swing / fm / tm / sm / scalp / fat / touch / gapmm)",
     ("longshot_plan", "wall_plan", "sleeve_plan", "swing_plan", "fm_plan", "tm_plan", "sm_plan", "scalp_plan", "fat_plan", "touch_plan",
      "gapmm_paper_on", "gapmm_paper_step", "gapmm_gate", "c98_migration", "run_sell_now", "buffer_calc")),
    ("track_* exits + lifecycle", ("track_",)),
    ("controller / bucket_usage / cash context", ("controller", "bucket_usage", "entry_budget", "kappa", "cash_short_check", "update_mids",
                                                   "update_tmm", "manual_reserve_left", "el_reserve", "rot_claim", "fmm_priority", "risk",
                                                   "pick_focus", "fast_reconcile", "feed_report", "take_snapshot", "cap")),
    ("per-bot planning loop (bot_snapshot, bots.plan, rule0, reconcile, admit, place)",
     ("bot_snapshot", "plan", "rule0", "gate", "reconcile", "repeg_grace", "no_self_cross", "fit_reducing", "admit", "sell_undercut_update",
      "market_layers", "fat_tiers", "urgency", "trim_sells", "place", "cancel", "drop_queued", "is_frozen", "wd_after", "entries_allowed",
      "<lambda>@bots", "event", "sample", "capacity", "<genexpr>", "<listcomp>", "<setcomp>", "<dictcomp>", "sorted", "sum", "any", "max", "min")),
    ("pairs (run_pairs / repair_baskets)", ("run_pairs", "repair_baskets")),
    ("side job: leaderboard logger", ("log_leaders", "log_leaders_day", "leaders_marks_check")),
    ("side jobs: intel / hourly / new markets", ("intel_step", "intel_daily", "hourly", "scan_new_markets")),
    ("fast_step body (election run_queue, fast fills)", ("run_queue", "fills@fake", "fast_pull")),
]


def _fname(k):
    f, line, name = k
    return f"{os.path.basename(f).replace('.py', '')}:{line}({name})"


def _classify(k):
    f, line, name = k
    mod = os.path.basename(f).replace(".py", "")
    for g, keys in SUB:
        for key in keys:
            if ":" in key:
                m, n = key.split(":")
                if mod.startswith(m) and name == n:
                    return g
            elif "@" in key:
                n, m = key.split("@")
                if name == n and ((m == "jx" and mod == "j_executor") or (m == "bots" and mod == "e_bots") or (m == "fake" and mod in ("fake_sig", "profile_wave"))):
                    return g
            elif key.endswith("_") and name.startswith(key):
                return g
            elif key == "_fill_events" and name.endswith("_fill_events"):
                return g
            elif name == key:
                return g
    return None


def report(d, top=12):
    import pstats as ps_
    out = []
    for w in ("rest", "wave"):
        pl = os.path.join(d, f"{w}_loop.prof")
        if not os.path.exists(pl):
            continue
        st = ps_.Stats(pl)
        S = st.stats
        summ = json.load(open(os.path.join(d, "summary.json")))["summary"][w]
        dur = summ["window_s"]
        roots = [k for k in S if k[2] in ("step", "fast_step") and k[0].endswith("f_engine.py")]
        tot = sum(S[k][3] for k in roots)
        out.append(f"\n=== {w}: {summ['loops']} loops over {dur / 60:.0f} min; loop CPU (cProfile, wall) {tot:.1f} s = {100 * tot / dur:.0f}% of a core "
                   f"(process_time: loop {summ['cpu'].get('loop', 0):.1f} s, feed {summ['cpu'].get('feed', 0):.1f} s, lane {summ['cpu'].get('lane', 0):.2f} s, "
                   f"executor {summ['cpu'].get('exec', 0):.2f} s); step took p50 {summ['took_p50']} s / p95 {summ['took_p95']} s / max {summ['took_max']} s, "
                   f"period mean {summ['period_mean']} s")
        groups, unk = collections.Counter(), collections.Counter()
        for k, (cc, nc, tt, ct, callers) in S.items():
            for r in roots:
                if r in callers:
                    c = callers[r]
                    g = _classify(k) or "other (unmapped)"
                    groups[g] += c[3]
                    if g == "other (unmapped)":
                        unk[_fname(k)] += c[3]
        for r in roots:
            groups["step() own lines"] += S[r][2]
        out.append(f"{'subsystem':<92} {'cum s':>8} {'share':>6}")
        for g, v in groups.most_common():
            out.append(f"{g:<92} {v:8.1f} {100 * v / tot:5.1f}%")
        if unk:
            out.append("  unmapped: " + ", ".join(f"{k} {v:.1f}" for k, v in unk.most_common(8)))
        out.append(f"\ntop functions by cumulative time ({w}; share = cum / loop CPU)")
        out.append(f"{'function':<60} {'cum s':>8} {'self s':>8} {'calls':>10} {'share':>6}")
        rows = sorted(((k, v) for k, v in S.items() if k not in roots), key=lambda kv: -kv[1][3])
        for k, (cc, nc, tt, ct, _) in rows[:top * 3]:
            out.append(f"{_fname(k)[:60]:<60} {ct:8.1f} {tt:8.1f} {nc:10,} {100 * ct / tot:5.1f}%")
        out.append(f"\ntop functions by self time ({w})")
        for k, (cc, nc, tt, ct, _) in sorted(S.items(), key=lambda kv: -kv[1][2])[:top + 8]:
            out.append(f"{_fname(k)[:60]:<60} {ct:8.1f} {tt:8.1f} {nc:10,} {100 * tt / tot:5.1f}%")
        for r in ("feed", "lane", "exec"):
            pf = os.path.join(d, f"{w}_{r}.prof")
            if os.path.exists(pf):
                s2 = ps_.Stats(pf).stats
                t2 = max((v[3] for v in s2.values()), default=0)
                best = sorted(s2.items(), key=lambda kv: -kv[1][3])[:6]
                out.append(f"\n{w} {r}: {t2:.2f} s total; " + "; ".join(f"{_fname(k)} {v[3]:.2f}" for k, v in best))
    txt = "\n".join(out)
    open(os.path.join(d, "report.txt"), "w").write(txt)
    print(txt)


# ------------------------------------------------------------------------------------------------------------- the run
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--start", default="14:30")
    ap.add_argument("--rest", default="14:33-14:48")
    ap.add_argument("--wave", default="16:30-17:30")
    ap.add_argument("--until", default="17:31")
    ap.add_argument("--ff-loop", type=float, default=10.0, help="loop period outside the profiled windows (s)")
    ap.add_argument("--smoke", type=int, default=0, help="run N loops from --start, profiled, then stop")
    ap.add_argument("--cash", type=float, default=67_000.0)
    ap.add_argument("--trim", action="store_true", help="Piero's night list: FMM / carry / rotation / catcher planners suspended, "
                    "dashboard + state every 5 min, leaderboard loggers every 30 min")
    ap.add_argument("--cache", action="store_true", help="harness-only what-if: time.strptime / time.mktime memoized and "
                    "Ledger.tranche_avg cached until the ledger's fills / orders change (no engine file is edited)")
    ap.add_argument("--day", default="2026-11-03", help="the engine's date (the World's Nov 3 timeline is shifted onto it)")
    ap.add_argument("--mode", default="live", help="election mode written into the run's overrides.json (live = the night)")
    ap.add_argument("--noprof", action="store_true", help="no cProfile (process_time per role only): the profiler's overhead")
    ap.add_argument("--report", action="store_true", help="only print the subsystem / top-function tables of --out's .prof files")
    a = ap.parse_args()
    if a.report:
        return report(os.path.abspath(a.out))
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    code = copy_code(out)
    data = copy_data(out, a.mode)
    os.environ["SIG_DATA"] = data
    DAY = a.day
    t_start = pt(DAY, a.start)
    WOFF = pt("2026-11-03", a.start) - t_start             # engine time + WOFF = World time
    win = {}
    for name, spec in (("rest", a.rest), ("wave", a.wave)):
        if spec and spec != "-":
            s, e = spec.split("-")
            win[name] = (pt(DAY, s), pt(DAY, e))
    t_until = pt(DAY, a.until)
    if a.smoke:
        win = {"smoke": (t_start, t_start + 1e9)}

    sys.path.insert(0, code)
    sys.path.insert(1, os.path.join(ROOT, "tests"))
    import night_world as nw                               # World first: it builds on the real clock's mktime only
    t0w = _RT()
    W0 = nw.World(seed=a.seed)
    W = ShiftedWorld(W0, WOFF) if WOFF else W0
    msgs = [(m[0] - WOFF,) + tuple(m[1:]) for m in W0.messages() if t_start - 120 <= m[0] - WOFF <= t_until]
    print(f"world: {len(W.legs)} books, {len(msgs):,} PM quotes {a.start}-{a.until} ({_RT() - t0w:.1f} s)", flush=True)
    install_clock(t_start)

    import a_config                                        # noqa: E402  (DATA from SIG_DATA)
    assert os.path.realpath(a_config.DATA) == os.path.realpath(data), a_config.DATA
    from fake_sig import FakeSIG                           # noqa: E402
    state = json.load(open(os.path.join(DATA_SRC, "state.json")))
    U = json.load(open(os.path.join(ROOT, "tests", "fixtures", "universe.json")))
    NightFake, FakeMarketFeed = build_fakes(FakeSIG, W, state, U["markets"], a.cash)
    fake = NightFake()
    sys.modules["b_sig"] = fake
    import c_polymarket as pm                              # noqa: E402  (the real module; only the REST fairs are replaced)
    fair_static = {str(b["ex"]): b.get("fair") for b in state["bots"]}

    def fairs(mp):
        now, outd = vtime(), {}
        m = min(max(int((now + WOFF - nw.T_START) // 60), 0), W.minutes - 1)
        for x in mp:
            ex = str(x["ex"])
            if ex in W.legs:
                b, a_ = W.legs[ex].quote[m]
                outd[ex] = (b, a_, 50_000.0)
            elif fair_static.get(ex) is not None:
                f = fair_static[ex]
                outd[ex] = (round(f - 0.005, 4), round(f + 0.005, 4), 50_000.0)
        return outd
    pm.fairs = fairs
    pm._get = _no_net
    pm.map_one = lambda sm, events=None: None
    import i_realtime as rt                                # noqa: E402
    if a.cache:
        import d_ledger
        install_cache(d_ledger)
        print("CACHE: time.strptime / time.mktime memoized; Ledger.tranche_avg cached per ledger change", flush=True)
    import f_engine                                        # noqa: E402
    f_engine.api = fake
    f_engine.ThreadPoolExecutor = SyncPool
    f_engine.datetime = VDatetime
    print(f"engine code {code} (release {open(os.path.join(REL, 'RELEASE')).read().strip()}), data {data}", flush=True)

    t0 = _RT()
    so = sys.stdout
    log = open(os.path.join(out, "run", "engine_stdout.log"), "w")
    sys.stdout = log
    try:
        eng = f_engine.Engine(None)
    finally:
        sys.stdout = so
    print(f"engine built in {_RT() - t0:.1f} s: {len(eng.bots)} bots, {len(eng.pairs)} pair bots", flush=True)
    if a.trim:                                             # docs/night_oct27_spec.md item 3, Nov 3 14:30 -> Nov 4 09:00
        f_engine.DASH_EVERY_S = f_engine.STATE_EVERY_S = 300
        f_engine.LEADERS_EVERY_S = f_engine.LEADERS_DAY_EVERY_S = 1800
        for nm in ("fmm_plan", "carry_plan", "rot_plan", "rotation_plan", "dump_plan"):
            setattr(eng, nm, lambda *a_, **k_: {})
        eng.fmm_gate = lambda *a_, **k_: None
        print("TRIM: fmm_plan / fmm_gate / carry_plan / rot_plan / rotation_plan / dump_plan suspended; dashboard + state every 300 s; "
              "leaders loggers every 1800 s", flush=True)
    feed = rt.PolyFeed({ex: [f"tok{ex}"] for ex in W.legs}, eng.rt_event, clock=time.time, log=lambda k, s: eng.event(k, s))
    feed.on_quote = eng.el_quote
    eng.pm_feed = feed
    eng.el_lane_ev = threading.Event()
    eng.mkt_feed = FakeMarketFeed(fake)
    rng = random.Random(f"{a.seed}:prof")

    # profilers: per window x role
    profs = {(w, r): cProfile.Profile() for w in win for r in ("loop", "feed", "lane", "exec")}
    stats = {w: collections.Counter() for w in win}
    loops = {w: [] for w in win}

    def window(t):
        for w, (s, e) in win.items():
            if s <= t < e:
                return w
        return None

    def run(role, fn, *args):
        w = window(vtime())
        if w is None:
            return fn(*args)
        p = profs[(w, role)]
        c0 = time.process_time()
        if not a.noprof:
            p.enable()
        try:
            return fn(*args)
        finally:
            if not a.noprof:
                p.disable()
            stats[w][role] += time.process_time() - c0

    def drain():
        n = 0
        for _ in range(400):
            k = 0
            if eng.ex.cancels:
                k += eng.ex.step_cancels()
            if eng.ex.queue:
                k += eng.ex.step_placements()
            n += k
            if not k:
                break
        return n

    mi = [0]

    def feed_until(target, stop_on_wake):
        """The feed thread + the call lane until `target` (or the first wake when stop_on_wake). -> the time it stopped."""
        while mi[0] < len(msgs) and msgs[mi[0]][0] <= target:
            t, ex, b, a_ = msgs[mi[0]]
            mi[0] += 1
            set_clock(t)
            feed.last_msg_t = vtime()
            run("feed", feed._apply, f"tok{ex}", b, a_)
            if eng.el_lane_ev.is_set():
                eng.el_lane_ev.clear()
                run("lane", eng.el_lane_once)
                run("exec", drain)
            if stop_on_wake and eng.wake.is_set():
                return vtime()
        set_clock(target)
        return vtime()

    # the subscription snapshot: every book's current quote
    for ex, L in W.legs.items():
        m = max(0, int((vtime() + WOFF - nw.T_START) // 60))
        b, a_ = L.quote[m]
        feed._apply(f"tok{ex}", b, a_)
    while mi[0] < len(msgs) and msgs[mi[0]][0] < vtime():
        mi[0] += 1

    n, sum_t = 0, collections.Counter()
    sys.stdout = log
    try:
        while vtime() < t_until:
            t = vtime()
            w = window(t)
            c0, r0 = time.process_time(), _RT()
            try:
                run("loop", eng.step)
            except Exception as e:                         # as main(): logged, the loop goes on
                eng.event("ERR", repr(e))
            run("exec", drain)
            took = vtime() - t
            cpu_step = time.process_time() - c0
            if w is not None or a.smoke:
                period = max(1.0, min(f_engine.LOOP_S, f_engine.FILL_POLL_S) - took)
                woke = feed_until(vtime() + period, stop_on_wake=True)
                eng.wake.clear()
                c1 = time.process_time()
                try:
                    run("loop", eng.fast_step, vtime())
                except Exception as e:
                    eng.event("ERR", f"fast path: {e!r}")
                run("exec", drain)
                cpu_fast = time.process_time() - c1
                with eng.rt_lock:
                    ours = any(k != "pm_move" for _, k, _ in eng.rt_pending)
                gap = f_engine.loop_gap(fake.health(60)["reads_last_min"], ours, vtime())
                feed_until(max(vtime(), t + gap), stop_on_wake=False)
                if w is not None:
                    loops[w].append(dict(t=t, took=round(took, 3), cpu_step=round(cpu_step, 3), cpu_fast=round(cpu_fast, 4),
                                         period=round(vtime() - t, 2), woke=round(woke - t, 2)))
            else:
                try:
                    eng.fast_step(vtime())
                except Exception:
                    pass
                drain()
                feed_until(t + a.ff_loop, stop_on_wake=False)
            fake.resting_fills(vtime(), rng)
            n += 1
            if n % 30 == 0 or (a.smoke and n >= a.smoke):
                print(f"{time.strftime('%H:%M:%S')} loop {n} window {w} took {took:.2f}s cpu {cpu_step:.2f}s (wall {_RT() - r0:.1f})", file=so, flush=True)
            if a.smoke and n >= a.smoke:
                break
    finally:
        sys.stdout = so

    # ------------------------------------------------------------------------------------------------- report files
    summ = {}
    for w in win:
        ls = loops[w]
        dur = (win[w][1] - win[w][0]) if not a.smoke else sum(x["period"] for x in ls)
        for r in ("loop", "feed", "lane", "exec"):
            p = profs[(w, r)]
            p.create_stats()
            if not p.stats:
                continue
            p.dump_stats(os.path.join(out, f"{w}_{r}.prof"))
            s = io.StringIO()
            ps = pstats.Stats(p, stream=s)
            ps.sort_stats("cumulative").print_stats(80)
            ps.sort_stats("tottime").print_stats(60)
            open(os.path.join(out, f"{w}_{r}.txt"), "w").write(s.getvalue())
        tk = sorted(x["took"] for x in ls)
        summ[w] = dict(loops=len(ls), window_s=dur, cpu=dict(stats[w]), cpu_share={k: round(v / dur, 3) for k, v in stats[w].items()} if dur else {},
                       took_p50=tk[len(tk) // 2] if tk else None, took_p95=tk[int(len(tk) * 0.95)] if tk else None,
                       took_max=tk[-1] if tk else None, period_mean=round(sum(x["period"] for x in ls) / len(ls), 2) if ls else None)
    json.dump(dict(summary=summ, loops=loops, calls=len(getattr(eng.election, "calls", {}) or {}),
                   writes=len(fake.timeline), fills=len(fake.fill_log)), open(os.path.join(out, "summary.json"), "w"), indent=1)
    print(json.dumps(summ, indent=1), flush=True)
    print(f"calls {len(getattr(eng.election, 'calls', {}) or {})}, SIG writes {len(fake.timeline)}, fills {len(fake.fill_log)}", flush=True)
    os._exit(0)


if __name__ == "__main__":
    main()
