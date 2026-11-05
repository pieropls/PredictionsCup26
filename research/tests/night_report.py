"""Election-night rehearsal, the scenarios and the numbers (docs/night_sim.md). Run: python3 -m tests.sim_election [--md out.md]
[--only name,name] [--seed N]. The perfect-information bound replays the same quote stream through a copy of n_election's call rule,
takes only the TRUE side's first call per book, and buys it at that instant on the (untouched) SIG book at <= PRICE_CAP for all the depth,
with the same per-race / per-state caps and cash floor, cash compounding in call order (no recycling)."""
import math
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # the repo root (tests are in research/tests)
sys.path[:0] = [os.path.join(ROOT, d) for d in ("engine", "dashboard", "research/tests")]
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "research", "tests"))
import night_rig as rig                                   # noqa: E402
import night_world as nw                                  # noqa: E402
import n_election as el                                   # noqa: E402

REF_OLD = 12_100                                          # tests/sim_election.py normal night (v1 rules, 100k, Oct 17 re-run)
C = lambda name, d: getattr(el, name, d)                  # noqa: E731


def rule_fires(W, msgs):
    """A copy of n_election v2's call rule on the quote stream: {(ex, side): (t, side bid, strict)} for the first fire per book side
    (after the race's last close, PM spread <= 3c, the race's other books' mid <= 0.04 for a YES call)."""
    hold, hold_s, strict_p = C("CALL_HOLD", 0.95), C("CALL_HOLD_S", 20.0), C("CALL_STRICT", 0.98)
    g_sp, g_cp = C("GUARD_SPREAD", 0.03), C("GUARD_COMPLEMENT", 0.04)
    last_q, run, fired = {}, {}, {}
    others = {ex: [e for e in W.by_race[W.meta[ex]["race"]] if e != ex] for ex in W.legs}
    for t, ex, b, a in msgs:
        last_q[ex] = (b, a)
        L26 = W.last[W.meta[ex]["race"]]
        for side in ("yes", "no"):
            sb = b if side == "yes" else 1 - a
            key = (ex, side)
            if sb < hold - 1e-9:
                run.pop(key, None)
                continue
            rs = run.setdefault(key, t)
            if key in fired or t < L26:
                continue
            strict = sb >= strict_p - 1e-9
            if not (strict or t - rs >= hold_s - 1e-9):
                continue
            if a - b > g_sp + 1e-9:
                continue
            if side == "yes":
                hi = max([(last_q[e][0] + last_q[e][1]) / 2 for e in others[ex] if e in last_q], default=0.0)
                if hi > g_cp + 1e-9:
                    continue
            fired[key] = (t, sb, strict)
    return fired


def correct(W, ex, side):
    return (side == "yes") == (W.payout(ex) == 1.0)


def bound(W, starts, cash0, cash_limit=True, under_pm=None, every=300.0):
    """Hindsight allocation: every lot the book offers on the TRUE side of each book from its start time, sampled every 5 min until
    settlement (the first sample: the whole depth; later ones: only what the book refills in between, 1 - e^(-5/10)), at <= PRICE_CAP (and <= the PM side bid - under_pm if given); the cheapest lots first
    under 15k shares a race, 20k at cost a state and the cash (start - 7k floor, no recycling). starts: {(ex, side): t}.
    -> (P&L, {race: cost})"""
    cap_p, race_sh, state_cap, floor = C("PRICE_CAP", 0.97), C("RACE_SHARES", 15_000), C("STATE_CAP", 20_000.0), C("CASH_FLOOR", 7_000)
    saved, W.consumed = W.consumed, {}
    lots = []
    for (ex, side), t0 in starts.items():
        if not correct(W, ex, side):
            continue
        t, first = t0, True
        while t <= nw.T_END:
            lim = cap_p
            if under_pm is not None:                       # the rule's bound: only while PM still shows the call
                b, a = W.legs[ex].quote[min(W.minutes - 1, int((t - nw.T_START) // nw.MIN))]
                sb = b if side == "yes" else 1 - a
                lim = min(lim, sb - under_pm) if sb >= C("CALL_HOLD", 0.95) - 1e-9 else -1.0
            for p, q in W.side_levels(ex, side, t):
                if p > lim + 1e-9:
                    break
                lots.append((p, t, ex, q if first else q * (1 - math.exp(-every / 600))))
            if lim > 0:
                first = False
            else:
                first = True                               # the call lapsed: when it is back, the book is whole again
            t += every
    W.consumed = saved
    lots.sort()
    cash, pnl, sh_r, cost_r, cost_s = cash0 - floor, 0.0, {}, {}, {}
    for p, t, ex, q in lots:
        race = W.meta[ex]["race"]
        st = W.meta[ex].get("state") or race.split(" ")[0]
        room = min(q, race_sh - sh_r.get(race, 0), (state_cap - cost_s.get(st, 0.0)) / p, (cash / p) if cash_limit else math.inf)
        room = int(max(0, room))
        if room < 1:
            continue
        sh_r[race] = sh_r.get(race, 0) + room
        cost_r[race] = cost_r.get(race, 0.0) + room * p
        cost_s[st] = cost_s.get(st, 0.0) + room * p
        cash -= room * p
        pnl += room * (1 - p)
    return pnl, cost_r


def bounds(W, fires, cash0):
    """bound_info: perfect information (the winner known from each race's last close, <= 0.97); bound_call: the rule in hindsight (every
    correct call from its first fire, at <= min(0.97, PM bid - 2c), the cash put on the cheapest of those lots, never a false call)."""
    info = {}
    for r in W.races:
        for ex in W.by_race[r]:
            for side in ("yes", "no"):
                if correct(W, ex, side) and W.legs[ex].role != "x":
                    info[(ex, side)] = W.last[r]
    calls = {k: v[0] for k, v in fires.items()}
    b_info, _ = bound(W, info, cash0)
    b_call, _ = bound(W, calls, cash0, under_pm=C("PRICE_UNDER_PM", 0.02))
    _, need = bound(W, calls, cash0, cash_limit=False, under_pm=C("PRICE_UNDER_PM", 0.02))
    return dict(info=b_info, call=b_call, need=need)


def pct(v, p):
    v = sorted(v)
    if not v:
        return None
    return v[min(len(v) - 1, int(round(p * (len(v) - 1))))]


def metrics(r, bd):
    W, E = r.W, r.E
    pay = {ex: W.payout(ex) for ex in W.legs}
    val = sum((q * pay[ex] if q > 0 else -q * (1 - pay[ex])) for ex, q in r.fake.pos.items())
    pnl = r.cash + val - r.cash0
    by_layer, sn_sh, sn_cost = {}, 0.0, 0.0
    fills_by = {}
    for t, oid, ex, side, action, q, p, layer, how in r.fills:
        win = pay[ex] if side == "yes" else 1 - pay[ex]
        v = q * (win - p) if action == "buy" else q * (p - win)
        by_layer[layer] = by_layer.get(layer, 0.0) + v
        if layer == "callsnipe":
            sn_sh += q
            sn_cost += q * p
            fills_by.setdefault((ex, side), []).append((t, q, p, v))
    recs = list(getattr(E, "records", []))
    calls = [x for x in recs if x.get("kind") == "CALL"]
    if not calls:                                         # v1 has no records: rebuild from E.calls
        calls = [dict(t=c["t"], ex=ex, side=c["side"], rule="v1", strict=False, signal_t=c.get("signal_t", c["t"])) for ex, c in E.calls.items()]
    false = [c for c in calls if not correct(W, c["ex"], c["side"])]
    false_cost = sum(q * p for c in false for _, q, p, _ in fills_by.get((c["ex"], c["side"]), []))
    false_pnl = sum(v for c in false for *_, v in fills_by.get((c["ex"], c["side"]), []))
    revs = [x for x in recs if x.get("kind") == "REVERSAL"]
    rev_true = sum(1 for x in revs if correct(W, x["ex"], x["side"]))
    # latency: each snipe's triggering PM quote -> the write that carried it; per call: the call quote -> its first order
    writes = list(r.fake.write_log)
    sent_at, wi = {}, 0
    batches = [(t, d) for t, k, d in writes if k in ("batch", "multileg") and isinstance(d, list)]
    for trig, tp, ex, side, layer, px, q in r.placed:
        if layer != "callsnipe":
            continue
        tw = next((t for t, d in batches if t >= tp - 1e-6 and any(o[0] == ex and o[1] == side and abs(o[3] - px) < 1e-9 and int(o[4]) == q
                                                                     for o in d)), None)
        if tw is not None:
            sent_at.setdefault(ex, []).append((trig, tw))
    lat_q = [tw - trig for v in sent_at.values() for trig, tw in v if trig is not None]
    lat, lat_sig = [], []
    for c in calls:
        tw = next((tw for trig, tw in sent_at.get(c["ex"], []) if trig is not None and abs(trig - c["t"]) < 1e-6), None)
        if tw is not None:
            lat.append(tw - c["t"])
            st_ = c.get("signal_t") or c["t"]
            if st_ >= W.last[W.meta[c["ex"]]["race"]]:          # the run began after the close (not a safe race sitting at 0.98 all day)
                lat_sig.append(tw - st_)
    wt = sorted(t for t, k, d in writes)
    peak, j = 0, 0
    for i, t in enumerate(wt):
        while wt[j] <= t - 60:
            j += 1
        peak = max(peak, i - j + 1)
    per_min = {}
    for t in wt:
        per_min[int(t // 60)] = per_min.get(int(t // 60), 0) + 1
    over25 = sum(1 for v in per_min.values() if v > 25)
    fails = sum(c.get("fails", 0) for c in E.calls.values())
    stops = len(getattr(E, "race_stop", ())) - len({x["race"] for x in revs})
    called_true = {W.meta[c["ex"]]["race"] for c in calls if correct(W, c["ex"], c["side"])}
    need = bd["need"]
    cash_min = min([c for _, c in r.cash_path] + [r.cash])
    k = r.kill
    kill = None
    if k:
        done = k["done"] or r.clock.time()
        kill = dict(t=nw.hm(k["t"]), n=k["n"], secs=round(done - k["t"], 1), writes=len(r.fake.write_log) - k["w0"])
    waves = wave_table(r, fills_by, need)
    night = sum(v for k_, v in by_layer.items() if k_ in C("SNIPE_LAYERS", ("callsnipe", "settle", "drift")))
    calls_at = len(lat)
    gap_calls = 0
    if r.feed_down:
        gap_calls = sum(1 for c in calls if abs(c["t"] - r.feed_down[1]) < 1e-6)
    return dict(name=r.name, mode=r.mode, pnl=round(pnl), night=round(night), leftover=round(pnl - night), calls_at=calls_at,
                gap_calls=gap_calls, by_layer={k_: round(v) for k_, v in by_layer.items()}, bound=round(bd["call"]), bound_info=round(bd["info"]),
                sn_sh=round(sn_sh), sn_cost=round(sn_cost), avg_px=round(sn_cost / sn_sh, 3) if sn_sh else None,
                calls=len(calls), rule_b=sum(1 for c in calls if c.get("strict") or c.get("rule") == "B"),
                races_called=len(called_true), races=len(W.races), false=len(false), false_cost=round(false_cost), false_pnl=round(false_pnl),
                revs=len(revs), rev_true=rev_true, nocall=sum(1 for x in recs if x.get("kind") == "NOCALL"),
                precall=sum(1 for x in recs if x.get("kind") == "PRECALL"),
                lat_n=len(lat), lat50=pct(lat, 0.5), lat95=pct(lat, 0.95), sig50=pct(lat_sig, 0.5), sig95=pct(lat_sig, 0.95),
                q50=pct(lat_q, 0.5), q95=pct(lat_q, 0.95), q_n=len(lat_q),
                writes=len(wt), peak=peak, over25=over25, orders=r.ex.sent_orders, stale=r.ex.dropped_stale,
                unknown=r.xcount.get("UNKNOWN", 0), rejects=r.xcount.get("ERR", 0) + r.xcount.get("REJECT", 0), trims=sum(r.ex.trims.values()),
                fails=fails, fail_stops=max(0, stops), place_none=r.place_none, ttl_rejects=r.fake.ttl_rejects,
                cash_min=round(cash_min), cash_end=round(r.cash), pulled=len(E.pulled), seeded=r.seeded,
                left_entries=sum(1 for o in r.fake.book.values() if o.get("expires") is None),
                kill=kill, waves=waves, msgs=r.n_msgs, wall=round(r.wall, 1), need_total=round(sum(need.values())))


def wave_table(r, fills_by, need):
    W = r.W
    waves = sorted(set(W.last.values()))
    out = []
    path = r.cash_path
    for i, w in enumerate(waves):
        nxt = waves[i + 1] if i + 1 < len(waves) else nw.T_END
        races = [x for x in W.races if W.last[x] == w]
        exs = {e for x in races for e in W.by_race[x]}
        cost = sum(q * p for (ex, side), fs in fills_by.items() if ex in exs for _, q, p, _ in fs)
        pnl = sum(v for (ex, side), fs in fills_by.items() if ex in exs for *_, v in fs)
        at = [c for t, c in path if t <= w]
        free_at = at[-1] if at else r.cash0
        window = [c for t, c in path if w <= t < nxt] or [free_at]
        calls = sum(1 for c in getattr(r.E, "records", []) if c.get("kind") == "CALL" and W.meta[c["ex"]]["race"] in races)
        out.append(dict(wave=nw.hm(w)[:5], races=len(races), calls=calls, need=round(sum(need.get(x, 0.0) for x in races)),
                        used=round(cost), pnl=round(pnl), free_at=round(free_at - C("CASH_FLOOR", 7000)),
                        free_min=round(min(window) - C("CASH_FLOOR", 7000))))
    return out


# ------------------------------------------------------------------------------------------------------- scenarios
def P(h):
    return nw.pt(nw.DAY, h)


def faults_busy():
    """SIG 5xx / 429 bursts across the busy hour: a 503 every 4 min 16:55-18:30 PT, a 60-s 429 at 17:00:30 and 18:00:30, a partial
    batch and a timed-out-but-landed write at 18:01, a DELETE that times out at the 16:50 pulls."""
    f = [(P("16:55") + k * 240, ["503"]) for k in range(int((P("18:30") - P("16:55")) / 240))]
    f += [(P("17:00") + 30, [("429", 60)]), (P("18:00") + 30, [("429", 60)]), (P("18:01"), [("partial", 1), "timeout_ok"]),
          (P("16:50"), ["cancel_timeout_fail"])]
    return f


SCENARIOS = [
    ("normal", dict(), dict()),
    ("normal, lane", dict(), dict(mode="lane")),
    ("slow count x2-3", dict(stretch=(2.0, 3.0)), dict()),
    ("reversal night", dict(reversals=4, strict_reversals=2), dict()),
    ("thin PM books", dict(thin=True), dict()),
    ("40 calls in 20 min (18:05) + 234 pulls, cash 300k", dict(stress40=True), dict(n_entry=234, cash0=300_000)),
    ("same + kill at 18:15", dict(stress40=True), dict(n_entry=234, cash0=300_000, kill_at=P("18:15"))),
    ("SIG 5xx / 429 bursts", dict(), dict(faults=faults_busy())),
    ("PM feed down 3 min 18:00", dict(), dict(feed_down=(P("18:00"), P("18:03")))),
    ("SIG gap 0 (SIG = PM)", dict(gap=0.0), dict()),
    ("SIG gap 1.0 (today's)", dict(gap=1.0), dict()),
    ("normal, 100k start", dict(), dict(cash0=100_000)),
]


def run_one(name, wkw, rkw, seed=7, verbose=False):
    W = nw.World(seed=seed, **wkw)
    fires = rule_fires(W, W.messages(rkw.get("feed_down")))
    bd = bounds(W, fires, float(rkw.get("cash0", 67_000)))
    W.consumed = None
    R = rig.Rig(W, name=name, seed=seed, verbose=verbose, **rkw).run()
    return metrics(R, bd), R


def fmt(v, nd=0):
    if v is None:
        return "–"
    if isinstance(v, float) and nd:
        return f"{v:.{nd}f}"
    return f"{v:,.0f}" if isinstance(v, (int, float)) else str(v)


def table(rows):
    fl = C("CASH_FLOOR", 7000)
    L = ["| scenario | P&L night layers | leftover entry bids | bound: the rule in hindsight | bound: perfect information | sniper buys: shares / cost / avg | "
         "calls (rule B) · with an order at the call | races called / 116 | false calls (P&L) | reversal stops (on true winners) | "
         "call → sent p50 / p95 s | PM quote → sent, every snipe, p50 / p95 s | writes (peak in 60 s) | stale drops · unknown writes · failed snipes | "
         "usable cash left | kill switch |",
         "|---|---:|---:|---:|---:|---|---|---:|---|---|---|---|---|---|---:|---|"]
    for m in rows:
        k = m["kill"]
        ks = "" if not k else f"{k['secs']} s for {k['n']} cancels ({k['writes']} writes)"
        L.append(f"| {m['name']} | **{m['night']:+,}** | {m['leftover']:+,} | {m['bound']:+,} | {m['bound_info']:+,} | "
                 f"{m['sn_sh']:,} / {m['sn_cost']:,} / {fmt(m['avg_px'], 3)} | {m['calls']} ({m['rule_b']}) · {m['calls_at']} | {m['races_called']} | "
                 f"{m['false']} ({m['false_pnl']:+,}) | {m['revs']} ({m['rev_true']}) | {fmt(m['lat50'], 1)} / {fmt(m['lat95'], 1)} | "
                 f"{fmt(m['q50'], 1)} / {fmt(m['q95'], 1)} | {m['writes']} ({m['peak']}) | {m['stale']} · {m['unknown']} · {m['fails']} | "
                 f"{m['cash_end'] - fl:,} | {ks} |")
    return L


def wave_md(m):
    L = [f"| wave (last close PT) | races | calls | cash needed (bound, no cash limit) | cash used | P&L of those buys | usable at the wave | min cash in the wave |",
         "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for w in m["waves"]:
        L.append(f"| {w['wave']} | {w['races']} | {w['calls']} | {w['need']:,} | {w['used']:,} | {w['pnl']:+,} | {w['free_at']:,} | {w['free_min']:,} |")
    return L


def main(argv=()):
    argv = list(argv)
    seed = int(argv[argv.index("--seed") + 1]) if "--seed" in argv else 7
    only = argv[argv.index("--only") + 1].split(",") if "--only" in argv else None
    md = argv[argv.index("--md") + 1] if "--md" in argv else None
    verbose = "-v" in argv
    rows, t0 = [], time.time()
    for name, wkw, rkw in SCENARIOS:
        if only and not any(o in name for o in only):
            continue
        m, _ = run_one(name, wkw, rkw, seed=seed, verbose=verbose)
        rows.append(m)
        print(f"{name:34} night {m['night']:+8,} left {m['leftover']:+6,} bound {m['bound']:+8,} buys {m['sn_sh']:7,} sh cost {m['sn_cost']:7,} calls {m['calls']:3} "
              f"races {m['races_called']:3} false {m['false']} ({m['false_pnl']:+,}) revs {m['revs']}/{m['rev_true']} lat {fmt(m['lat50'], 1)}/"
              f"{fmt(m['lat95'], 1)} writes {m['writes']} peak {m['peak']} stale {m['stale']} unk {m['unknown']} fails {m['fails']} "
              f"cash_min {m['cash_min']:,} kill {m['kill']} msgs {m['msgs']:,} {m['wall']} s", flush=True)
    L = [f"n_election: {os.path.getmtime(el.__file__) and time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(el.__file__)))}; "
         f"seed {seed}; {len(rows)} scenarios in {time.time() - t0:.0f} s", ""] + table(rows)
    for m in rows:
        if m["name"] in ("normal", "slow count x2-3"):
            L += ["", f"**Cash per wave, {m['name']}**", ""] + wave_md(m)
    out = "\n".join(L)
    print(out)
    if md:
        with open(md, "w") as fh:
            fh.write(out + "\n")
        print("wrote", md)
    return rows
