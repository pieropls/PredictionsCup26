"""Allocation study (Piero 22:40 Oct 27 item 2): how the call sniper splits usable cash, on the election-night sim.

Rig: research/night/recycle_study.py's (tests/night_world + night_rig + the real j_executor on the fake SIG), recycling as built in C121
(0.95, 25% depth, today's bid structure, 30k a wave, sell only while a later wave has uncalled races), fresh read-only copies of the
server's state / ledger / poll_close (22:44 Oct 27). n_election is the study copy n_election_alloc.py with these modes:
  A  equal       the 1x wave share (live today)
  B  edge        a wave's races share one pool; queued calls fill in order of edge per $ = (PM side bid - ask) / ask, each up to its caps
  C  edge_hold   B + 30 / 40 / 50% of usable (at the first call) kept for the coin flips (a book's PM mid 0.25-0.75 before its race's
                 first close - 10 min), split equally among them, released once < 3 coin flips are uncalled
Usable 45k / 60k; normal 8, slow 5, reversal 5 seeds. Night P&L = snipes at settlement + recycling + held favourites at settlement.
Results of the Oct 27 23:00 runs: alloc_res*.json here (docs/night_alloc.md). Study only: nothing of this is in the engine.
Usage: python3 research/night/alloc_study/alloc_study.py out.json --srv <dir with state.json, ledger.json, poll_close.json> [--jobs 10]
       [--only normal:1,...] [--tags A,B,C40,C40+sig,...]   (+sig: SIG lags PM 6x longer, 10x the depth)"""
import json
import os
import sys
import time

import importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("n_election", os.path.join(HERE, "n_election_alloc.py"))
el = importlib.util.module_from_spec(_spec)
sys.modules["n_election"] = el                              # the study copy (n_election_alloc.diff): every later `import n_election` gets it
_spec.loader.exec_module(el)
sys.path.insert(0, os.path.dirname(HERE))
import recycle_study as rsy  # noqa: E402
import reserve_sizing as rs  # noqa: E402

SRV = sys.argv[sys.argv.index("--srv") + 1] if "--srv" in sys.argv else os.path.join(HERE, "srv")   # copies of state / ledger / poll_close
REC = rsy.cfg_of(0.95, 0.25, clip=True, wave_cap=30_000.0, gate3=True)          # C121 as built
VARIANTS = {"A": ("equal", None), "B": ("edge", None), "C30": ("edge_hold", 0.3), "C40": ("edge_hold", 0.4), "C50": ("edge_hold", 0.5)}
EXTRA = {"C40p": ("edge_hold_plus", 0.4), "AH30": ("equal_hold", 0.3), "AH40": ("equal_hold", 0.4)}   # diagnostics
VARIANTS.update(EXTRA)
USABLE = (45_000, 60_000)


def run_job(job):
    scen, seed, usable, tag, srv = job
    import tests  # noqa: F401  (SIG_DATA is a temp dir, never data/)
    nw = rsy.patch(srv)
    import night_rig as rig
    import night_report as nr
    assert rig.el is el
    base, _, world = tag.partition("+")
    el.ALLOC, el.HOLD_FRAC = VARIANTS[base][0], VARIANTS[base][1] or el.HOLD_FRAC
    W = nw.World(seed=seed, **rs.SCEN[scen])
    if world == "sig":                                      # sensitivity: SIG lags PM 6x longer (30-120 min), 10x the depth
        for L in W.legs.values():
            L.tau *= 6.0
            L.depth = [d * 10 for d in L.depth]
            W.build_sig(L)                                  # the SIG path is precomputed at World() time: rebuild it with the new lag
    held, _ = rsy.held_stock(srv, W, nw)
    R = rsy.recycle_rig(rig, el, nw)(W, REC, held, name=scen, seed=seed, cash0=usable + rs.FLOOR).run()
    m = nr.metrics(R, dict(call=0, info=0, need={}))
    pay = {ex: W.payout(ex) for ex in W.legs}
    side_pay = lambda ex, side: pay[ex] if side == "yes" else 1 - pay[ex]          # noqa: E731
    coin = {r: any(el.COIN_LO <= W.pm_mid(e, W.first[r] - el.PULL_BEFORE_S) <= el.COIN_HI for e in W.legs if W.meta[e]["race"] == r)
            for r in W.races}
    snipe = 0.0
    grp = {"coin": [0.0, 0.0, 0.0, 0.0], "other": [0.0, 0.0, 0.0, 0.0]}          # [P&L, cost, shares, cost on winning sides]
    for t, oid, ex, side, action, q, p, layer, how in R.fills:
        if layer in el.SNIPE_LAYERS and action == "buy":
            snipe += q * (side_pay(ex, side) - p)
            if layer == "callsnipe":
                g = grp["coin" if coin[W.meta[ex]["race"]] else "other"]
                g[0] += q * (side_pay(ex, side) - p)
                g[1] += q * p
                g[2] += q
                g[3] += q * p if side_pay(ex, side) == 1.0 else 0.0
    fav = sum(h["sh"] * (side_pay(h["ex"], h["side"]) - h["avg"]) for h in held)
    rec = sum(x * (p - side_pay(ex, side)) for t, ex, side, p, x, k in R.sales)
    t22 = nw.pt(nw.DAY, "22:00")
    c22 = [c for t, c in R.cash_path if t <= t22]
    called = {W.meta[e]["race"] for e in R.E.calls}
    misses = {}
    for r_ in R.E.records:
        if r_["kind"] == "MISS":
            misses[r_["reason"]] = misses.get(r_["reason"], 0) + 1
    return dict(scen=scen, seed=seed, usable=usable, tag=tag, night=round(snipe + rec + fav), snipe=round(snipe), rec=round(rec), fav=round(fav),
                coin_sh=round(grp["coin"][2]), coin_win=round(grp["coin"][3]), other_sh=round(grp["other"][2]), other_win=round(grp["other"][3]),
                coin_pnl=round(grp["coin"][0]), coin_cost=round(grp["coin"][1]), other_pnl=round(grp["other"][0]), other_cost=round(grp["other"][1]),
                unused22=round((c22[-1] if c22 else R.cash) - rs.FLOOR), cash_end=round(R.cash - rs.FLOOR), hold0=round(R.E.hold0 or 0),
                hold_off=R.E.hold_off, n_coin=sum(coin.values()), n_coin_called=sum(1 for r_ in called if coin.get(r_)), n_called=len(called),
                misses=misses, rec_cash=round(sum(x * p for t, ex, side, p, x, k in R.sales)), false_pnl=m["false_pnl"])


TAGS = None


def plan(only=None):
    out = []
    for scen, n in rsy.SEEDS.items():
        for s in range(1, n + 1):
            if only and f"{scen}:{s}" not in only:
                continue
            out += [(scen, s, u, tag, SRV) for u in USABLE for tag in (TAGS or VARIANTS)]
    return out


def main():
    a = sys.argv[1:]
    out = a[0]
    jobs = int(a[a.index("--jobs") + 1]) if "--jobs" in a else 10
    only = a[a.index("--only") + 1].split(",") if "--only" in a else None
    global TAGS
    TAGS = a[a.index("--tags") + 1].split(",") if "--tags" in a else None
    import multiprocessing as mp
    P, res, t0 = plan(only), [], time.time()
    with mp.get_context("fork").Pool(min(jobs, len(P))) as pool:
        for i, r in enumerate(pool.imap_unordered(run_job, P)):
            res.append(r)
            if i % 20 == 0:
                print(f"{i + 1}/{len(P)} {time.time() - t0:.0f} s", flush=True)
    json.dump(res, open(out, "w"))
    print(f"done {len(res)} in {time.time() - t0:.0f} s -> {out}")


if __name__ == "__main__":
    main()
