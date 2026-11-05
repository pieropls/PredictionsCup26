"""Share-multiplier study (Piero 00:00 Oct 29 item 3): variant A (the equal wave share, live) at the live cap min(0.97, PM side bid - 3c)
with WAVE_MULT 1.0 / 1.25 / 1.5 / 2.0, on the election-night sim.

Rig: research/night/recycle_study.py's (tests/night_world + night_rig + the real j_executor on the fake SIG), recycling as built in C121
(0.95, 25% depth, today's bid structure, 30k a wave, sell only while a later wave has uncalled races), fresh read-only copies of the
server's state / ledger / poll_close (--srv). n_election is the engine's own (the tree = C124); only WAVE_MULT changes per run.
Usable 45k / 60k; normal 8, slow 5, reversal 5 seeds. Night P&L = snipes at settlement + recycling + held favourites at settlement.
"Cash already spent": a race of a later wave (not the night's first) whose first book got a `cash` MISS with race cost 0 where the
wave share, not the 20k state cap, was under 10 shares at the ask (n_spent; n_statecap counts the state-cap ones apart); and the
usable cash when each wave's first call came (waves). Both within 10 min of the race's call (later retries
run after the night's cash is all spent: the sim runs to Nov 4 morning).
Usage: python3 mult_study.py out.json --srv DIR [--jobs 10] [--only normal:1] [--tags 1.0,2.0] [--world sig]"""
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import recycle_study as rsy  # noqa: E402  (puts the project root first: `import n_election` is the engine's)
import reserve_sizing as rs  # noqa: E402

REC = rsy.cfg_of(0.95, 0.25, clip=True, wave_cap=30_000.0, gate3=True)          # C121 as built
MULTS = (1.0, 1.25, 1.5, 2.0)
USABLE = (45_000, 60_000)
SPENT_S = 600                                               # a race's first 10 min after its call
SRV = None
WORLD = ""
DET = re.compile(r"usable (-?[\d,]+), wave share (-?[\d,]+), race cost (-?[\d,]+), state cost (-?[\d,]+)")


def cash_spent(detail, cap):
    """A `cash` MISS on a race's first book where the wave share (not the 20k state cap) left < 10 shares: the cash was spent."""
    m = DET.search(detail or "")
    if not m:
        return None
    usable, share, race, state = (float(x.replace(",", "")) for x in m.groups())
    return race == 0 and share < cap - state


def run_job(job):
    scen, seed, usable, mult, srv, world = job
    import tests  # noqa: F401  (SIG_DATA is a temp dir, never data/)
    nw = rsy.patch(srv)
    import night_rig as rig
    import night_report as nr
    import n_election as el
    assert rig.el is el and os.path.realpath(el.__file__) == os.path.realpath(os.path.join(rsy.ROOT, "n_election.py")), el.__file__
    assert el.PRICE_UNDER_PM == 0.03 and el.PRICE_CAP == 0.97, (el.PRICE_UNDER_PM, el.PRICE_CAP)
    el.WAVE_MULT = mult
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
    snipe = sum(q * (side_pay(ex, side) - p) for t, oid, ex, side, action, q, p, layer, how in R.fills
                if layer in el.SNIPE_LAYERS and action == "buy")
    spend = sum(q * p for t, oid, ex, side, action, q, p, layer, how in R.fills if layer == "callsnipe" and action == "buy")
    fav = sum(h["sh"] * (side_pay(h["ex"], h["side"]) - h["avg"]) for h in held)
    rec = sum(x * (p - side_pay(ex, side)) for t, ex, side, p, x, k in R.sales)
    t22 = nw.pt(nw.DAY, "22:00")
    c22 = [c for t, c in R.cash_path if t <= t22]
    called = {W.meta[e]["race"] for e in R.E.calls}
    first_wave = min((W.first[r] for r in called), default=None)
    later = {r for r in called if W.first[r] != first_wave}
    bought = {r_["race"] for r_ in R.E.records if r_["kind"] == "ORDER"}
    t_call = {}
    for e, c in R.E.calls.items():
        t_call[W.meta[e]["race"]] = min(t_call.get(W.meta[e]["race"], float("inf")), c["t"])
    early = lambda r_: r_["t"] - t_call.get(r_["race"], r_["t"]) <= SPENT_S          # noqa: E731  (the call's own try, not a retry hours later)
    spent = {r_["race"] for r_ in R.E.records if r_["kind"] == "MISS" and r_.get("layer") == "callsnipe" and r_["reason"] == "cash"
             and cash_spent(r_.get("detail"), el.STATE_CAP) and r_["race"] in later and early(r_)}
    statecap = {r_["race"] for r_ in R.E.records if r_["kind"] == "MISS" and r_.get("layer") == "callsnipe" and r_["reason"] == "cash"
                and cash_spent(r_.get("detail"), el.STATE_CAP) is False and "race cost 0," in (r_.get("detail") or "") and r_["race"] in later
                and early(r_)}
    waves_spent = {W.first[r] for r in spent}
    waves = {}                                              # per wave (its first close): [usable when its first call came, callsnipe spend, races called]
    for e, c in R.E.calls.items():
        w = waves.setdefault(W.first[W.meta[e]["race"]], [None, 0.0, set(), float("inf")])
        w[2].add(W.meta[e]["race"])
        w[3] = min(w[3], c["t"])
    for t, oid, ex, side, action, q, p, layer, how in R.fills:
        if layer == "callsnipe" and action == "buy":
            waves.setdefault(W.first[W.meta[ex]["race"]], [None, 0.0, set(), float("inf")])[1] += q * p
    for w in waves.values():
        before = [c for t, c in R.cash_path if t <= w[3]]
        w[0] = round((before[-1] if before else usable + rs.FLOOR) - rs.FLOOR)
    waves = {str(round(k)): [v[0], round(v[1]), len(v[2])] for k, v in sorted(waves.items())}
    misses = {}
    for r_ in R.E.records:
        if r_["kind"] == "MISS":
            misses[r_["reason"]] = misses.get(r_["reason"], 0) + 1
    return dict(scen=scen, seed=seed, usable=usable, mult=mult, world=world, night=round(snipe + rec + fav), snipe=round(snipe), rec=round(rec),
                fav=round(fav), spend=round(spend), unused22=round((c22[-1] if c22 else R.cash) - rs.FLOOR), cash_end=round(R.cash - rs.FLOOR),
                n_called=len(called), n_later=len(later), n_spent=len(spent), n_spent_never=len(spent - bought), n_waves_spent=len(waves_spent), n_statecap=len(statecap),
                n_waves_later=len({W.first[r] for r in later}), waves=waves, first_wave=round(first_wave or 0), misses=misses, false_pnl=m["false_pnl"])


def plan(only=None, mults=MULTS):
    out = []
    for scen, n in rsy.SEEDS.items():
        for s in range(1, n + 1):
            if only and f"{scen}:{s}" not in only:
                continue
            out += [(scen, s, u, m, SRV, WORLD) for u in USABLE for m in mults]
    return out


def main():
    a = sys.argv[1:]
    out = a[0]
    jobs = int(a[a.index("--jobs") + 1]) if "--jobs" in a else 10
    only = a[a.index("--only") + 1].split(",") if "--only" in a else None
    mults = tuple(float(x) for x in a[a.index("--tags") + 1].split(",")) if "--tags" in a else MULTS
    global SRV, WORLD
    SRV = os.path.abspath(a[a.index("--srv") + 1])
    WORLD = a[a.index("--world") + 1] if "--world" in a else ""
    import multiprocessing as mp
    P, res, t0 = plan(only, mults), [], time.time()
    with mp.get_context("fork").Pool(min(jobs, len(P))) as pool:
        for i, r in enumerate(pool.imap_unordered(run_job, P)):
            res.append(r)
            if i % 20 == 0:
                print(f"{i + 1}/{len(P)} {time.time() - t0:.0f} s", flush=True)
    json.dump(res, open(out, "w"))
    print(f"done {len(res)} in {time.time() - t0:.0f} s -> {out}")


if __name__ == "__main__":
    main()
