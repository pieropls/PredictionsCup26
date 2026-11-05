"""The side numbers docs/night_sim.md quotes beyond the scenario table: seed ranges, the cost of wrong calls with cash not binding, the
lane's value with cash not binding, where the cash goes (safe vs contested), and the settlement-morning check.
Usage: python3 research/night/night_sim_extras.py   (offline; ~2 min)"""
import collections
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))
import tests  # noqa: E402,F401  (the package guard: SIG_DATA is a temp dir, never data/)
import night_report as nr  # noqa: E402
import night_world as nw  # noqa: E402

SEEDS = range(1, 9)


def rng(v):
    v = sorted(v)
    return f"{v[0]:+,} .. {v[len(v) // 2]:+,} .. {v[-1]:+,}"


def sweep():
    for name, wkw, rkw in [("normal", {}, {}), ("slow count x2-3", dict(stretch=(2.0, 3.0)), {}),
                           ("reversal night", dict(reversals=4, strict_reversals=2), {})]:
        rows = [nr.run_one(name, wkw, rkw, seed=s)[0] for s in SEEDS]
        print(f"{name}: night P&L min..med..max {rng([m['night'] for m in rows])}; rule bound {rng([m['bound'] for m in rows])}; "
              f"races called {min(m['races_called'] for m in rows)}-{max(m['races_called'] for m in rows)}; "
              f"false calls {min(m['false'] for m in rows)}-{max(m['false'] for m in rows)} (P&L {rng([m['false_pnl'] for m in rows])}); "
              f"reversal stops {min(m['revs'] for m in rows)}-{max(m['revs'] for m in rows)} (on winners {min(m['rev_true'] for m in rows)}-"
              f"{max(m['rev_true'] for m in rows)}); call->sent p95 {min(m['lat95'] for m in rows):.1f}-{max(m['lat95'] for m in rows):.1f} s")


def wrong_calls_rich():
    for c0 in (67_000, 300_000):
        rows = [nr.run_one("rev", dict(reversals=4, strict_reversals=2), dict(cash0=c0), seed=s)[0] for s in SEEDS]
        v = [m["false_pnl"] for m in rows]
        print(f"reversal night, cash {c0:,}: wrong-call P&L {rng(v)}, mean {sum(v) / len(v):+,.0f}; guard stops on the wrong calls "
              f"{[m['revs'] - m['rev_true'] for m in rows]}")


def lane_rich():
    for mode in ("engine", "lane"):
        v = [nr.run_one("n", {}, dict(cash0=300_000, mode=mode), seed=s)[0]["night"] for s in SEEDS]
        print(f"normal, 300k, {mode}: night P&L {rng(v)}, mean {sum(v) / len(v):+,.0f}")


def where_cash_goes():
    for name, wkw in [("normal", {}), ("SIG gap 0", dict(gap=0.0))]:
        m, R = nr.run_one(name, wkw, {})
        W, agg, early = R.W, collections.defaultdict(lambda: [0.0, 0.0]), 0.0
        for t, oid, ex, side, a, q, p, lay, how in R.fills:
            if lay != "callsnipe":
                continue
            k = "safe (PM >= 0.95 pre-close)" if W.p26[W.meta[ex]["race"]] >= 0.95 else "contested"
            pay = W.payout(ex) if side == "yes" else 1 - W.payout(ex)
            agg[k][0] += q * p
            agg[k][1] += q * (pay - p)
            early += q * p if t < nw.pt(nw.DAY, "17:30") else 0
        tot = sum(c for c, _ in agg.values())
        print(f"{name}: " + "; ".join(f"{k} cost {c:,.0f} ({c / tot:.0%}) edge {v / c:.1%}" for k, (c, v) in agg.items())
              + f"; spent before 17:30 PT {early / tot:.0%}")


def settlement_check():
    for depth in (False, True):
        m, R = nr.run_one("settle", {}, dict(cash0=2_000_000, book_depth=depth))
        c = collections.Counter(p[4] for p in R.placed)
        print(f"settlement morning, 2M cash, el_book depth {'real' if depth else '0 (f_engine.el_book)'}: orders placed by layer {dict(c)}; "
              f"settle P&L {m['by_layer'].get('settle', 0):+,}")


if __name__ == "__main__":
    W = nw.World(seed=7)
    print(f"templates left out as one-sided books: {[t.key for t in W.templates if t.thin]}; 2026 favourites that lose: "
          f"{sum(1 for r in W.races if not W.template[r].fav_won)} of {len(W.races)}")
    sweep()
    wrong_calls_rich()
    lane_rich()
    where_cash_goes()
    settlement_check()
