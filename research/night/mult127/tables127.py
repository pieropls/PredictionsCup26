"""Tables for run127.py (Piero 15:50 Oct 31 item 2): per usable / multiplier / night kind — mean, p10, P(night >= 15k), P(night >= 20k),
the lowest usable at a later wave's first call (% of the start), the runs with a later wave under 25%, unused at 22:00; and his rule:
the multiplier with the highest P(night >= 20k) on normal and slow nights, provided no run leaves a later wave under 25% of the start.
Only seeds that every multiplier finished count (the same worlds). Usage: python3 tables127.py res.jsonl [res_ref.jsonl]"""
import json
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "mult_study"))
from mult_tables import p10  # noqa: E402

SCENS = ("normal", "slow", "reversal")


def later_low(r):
    """The lowest usable at a later wave's (not the night's first) first call, as a share of the start."""
    w = [v[0] for k, v in r["waves"].items() if int(k) != r["first_wave"] and v[0] is not None]
    return min(w) / r["usable"] if w else None


def main():
    rows = [json.loads(ln) for f in sys.argv[1:] for ln in open(f) if ln.strip()]
    mults = sorted({r["mult"] for r in rows})
    by = {}
    for r in rows:
        by.setdefault((r["usable"], r["scen"], r["seed"]), {})[r["mult"]] = r
    full = {k: v for k, v in by.items() if all(m in v for m in mults)}           # the same worlds for every multiplier
    for u in sorted({k[0] for k in full}, reverse=True):
        print(f"\n**Usable {u / 1000:.0f}k** (seeds: " + ", ".join(f"{s} {sum(1 for k in full if k[0] == u and k[1] == s)}" for s in SCENS) + ")\n")
        print("| multiplier | night | n | mean | p10 | P(>= 15k) | P(>= 20k) | lowest usable at a later wave's first call | runs with a later wave < 25% | unused at 22:00 (mean) |")
        print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        stat = {}
        for m in mults:
            for s in SCENS:
                rs_ = [v[m] for k, v in full.items() if k[0] == u and k[1] == s]
                if not rs_:
                    continue
                x = [r["night"] for r in rs_]
                lows = [later_low(r) for r in rs_]
                lows = [v for v in lows if v is not None]
                under = sum(1 for v in lows if v < 0.25)
                st = dict(n=len(x), mean=sum(x) / len(x), p10=p10(x), p15=sum(v >= 15_000 for v in x) / len(x), p20=sum(v >= 20_000 for v in x) / len(x),
                          low=min(lows) if lows else None, under=under, unused=sum(r["unused22"] for r in rs_) / len(rs_))
                stat[(m, s)] = st
                print(f"| {m:g} | {s} | {st['n']} | {st['mean'] / 1000:.1f}k | {st['p10'] / 1000:.1f}k | {st['p15']:.0%} | {st['p20']:.0%} | "
                      + (f"{st['low']:.0%}" if st["low"] is not None else "n/a") + f" | {under} | {st['unused'] / 1000:.1f}k |")
        print()
        for s in ("normal", "slow"):
            ok = [m for m in mults if (m, s) in stat and stat[(m, s)]["under"] == 0]
            best = max(ok, key=lambda m: stat[(m, s)]["p20"], default=None)
            print(f"- rule on {s} nights: " + ", ".join(f"{m:g}: P(>= 20k) {stat[(m, s)]['p20']:.0%}" + ("" if (m, s) in stat and stat[(m, s)]['under'] == 0
                  else f" (OUT: {stat[(m, s)]['under']} runs < 25%)") for m in mults if (m, s) in stat) + (f" -> {best:g}" if best else " -> none"))
        both = [m for m in mults if all((m, s) in stat and stat[(m, s)]["under"] == 0 for s in ("normal", "slow"))]
        pick = max(both, key=lambda m: (stat[(m, "normal")]["p20"] + stat[(m, "slow")]["p20"]), default=None)
        rev = [m for m in mults if (m, "reversal") in stat and stat[(m, "reversal")]["under"] > 0]
        print(f"- normal + slow together (sum of the two P(>= 20k)): {pick:g}" if pick is not None else "- normal + slow together: none qualifies")
        print("- reversal nights with a later wave < 25%: " + (", ".join(f"{m:g} ({stat[(m, 'reversal')]['under']} runs)" for m in rev) or "none"))


if __name__ == "__main__":
    main()
