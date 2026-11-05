"""Tables for the share-multiplier study (mult_study.py's JSON): night P&L mean / p10, unused at 22:00, the later waves' cash, and
Piero's rule (00:00 Oct 29 item 3): the highest multiplier whose mean beats 1.0 by >= 10% on normal AND slow, with reversal p10 >= 1.0's.
Usage: python3 mult_tables.py mult_res.json [more.json ...]"""
import json
import sys
import time

SCENS = ("normal", "slow", "reversal")


def p10(xs):
    xs = sorted(xs)
    if not xs:
        return None
    k = 0.1 * (len(xs) - 1)
    lo = int(k)
    return xs[lo] + (xs[min(lo + 1, len(xs) - 1)] - xs[lo]) * (k - lo)


def k_(x):
    return f"{x / 1000:.1f}k"


def main():
    rows = [r for f in sys.argv[1:] for r in json.load(open(f))]
    by = {}
    for r in rows:
        by.setdefault((r["world"], r["usable"], r["mult"]), {}).setdefault(r["scen"], []).append(r)
    for world in sorted({r["world"] for r in rows}):
        for u in sorted({r["usable"] for r in rows}):
            ms = sorted(m for w, uu, m in by if w == world and uu == u)
            base = by.get((world, u, 1.0))
            print(f"\n**{'slow-SIG world' if world == 'sig' else 'sim'}, usable {u / 1000:.0f}k**\n")
            print("| multiplier | normal: mean / p10 | Δ vs 1.0 | slow: mean / p10 | Δ vs 1.0 | reversal: mean / p10 | Δ vs 1.0 | "
                  "unused at 22:00 (normal / slow / reversal) | later waves: lowest usable at a wave's first call, % of start | rule |")
            print("|---|---|---:|---|---:|---|---:|---|---|---|")
            for m in ms:
                cell, ok = [], []
                for s in SCENS:
                    rs_ = sorted(by[(world, u, m)].get(s, []), key=lambda r: r["seed"])
                    nt = [r["night"] for r in rs_]
                    mean = sum(nt) / len(nt)
                    cell.append(f"{k_(mean)} / {k_(p10(nt))}")
                    if base and m != 1.0:
                        b_ = sorted(base[s], key=lambda r: r["seed"])
                        bm = sum(r["night"] for r in b_) / len(b_)
                        up = sum(1 for x, y in zip(rs_, b_) if x["night"] > y["night"])
                        d = (mean - bm) / abs(bm)
                        cell.append(f"{d:+.0%} ({up}/{len(b_)} up)")
                        ok.append(d >= 0.10 if s in ("normal", "slow") else p10(nt) >= p10([r["night"] for r in b_]))
                    else:
                        cell.append("–")
                un = " / ".join(k_(sum(r["unused22"] for r in by[(world, u, m)][s]) / len(by[(world, u, m)][s])) for s in SCENS)
                allr = [r for s in SCENS for r in by[(world, u, m)][s]]
                lows = []
                for r in allr:
                    later = [v[0] for k, v in r["waves"].items() if int(k) != r["first_wave"] and v[0] is not None]
                    lows.append(min(later) / r["usable"] if later else 1.0)
                lowest = min(lows)
                n25 = sum(1 for x in lows if x < 0.25)
                spent = sum(r["n_spent"] for r in allr)
                scap = sum(r.get("n_statecap", 0) for r in allr)
                rule = "–" if m == 1.0 else ("QUALIFIES" if all(ok) else "no (" + ", ".join(n for n, o in zip(("normal +10%", "slow +10%", "reversal p10"), ok) if not o) + ")")
                print(f"| {m:g} | " + " | ".join(cell) + f" | {un} | {lowest:.0%} (runs under 25%: {n25} of {len(allr)}; races that found no cash: {spent}; held by the state cap: {scap}) | {rule} |")
    # cash moved between waves (normal nights, sim): mean callsnipe spend per wave by multiplier
    print("\n**Callsnipe spend per wave, normal nights (sim), mean over seeds**\n")
    for u in sorted({r["usable"] for r in rows}):
        ms = sorted(m for w, uu, m in by if w == "" and uu == u)
        waves = sorted({int(k) for r in rows if r["world"] == "" and r["usable"] == u and r["scen"] == "normal" for k in r["waves"]})
        print(f"\nusable {u / 1000:.0f}k\n")
        print("| multiplier | " + " | ".join(time.strftime("%H:%M", time.localtime(w)) for w in waves) + " | total |")
        print("|---|" + "---:|" * (len(waves) + 1))
        for m in ms:
            rs_ = by[("", u, m)]["normal"]
            sp = [sum(r["waves"].get(str(w), [0, 0, 0])[1] for r in rs_) / len(rs_) for w in waves]
            print(f"| {m:g} | " + " | ".join(k_(x) for x in sp) + f" | {k_(sum(sp))} |")


if __name__ == "__main__":
    main()
