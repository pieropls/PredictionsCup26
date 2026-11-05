"""Night size for top 50 (Piero 15:50 Oct 31 item 2): research/night/mult_study's run_job (the election-night sim, the engine's own
n_election with only WAVE_MULT changed) at usable 60k (50k as a sensitivity: the half-rate ramp projection) for WAVE_MULT 2.0 / 2.5 / 3.0,
the same worlds (seeds) for every multiplier. Seeds run in rounds (normal 8 / slow 5 / reversal 5 first, then more), each result appended as
one JSON line, so any stop leaves a balanced set. Mac only, on scratchpad copies (state / ledger from the Mac mirror 15:45, poll_close Oct 7).
Usage: python3 run127.py OUT.jsonl SRV_DIR [--jobs 12] [--until 'YYYY-MM-DD HH:MM'] [--mults 1.5]"""
import json
import multiprocessing as mp
import os
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "mult_study"))
import mult_study as ms  # noqa: E402

MULTS = (2.0, 2.5, 3.0)
USABLE = (60_000, 50_000)
ROUNDS = [dict(normal=(1, 8), slow=(1, 5), reversal=(1, 5)),          # the minimum Piero asked for (and the earlier studies' worlds)
          dict(normal=(9, 100), slow=(6, 50), reversal=(6, 50)),
          dict(normal=(101, 400), slow=(51, 200), reversal=(51, 200)),
          dict(normal=(401, 1000), slow=(201, 500), reversal=(201, 500))]


def main():
    out, srv = sys.argv[1], os.path.abspath(sys.argv[2])
    a = sys.argv[3:]
    jobs = int(a[a.index("--jobs") + 1]) if "--jobs" in a else 12
    until = time.mktime(time.strptime(a[a.index("--until") + 1], "%Y-%m-%d %H:%M")) if "--until" in a else float("inf")
    mults = tuple(float(x) for x in a[a.index("--mults") + 1].split(",")) if "--mults" in a else MULTS
    t0 = time.time()
    with open(out, "a") as fh, mp.get_context("fork").Pool(jobs) as pool:
        for i, rd in enumerate(ROUNDS):
            P = [(scen, s, u, m, srv, "") for scen, (lo, hi) in rd.items() for s in range(lo, hi + 1) for u in USABLE for m in mults]
            if time.time() > until:
                print(f"round {i + 1} not started: past --until", flush=True)
                break
            for r in pool.imap_unordered(ms.run_job, P):
                fh.write(json.dumps(r) + "\n")
                fh.flush()
            print(f"round {i + 1} done: {len(P)} runs, {time.time() - t0:.0f} s", flush=True)


if __name__ == "__main__":
    main()
