#!/usr/bin/env python3
"""Piero 10:15 Oct 26 item 3: the rehearsal's pass / fail, one line each, from the engine's own files (read-only). Runs on the server
right after rehearsal_eval.py (ops.py rehearsal-eval, 21:10) or anywhere with --base pointing at a copy laid out like /opt/sig-engine.

  1 calls fired on all 48 expected, none on the 9 no-call races      5 the probe's SIG round trip (ms)
  2 the reversal guard on all 4 (REVERSAL row, no order after it)    6 recycling at 0.97 exercised in at least one wave
  3 poll-close pulls >= 400 s before each close                      7 0 ERR / 0 PAUSE in the window
  4 call -> a race's first order from the first takeable ask, p95 < 500 ms  8 engine CPU (peak < 85%) and loop (p95 < 3 s), window + waves
                                                                     9 SIG writes/min peak vs the limit, 429s (rehearsal 2)

Usage: rehearsal_criteria.py [--base /opt/sig-engine] [--expected /opt/research/night/rehearsal_plan_expected.json]
                             [--start "2026-10-26 15:00:00"] [--hours 6.25]"""
import argparse
import collections
import json
import os
import re
import subprocess
import time

os.environ.setdefault("TZ", "America/Los_Angeles")
time.tzset()
LOOP = re.compile(r"^(\d\d):(\d\d):(\d\d) loop (\d+) \|.*?took (\d+)s")


def ep(day, hms):
    return time.mktime(time.strptime(f"{day} {hms}", "%Y-%m-%d %H:%M:%S"))


def jsonl(path, t0, t1, key="ts"):
    out = []
    if not os.path.exists(path):
        return out
    with open(path, "rb") as fh:
        fh.seek(max(0, os.path.getsize(path) - 40_000_000))
        for ln in fh:
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            t = r.get(key)
            if t is None and r.get("t"):
                try:
                    t = time.mktime(time.strptime(r["t"], "%Y-%m-%d %H:%M:%S"))
                except (ValueError, TypeError):
                    continue
            if t is not None and t0 <= float(t) <= t1:
                r["_t"] = float(t)
                out.append(r)
    return out


def engine_log_window(path, day, t0, t1):
    """[(epoch, line)] of engine.log between t0 and t1 on `day`: event lines take the time of the loop header above them."""
    with open(path, "rb") as fh:
        fh.seek(max(0, os.path.getsize(path) - 12_000_000))
        lines = fh.read().decode(errors="replace").splitlines()
    # today's part: walk back from the end while the loop headers' clock does not jump forward (yesterday -> today)
    start, last = 0, None
    for i in range(len(lines) - 1, -1, -1):
        m = LOOP.match(lines[i])
        if m:
            s = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
            if last is not None and s > last + 600:
                start = i + 1
                break
            last = s
    out, cur = [], None
    for ln in lines[start:]:
        m = LOOP.match(ln)
        if m:
            cur = ep(day, f"{m.group(1)}:{m.group(2)}:{m.group(3)}")
        if cur is not None and t0 <= cur <= t1:
            out.append((cur, ln))
    return out


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="/opt/sig-engine")
    ap.add_argument("--expected", default="/opt/research/night/rehearsal_plan_expected.json")
    ap.add_argument("--start", default="2026-10-27 15:00:00")
    ap.add_argument("--hours", type=float, default=6.25)
    a = ap.parse_args()
    day = a.start[:10]
    S = time.mktime(time.strptime(a.start, "%Y-%m-%d %H:%M:%S"))
    E = S + a.hours * 3600
    exp = json.load(open(a.expected))
    races = exp["races"]
    D = f"{a.base}/data"
    calls_all = [r for r in jsonl(f"{D}/election/calls.jsonl", S - 900, E) if r.get("injected")]
    calls = calls_all
    shadow = jsonl(f"{D}/election/shadow.jsonl", S - 900, E)
    log = engine_log_window(f"{D}/engine.log", day, S - 900, E)
    out = []

    # 1. calls
    want = {(ev["ex"], ev["side"]) for r in races for ev in r["events"] if ev["kind"] == "CALL"}
    got = {(str(r["ex"]), r["side"]) for r in calls if r["kind"] == "CALL"}
    nocall = set(exp["summary"].get("no_call_races") or [])
    bad_nc = sorted({r.get("race") for r in calls if r["kind"] == "CALL" and r.get("race") in nocall})
    extra = got - want
    miss = want - got
    out.append(f"REHEARSAL 1 calls: {'PASS' if not miss and not bad_nc else 'FAIL'} — {len(want & got)} of {len(want)} expected CALL rows fired"
               + (f" (missing: {', '.join(f'{x} {s}' for x, s in sorted(miss))})" if miss else "")
               + f"; {len(bad_nc)} of the {len(nocall)} no-call races called" + (f" ({', '.join(bad_nc)})" if bad_nc else "")
               + (f"; {len(extra)} other unexpected CALL rows" if extra else ""))

    # 2. reversals
    want_r = {(ev["ex"], ev["side"]) for r in races for ev in r["events"] if ev["kind"] == "REVERSAL"}
    rev = {(str(r["ex"]), r["side"]): r["_t"] for r in calls if r["kind"] == "REVERSAL"}
    after = 0
    for (ex, side), tr in rev.items():
        after += sum(1 for o in shadow if o.get("kind") == "order" and o["_t"] > tr + 1
                     and any(str(i[0]) == ex for i in (o.get("items") or [])) and o.get("why") in ("call", "callsnipe", "drift"))
    miss_r = want_r - set(rev)
    out.append(f"REHEARSAL 2 reversal guard: {'PASS' if not miss_r and not after else 'FAIL'} — {len(want_r & set(rev))} of {len(want_r)} expected "
               f"REVERSAL rows (race stopped), {after} would-be orders after a reversal")

    # 3. poll-close pulls
    pulls = {}
    for t, ln in log:
        m = re.search(r"POLLS (.+?): first poll close", ln)
        if m and m.group(1) not in pulls:
            pulls[m.group(1)] = t
    leads, nopull = {}, []
    for r in races:
        close = ep(day, r["close_pt"])
        if r["race"] in pulls:
            leads[r["race"]] = close - pulls[r["race"]]
        else:
            nopull.append(r["race"])
    low = {k: v for k, v in leads.items() if v < 400}
    out.append(f"REHEARSAL 3 poll-close pulls: {'PASS' if not low and not nopull else 'FAIL'} — {len(leads)} of {len(races)} races pulled, lead "
               f"min {min(leads.values()):.0f} s / median {pct(list(leads.values()), 0.5):.0f} s before the close (target >= 400 s; loop-clock, +-10 s)"
               if leads else "REHEARSAL 3 poll-close pulls: FAIL — no POLLS events found in the window")
    if low or nopull:
        out[-1] += (f"; under 400 s: {', '.join(f'{k} {v:.0f} s' for k, v in sorted(low.items()))}" if low else "") + (f"; no pull: {', '.join(nopull)}" if nopull else "")

    # 4. call -> first order, from the first takeable ask (C118 ORDER rows: the wait since the later of the call and the last failed try).
    # Piero 00:00 Oct 29 item 2: only each race's FIRST order counts; its later books (the shadow top-ups after a cash hold expires) get a line of their own
    orders_all = sorted((r for r in calls_all if r["kind"] == "ORDER"), key=lambda r: float(r["ts"]))
    orders, tops, ordered = [], [], set()
    for r in orders_all:
        (tops if r.get("race") in ordered else orders).append(r)
        ordered.add(r.get("race"))
    misses = [r for r in calls_all if r["kind"] == "MISS"]
    waits = [r["wait_s"] * 1000 for r in orders if r.get("wait_s") is not None]
    since = [r["since_call_s"] * 1000 for r in orders if r.get("since_call_s") is not None]
    p95w = pct(waits, 0.95)
    reasons = collections.Counter(r.get("reason") for r in misses)
    if waits:
        out.append(f"REHEARSAL 4 call -> first order from the first takeable ask: {'PASS' if p95w < 500 else 'FAIL'} — n {len(waits)}, p50 {pct(waits, 0.5):.0f} ms, "
                   f"p95 {p95w:.0f} ms, max {max(waits):.0f} ms (target p95 < 500 ms); call -> first order p50 {pct(since, 0.5) / 1000:.1f} s / p95 "
                   f"{pct(since, 0.95) / 1000:.1f} s; calls with no order {len({(str(r['ex'])) for r in calls if r['kind'] == 'CALL'} - {str(r['ex']) for r in orders_all})}; "
                   f"MISS reasons: {', '.join(f'{k} x{v}' for k, v in reasons.most_common(4)) or 'none'}")
        tw = [r["wait_s"] * 1000 for r in tops if r.get("wait_s") is not None]
        tsc = [r["since_call_s"] for r in tops if r.get("since_call_s") is not None]
        out.append("REHEARSAL 4 top-ups (a race's later books, not in criterion 4): "
                   + (f"n {len(tops)}, wait p50 {pct(tw, 0.5):.0f} ms / p95 {pct(tw, 0.95):.0f} ms / max {max(tw):.0f} ms; "
                      f"call -> top-up p50 {pct(tsc, 0.5):.0f} s / max {max(tsc):.0f} s; "
                      + ", ".join(f"{r.get('race')} {r.get('side', '').upper()} {r['wait_s']:.1f} s" for r in sorted(tops, key=lambda r: -(r.get("wait_s") or 0))[:5])
                      if tw else "none"))
    else:
        first_call = {}
        for r in calls:
            if r["kind"] == "CALL":
                first_call.setdefault(str(r["ex"]), r["_t"])
        lat = []
        for o in shadow:
            if o.get("kind") == "order" and o.get("why") == "call" and o.get("ts"):
                ex = str((o.get("items") or [[None]])[0][0])
                if ex in first_call and 0 <= float(o["ts"]) - first_call[ex] < 600:
                    lat.append((float(o["ts"]) - first_call[ex]) * 1000)
        p95 = pct(lat, 0.95)
        out.append(f"REHEARSAL 4 call -> order handed to the executor: {'PASS' if (p95 is not None and p95 < 500) else ('NO DATA' if p95 is None else 'FAIL')} — "
                   + (f"call row -> would-be order n {len(lat)} p50 {pct(lat, 0.5):.0f} ms p95 {p95:.0f} ms max {max(lat):.0f} ms" if lat else "no would-be call orders")
                   + " (target p95 < 500 ms; no ORDER rows: a pre-C118 engine)")

    # 5. probe
    pr = [ln for _, ln in log if "PROBE" in ln and "SIG answered in" in ln]
    pm = re.search(r"SIG answered in (\d+) ms", pr[-1]) if pr else None
    out.append(f"REHEARSAL 5 probe SIG round trip: " + (f"{pm.group(1)} ms ({pr[-1].split('PROBE', 1)[1].strip()[:160]})" if pm else
                                                        "no probe in this rehearsal"))

    # 6. recycling at 0.97
    rec = [r for r in shadow if r.get("kind") == "recycle"]
    by_w = collections.defaultdict(lambda: [0, 0])
    for r in rec:
        by_w[r.get("wave")][0] += 1
        by_w[r.get("wave")][1] += r.get("verdict") == "sell"
    sells = sum(v[1] for v in by_w.values())
    holds = collections.Counter(re.sub(r"[\d.,]+", "#", r.get("verdict", ""))[:40] for r in rec if r.get("verdict") != "sell")
    thr = sorted({r.get("threshold") for r in rec if r.get("threshold") is not None})
    out.append(f"REHEARSAL 6 recycling at {'/'.join(f'{x:.2f}' for x in thr) or 'n/a'}: {'PASS' if by_w else 'FAIL'} — decisions in {len(by_w)} waves ({len(rec)} lot decisions, "
               f"{sells} would-be sales)" + (f"; holds: {', '.join(f'{k} x{v}' for k, v in holds.most_common(3))}" if holds else "")
               + ("" if sells else "; the sale branch was not reached (no SIG bid at the threshold on our lots of the called sides, or no later wave had uncalled races)" if by_w else ""))

    # 7. ERR / PAUSE
    errs = [ln for _, ln in log if re.match(r"^\s+ERR\s", ln)]
    try:
        rl = open(f"{a.base}/docs/RUN_LOG.md", "rb")
        rl.seek(max(0, os.path.getsize(f"{a.base}/docs/RUN_LOG.md") - 3_000_000))
        rlines = rl.read().decode(errors="replace").splitlines()
    except OSError:
        rlines = []
    hm0, hm1 = time.strftime("%H:%M", time.localtime(S)), time.strftime("%H:%M", time.localtime(E))
    pauses = [ln for ln in rlines[-6000:] if re.match(r"^- \[?(\d\d:\d\d)\]? PAUSE", ln) and hm0 <= re.match(r"^- \[?(\d\d:\d\d)", ln).group(1) <= hm1]
    out.append(f"REHEARSAL 7 errors: {'PASS' if not errs and not pauses else 'FAIL'} — ERR {len(errs)} / PAUSE {len(pauses)} from {hm0} to {hm1}"
               + (f" (first ERR: {errs[0].strip()[:150]})" if errs else ""))

    # 8. CPU and loop time: the window and each wave (targets: CPU peak < 85% of a core, loop p95 < 3 s); SIG writes / 429s
    cpu_all = jsonl(f"{D}/ops/cpu.jsonl", S - 120, E + 120, key="t")

    def cpu_in(t0, t1):
        r = [x for x in cpu_all if t0 - 70 <= x["t"] <= t1 + 70]
        return [100 * (y["cpu_s"] - x["cpu_s"]) / (y["t"] - x["t"]) for x, y in zip(r, r[1:]) if y["pid"] == x["pid"] and y["t"] > x["t"]]

    def loops_in(t0, t1):
        return [int(LOOP.match(ln).group(5)) for t, ln in log if t0 <= t <= t1 and LOOP.match(ln)]
    cp, lp = cpu_in(S, E), loops_in(S, E)
    sig = [(t, re.search(r"sig w/min (\d+) r/min (\d+) 429/10m (\d+)", ln)) for t, ln in log if "sig w/min" in ln]
    sig = [(t, int(m.group(1)), int(m.group(2)), int(m.group(3))) for t, m in sig if m]
    ok_cpu = bool(cp) and max(cp) < 85
    ok_loop = bool(lp) and pct(lp, 0.95) < 3
    out.append(f"REHEARSAL 8 CPU and loop (window): {'PASS' if ok_cpu and ok_loop else 'FAIL'} — engine CPU "
               + (f"mean {sum(cp) / len(cp):.0f}% / peak {max(cp):.0f}% of a core (target < 85%)" if cp else "n/a")
               + (f"; loop mean {sum(lp) / len(lp):.1f} s / p95 {pct(lp, 0.95)} s / max {max(lp)} s (target p95 < 3 s)" if lp else "; loop n/a"))
    waves = collections.OrderedDict()
    for r in races:
        a_, b_ = ep(day, r["start_pt"]), ep(day, r["last_quote_pt"])
        w0, w1 = waves.get(r["wave"], (a_, b_))
        waves[r["wave"]] = (min(w0, a_), max(w1, b_))
    per = []
    for w, (w0, w1) in waves.items():
        c_, l_ = cpu_in(w0, w1), loops_in(w0, w1)
        per.append(f"{w} ({sum(1 for r in races if r['wave'] == w)} races): CPU " + (f"{sum(c_) / len(c_):.0f}% / {max(c_):.0f}%" if c_ else "n/a")
                   + (f", loop p95 {pct(l_, 0.95)} s max {max(l_)} s" if l_ else ""))
    out.append("REHEARSAL 8b per wave (CPU mean / peak, loop): " + "; ".join(per))
    if sig:
        wmax = max(x[1] for x in sig)
        out.append(f"REHEARSAL 9 SIG API: writes/min peak {wmax} of 30 (the call lane keeps 5 of them while live: other layers <= 25); "
                   f"reads/min peak {max(x[2] for x in sig)} of 100; 429s in 10 min peak {max(x[3] for x in sig)}"
                   + (" — PASS" if wmax <= 30 and max(x[3] for x in sig) == 0 else " — LOOK"))
    else:
        out.append("REHEARSAL 9 SIG API: n/a (no 'sig w/min' in the loop lines: a pre-C119 engine)")
    print("\n".join(out))


if __name__ == "__main__":
    main()
