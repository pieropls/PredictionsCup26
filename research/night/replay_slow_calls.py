#!/usr/bin/env python3
"""C118 (Piero spec Oct 27 item 1): replay the Oct 26 rehearsal's 8 slow calls through the old and the new call-lane path.

Each case: the call (PM bid at the call, rule A from a 20-s run at >= 0.95) on a market whose SIG feed book is stale (no update in 10 s,
the rehearsal's quiet markets), and the ask the first order took later (its price and size, from shadow.jsonl). The engine's own code
runs: n_election.Election (detector, lane queue, _buy) and f_engine.Engine.el_levels / el_rest_book (bound to a stub with the engine's
attributes; the REST read answers the book the order later took).
  old path: no REST read -> the fallback book has depth 0 -> nothing until the feed's next book (the observed delay)
  new path: one REST read from the lane at the call -> the order goes out at once
Usage: python3 research/night/replay_slow_calls.py"""
import os
import sys
import threading
import time
import types

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))
sys.modules.setdefault("b_sig", __import__("fake_sig").FakeSIG() if hasattr(__import__("fake_sig"), "FakeSIG") else types.ModuleType("b_sig"))
import f_engine  # noqa: E402
import n_election as el  # noqa: E402

# (race, ex, side, PM bid at the call, the order's price, its size, observed call -> first order ms) — Oct 26 rehearsal, server files
CASES = [("Kentucky Senate", "956", "yes", 0.982, 0.950, 63, 12597), ("Virginia Senate", "1053", "no", 0.976, 0.945, 63, 24095),
         ("Massachusetts Senate", "1069", "no", 0.974, 0.940, 66, 285115), ("PA-07 House race", "924", "no", 0.965, 0.915, 62, 13998),
         ("Colorado Senate", "946", "no", 0.969, 0.930, 63, 23815), ("Iowa Senate", "950", "yes", 0.970, 0.925, 61, 8808),
         ("Hawaii Governor", "990", "yes", 0.974, 0.940, 57, 6851), ("U.S. Senate control", "842", "yes", 0.973, 0.915, 60, 13707)]


def stub(rest_on, book):
    """The engine attributes el_levels / el_rest_book use; the REST read answers `book` (yes-price levels)."""
    s = types.SimpleNamespace(el_mode_cached="live", el_taken={}, el_rest_lock=threading.Lock(), el_rest_cache={}, el_rest_t=[], el_rest_n=0,
                              EL_REST_PER_MIN=f_engine.Engine.EL_REST_PER_MIN, EL_REST_EVERY_S=f_engine.Engine.EL_REST_EVERY_S,
                              EL_REST_CACHE_S=f_engine.Engine.EL_REST_CACHE_S, logs=[])
    s.feed_book = lambda ex: None                           # stale: no update for > 10 s
    s.name = lambda ex: ex
    s.el_log = lambda k, x: s.logs.append((k, x))
    f_engine.api.orderbook = lambda ex, depth=10: book
    s.el_rest_book = (lambda ex: f_engine.Engine.el_rest_book(s, ex)) if rest_on else (lambda ex: None)
    s.el_levels = lambda ex, side: f_engine.Engine.el_levels(s, ex, side)
    return s


def run(case, rest_on):
    race, ex, side, pm_bid, px, qty, obs_ms = case
    yes_px = px if side == "yes" else round(1 - px, 3)
    book = ([], [(yes_px, float(qty) * 10)]) if side == "yes" else ([(yes_px, float(qty) * 10)], [])
    s = stub(rest_on, book)
    other = "x" + ex
    meta = {ex: dict(race=race, party="D", state=race.split()[0]), other: dict(race=race, party="R", state=race.split()[0])}
    clock = [10_000.0]
    placed = []
    E = el.Election(meta, {race: 9_000.0}, lambda: clock[0], lambda items, layer, ttl: (placed.append((time.perf_counter(), items)), "k1")[1],
                    lambda o, reason="": True, lambda e: (0.0, 1.0, 0), lambda k, x: None, manual=(), first_close={race: 9_000.0},
                    levels=s.el_levels, place_dry=lambda *a: "dry")
    yes_bid = pm_bid if side == "yes" else round(1 - pm_bid - 0.005, 3)
    yes_ask = round(yes_bid + 0.005, 3)
    for t in (10_000.0, 10_010.0):                          # the 20-s run at >= 0.95 begins (rule A)
        clock[0] = t
        E.on_pm(ex, (yes_bid + yes_ask) / 2, t, 200_000.0, {}, pm_bid=yes_bid, pm_ask=yes_ask, defer=True)
        E.on_pm(other, 0.02, t, 200_000.0, {}, pm_bid=0.015, pm_ask=0.02, defer=True)
    clock[0] = 10_021.0
    t0 = time.perf_counter()
    E.on_pm(ex, (yes_bid + yes_ask) / 2, clock[0], 200_000.0, {}, pm_bid=yes_bid, pm_ask=yes_ask, defer=True)   # the call
    E.run_queue(clock[0], 200_000.0, {})                    # the call lane's pass (it wakes on the call)
    miss = [r["reason"] for r in E.records if r["kind"] == "MISS"]
    return ((placed[0][0] - t0) * 1000 if placed else None), miss, ex in E.calls


def main():
    print("| race | side | observed call -> first order | old path (no REST read) | new path (REST read from the lane) |")
    print("|---|---|---:|---|---:|")
    lat = []
    for c in CASES:
        old_ms, old_miss, called = run(c, False)
        new_ms, new_miss, _ = run(c, True)
        lat.append(new_ms if new_ms is not None else float("inf"))
        print(f"| {c[0]} | {c[2].upper()} | {c[6] / 1000:.1f} s | {'order ' + f'{old_ms:.1f} ms' if old_ms is not None else 'no order: ' + (old_miss[-1] if old_miss else 'n/a')}"
              f"{'' if called else ' (no call)'} | {f'{new_ms:.2f} ms' if new_ms is not None else 'no order: ' + (new_miss[-1] if new_miss else 'n/a')} |")
    lat.sort()
    print(f"\nnew path: p50 {lat[len(lat) // 2]:.2f} ms, p95 {lat[min(len(lat) - 1, int(0.95 * len(lat)))]:.2f} ms, max {lat[-1]:.2f} ms "
          f"(engine CPU time on this Mac; on the night add the executor hand-over, lane p95 21 ms on Oct 26, and SIG's ~360 ms round trip)")


if __name__ == "__main__":
    main()
