"""The whole engine (f_engine.py + j_executor.py) against the fake exchange (tests/fake_sig.py) and a fake Polymarket.
No network, nothing sent.   python3 -m unittest tests.test_engine

  scenario          position exits, foreign orders, inventory I2, atomic pair sets, repairs, UNKNOWN freeze + audit
  kill-restart      three kills mid-batch: zero duplicates, every order sent after a restart was planned after it
  stop under load   slow writes, 429, DELETE timeouts, unreadable book: exits and foreign orders survive, nothing placed
  plan-cycle        writes taking 2 s each in the executor's threads do not slow the plan cycle
  dashboard         read-only controls, bound to 127.0.0.1
"""
import collections
import contextlib
import csv
import datetime
import io
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import types
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp(prefix="sig_engine_test_")
import a_config  # noqa: E402

for k, v in dict(LOOP_S=0, PNL_EVERY_S=0, PAIR_MIN_LOCK=0, DATA=TMP, POSITION_REF_FILE=f"{TMP}/position_ref.json", LEDGER_FILE=f"{TMP}/ledger.json",
                 PNL_FILE=f"{TMP}/pnl.csv", STATE_FILE=f"{TMP}/state.json", DASHBOARD_FILE=f"{TMP}/dashboard.html",
                 PID_FILE=f"{TMP}/engine.pid", LOCK_FILE=f"{TMP}/engine.lock", OVERRIDES_FILE=f"{TMP}/overrides.json",
                 HOST_ID_FILE=f"{TMP}/host_id", NIGHT_LOG_FILE=f"{TMP}/NIGHT_LOG.md", STATE_EVERY_S=0,
                 DASHBOARD_URL_FILE=f"{TMP}/dashboard_url.txt").items():
    setattr(a_config, k, v)

from fake_sig import FakeSIG  # noqa: E402

U = json.load(open(os.path.join(ROOT, "tests", "fixtures", "universe.json")))
SIGPX = {p["exchangeId"]: (p["bestBid"], p["bestAsk"], p["latestPrice"]) for p in U["prices"]}
AK, RI, MA, LONE = "1057", "1073", "1060", "9999"          # AK, RI: position bots; MA Gov R: market-bot inventory
POS = {AK: -11024, RI: 2137, MA: -1008}
AK_LADDER = [(0.4, 2594), (0.5, 2594), (0.6, 2594), (0.775, 3242)]
AK_HALF = [(0.4, 1297), (0.5, 1297), (0.6, 1297), (0.775, 1621)]     # after D1 + D1b: the ladder on the remaining 50%


class Killed(BaseException):
    """The process dies (kill -9): nothing after this point runs."""


def new_fake(**kw):
    f = FakeSIG(U["markets"], SIGPX, POS, seed=7, **kw)
    f.processing = (0.01, 0.05)                           # SIG answers fast unless a test says otherwise
    f.manual = dict(ri1=f.manual_order(RI, "yes", "sell", 0.955, 1000),       # sells placed by hand on the website
                    ri2=f.manual_order(RI, "yes", "sell", 0.955, 1137),
                    lone=f.manual_order(LONE, "yes", "buy", 0.10, 100))       # on an exchange no bot owns
    return f


sys.modules["b_sig"] = new_fake()                         # f_engine.api is replaced per test

pmf = types.ModuleType("c_polymarket")
TITLE = re.compile(r"Will the (\w+) Party win the (.+?)\??$")
pmf.race_party = lambda t: (lambda m: (m.group(2), m.group(1)[0]) if m else None)(TITLE.match(t.strip()))
FAIR, SHIFT = {AK: 0.785, RI: 0.965, "1059": 0.93, "1060": 0.07}, [0.0]   # MA Gov legs: the NO set is EV-positive


def market_map(sig):
    with open(os.path.join(ROOT, "data", "pm_map.json")) as fh:
        mp = json.load(fh)
    for m in mp:
        m.setdefault("pm_ids", [m.pop("pm_id")] if "pm_id" in m else [])
    have = {m["ex"] for m in mp}
    for ex in FAIR:                                       # like PM_EXTRA in production (AK is a candidate sum)
        if ex not in have:
            mp.append(dict(ex=ex, race="r", party="D", mid="0", pm_ids=["x"]))
    return mp


def fairs(mp):
    out = {}
    for m in mp:
        f = FAIR.get(m["ex"])
        if f is None:
            b, a, _ = SIGPX.get(m["ex"], (None, None, None))
            if b is None or a is None:
                continue
            f = min(0.98, max(0.02, (b + a) / 2 + 0.004 + SHIFT[0]))
        out[m["ex"]] = (round(f - 0.005, 4), round(f + 0.005, 4), 50_000.0)
    return out


NEW_MAPS = {}                                             # sniper tests: title -> map entry


def map_one(sm, events=None):
    return NEW_MAPS.get(sm["title"])


import importlib.util  # noqa: E402
_spec = importlib.util.spec_from_file_location("real_c_polymarket", os.path.join(ROOT, "c_polymarket.py"))
_real_pm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_real_pm)
pmf.market_map, pmf.fairs, pmf.map_one, pmf.FairTracker = market_map, fairs, map_one, _real_pm.FairTracker
sys.modules["c_polymarket"] = pmf

import d_ledger  # noqa: E402
import e_bots as bots  # noqa: E402
import f_engine  # noqa: E402
import j_executor as jx  # noqa: E402
import l_intel  # noqa: E402

d_ledger.LEDGER_FILE = a_config.LEDGER_FILE               # whichever test module imported d_ledger first


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


ALL_ON = {l: dict(state="full") for l in ("position", "mm", "ladder", "pairs", "sniper", "leaders")}   # recycler: own tests (D1)
RECYCLER_ON = dict(position=dict(state="full"), recycler=dict(state="full"))


def stamp_():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def fresh_data(layers=ALL_ON):
    shutil.rmtree(TMP, ignore_errors=True)
    os.makedirs(TMP)
    with open(a_config.OVERRIDES_FILE, "w") as fh:
        json.dump(dict(layers=layers), fh)
    with open(a_config.POSITION_REF_FILE, "w") as fh:
        json.dump({"AK-GOV-D": {"ref": 12971, "side": "no"}, "RI-GOV-D": {"ref": 2137, "side": "yes"}}, fh)
    SHIFT[0] = 0.0


def engine(fake, only=None):
    """A new engine process on the same data folder (ledger.json as of its last save, journal from disk). Fair follows
    the fake Polymarket at once (the A6 smoothing and jump confirmation have their own test)."""
    f_engine.api = fake
    eng = f_engine.Engine(only)
    eng.ft.half_life, eng.ft.jump = 1e-9, 9.0
    return eng


def drain(eng, rounds=400):
    """Run the executor's two lanes inline until the queue is empty (or the write budget runs out)."""
    for _ in range(rounds):
        if not (eng.ex.queue or eng.ex.cancels):
            return
        eng.ex.step_cancels()
        eng.ex.step_placements()


def loop(eng, fake):
    fake._writes.clear()                                  # a minute passes between loops: fresh write budget
    fake.calls.clear()
    eng.step()
    drain(eng)


def resting(fake, ex, action=None):
    return sorted((o["price"], int(o["qty"])) for o in fake.open_orders() if o["ex"] == ex and (action is None or o["action"] == action))


def reducing(fake, ex):
    return sum(o["qty"] for o in fake.open_orders() if o["ex"] == ex and jx.reduces(o["side"], o["action"], fake.pos.get(ex, 0)))


def engine_order_problems(fake, ledger):
    """Every resting order that isn't one of the test's hand-placed ones was placed by some engine process. Problems:
    an orphan (the ledger doesn't know it: never cancelled again, and it eats I2 capacity), or two orders of one bot at
    one price (a duplicate). Pair sets are separate decisions (own set id): for them, a set id recorded twice or an
    order in two sets."""
    manual, seen, out = set(fake.manual.values()), {}, []
    ids = [b["id"] for b in ledger.baskets]
    out += [("set sent twice", i) for i in set(ids) if ids.count(i) > 1]
    legs = [o for b in ledger.baskets for o in b["orders"]]
    out += [("order in two sets", o) for o in set(legs) if legs.count(o) > 1]
    for o in fake.open_orders():
        if int(o["id"]) in manual:
            continue
        rec = ledger.orders.get(o["id"])
        if rec is None:
            out.append(("orphan", o["ex"], o["side"], o["action"], o["price"]))
            continue
        if rec["sub"].startswith("pairset:"):
            continue
        k = (rec["bot"], o["ex"], o["side"], o["action"], o["price"])
        if k in seen:
            out.append(("duplicate",) + k)
        seen[k] = o["id"]
    return out


class EngineCase(unittest.TestCase):
    def setUp(self):
        fresh_data()
        self.addCleanup(setattr, f_engine, "LIQUIDATE", f_engine.LIQUIDATE)
        f_engine.LIQUIDATE = {}                           # the AK sale (day 4 §3) has its own test
        for k in ("PNL_READ_S", "ORDERS_READ_S"):         # day 4 §4 read caching has its own test; the rest read fresh each loop
            self.addCleanup(setattr, f_engine, k, getattr(f_engine, k))
            setattr(f_engine, k, 0)
        self.out = contextlib.redirect_stdout(io.StringIO())   # the engine narrates every order; keep test output short
        self.out.__enter__()

    def tearDown(self):
        self.out.__exit__(None, None, None)

    def assert_exits_intact(self, fake, before):
        for i, o in before.items():
            self.assertIn(i, fake.book, f"exit/foreign order {i} {o} survived")
            self.assertEqual((fake.book[i]["price"], fake.book[i]["qty"]), (o["price"], o["qty"]))


class ScenarioTests(EngineCase):
    def test_full_scenario(self):
        fake = new_fake()
        eng = engine(fake)
        kinds = {k: sum(1 for b in eng.bots if b.kind == k) for k in ("position", "market")}
        self.assertEqual(kinds["position"], 2)
        self.assertGreater(kinds["market"], 220)
        self.assertGreater(len(eng.pairs), 100)
        self.assertEqual(len(eng.owner), len(eng.bots))

        loop(eng, fake)
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER, "AK ladder after the 0.30 tranche filled")
        self.assertIn((0.1, 3000), resting(fake, AK, "buy"), "AK add at 0.10")
        self.assertEqual(resting(fake, RI, "sell"), [(0.955, 1000), (0.955, 1137)],
                         "RI: the hand-placed sells already cover the holding; they are foreign, so never cancelled, "
                         "and the RI bot adds nothing on top of them")
        self.assertIn(fake.manual["lone"], fake.book, "manual order on an unowned exchange survives")
        for ex in (AK, RI, MA):
            self.assertLessEqual(reducing(fake, ex), abs(fake.pos[ex]), f"I2 on {ex}")
        self.assertEqual(fake.canonicalized, [], "no sell was ever turned into an opposite buy")
        self.assertFalse([e for e in eng.events if e["kind"] == "PLACE" and "RI-GOV-D" in e["text"]], "no RI churn")

        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
        self.assertGreater(len(mm), 50, "MM v2 quotes on the markets that pass the ROC filter")
        for o in mm:                                      # every MM entry rests below fair by e(p) and below the ask
            f = eng.side_fair(o["ex"], o["side"])
            self.assertLess(o["price"], f - 0.005 + 1e-9, o)

        # pair sets went out atomically, with a TTL, and were recorded once SIG acked every leg
        sets = [c for c in fake.calls if c[0] == "multileg"]
        self.assertTrue(sets and all(c[1][1] == a_config.PAIR_TTL_S for c in sets))
        self.assertTrue(eng.ledger.baskets and all(len(b["orders"]) == b["legs"] for b in eng.ledger.baskets))

        for _ in range(15):                               # ramp until the plan is fully resting
            loop(eng, fake)
            if "deferred 0" in eng.summary and "pair sets 0" in eng.summary and not eng.ex.queue:
                break
        st = read_json(a_config.STATE_FILE)
        self.assertGreaterEqual(st["risk"]["cash"] - st["risk"]["resting"], a_config.CASH_RESERVE, "hard cash floor")
        self.assertIn("executor", st)
        loop(eng, fake)
        self.assertEqual([c for c in fake.calls if c[0] != "multileg"], [], "identical loop: no churn")
        self.assertEqual(engine_order_problems(fake, eng.ledger), [])

        # a complete NO set fills on both legs -> locked; a set filling one leg only -> repaired at break-even
        bk = eng.ledger.baskets[0]
        for oid in bk["orders"]:
            if int(oid) in fake.book:
                fake.fill(oid, 100)
        b2 = eng.ledger.baskets[1] if len(eng.ledger.baskets) > 1 else None
        if b2:
            if int(b2["orders"][0]) in fake.book:
                fake.fill(b2["orders"][0], 200)
            b2["ts"] -= a_config.PAIR_TTL_S + 20
        loop(eng, fake)
        st = read_json(a_config.STATE_FILE)
        self.assertAlmostEqual(sum(p["locked"] for p in st["pairs"]), 100 * bk["margin"], places=2)
        if b2:
            self.assertTrue(any(c[0] == "batch" for c in fake.calls), "repair order sent for the short leg")
            self.assertTrue(b2.get("repairs"), "repair recorded on the basket")
        self.assertTrue(eng.ledger.arb_hold(), "market bots leave complete sets alone")

        # a write that lands but times out (UNKNOWN): the market freezes, the audit adopts what landed, no duplicate
        for i in [i for i, o in fake.book.items() if o["ex"] == AK]:
            del fake.book[i]                              # the AK orders get cancelled by hand on the website
        fake.faults = ["timeout_ok"]
        loop(eng, fake)
        self.assertTrue(eng.ex.is_frozen(AK), "UNKNOWN outcome: AK frozen until the audit")
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER)
        loop(eng, fake)
        self.assertFalse(eng.ex.is_frozen(AK), "the audit resolved it")
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER, "adopted, not placed twice")
        loop(eng, fake)
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER)
        self.assertEqual(engine_order_problems(fake, eng.ledger), [])
        self.assertTrue(all(fake.book[int(i)]["ex"] != AK or i in eng.ledger.orders for i in map(str, fake.book)),
                        "every AK order is ours again")

        # stop with SIG timing out: only engine quotes within 3c of the touch are cancelled, one DELETE each; position
        # exits and foreign orders are never touched; nothing is re-placed; never a tournament-wide cancel
        keep = {i: dict(o) for i, o in fake.book.items() if o["ex"] in (AK, RI) or i == fake.manual["lone"]}
        engine_open = {i for i, o in fake.book.items() if str(i) in eng.ledger.orders and o["ex"] not in (AK, RI)}
        fake.cancel_faults = ["cancel_timeout_fail", None, "cancel_timeout_ok"] * 30
        fake.open_orders = lambda: (_ for _ in ()).throw(TimeoutError("simulated SIG timeout"))   # book unreadable
        fake._writes.clear()
        fake.calls.clear()
        eng.pull_at_risk(limit_s=5)
        del fake.open_orders
        self.assertFalse([c for c in fake.calls if c[0] in ("cancel_all", "batch", "multileg")], "only single DELETEs")
        self.assert_exits_intact(fake, keep)
        cancelled = {int(c[1]) for c in fake.calls if c[0] == "cancel"}
        self.assertTrue(cancelled and cancelled <= engine_open, "only engine-owned orders were cancelled")
        self.assertTrue(eng.ex.halted and eng.ex.place([("x", "inventory", "mm", (MA, "yes", "buy", 0.1, 50))]) is None,
                        "after a stop nothing new is accepted")
        import importlib.util
        spec = importlib.util.spec_from_file_location("real_b_sig", os.path.join(ROOT, "b_sig.py"))
        real = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(real)
        with self.assertRaises(ValueError):
            real.cancel_all(None)                         # the real client refuses a tournament-wide cancel
        self.assertTrue(os.path.getsize(a_config.DASHBOARD_FILE) > 10_000)


class RestartTests(EngineCase):
    report = []

    def test_three_kill_restarts_mid_batch(self):
        """Kill the engine three times while a batch is half-landed (one earlier write acked after the ledger's last
        save), with prices moving while it is down: zero duplicates, I2 holds, and every order sent after a restart was
        wanted by a plan made after that restart."""
        fake = new_fake(writes_per_min=10_000)
        classic_capital(self, tmm=False)                  # restart safety needs many MM requotes (classic MM, 10k reserve)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        for r in range(3):
            SHIFT[0] = 0.02 * (1 if r % 2 == 0 else -1)   # fairs move: the bots re-quote, many writes
            real, writes = fake._place, []

            def dying(orders, ttl, key, atomic, k=r + 1):
                writes.append(key)
                if len(writes) == 1:                      # acked after the ledger's last save: only the journal knows
                    return real(orders, ttl, key, atomic)
                for o in orders[:k]:                      # the next write lands k orders, then the process dies
                    fake._land(o, ttl)
                writes.append((len(orders), min(k, len(orders))))
                raise Killed()
            fake._place = dying
            fake._writes.clear()
            eng.step()                                    # plans and saves the ledger; the lanes then send
            with self.assertRaises(Killed):
                drain(eng)
            fake._place = real
            SHIFT[0] += 0.01                              # prices moved while it was down

            restart_t = time.time()
            eng = engine(fake)                            # the new process
            report = dict(restart=r + 1, killed_write=f"{writes[-1][1]} of {writes[-1][0]} orders landed",
                          frozen_markets=len(eng.ex.frozen), recovered_from_journal=eng.ex.recovered)
            self.assertTrue(eng.ex.frozen, f"restart {r}: the half-landed write froze its markets")
            self.assertGreaterEqual(eng.ex.recovered, 1, f"restart {r}: the acked-but-unsaved write is ours again")
            wanted = set()
            for _ in range(3):
                loop(eng, fake)
                wanted |= {(b.name, b.ex, w["side"], w["action"], w["px"]) for b in eng.bots for w in b.view["wants"]}
            self.assertFalse(eng.ex.frozen, f"restart {r}: the audit resolved every UNKNOWN write")
            self.assertEqual(engine_order_problems(fake, eng.ledger), [], f"restart {r}: duplicates")
            for ex in {o["ex"] for o in fake.open_orders()}:
                self.assertLessEqual(reducing(fake, ex), abs(fake.pos.get(ex, 0)) + (2137 if ex == RI else 0), f"I2 on {ex}")
            self.assertEqual(fake.canonicalized, [])
            self.assertEqual(resting(fake, AK, "sell"), AK_LADDER)
            names, checked = {b.name for b in eng.bots}, 0
            for e in eng.journal.entries.values():        # nothing planned before the restart was sent after it
                if e.get("kind") == "batch" and e.get("ts", 0) >= restart_t:
                    for it in e["items"]:
                        if it["bot"] in names:
                            o = it["order"]
                            self.assertIn((it["bot"], o[0], o[1], o[2], o[3]), wanted, f"restart {r}: {it} priced before the restart")
                            checked += 1
            self.assertGreater(checked, 0)
            report.update(orders_sent_after_restart=checked, duplicates=0, resting_engine_orders=sum(
                1 for o in fake.open_orders() if o["id"] in eng.ledger.orders))
            RestartTests.report.append(report)


def wait_for(cond, timeout=15.0):
    end = time.time() + timeout
    while not cond():
        if time.time() > end:
            raise AssertionError("timed out waiting")
        time.sleep(0.01)


class LoadTests(EngineCase):
    def test_stop_under_load_keeps_exits(self):
        """Stop while a write is on the wire (batches 1.5 s, DELETEs 0.05 s, SIG 1-10 s per order), with DELETE timeouts
        and the book unreadable at stop time. The write on the wire lands normally / gets a 429 / times out but lands."""
        LoadTests.stop_report = []
        for outcome, budget in ((None, 10_000), (("429", 0.5), 10_000), ("timeout_ok", 10_000)):   # budget limits: scenario test
            with self.subTest(in_flight=outcome):
                fresh_data()
                fake = new_fake(writes_per_min=budget)
                eng = engine(fake)
                for _ in range(3):
                    loop(eng, fake)
                keep = {i: dict(o) for i, o in fake.book.items() if o["ex"] in (AK, RI) or i == fake.manual["lone"]}
                fake.real_delay = {"cancel": 0.05, "batch": 1.5, "multileg": 1.5}
                fake.processing = (0.5, 2.0) if outcome is None else (1.0, 10.0)   # 'lands': fast enough to finish in 60 s
                fake.faults = [outcome] if outcome else []
                fake.cancel_faults = ["cancel_timeout_fail", None, "cancel_timeout_ok"] * 30
                SHIFT[0] = 0.02                           # everything re-quotes: cancels and placements queue up
                fake._writes.clear()
                fake.calls.clear()
                eng.ex.start()
                try:
                    eng.step()
                    wait_for(lambda: [k for k, _ in list(fake.writing.values()) if k in ("batch", "multileg")])
                    stop_t = time.time()
                    wire = [it[3] for j in list(eng.ex.in_flight.values()) for it in j["items"]]
                    queued = sum(len(j["items"]) for _, _, j in list(eng.ex.queue))
                    fake.open_orders = lambda: (_ for _ in ()).throw(TimeoutError("simulated SIG timeout"))
                    eng.pull_at_risk(limit_s=6)
                finally:
                    eng.ex.stop(timeout=5)
                    with eng.ex.wake:
                        eng.ex.cancels.clear()
                    fake.__dict__.pop("open_orders", None)
                after = [k for k, t0, _ in fake.write_intervals() if t0 > stop_t]
                self.assertTrue(after and set(after) == {"cancel"}, f"after the stop began only DELETEs went out: {after}")
                self.assert_exits_intact(fake, keep)
                self.assertEqual(fake.canonicalized, [])
                cancelled = [str(c[1]) for c in fake.calls if c[0] == "cancel"]
                self.assertFalse({str(i) for i in keep} & set(cancelled), "no exit or foreign order was even attempted")
                self.assertTrue(all(i in eng.ledger.orders for i in cancelled), "only engine-owned orders")
                self.assertFalse([c for c in fake.calls if c[0] == "cancel_all"])
                rep = eng.stop_report
                landed = [i for k in rep["wire_keys"] for i in (eng.journal.entries.get(k, {}).get("placed") or []) if i]
                if outcome is None:                       # it landed: its quotes at the touch got DELETEs like the rest
                    hit = set(landed) & set(rep["at_risk"])
                    self.assertTrue(hit and hit <= set(cancelled), (landed, rep, cancelled[:10]))
                else:                                     # 429: never re-sent; UNKNOWN: frozen for the audit, not ours yet
                    self.assertEqual((landed, rep["late"]), ([], []))
                if outcome == "timeout_ok":
                    self.assertTrue({o[0] for o in wire} <= set(eng.ex.frozen))
                pulls = [e["text"] for e in eng.events if e["kind"] == "PULL"]
                LoadTests.stop_report.append(dict(in_flight_outcome=outcome or "lands", write_budget=budget,
                                                  on_the_wire=len(wire), placements_queued=queued,
                                                  writes_after_stop=dict((k, after.count(k)) for k in set(after)),
                                                  exits_and_foreign_kept=f"{len(keep)}/{len(keep)}", pull=pulls[-1]))

    def test_plan_cycle_is_not_blocked_by_writes(self):
        """The same plan cycle with SIG answering instantly, then with every batch taking 2 s on the wire in the
        executor's threads (DELETEs 0.05 s). Each loaded cycle starts while a batch is on the wire."""
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        base = []
        for s in (0.02, -0.02, 0.02):                     # baseline: instant writes, lanes idle during the plan
            SHIFT[0] = s
            t = time.time()
            eng.step()
            base.append(time.time() - t)
            drain(eng)
        fake.real_delay = {"cancel": 0.05, "batch": 2.0, "multileg": 2.0}
        fake.timeline.clear()
        eng.ex.start()
        loaded, overlap, kinds = [], [], []
        try:
            SHIFT[0] = -0.02
            eng.step()                                    # queues re-quotes: the lanes start sending
            for s in (0.02, -0.02, 0.02, -0.02):
                wait_for(lambda: [k for k, _ in list(fake.writing.values()) if k in ("batch", "multileg")])
                SHIFT[0] = s
                t = time.time()
                eng.step()
                t1 = time.time()
                loaded.append(t1 - t)
                spans = [(k, max(s0, t), min(e, t1)) for k, s0, e in fake.write_intervals() if e > t and s0 < t1]
                overlap.append(round(sum(e - s0 for _, s0, e in spans), 3))
                kinds.append(sorted({k for k, _, _ in spans}))
        finally:
            eng.ex.halt()
            with eng.ex.wake:
                eng.ex.cancels.clear()
            eng.ex.stop(timeout=3)
        self.assertTrue(all("batch" in k or "multileg" in k for k in kinds), f"a batch was on the wire in every cycle: {kinds}")
        self.assertLess(max(loaded), 1.0, f"plan cycles {loaded} vs 2 s writes: blocked")
        self.assertLess(max(loaded), max(base) + 0.75, f"loaded {loaded} vs baseline {base}")
        LoadTests.timing_report = dict(baseline_cycle_s=[round(x, 3) for x in base], loaded_cycle_s=[round(x, 3) for x in loaded],
                                       batch_write_s=2.0, write_seconds_on_the_wire_during_each_cycle=overlap, kinds=kinds)


class DashboardWiringTests(EngineCase):
    def test_gate_B_every_control_once_against_the_fake_exchange(self):
        """Gate B: the page loads in < 2 s, updates live, and every control works through the engine (flags, risk gate,
        executor), persisted to overrides.json and logged to NIGHT_LOG."""
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest("fastapi not installed")
        import g_dashboard as dashboard
        fake = new_fake()
        eng = engine(fake)
        for _ in range(3):
            loop(eng, fake)
        app = dashboard.create_app(lambda: eng.last_state, f_engine.EngineControls(eng), market_detail=eng.market_detail, token="tok")
        c = TestClient(app, base_url="http://127.0.0.1:8765")
        t0 = time.time()
        r = c.get("/?t=tok")
        self.assertEqual(r.status_code, 200)
        self.assertLess(time.time() - t0, 2.0)
        self.assertLess(len(dashboard.dumps(eng.last_state)), 300_000, "initial snapshot <= 300 KB")
        post = lambda action, args=None, confirm=True: c.post("/api/control?t=tok", json=dict(action=action, args=args or {}, confirm=confirm))
        keep = {i: dict(o) for i, o in fake.book.items() if o["ex"] in (AK, RI) or i == fake.manual["lone"]}

        self.assertEqual(post("pause", confirm=False).status_code, 200)
        self.assertTrue(eng.controls.paused)
        fake.calls.clear()
        SHIFT[0] = 0.02                                   # would re-quote everything, but new risk is paused
        loop(eng, fake)
        self.assertFalse([cl for cl in fake.calls if cl[0] in ("batch", "multileg")], "paused: no new orders")
        self.assertNotIn((0.1, 3000), resting(fake, AK, "buy"), "paused: the AK spike bid (an add) is pulled too")
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER, "exits stay")
        self.assertEqual(post("resume", confirm=False).status_code, 200)
        self.assertFalse(eng.controls.paused)
        for _ in range(10):                               # minutes pass: the pause's cancels go first (I6), then the adds
            loop(eng, fake)
            if not (eng.ex.cancels or eng.ex.queue):
                break
        self.assertIn((0.1, 3000), resting(fake, AK, "buy"), "resumed: the spike bid is back")

        self.assertEqual(post("layer_state", dict(layer="mm", state="pilot")).status_code, 200)
        self.assertEqual(eng.controls.state("mm"), "pilot")
        self.assertEqual(post("layer_state", dict(layer="position", state="off")).status_code, 400, "exits can't be turned off")
        self.assertEqual(post("layer_state", dict(layer="mm", state="pilot"), confirm=False).status_code, 400)

        self.assertEqual(post("layer_caps", dict(layer="ladder", caps=dict(resting=20000))).status_code, 200)
        self.assertEqual(eng.cap("ladder", "resting"), 20000)
        self.assertEqual(post("layer_caps", dict(layer="ladder", caps=dict(resting=999999))).status_code, 400, "bounded")

        self.assertEqual(post("market", dict(ex=MA, paused=True, exclude=["mm"])).status_code, 200)
        self.assertFalse(eng.controls.allows(MA, "recycler"))
        self.assertEqual(post("market", dict(ex="nope", paused=True)).status_code, 400)

        self.assertEqual(post("kappa", dict(value=0.02, minutes=30)).status_code, 200)
        self.assertEqual(eng.kappa(), (0.02, "dashboard override"))
        self.assertEqual(post("kappa", dict(value=0.05, minutes=30)).status_code, 400, "0..3c only")

        loop(eng, fake)                                   # quotes resting again before the pull
        keep = {i: dict(o) for i, o in fake.book.items() if o["ex"] in (AK, RI) or i == fake.manual["lone"]}
        quotes = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] not in
                  eng.REDUCING and eng.ledger.orders[o["id"]]["bot"] not in eng.position_names and o["action"] == "buy"]
        self.assertTrue(quotes)
        r = post("pull_quotes")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(eng.controls.paused, "pull also pauses new risk (otherwise the quotes come straight back)")
        for _ in range(20):                               # a minute of write budget at a time, until every DELETE is out
            fake._writes.clear()
            drain(eng)
            if not eng.ex.cancels:
                break
        left = [o for o in fake.open_orders() if o["id"] in {q["id"] for q in quotes}]
        self.assertEqual(left, [], "every engine quote cancelled")
        self.assert_exits_intact(fake, keep)

        self.assertEqual(post("read_only", dict(on=True), confirm=False).status_code, 200)
        self.assertEqual(post("pause", confirm=False).status_code, 400, "read-only: controls refused")
        self.assertEqual(post("read_only", dict(on=False), confirm=False).status_code, 200)

        self.assertEqual(c.get(f"/api/market/{AK}?t=tok").status_code, 200)
        with open(a_config.OVERRIDES_FILE) as fh:
            ov = json.load(fh)
        self.assertEqual((ov["layers"]["mm"]["state"], ov["layers"]["ladder"]["caps"]["resting"], ov["markets"][MA]["paused"]), ("pilot", 20000, True))
        with open(a_config.NIGHT_LOG_FILE) as fh:
            log = fh.read()
        for s in ("PAUSE new risk (dashboard", "RESUME new risk", "layer mm: full -> pilot (dashboard", "layer ladder caps",
                  f"market {MA}", "kappa override", "read-only mode on"):
            self.assertIn(s, log)
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from test_dashboard_server import render_views
        render_views(self, dashboard.render(eng.last_state))
        with open(a_config.DASHBOARD_FILE) as fh:
            render_views(self, fh.read())

    def test_live_host_check_uses_the_stable_name_not_the_wifi_hostname(self):
        from unittest import mock
        import socket
        f_engine.api = new_fake()

        def main_with(scutil):
            run = (mock.patch.object(f_engine.subprocess, "run", side_effect=OSError("no scutil")) if scutil is None else
                   mock.patch.object(f_engine.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=scutil + "\n")))
            with run, mock.patch.object(socket, "gethostname", return_value="wifi-10-43-123-14"), \
                    mock.patch.object(sys, "argv", ["f_engine.py", "--live"]), \
                    mock.patch.object(f_engine, "Engine", side_effect=SystemExit("engine would start")):
                with self.assertRaises(SystemExit) as cm:
                    f_engine.main()
            return str(cm.exception)

        self.assertEqual(main_with("MacBookPino"), "engine would start", "the Wi-Fi hostname changed; the machine did not")
        os.remove(a_config.PID_FILE)
        msg = main_with("someone-elses-mac")
        self.assertIn("refusing --live", msg)
        self.assertFalse(os.path.exists(a_config.PID_FILE), "no pid file, no lock taken")
        self.assertIn("refusing --live", main_with(None), "no scutil and no data/host_id: refuse")
        with open(a_config.HOST_ID_FILE, "w") as fh:
            fh.write("MacBookPino\n")
        self.assertEqual(main_with(None), "engine would start", "a VM identifies itself with data/host_id")


class PayloadTests(EngineCase):
    def test_payload_with_a_one_leg_basket(self):
        """Live ledgers hold sets with legs that never filled (no average price): the payload must still build."""
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        oid = next(iter(eng.ledger.orders))
        eng.ledger.baskets.append(dict(id="x:1", race="MA Gov", side="no", qty=100, margin=0.01, legs=2, orders=[oid, "missing"],
                                       repaired=True, ts=0))
        eng.ledger.orders[oid]["filled"], eng.ledger.orders[oid]["cost"] = 50.0, 20.0
        s = eng.dash_state()
        self.assertTrue(s["pairs"]["sets"])


class FairTrackerTests(unittest.TestCase):
    def test_A6_ewma_and_confirmed_jumps(self):
        ft = _real_pm.FairTracker()
        ft.update({"1": (0.49, 0.51, 5000)}, 0.0)
        self.assertAlmostEqual(ft.fair("1", 0.0), 0.50)
        ft.update({"1": (0.50, 0.52, 5000)}, 20.0)            # 1c move: smoothed (half-life 20 s -> half way)
        self.assertAlmostEqual(ft.fair("1", 20.0), 0.505)
        ft.update({"1": (0.54, 0.56, 5000)}, 21.0)            # >= 1.5c jump: not acted on yet
        self.assertAlmostEqual(ft.fair("1", 21.0), 0.505)
        ft.update({"1": (0.54, 0.56, 5000)}, 24.0)            # still there after 3 s: not yet
        self.assertAlmostEqual(ft.fair("1", 24.0), 0.505)
        ft.update({"1": (0.54, 0.56, 5000)}, 26.5)            # persisted 5 s: fair jumps to it
        self.assertAlmostEqual(ft.fair("1", 26.5), 0.55)
        ft.update({"1": (0.60, 0.62, 5000)}, 30.0)            # a spike that reverts is never acted on
        ft.update({"1": (0.54, 0.56, 5000)}, 36.0)
        self.assertAlmostEqual(ft.fair("1", 36.0), 0.55, places=3)
        ft.update({"1": (0.40, 0.50, 5000)}, 40.0)            # spread > 4c: no fair
        self.assertIsNone(ft.fair("1", 40.0))
        ft.update({"2": (0.49, 0.51, 500)}, 40.0)             # liquidity < 2k: no fair
        self.assertIsNone(ft.fair("2", 40.0))
        ft.update({"3": (0.49, 0.51, 5000)}, 40.0)
        self.assertIsNone(ft.fair("3", 101.0), "stale > 60 s: no fair")


class RecyclerTests(EngineCase):
    def test_recycler_with_D1_frees_cash_and_keeps_I2(self):
        fresh_data(layers=RECYCLER_ON)
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        for i in range(6):
            loop(eng, fake)
            for ex in (AK, MA):                           # I2 after every loop, while AK is re-sized
                self.assertLessEqual(reducing(fake, ex), abs(fake.pos[ex]) + 1e-9, f"loop {i}: I2 on {ex}")
        self.assertEqual(fake.canonicalized, [])
        self.assertEqual(eng.ledger.sub(AK, "recycle:ak-d1"), -5512.0, "D1 + D1b: 50% of 11,024 NO released to the recycler")
        self.assertEqual(eng.ledger.sub(AK, "manual"), -5512.0)
        self.assertEqual(resting(fake, AK, "sell"), AK_HALF, "ladder on the other 50%")
        self.assertIn((0.1, 3000), resting(fake, AK, "buy"), "spike bid kept")
        # AK NO fair 0.215, SIG NO bid 0.24 >= 0.22: sell the D1 tranche into the bid = buy YES at 0.76
        d1 = [o for o in fake.open_orders() if o["ex"] == AK and o["side"] == "yes" and o["id"] in eng.ledger.orders]
        self.assertEqual([(o["price"], o["qty"]) for o in d1], [(0.76, 5512.0)])
        self.assertEqual(eng.ledger.orders[d1[0]["id"]]["sub"], "recycle:ak-d1")
        # MA R inventory: 1,008 NO at fair 0.93; NO bid 0.90 < 0.935: rest at max(0.94, NO ask - tick 0.92) -> buy YES 0.06
        ma = [o for o in fake.open_orders() if o["ex"] == MA and o["id"] in eng.ledger.orders]
        self.assertEqual([(o["side"], o["price"], o["qty"]) for o in ma], [("yes", 0.06, 1008.0)])
        with open(a_config.NIGHT_LOG_FILE) as fh:
            self.assertIn("5,512 AK NO moved to sub-book recycle:ak-d1", fh.read())
        # the D1 order fills: realized cash, the tranche sub-book empties, AK exits untouched
        fake.fill(d1[0]["id"], 5512)
        loop(eng, fake)
        self.assertEqual(eng.ledger.sub(AK, "recycle:ak-d1"), 0.0)
        f = [x for x in eng.ledger.fills if x["order"] == d1[0]["id"]][0]
        self.assertGreaterEqual(1 - f["price"], 0.215 + 0.005 - 1e-9, "sold at >= fair + 0.5c (the recycler gate)")
        self.assertEqual(resting(fake, AK, "sell"), AK_HALF)
        st = eng.ex.stats()
        self.assertEqual(st["trims"].get("recycle", 0), 0, "no I2 trims hit")


class ControllerTests(EngineCase):
    def test_kill_rule_loss_stop_and_5xx_pause(self):
        classic_capital(self)
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        mm, seen = [], set()
        for o in fake.open_orders():                      # one MM entry per market (the turnover MM quotes both sides)
            if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm" and o["ex"] not in seen \
                    and not jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0)):
                seen.add(o["ex"])
                mm.append(o)
        for o in mm[:12]:                                 # MM gets filled, then fair moves hard against every fill
            fake.fill(o["id"], o["qty"])
            FAIR[o["ex"]] = 0.02 if o["side"] == "yes" else 0.98
        try:
            loop(eng, fake)
            loop(eng, fake)
        finally:
            for o in mm[:12]:
                FAIR.pop(o["ex"], None)
            FAIR.update({AK: 0.785, RI: 0.965, "1059": 0.93, "1060": 0.07})
        self.assertEqual(eng.controls.state("mm"), "off")
        self.assertIn("P&L at fair", eng.controls.layer("mm")["killed"]["reason"])
        loop(eng, fake)
        left = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
                and not jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0))]
        self.assertEqual(left, [], "a killed layer's entry quotes are pulled")
        bot_loss = sum(eng.fill_pnl_fair(f) for f in eng.fills_since(time.time() - 86400))
        if bot_loss < -a_config.LOSS_STOP:
            self.assertTrue(eng.controls.paused, "D5 loss stop")
        eng.controls.set_paused(False, "test")
        from unittest import mock
        bad = dict(read=dict(n=50, bad_rate=0.9), write=dict(n=0, bad_rate=0.0))
        with mock.patch.object(fake, "health", return_value=bad, create=True):
            eng.bad_since = time.time() - 601
            loop(eng, fake)
        self.assertTrue(eng.controls.paused, "SIG 5xx > 50% for 10 min pauses new risk")
        with open(a_config.NIGHT_LOG_FILE) as fh:
            log = fh.read()
        self.assertIn("KILL layer mm", log)
        self.assertIn("SIG 5xx/timeouts > 50% for 10 min", log)


def classic_capital(case, tmm=True):
    """MM mechanics tests: the pre-afternoon 10k opportunity reserve and 4% ROC (the fixture account can't fund a 25k
    reserve), optionally the classic MM v2 (requotes on every Polymarket move)."""
    for k, v in (("ROC_HOLD_MIN", a_config.ROC_MIN), ("OPPORTUNITY_RESERVE", 10_000), ("TMM_ENABLED", tmm)):
        case.addCleanup(setattr, f_engine, k, getattr(f_engine, k))
        setattr(f_engine, k, v)


def mid_market(eng, lo=0.35, hi=0.65):
    """A mapped market priced near the middle (ROC passes on both sides), no position, no pair."""
    legs = {ex for pb in eng.pairs for ex in pb.legs}
    for b in eng.bots:
        bid, ask, _ = eng.px.get(b.ex, (None, None, None))
        if b.kind == "market" and bid and ask and lo <= (bid + ask) / 2 <= hi and b.ex not in POS and b.ex in eng.fair:
            return b.ex
    raise AssertionError("no mid-priced market in the fixture")


def seed_ref(eng, ex, mid=None, minutes=20):
    b, a, _ = eng.px[ex]
    mid = (b + a) / 2 if mid is None else mid
    now = time.time()
    eng.mids[ex] = [(now - 60 * (minutes - i), mid, a - b) for i in range(minutes + 1)]


class LadderTests(EngineCase):
    def test_ladder_tranche_rebound_and_news_guard(self):
        fresh_data(layers=dict(position=dict(state="full"), ladder=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        ex = mid_market(eng)
        seed_ref(eng, ex)
        for _ in range(4):
            loop(eng, fake)
            seed_ref(eng, ex)
        lad = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders]
        self.assertTrue(lad and all(eng.ledger.orders[o["id"]]["layer"] == "ladder" for o in lad), lad)
        ref, f = eng.ref(ex), eng.fair[ex]
        for o in lad:                                     # entry = min(ref_side - off, fair_side - 2c*s(p)), below the ask
            fs = f if o["side"] == "yes" else 1 - f
            self.assertLessEqual(o["price"], fs - 0.02 * bots.s_of(fs) + 1e-9)
            self.assertTrue(eng.ledger.orders[o["id"]]["sub"].startswith("tranche:"))
        yes = max((o for o in lad if o["side"] == "yes"), key=lambda o: o["price"])
        sub = eng.ledger.orders[yes["id"]]["sub"]
        fake.fill(yes["id"], yes["qty"])                  # a sweep fills the top YES level
        for _ in range(3):
            loop(eng, fake)
            seed_ref(eng, ex)
        self.assertEqual(eng.ledger.tranches[sub]["state"], "open")
        rb = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["sub"] == sub
              and eng.ledger.orders[o["id"]]["layer"] == "rebound"]
        self.assertEqual(len(rb), 1, "one rebound for the tranche")
        self.assertEqual((rb[0]["side"], rb[0]["qty"]), ("no", yes["qty"]), "A1: the rebound is the complement buy")
        self.assertGreaterEqual(1 - rb[0]["price"], f - eng.kappa()[0] - 1e-9, "A2: never below fair - kappa")
        self.assertGreaterEqual(1 - rb[0]["price"], yes["price"] + 0.01 - 1e-9, "at least fill + 1c")
        FAIR[ex] = f - eng.ledger.tranches[sub]["off"]     # news: fair drops by more than off/2
        try:
            loop(eng, fake)
            loop(eng, fake)
        finally:
            FAIR.pop(ex, None)
        self.assertEqual(eng.ledger.tranches[sub]["state"], "news")
        self.assertEqual(eng.ledger.sub(ex, sub), 0)
        self.assertEqual(eng.ledger.sub(ex, "inventory"), yes["qty"], "the tranche went to the recycler's inventory")
        self.assertNotIn(int(rb[0]["id"]), fake.book, "its rebound was cancelled")


class CapTests(EngineCase):
    def test_ladder_resting_cap_counts_queued_orders_and_kappa_ignores_ladders(self):
        fresh_data(layers=dict(position=dict(state="full"), ladder=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.pilot_sets = {}
        loop(eng, fake)
        for b in eng.bots:
            if b.kind == "market" and b.ex in eng.fair:
                seed_ref(eng, b.ex)
        eng.controls.set_caps("ladder", dict(resting=2000), by="test")
        for _ in range(8):                                # each loop admits more; queued orders must count
            eng.step()                                    # (no drain: placements stay queued between loops)
            for b in eng.bots:
                if b.kind == "market" and b.ex in eng.fair:
                    seed_ref(eng, b.ex)
        used = eng.bucket_usage()
        self.assertLessEqual(used["ladder_resting"], 2000 + 1e-6, "queued ladder orders count against the resting cap")
        self.assertGreater(used["ladder_resting"], 0)
        k, why = eng.kappa()
        self.assertEqual(k, 0.0, f"ladder bids don't lock cash: kappa stays 0 ({why})")


class RequoteCapTests(EngineCase):
    def test_requote_cancels_are_capped_and_never_doubled(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        mm0 = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
        self.assertGreater(len(mm0), 40)
        SHIFT[0] = -0.01                                  # fair down 1c: every MM quote leaves its keep band
        fake.calls.clear()
        loop(eng, fake)
        cancels = [eng.ledger.orders[c[1]] for c in fake.calls if c[0] == "cancel"]
        urgent = [r for r in cancels if eng.owner[r["ex"]].urgency(r) > 0]
        self.assertEqual(len(eng.requotes), a_config.REQUOTE_CANCELS_PER_MIN, "non-urgent requote cancels capped per minute")
        self.assertEqual(len(cancels) - len(urgent), a_config.REQUOTE_CANCELS_PER_MIN, "picked-off quotes (at/above fair) always go")
        self.assertIn("deferred", eng.summary)
        self.assertNotIn("deferred 0 ", eng.summary, "the rest wait for the next minute")
        for e_ in {o["ex"] for o in fake.open_orders() if o["id"] in eng.ledger.orders}:
            mine = [o for o in fake.open_orders() if o["ex"] == e_ and o["id"] in eng.ledger.orders
                    and eng.ledger.orders[o["id"]]["layer"] == "mm"]
            for side in ("yes", "no"):
                self.assertLessEqual(len([o for o in mine if o["side"] == side and not jx.reduces(side, "buy", fake.pos.get(e_, 0))]), 1,
                                     f"one MM quote per side on {e_}: a deferred cancel defers its replacement")


class FloorTests(EngineCase):
    def test_hard_cash_floor_is_restored_after_fills(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        committed = eng.committed()
        self.assertGreater(committed, 1000)
        rich = fake.pnl()
        fake.pnl = lambda: dict(rich, totalAccountValue=rich["totalHoldingsValue"] + a_config.CASH_RESERVE + committed / 2)
        loop(eng, fake)                                   # cash fell (fills elsewhere): half the quotes no longer fit
        loop(eng, fake)
        self.assertLessEqual(eng.committed() + a_config.CASH_RESERVE,
                             fake.pnl()["totalAccountValue"] - fake.pnl()["totalHoldingsValue"] + 1e-6, "floor restored")
        self.assertTrue(any(e["kind"] == "FLOOR" for e in eng.events))


class CashGlitchTests(EngineCase):
    def test_one_high_cash_read_does_not_open_the_gates(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        tight = dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 500)
        fake.pnl = lambda: tight
        eng = engine(fake)
        for _ in range(3):
            loop(eng, fake)
        before = eng.committed()
        self.assertLessEqual(before, 500 + 1e-6)
        fake.pnl = lambda: dict(tight, totalAccountValue=tight["totalAccountValue"] + 10_000)   # one glitchy read
        loop(eng, fake)
        self.assertLessEqual(eng.committed(), 500 + 1e-6, "admission used the minimum of the last 3 cash reads")


class ReserveTests(EngineCase):
    def test_queued_exit_is_not_committed(self):
        fresh_data(layers=dict(position=dict(state="full")))
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        ex, pos = MA, eng.book[MA]["pos"]                 # MA Gov R: market-bot inventory (NO 1,008)
        exit_side = "no" if pos > 0 else "yes"           # buying the other side reduces the holding
        eng.ex.place([("t", "mm", "mm", (ex, exit_side, "buy", 0.5, 100))])
        self.assertEqual(eng.committed(), 0.0, "a queued exit is not an entry")
        self.assertEqual(eng.bucket_usage()["committed"], 0.0, "nor in the risk gate's bucket usage")
        eng.ex.place([("t", "mm", "mm", (ex, "yes" if exit_side == "no" else "no", "buy", 0.5, 100))])
        self.assertAlmostEqual(eng.committed(), 50.0, msg="a queued entry is")
        self.assertAlmostEqual(eng.bucket_usage()["committed"], 50.0)

    def test_opportunity_reserve_tiers(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full"), sniper=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + a_config.OPPORTUNITY_RESERVE + 500)
        eng = engine(fake)
        for _ in range(3):
            loop(eng, fake)
        self.assertLessEqual(eng.committed(), 500 + 1e-6, "MM may only commit cash above floor + reserve")
        b = next(b for b in eng.bots if b.kind == "market" and b.ex in eng.fair and b.ex not in POS)
        f = eng.side_fair(b.ex, "yes")
        big = dict(layer="take", side="yes", px=round(f - 0.09, 3), qty=10_000, sub="sniper")
        small = dict(layer="take", side="yes", px=round(f - 0.05, 3), qty=10_000, sub="sniper")
        for w in (big, small):
            w.update(lo=w["px"] - 0.01, hi=w["px"])
            w["qty"] = int(3000 / w["px"])
        used = eng.bucket_usage()
        cash = min(eng.cash_hist)
        self.assertEqual(len(eng.admit(b, [dict(small)], dict(used, committed=used["committed"]), cash)), 0,
                         "an ordinary take can't dip into the reserve")
        self.assertEqual(len(eng.admit(b, [dict(big)], dict(used, committed=used["committed"]), cash)), 1,
                         "a take >= 8c through fair may use the reserve, down to the floor")
        # cash falls under floor + reserve: MM quotes are pulled (non-reserve first)
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 2000)
        for _ in range(4):
            loop(eng, fake)
        mm_left = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
                   and not jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0))]
        self.assertEqual(mm_left, [], "no MM entry quotes while cash is below floor + reserve")


class ReleaseTests(EngineCase):
    def test_release_sells_cheap_to_free_holdings_into_the_bid(self):
        fresh_data(layers=dict(position=dict(state="full"), recycler=dict(state="full"), release=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        probe = engine(fake)
        loop(probe, fake)
        ex = mid_market(probe)                            # a mid-priced market: selling YES into the bid costs ~2%
        fresh_data(layers=dict(position=dict(state="full"), recycler=dict(state="full"), release=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        fake.pos[ex] = 2000
        fake.avg_cost[ex] = 0.40                          # day 4 §3: released because the SIG bid is above its entry cost
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 1000)
        eng = engine(fake)
        eng.ft.half_life = 1e-9
        loop(eng, fake)
        eng.release_t = 0
        loop(eng, fake)
        side, bid = "yes", fake.px[ex][0]
        cost = eng.cost_to_free(ex, side)
        self.assertTrue(0 <= cost <= eng.release_max_cost(), cost)
        allrel = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "release"]
        rel = [o for o in allrel if o["ex"] == ex]
        self.assertEqual([(o["ex"], o["side"], o["price"]) for o in rel], [(ex, "no", round(1 - bid, 3))], "sell YES at the bid")
        self.assertLessEqual(sum(o["qty"] for o in rel), 1000, "<= top-of-book depth (fake: 1,000)")
        self.assertFalse([o for o in allrel if o["ex"] in (AK, RI)], "manual holdings are kept")
        for o in allrel:                                  # anything else released costs <= the max (5% now)
            held = "yes" if o["side"] == "no" else "no"
            self.assertLessEqual(eng.cost_to_free(o["ex"], held), eng.release_max_cost() + 1e-9, o)
        rec = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders
               and eng.ledger.orders[o["id"]]["layer"] == "recycle"]
        self.assertLessEqual(sum(o["qty"] for o in rec) + sum(o["qty"] for o in rel), 2000, "the recycler steps aside (I2)")
        fake.fill(rel[0]["id"], rel[0]["qty"])
        loop(eng, fake)
        r = eng.released_today_view()
        self.assertAlmostEqual(r["sig"], rel[0]["qty"] * bid, places=2)
        self.assertTrue(0 <= r["avg_cost"] <= 0.031, r)
        with open(a_config.NIGHT_LOG_FILE) as fh:
            pass
        self.assertTrue(any(e["kind"] == "RELEASE" for e in eng.events))

    def test_release_off_does_nothing(self):
        fresh_data(layers=dict(position=dict(state="full"), recycler=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 1000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        self.assertEqual(eng.release_now, {})


class BreakerTests(EngineCase):
    def test_correlated_fills_trip_the_breaker(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
              and not jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0))]
        pro_d, seen = [], set()
        for o in mm:
            if eng.pol(o["ex"], o["side"]) == "D" and o["ex"] not in seen:
                seen.add(o["ex"])
                pro_d.append(o)
        self.assertGreaterEqual(len(pro_d), 5)
        for o in pro_d[:5]:
            fake.fill(o["id"], o["qty"])
        loop(eng, fake)
        self.assertEqual(eng.breaker, {}, "correlated fills at a flat Polymarket fair: MM flow, not news")
        for o in pro_d[:5]:                               # news: Polymarket moves against what we bought
            f = eng.fair[o["ex"]]
            FAIR[o["ex"]] = f - 0.03 if o["side"] == "yes" else f + 0.03
        try:
            loop(eng, fake)
        finally:
            for o in pro_d[:5]:
                FAIR.pop(o["ex"], None)
            FAIR.update({AK: 0.785, RI: 0.965, "1059": 0.93, "1060": 0.07})
        self.assertGreater(eng.breaker.get("D", 0), time.time())
        self.assertTrue(any("BREAKER pro-D" in a for a in eng.alerts))
        for _ in range(3):
            loop(eng, fake)
        left = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
                and eng.pol(o["ex"], o["side"]) == "D" and not jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0))]
        self.assertEqual(left, [], "no pro-D MM entries while the breaker holds")
        self.assertTrue([o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.pol(o["ex"], o["side"]) == "R"],
                        "pro-R quotes continue")


class BreakerCountTests(EngineCase):
    def test_one_busy_market_cannot_trip_the_breaker(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
              and not jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0))]
        d = [o for o in mm if eng.pol(o["ex"], o["side"]) == "D"]
        r = [o for o in mm if eng.pol(o["ex"], o["side"]) == "R"]
        busy = d[0]
        picks = [d[1], d[2], r[0], r[1]]                  # 5 markets: 3 lean D, 2 lean R (60%)
        picks = [o for o in picks if o["ex"] != busy["ex"]]
        for _ in range(8):                                # ... but one D market fills over and over
            fake.fill(busy["id"], 10)
        for o in picks:
            fake.fill(o["id"], o["qty"])
        loop(eng, fake)
        self.assertEqual(eng.breaker, {}, "60% of markets is not a correlated move, however many fills one market has")


class PairsTests(EngineCase):
    def setUp(self):
        super().setUp()
        fresh_data(layers=dict(position=dict(state="full"), pairs=dict(state="full")))

    def ma_set(self, eng, fake):
        loop(eng, fake)
        sets = [b for b in eng.ledger.baskets if b["race"] == "Massachusetts Governor"]
        self.assertTrue(sets, [e["text"] for e in eng.events if e["kind"] == "PAIR"][-3:])
        return sets[-1]

    def test_fill_loop_sends_exits_before_new_sets(self):
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.rt_event("fills", set(), time.time())        # woken by our own fill: no new-set book reads this loop
        fake.book_reads = 0
        loop(eng, fake)
        self.assertEqual(fake.book_reads, 0)
        self.assertFalse([b for b in eng.ledger.baskets if b["race"] == "Massachusetts Governor"])
        loop(eng, fake)                                  # the 2 re-polls of a fill REST hasn't shown yet are fill loops too
        loop(eng, fake)
        self.ma_set(eng, fake)                           # then the loop screens and sends as usual

    def test_fill_notification_before_rest_repolls_twice(self):
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        t_ev = time.time()
        eng.rt_event("fills", set(), t_ev)               # REST has no new fill yet
        for n in (1, 2):
            loop(eng, fake)
            self.assertEqual(eng.fill_retry.get(t_ev), n)
            self.assertIn(t_ev, [t_ for t_, k, _ in eng.rt_pending if k == "fills"], "re-polled in 2 s")
        loop(eng, fake)
        self.assertNotIn(t_ev, [t_ for t_, k, _ in eng.rt_pending], "gives up after 2 re-polls")

    def test_ev_positive_set_then_early_unwind(self):
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        bk = self.ma_set(eng, fake)
        self.assertEqual((bk["side"], bk["legs"]), ("no", 2))
        self.assertGreaterEqual(bk["margin"], 0.02 - 1e-9, "A12: >= 2c locked")
        for oid in bk["orders"]:
            fake.fill(oid, bk["qty"])
        loop(eng, fake)
        self.assertEqual(self.ledger_matched(eng, bk), bk["qty"])
        fake.px["1059"], fake.px["1060"] = (0.92, 0.93, 0.93), (0.06, 0.07, 0.07)   # YES asks sum 1.00 <= n-1+kappa
        loop(eng, fake)
        un = [c for c in fake.calls if c[0] == "multileg"]
        self.assertTrue(un, "atomic unwind sent")
        self.assertIn(bk["state"], ("unwinding", "unwound"))
        self.assertTrue(any(e["kind"] == "UNWIND" for e in eng.events))

    def test_uneven_unwind_leaves_residue_to_the_recycler(self):
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        bk = self.ma_set(eng, fake)
        for oid in bk["orders"]:
            fake.fill(oid, bk["qty"])
        loop(eng, fake)
        fake.px["1059"], fake.px["1060"] = (0.92, 0.93, 0.93), (0.06, 0.07, 0.07)
        loop(eng, fake)
        un = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "unwind"]
        self.assertEqual(len(un), 2)
        fake.fill(un[0]["id"], un[0]["qty"])              # one leg unwinds fully, the other not at all
        fake.book.pop(int(un[1]["id"]))                   # ... and expires
        bk["unwind_t"] -= a_config.PAIR_TTL_S + 20
        eng.ex.recent.clear()
        loop(eng, fake)
        self.assertEqual(bk["state"], "closed")
        other = eng.ledger.orders[un[1]["id"]]["ex"]
        self.assertEqual(eng.ledger.sub(other, f"pairset:{bk['id']}"), 0, "the un-hedged leg left the set")
        self.assertLess(eng.ledger.sub(other, "inventory"), 0, "... to inventory (recycler)")
        self.assertTrue(any("one-leg residue to the recycler" in e["text"] for e in eng.events))

    @staticmethod
    def ledger_matched(eng, bk):
        return eng.ledger.matched(bk)

    def test_repair_sequence_and_residue_to_inventory(self):
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        bk = self.ma_set(eng, fake)
        fake.fill(bk["orders"][0], 400)                   # only one leg fills
        for oid in bk["orders"]:
            fake.book.pop(int(oid), None)                 # the rest expires
        bk["ts"] -= a_config.PAIR_TTL_S + 20
        loop(eng, fake)
        self.assertEqual(bk.get("repair_state"), "complete", "1: complete the short leg")
        rep = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "repair"]
        self.assertEqual(len(rep), 1)
        fake.book.pop(int(rep[0]["id"]))                  # 2: it expired unfilled (REST shows it gone)
        bk["repair_t"] -= 20
        eng.ex.recent.clear()                             # a minute later: no longer 'just acked'
        loop(eng, fake)
        self.assertEqual(bk.get("repair_state"), "unwind", "3: unwind the long leg's excess at its average")
        rep2 = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "repair"]
        self.assertEqual(len(rep2), 1)
        fake.book.pop(int(rep2[0]["id"]))
        bk["repair_t"] -= 20
        eng.ex.recent.clear()
        loop(eng, fake)
        self.assertEqual(bk.get("repair_state"), "done", "4: residue to the recycler")
        leg_ex = eng.ledger.orders[bk["orders"][0]]["ex"]
        self.assertEqual(eng.ledger.sub(leg_ex, f"pairset:{bk['id']}"), 0)
        self.assertTrue(any("residue" in e["text"] for e in eng.events))

    def test_set_with_unknown_write_is_adopted_as_a_set(self):
        fresh_data(layers=dict(position=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)                                   # exits resting first
        eng.controls.set_layer("pairs", "full", "test")
        fake.faults = ["timeout_ok"]                      # the set's atomic write lands, the client hears nothing
        loop(eng, fake)
        self.assertTrue(eng.ex.frozen, "UNKNOWN multileg: legs frozen")
        loop(eng, fake)
        ad = [b for b in eng.ledger.baskets if b.get("adopted")]
        self.assertEqual(len(ad), 1, "recorded as a set after the audit")
        self.assertEqual(len(ad[0]["orders"]), 2)


class SniperTests(EngineCase):
    def test_new_market_mapped_is_sniped_unmapped_is_alerted(self):
        fresh_data(layers=dict(position=dict(state="full"), sniper=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        good = dict(id="9001", title="Will the Democratic Party win the Test Race?", status="open", exchanges=[dict(id="7777")])
        bad = dict(id="9002", title="Will the Republican Party win the Unknown Race?", status="open", exchanges=[dict(id="7778")])
        fake.markets += [bad, good]
        fake.px["7777"], fake.px["7778"] = (0.40, 0.45, 0.42), (0.40, 0.45, 0.42)
        NEW_MAPS[good["title"]] = dict(race="Test Race", party="D", mid="9001", ex="7777", pm_ids=["p1"])
        FAIR["7777"] = 0.60
        try:
            eng.scan_t = 0
            loop(eng, fake)
            loop(eng, fake)
        finally:
            NEW_MAPS.clear()
            FAIR.pop("7777", None)
        self.assertIn("7777", eng.owner)
        self.assertIn("7778", eng.unmapped_new, "unmapped: a bot for 0.05 fat-finger bids only, mapping retried every 10 min")
        self.assertFalse([o for o in fake.open_orders() if o["ex"] == "7778" and o["id"] in eng.ledger.orders],
                         "no fair: no takes (fat-finger is off in this test)")
        takes = [o for o in fake.open_orders() if o["ex"] == "7777" and o["id"] in eng.ledger.orders]
        self.assertTrue(takes and takes[0]["side"] == "yes" and takes[0]["price"] <= 0.60 - 0.04 * bots.s_of(0.6) + 1e-9, takes)
        notional = sum(o["qty"] * o["price"] for o in takes)
        self.assertLessEqual(notional, eng.cap("sniper", "per_market") + 1e-6)
        self.assertGreater(notional, 1000, "a new market gets the whole per-market cap, not the 150 gap-take size")
        self.assertTrue(any("NEW MARKET UNMAPPED" in a for a in eng.alerts))


class TakeCapTests(EngineCase):
    def test_sniper_cap_is_per_race(self):
        fresh_data(layers=dict(position=dict(state="full"), sniper=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        pb = next(p for p in eng.pairs if len(p.legs) == 2 and not set(p.legs) & set(POS))
        d, r = pb.legs
        for ex, side_fair in ((d, 0.70), (r, 0.30)):      # both legs gapped: buying D YES and R NO is one bet
            FAIR[ex] = side_fair
            fake.px[ex] = (round(side_fair - 0.10, 3), round(side_fair - 0.08, 3), side_fair - 0.09) if ex == d else \
                          (round(side_fair + 0.08, 3), round(side_fair + 0.10, 3), side_fair + 0.09)
        try:
            for _ in range(6):
                loop(eng, fake)
                for o in [o for o in fake.open_orders() if o["ex"] in (d, r) and o["id"] in eng.ledger.orders]:
                    fake.fill(o["id"], o["qty"])          # the gap refills: every take fills
                eng.ex.recent.clear()                     # a minute passes between takes
        finally:
            FAIR.pop(d, None)
            FAIR.pop(r, None)
        spent = sum(f["qty"] * f["price"] for f in eng.ledger.fills if f["ex"] in (d, r) and f["layer"] == "take")
        self.assertGreater(spent, 500)
        self.assertLessEqual(spent, eng.cap("sniper", "per_market") + 1e-6, "one race, one per-market cap")


class FatFingerTests(EngineCase):
    LAYERS = dict(position=dict(state="full"), fatfinger=dict(state="full"))

    def setUp(self):
        super().setUp()
        self.addCleanup(setattr, f_engine, "FAT_TIER_A_ONLY", f_engine.FAT_TIER_A_ONLY)
        f_engine.FAT_TIER_A_ONLY = False                  # every eligible side (tier-A-only has its own test)

    def rig(self, cash_above_floor=400_000):
        """cash 400k above the floor: the Oct 5 22:50 entry budget (2 x free, fat-finger last) never binds here; CashGuardTests covers it."""
        fresh_data(layers=self.LAYERS)
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0                          # live-like pacing (the fake's EWMA starts at 60/min)
        return fake, eng

    def settle(self, eng, fake, n=12):
        for _ in range(n):
            eng.ex.capacity = 6000.0
            loop(eng, fake)

    def fat_orders(self, eng, fake, ex=None):
        return [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "fatfinger"
                and (ex is None or o["ex"] == ex)]

    def test_levels_sides_and_queue_step_up(self):
        fake, eng = self.rig()
        self.settle(eng, fake)
        mine = self.fat_orders(eng, fake)
        self.assertTrue(mine)
        for o in mine:                                    # only sides worth >= 0.35; never above 25% of the side's fair
            fs = eng.side_fair(o["ex"], o["side"])
            if fs is not None:
                self.assertGreaterEqual(fs, a_config.FAT_MIN_FAIR - 1e-9, o)
                self.assertLessEqual(o["price"], min(0.12, 0.25 * fs) + 1e-9, o)
        self.assertFalse([o for o in mine if o["ex"] in (AK, RI)], "never on manual positions")
        # queue step-up: someone already bids 0.10 (inside the 0.12 level's band (0.08, 0.12]) -> our level goes to 0.105
        ex, side = mine[0]["ex"], mine[0]["side"]
        yb = [(0.10, 500.0)] if side == "yes" else []
        ya = [(0.90, 500.0)] if side == "no" else []
        eng.fat_book[ex] = (time.time(), yb, ya)
        lv = eng.fat_levels(ex, side, eng.side_fair(ex, side), 1.0, "A")
        self.assertIn(0.105, [px for _, px, _ in lv])
        self.assertTrue(all(px <= 0.12 + 1e-9 for _, px, _ in lv))
        self.assertTrue(all(px <= 0.075 + 1e-9 for _, px, _ in eng.fat_levels(ex, side, 0.30, 1.0, "A")), "cap: 25% of a 0.30 side")

    def test_queue_read_ignores_our_own_bids(self):
        fake, eng = self.rig()
        self.settle(eng, fake)
        o = next(x for x in self.fat_orders(eng, fake) if eng.side_fair(x["ex"], x["side"]) and eng.side_fair(x["ex"], x["side"]) >= 0.48)
        ex, side = o["ex"], o["side"]
        ours = [x for x in self.fat_orders(eng, fake, ex) if x["side"] == side]
        yes = lambda x: x["price"] if side == "yes" else round(1 - x["price"], 3)
        book = [(yes(x), x["qty"]) for x in ours] + [((0.05 if side == "yes" else 0.95), 500.0)]
        real = fake.orderbook
        fake.orderbook = lambda e_, depth=10: ((sorted([b_ for b_ in book if b_[0] < 0.5], reverse=True), []) if side == "yes"
                                               else ([], sorted(b_ for b_ in book if b_[0] > 0.5))) if e_ == ex else real(e_, depth)
        eng.fat_book[ex] = (0, [], [])
        loop(eng, fake)
        _, yb, ya = eng.fat_book[ex]
        self.assertEqual([p for p, _ in (yb or ya)], [0.05 if side == "yes" else 0.95], "only the rival is left in the queue")
        self.assertEqual(sorted(px for _, px, _ in eng.fat_levels(ex, side, eng.side_fair(ex, side), 1.0, "A")), [0.12],
                         "only the 0.12 level (Oct 3); a rival at 0.05 is below its band (0.08, 0.12]")

    def test_optional_reads_wait_for_spare_budget(self):
        fake, eng = self.rig()
        fake.spare = False                                # SIG reads near the limit: the loop's own reads only
        fake.book_reads = 0
        self.settle(eng, fake, 4)
        self.assertEqual(fake.book_reads, 0, "no queue, depth or pair book reads without spare budget")
        self.assertTrue(self.fat_orders(eng, fake), "levels still go out, at the level price, without a queue read")
        fake.spare = True
        self.settle(eng, fake, 2)
        self.assertGreater(fake.book_reads, 0)

    def test_max_sweep_fills_best_price_first_and_logs_value(self):
        fake, eng = self.rig()
        self.settle(eng, fake)
        ex = next(o["ex"] for o in self.fat_orders(eng, fake) if eng.side_fair(o["ex"], o["side"]) and eng.side_fair(o["ex"], o["side"]) > 0.6)
        side = next(o["side"] for o in self.fat_orders(eng, fake, ex))
        levels = sorted((o for o in self.fat_orders(eng, fake, ex) if o["side"] == side), key=lambda o: -o["price"])
        left = 100_000                                    # a "Max" market order sweeps the book: best price first
        for o in levels:
            take = min(left, o["qty"])
            fake.fill(o["id"], take)
            left -= take
        loop(eng, fake)
        ev = [e for e in eng.events if e["kind"] == "FATFINGER"]
        self.assertEqual(len(ev), len(levels))
        fs = eng.side_fair(ex, side)
        want = sum((fs - o["price"]) * o["qty"] for o in levels)
        got = sum(float(e["text"].rsplit("value captured ", 1)[1].replace(",", "")) for e in ev)
        self.assertAlmostEqual(got, want, delta=len(levels))
        with open(a_config.NIGHT_LOG_FILE) as fh:
            self.assertIn("FATFINGER FILL", fh.read())

    def test_reserve_must_cover_the_worst_single_market_fill(self):
        fake, eng = self.rig(cash_above_floor=100)        # reserve below any side's worst case (unmapped half size: 180)
        self.settle(eng, fake, 4)
        self.assertEqual(self.fat_orders(eng, fake), [])
        self.assertTrue(any(v == "reserve not funded" for v in (eng.coverage or {}).get("reasons", {}).values()))

    def test_news_guard_and_breaker(self):
        fake, eng = self.rig()
        self.settle(eng, fake)
        o = next(o for o in self.fat_orders(eng, fake) if eng.side_fair(o["ex"], o["side"]) and eng.side_fair(o["ex"], o["side"]) > 0.6)
        ex, side = o["ex"], o["side"]
        f = eng.fair[ex]
        FAIR[ex] = f - 0.20 if side == "yes" else f + 0.20  # the side's fair falls 20c within minutes
        try:
            loop(eng, fake)
            loop(eng, fake)
        finally:
            FAIR.pop(ex, None)
            FAIR.update({AK: 0.785, RI: 0.965, "1059": 0.93, "1060": 0.07})
        self.assertFalse([x for x in self.fat_orders(eng, fake, ex) if x["side"] == side], "news guard pulled the side")
        self.assertTrue(any(e["kind"] == "NEWS" for e in eng.events))
        # breaker: fat-finger fills in 5 markets, one political direction, within 10 min
        fake2, eng2 = self.rig()
        self.settle(eng2, fake2)
        picks, seen = [], set()
        for x in self.fat_orders(eng2, fake2):
            if eng2.pol(x["ex"], x["side"]) == "D" and x["ex"] not in seen:
                seen.add(x["ex"])
                picks.append(x)
        self.assertGreaterEqual(len(picks), 5)
        for x in picks[:5]:
            fake2.fill(x["id"], 10)
        loop(eng2, fake2)
        self.assertGreater(eng2.breaker.get(("fat", "D"), 0), time.time())

    def test_pilot_gate(self):
        fake, eng = self.rig()
        self.settle(eng, fake, 3)
        c = eng.controls
        now = time.time()
        c.layer("fatfinger").update(state="pilot", since=now - 2 * 3600 - 60)
        eng.fat_sent.clear()
        fill = dict(key="k1", order="o1", ex="1000", side="yes", qty=100.0, price=0.60, action="buy", realized=0.0,
                    t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 600)), bot="b", layer="fatfinger", fair=0.80,
                    before_engine=False, edge=None, sig_edge=None, sig_mid=None)
        eng.ledger.fills.append(fill)                                    # filled 20c from fair (in the ledger): the gate holds
        eng.controller(now)
        self.assertEqual(c.state("fatfinger"), "pilot")
        fill["price"] = 0.05                                             # a real fat-finger catch: promote
        eng.controller(now)
        self.assertEqual(c.state("fatfinger"), "full")

    def test_full_cap_covers_95_percent(self):
        fake, eng = self.rig(cash_above_floor=400_000)       # under the Oct 5 entry budget (2 x free), 40k free would allow 80k of levels
        eng.controls.layer("fatfinger")["state"] = "full"
        self.settle(eng, fake, 25)
        cov = eng.dash_state()["coverage"]
        self.assertGreaterEqual(cov["pct"], 95.0, cov["uncovered"][:5])

    def test_cash_short_balance_below_zero_twice(self):
        fake, eng = self.rig()
        self.settle(eng, fake, 3)
        self.assertTrue(self.fat_orders(eng, fake))
        exits = [o for o in fake.open_orders() if o["ex"] == AK and o["action"] == "sell"]
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] - 500)
        loop(eng, fake)
        self.assertIsNone(eng.cash_short, "one negative read is not enough")
        loop(eng, fake)
        self.assertIsNotNone(eng.cash_short)
        self.assertTrue(eng.controls.paused, "new risk paused")
        for _ in range(3):
            loop(eng, fake)
        self.assertEqual(self.fat_orders(eng, fake), [], "entry orders cancelled")
        self.assertEqual(sorted((o["price"], o["qty"]) for o in fake.open_orders() if o["ex"] == AK and o["action"] == "sell"),
                         sorted((o["price"], o["qty"]) for o in exits), "position exits untouched")
        self.assertTrue(any(e["kind"] == "CASHSHORT" for e in eng.events))
        self.assertTrue(os.path.exists(os.path.join(TMP, "cash_short.jsonl")))

    def test_cash_short_on_a_funds_reject(self):
        fake, eng = self.rig()
        self.settle(eng, fake, 2)
        eng.ex.cash_reject('{"error":{"code":"INSUFFICIENT_BALANCE","message":"Insufficient buying power"}}')
        eng.ex.cash_reject('{"error":{"code":"PRICE_INVALID"}}')     # other rejects don't count
        loop(eng, fake)
        self.assertIsNotNone(eng.cash_short)
        self.assertEqual(len(eng.cash_short["why"]), 1)

    def test_vanished_remainder_never_pauses_on_its_own(self):
        # day 4 §1.1: Montana Senate R 08:52: the bid expired at its 6 h TTL ("vanished after 360 min") and paused the engine
        fake, eng = self.rig()
        self.settle(eng, fake, 3)
        o = self.fat_orders(eng, fake)[0]
        rec = eng.ledger.orders[o["id"]]
        rec["ts"] -= rec["ttl"] - 60                      # one minute from its TTL
        fake.fill(o["id"], 100)
        with fake.lock:
            fake.book.pop(next(k for k in fake.book if str(k) == o["id"]))   # expired
        for _ in range(3):
            loop(eng, fake)
        self.assertIsNone(eng.cash_short, "expiry at the TTL is not a cash short")
        self.assertFalse(eng.controls.paused)
        self.assertFalse(any(e["kind"] == "CASHCHECK" for e in eng.events), "an expiry needs no order read")
        # a remainder gone well before its TTL: one order read, logged as evidence; balance fine -> still no pause
        o2 = self.fat_orders(eng, fake)[0]
        eng.ledger.orders[o2["id"]]["ts"] -= 600
        fake.fill(o2["id"], 100)
        with fake.lock:
            fake.book.pop(next(k for k in fake.book if str(k) == o2["id"]))
        for _ in range(3):
            loop(eng, fake)
        self.assertTrue(any(e["kind"] == "CASHCHECK" and "no pause" in e["text"] for e in eng.events))
        self.assertIsNone(eng.cash_short)
        self.assertFalse(eng.controls.paused)

    def test_cash_short_auto_resumes_when_cash_is_back(self):
        fake, eng = self.rig()
        self.settle(eng, fake, 2)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] - 500)
        loop(eng, fake)
        loop(eng, fake)
        self.assertTrue(eng.controls.paused)
        self.assertEqual(eng.controls.d["auto_pause"]["kind"], "cash_short")
        self.assertEqual(eng.controls.d.get("decisions"), [], "safety stops don't queue decisions")
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 6_000)
        eng.controls.d["auto_pause"]["t"] -= 400         # 5 min after the pause
        for _ in range(3):
            loop(eng, fake)
        self.assertFalse(eng.controls.paused, "cash >= floor + 5k for 2 loops: resumed by itself")
        self.assertTrue(any(e["kind"] == "PAUSE" and "AUTO-RESUME" in e["text"] for e in eng.events))

    def test_gates_hold_on_flag_or_blind_fills(self):
        fake, eng = self.rig()
        self.settle(eng, fake, 2)
        c = eng.controls
        c.layer("fatfinger").update(state="pilot", since=time.time() - 2 * 3600 - 60)
        c.d["hold_gates"] = True                          # the hold flag: no promotion
        eng.controller(time.time())
        self.assertEqual(c.state("fatfinger"), "pilot")
        c.d["hold_gates"] = False
        fake.stall_fills()                                # the projection's sequence stops advancing
        for _ in range(2):
            loop(eng, fake)
        eng.cov_seq_t = time.time() - 400                 # ... for more than 5 minutes
        eng.truth_t = None                                # ... and order-truth booking not live
        eng.controller(time.time())
        self.assertEqual(c.state("fatfinger"), "pilot", "fills blind: gates held")
        self.assertIn("fills projection sequence", eng.fills_health(time.time())[1])
        eng.truth_t = time.time()                         # order-truth booking live again: gates may promote
        eng.fat_sent.clear()                              # (the gate also wants a quiet last hour of placements)
        eng.controller(time.time())
        self.assertEqual(c.state("fatfinger"), "full")

    def test_tier_a_only_plus_new_unmapped_markets(self):
        fake, eng = self.rig()
        f_engine.FAT_TIER_A_ONLY = True                   # Oct 3: tier-A sides only
        exs = sorted({b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS})
        tier_a = exs[:6]
        os.makedirs(os.path.join(TMP, "intel"), exist_ok=True)
        json.dump(dict(tier_a=tier_a), open(os.path.join(TMP, "intel", "tiers.json"), "w"))
        self.settle(eng, fake, 4)
        mine = self.fat_orders(eng, fake)
        self.assertTrue(mine)
        self.assertTrue(all(o["ex"] in tier_a for o in mine), "tier-A sides only")
        self.assertTrue(all(o["price"] <= 0.12 + 1e-9 for o in mine), "one level, at most 0.12")
        # a brand-new unmapped market (not tier A): 0.05 at half size on both sides
        lv = eng.fat_levels("x", "yes", 0.5, 0.5, "B")
        self.assertEqual(lv, [(0.05, 0.05, 3_000)])
        self.assertEqual(eng.fat_levels("x", "no", 0.5, 0.5, "A"), [(0.05, 0.05, 10_000)])

    def test_write_budget_is_about_zero_after_placement(self):
        fake, eng = self.rig()
        self.settle(eng, fake, 15)
        fake.calls.clear()
        for _ in range(5):
            loop(eng, fake)
        fat_writes = [c for c in fake.calls if c[0] in ("batch", "cancel")]
        self.assertEqual(fat_writes, [], "no refresh churn once placed")
        cov = eng.dash_state()["coverage"]
        self.assertGreater(cov["covered"], 0)
        why = {u["reason"] for u in cov["uncovered"]}
        self.assertFalse(any(r.startswith("being placed") for r in why), why)   # placement done; the rest is the cap
        self.assertTrue(all(u["reason"] for u in cov["uncovered"]), cov["uncovered"][:3])
        self.assertTrue(all(u["minutes"] >= 0 for u in cov["uncovered"]) and "over_5_min" in cov)
        if cov["uncovered"]:
            rest = sum(o["qty"] * min(o["price"], 1 - o["price"]) for o in self.fat_orders(eng, fake))
            self.assertGreater(rest, eng.cap("fatfinger", "resting") - 2_400, "cap is used before sides go uncovered")


class DumpV3Tests(EngineCase):
    """Day 4 §2: dump catcher v3, "shallow and fast": L1/L2/L3 at ref - 1 / 2 / 3.5c on every favourite side (fair >= 0.80),
    each <= fair - 3c; sell-back at max(pre-dump ref, fill + 1.5c) <= fair - 0.5c, stepping down after 60 min, inventory
    after 6 h; size = funded reserve / 8 (1.5k..6k), arena x 1.5, pilot x 0.5; concentration cap; slide freeze; gate."""

    def rig(self, state="full", cash_above_floor=300_000, ref_gap=0.06):
        """cash 300k above the floor: the Oct 5 22:50 entry budget (2 x free) then never binds; CashGuardTests covers it."""
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state=state)))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None
                and eng.fair[b.ex] >= 0.90]
        self.assertTrue(favs)
        self.gap = ref_gap
        for ex in favs:                                   # SIG trades under Polymarket on favourites (the plan: 5-12c)
            seed_ref(eng, ex, mid=eng.fair[ex] - ref_gap)
        return fake, eng, favs

    def settle(self, eng, fake, favs, n=4):
        for _ in range(n):
            eng.ex.capacity = 6000.0
            loop(eng, fake)
            for ex in favs:
                seed_ref(eng, ex, mid=eng.fair[ex] - self.gap)

    def dump_orders(self, eng, fake, ex=None, layer="dump"):
        return [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer
                and (ex is None or o["ex"] == ex)]

    def test_levels_prices_weights_and_size(self):
        fake, eng, favs = self.rig()
        self.settle(eng, fake, favs, 6)
        ex = favs[0]
        mine = sorted(self.dump_orders(eng, fake, ex), key=lambda o: -o["price"])
        fs, ref = eng.fair[ex], eng.ref(ex)
        bid = eng.px[ex][0]
        day = eng.day_window(time.time())
        offs, ws = (a_config.DUMP_V3_OFFSETS_DAY, a_config.DUMP_V3_WEIGHTS_DAY) if day else (a_config.DUMP_V3_OFFSETS, a_config.DUMP_V3_WEIGHTS)
        l0 = fs >= a_config.DUMP_L0_MIN_FAIR and bid <= fs - a_config.DUMP_L0_GAP + 1e-9
        self.assertEqual(len(mine), len(offs) + (1 if l0 else 0), mine)
        if l0:
            self.assertAlmostEqual(mine[0]["price"], bid, places=3, msg="L0 joins the best bid")
            mine = mine[1:]
        for o, off, w in zip(mine, offs, ws):
            self.assertEqual(o["side"], "yes")
            self.assertAlmostEqual(o["price"], bots.tick_down(min(ref - off, fs - 0.03, bid + 0.005)), places=3)
            self.assertLessEqual(o["price"], fs - 0.03 + 1e-9, "at least 3c under fair")
        cost = sum(o["qty"] * o["price"] for o in mine)
        funded = min(a_config.OPPORTUNITY_RESERVE, 40_000)
        size = min(max(a_config.DUMP_SIDE_MIN, min(a_config.DUMP_SIDE_MAX, funded / a_config.DUMP_SIDE_DIV)) * 1.5, a_config.DUMP_ARENA_MAX, funded / 2)
        self.assertLessEqual(cost, size + 60, "<= per-side size (day 5: funded / 4, x1.5 if arena, <= 15k, <= funded / 2)")
        shares = [o["qty"] for o in mine]
        self.assertGreater(shares[0] * mine[0]["price"], shares[2] * mine[2]["price"], "40 / 35 / 25 weights")
        # pilot: half size
        fake2, eng2, favs2 = self.rig(state="pilot")
        self.settle(eng2, fake2, favs2, 6)
        c2 = sum(o["qty"] * o["price"] for o in self.dump_orders(eng2, fake2, favs2[0]))
        self.assertLessEqual(c2, size * 0.5 + 60)

    def test_eligibility(self):
        fake, eng, favs = self.rig()
        self.assertEqual(eng.dump_eligible(AK), [], "never on manual markets")
        ex = favs[0]
        self.assertTrue(eng.dump_eligible(ex))
        eng.book[ex] = dict(eng.book.get(ex, {}), pos=-500)             # we hold NO: no YES bid
        self.assertEqual([s for s, _, _ in eng.dump_eligible(ex) if s == "yes"], [])
        low = next(b.ex for b in eng.bots if b.kind == "market" and eng.fair.get(b.ex) is not None and 0.25 < eng.fair[b.ex] < 0.75)
        seed_ref(eng, low)
        self.assertEqual(eng.dump_eligible(low), [], "no side with fair >= 0.80")
        mid = next(b.ex for b in eng.bots if b.kind == "market" and eng.fair.get(b.ex) is not None and 0.80 <= eng.fair[b.ex] < 0.90)
        seed_ref(eng, mid, mid=eng.fair[mid] - 0.06)
        self.assertEqual([s for s, _, _ in eng.dump_eligible(mid)], ["yes"], "0.80 <= fair < 0.90 is eligible in v3")

    def test_fill_sellback_stepdown_rearm_and_inventory(self):
        fake, eng, favs = self.rig()
        self.settle(eng, fake, favs, 6)
        ex = favs[0]
        top = max(self.dump_orders(eng, fake, ex), key=lambda o: o["price"])
        sub = eng.ledger.orders[top["id"]]["sub"]
        pre = eng.ref(ex)
        fake.fill(top["id"], top["qty"])
        self.settle(eng, fake, favs, 2)
        t = eng.ledger.tranches[sub]
        self.assertEqual((t["kind"], t["state"]), ("dump", "open"))
        self.assertFalse([o for o in self.dump_orders(eng, fake, ex) if eng.ledger.orders[o["id"]]["sub"] == sub], "a filled level isn't re-placed while its tranche is open")
        rb = [o for o in self.dump_orders(eng, fake, ex, "dumpexit")]
        self.assertEqual(len(rb), 1)
        sell = round(1 - rb[0]["price"], 3)
        want = min(max(pre, top["price"] + 0.015), eng.fair[ex] - 0.005)
        self.assertAlmostEqual(sell, bots.tick_up(max(want, top["price"] + 0.01)), places=3)
        self.assertLessEqual(sell, eng.fair[ex] - 0.005 + 1e-9)
        t["ts"] -= 1300                                   # 20 min unfilled (day 5 §1.4): max(fill + 1c, ref - 0.5c)
        self.settle(eng, fake, favs, 2)
        rb2 = self.dump_orders(eng, fake, ex, "dumpexit")
        want2 = min(max(top["price"] + 0.01, eng.ref(ex) - 0.005), eng.fair[ex] - 0.005)
        self.assertAlmostEqual(round(1 - rb2[0]["price"], 3), bots.tick_up(round(max(want2, top["price"] + 0.01), 6)), places=3)
        t["ts"] -= 2500                                   # 60 min unfilled: fill + 0.5c
        self.settle(eng, fake, favs, 2)
        rb2 = self.dump_orders(eng, fake, ex, "dumpexit")
        self.assertAlmostEqual(round(1 - rb2[0]["price"], 3), bots.tick_up(round(top["price"] + 0.005, 6)), places=3)
        fake.fill(rb2[0]["id"], rb2[0]["qty"])            # the sell-back fills: round trip, hold recorded, level re-armed
        self.settle(eng, fake, favs, 3)
        self.assertEqual(t["state"], "closed")
        self.assertIsNotNone(t.get("hold_min"))
        self.assertTrue([o for o in self.dump_orders(eng, fake, ex) if eng.ledger.orders[o["id"]]["sub"] == sub], "re-armed")
        # a tranche unsold after 6 h goes to inventory
        top2 = max(self.dump_orders(eng, fake, ex), key=lambda o: o["price"])
        sub2 = eng.ledger.orders[top2["id"]]["sub"]
        fake.fill(top2["id"], top2["qty"])
        self.settle(eng, fake, favs, 2)
        eng.ledger.tranches[sub2]["ts"] -= 7 * 3600
        self.settle(eng, fake, favs, 2)
        self.assertEqual(eng.ledger.tranches[sub2]["state"], "aged")
        self.assertEqual(eng.ledger.sub(ex, sub2), 0)

    def test_slide_freezes_the_reference_and_never_lowers(self):
        fake, eng, favs = self.rig()
        self.settle(eng, fake, favs, 6)
        ex = favs[0]
        now = time.time()
        eng.mid24[ex] = collections.deque([(now - 300 * k, eng.fair[ex] - 0.06) for k in range(20)][::-1], maxlen=288)
        before = {eng.ledger.orders[o["id"]]["sub"]: o["price"] for o in self.dump_orders(eng, fake, ex)}
        seed_ref(eng, ex, mid=eng.fair[ex] - 0.10)        # the 30-min median falls 4c under the 6-h one: a slide
        ref, sliding = eng.dump_ref_v3(ex)
        self.assertTrue(sliding)
        self.assertAlmostEqual(ref, eng.fair[ex] - 0.10, places=3, msg="frozen at the first sliding value")
        seed_ref(eng, ex, mid=eng.fair[ex] - 0.12)
        self.assertAlmostEqual(eng.dump_ref_v3(ex)[0], eng.fair[ex] - 0.10, places=3, msg="stays frozen while sliding")
        self.gap = 0.12
        loop(eng, fake)
        after = {eng.ledger.orders[o["id"]]["sub"]: o["price"] for o in self.dump_orders(eng, fake, ex)}
        for sub, p in before.items():
            self.assertGreaterEqual(after.get(sub, p), p - 1e-9, "levels not lowered while sliding")

    def test_concentration_cap_counts_the_position_and_is_stable(self):
        fake, eng, favs = self.rig()
        ex = next(e_ for e_ in favs if eng.meta[e_].get("race") not in a_config.RI_RACES)   # RI has its own engine allowance
        cap = next(c for lo, c in a_config.DUMP_CONC_CAPS if eng.fair[ex] >= lo - 1e-9)   # day 7 §2.2: 20k / 15k / 12k by fair
        fake.pos[ex] = int(cap / 0.95) + 500
        fake.avg_cost[ex] = 0.95                          # above the cap at cost: no room
        self.settle(eng, fake, favs, 4)
        self.assertFalse(self.dump_orders(eng, fake, ex))
        self.assertIn(cap, (12_000, 15_000, 20_000))
        other = next(e_ for e_ in favs if e_ != ex)
        self.assertTrue(self.dump_orders(eng, fake, other))
        seen = []
        for _ in range(4):
            self.settle(eng, fake, favs, 1)
            seen.append(sorted((o["price"], o["qty"]) for o in self.dump_orders(eng, fake, other)))
        self.assertTrue(all(s == seen[0] for s in seen), seen)

    def test_every_side_gets_levels_after_a_restart(self):
        """Day 9 §3 replaced 'L1 everywhere first' with the expected-edge ranking: with the budget not binding every
        eligible side gets its whole ladder on the first plan after a start."""
        fake, eng, favs = self.rig()
        for ex in favs:
            seed_ref(eng, ex, mid=eng.fair[ex] - 0.06)
        eng.step()                                        # the first plan after a start: nothing resting yet
        lv = [w["level"] for ws in eng.dump_now.values() for w in ws]
        self.assertTrue(lv)
        self.assertEqual(eng.dump_cov["cut"], 0, "nothing cut: the budget does not bind in this rig")
        self.assertGreaterEqual(len({ex for ex in eng.dump_now}), len(favs) - 1)

    def test_news_guard_and_breaker(self):
        fake, eng, favs = self.rig()
        self.settle(eng, fake, favs, 6)
        ex = favs[0]
        self.assertTrue(self.dump_orders(eng, fake, ex))
        FAIR[ex] = eng.fair[ex] - 0.04                    # Polymarket drops 4c on this favourite (> 3c)
        try:
            self.settle(eng, fake, favs, 3)
            self.assertGreater(eng.dump_block.get((ex, "yes"), 0), time.time())
            self.assertEqual(self.dump_orders(eng, fake, ex), [])
        finally:
            FAIR.pop(ex, None)
        fake2, eng2, favs2 = self.rig()
        self.settle(eng2, fake2, favs2, 6)
        by_dir = {}
        for o in self.dump_orders(eng2, fake2):
            by_dir.setdefault(eng2.pol(o["ex"], o["side"]), {}).setdefault((o["ex"], o["side"]), o)
        d, picks = max(by_dir.items(), key=lambda kv: len(kv[1]))
        if len(picks) < 4:
            self.skipTest("fixture lacks 4 same-party favourite sides")
        for o in list(picks.values())[:4]:
            fake2.fill(o["id"], 50)
        loop(eng2, fake2)
        self.assertLess(eng2.breaker.get(("dump", d), 0), time.time(), "no Polymarket move: dump flow, not news")
        for f in eng2.ledger.fills:
            if f["layer"] == "dump" and f.get("fair") is not None:
                f["fair"] += 0.02
        eng2.dump_fill_events([])
        self.assertGreater(eng2.breaker.get(("dump", d), 0), time.time())

    def test_gate(self):
        fake, eng, favs = self.rig(state="pilot")
        since = time.time() - 5 * 3600
        g = eng.dump_gate(since, time.time())
        self.assertFalse(g["ok"])
        for k in range(22):                               # 22 catches sold back within the hour, realized > 0
            eng.ledger.fills.append(dict(key=f"g{k}", order=f"g{k}", ex=favs[0], side="yes", qty=100.0, price=0.80, action="buy",
                                         realized=0.0, t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(since + 600 + k)), bot="b",
                                         layer="dump", before_engine=False, fair=0.9, mo30=0.005, edge=0.1, sig_edge=0.0, sig_mid=0.83))
            eng.ledger.fills.append(dict(key=f"x{k}", order=f"x{k}", ex=favs[0], side="no", qty=100.0, price=0.185, action="buy",
                                         realized=1.5, t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(since + 1800 + k)), bot="b",
                                         layer="dumpexit", before_engine=False, fair=0.1, edge=None, sig_edge=None, sig_mid=None))
            eng.ledger.tranches[f"dump:{favs[0]}:yes:{k}"] = dict(ex=favs[0], side="yes", kind="dump", state="closed", closed_ts=since + 1800, hold_min=20.0)
        g = eng.dump_gate(since, time.time())
        self.assertTrue(g["ok"], g["why"])
        c = eng.controls
        c.layer("dump").update(state="pilot", since=since)
        c.d["hold_gates"], c.d["gate_free"] = True, ["dump"]   # Piero: promote within the plan although the hold is on
        eng.truth_t = time.time()
        eng.controller(time.time())
        self.assertEqual(c.state("dump"), "full")

    def test_l0_joins_the_bid_sweep_follows_and_sells_back_like_a_tranche(self):
        """Day 7 §2.1: on a >= 90% side whose SIG best bid sits >= 5c under fair, L0 joins the best bid with 2,000 shares;
        a fill becomes a dump tranche selling at entry + 1.5c (entry + 0.5c after 60 min); while the tranche is open the
        level refills at the new best bid, at most 3 times per 10 min."""
        fake, eng, favs = self.rig(state="full")
        ex = max(favs, key=lambda e_: eng.fair[e_])
        f = eng.fair[ex]
        bid = bots.tick_down(round(f - 0.08, 3))
        fake.px[ex] = (bid, round(bid + a_config.TICK, 3), bid)
        self.settle(eng, fake, favs, 3)
        l0 = [o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9]
        self.assertEqual(len(l0), 1, self.dump_orders(eng, fake, ex))
        self.assertEqual(l0[0]["qty"], a_config.DUMP_L0_CLIP)
        self.assertEqual(eng.ledger.orders[l0[0]["id"]]["sub"], f"dump:{ex}:yes:L0")
        fake.fill(l0[0]["id"], l0[0]["qty"])
        self.settle(eng, fake, favs, 2)
        sub = f"dump:{ex}:yes:L0"
        self.assertEqual(eng.ledger.subbooks[ex].get(sub), a_config.DUMP_L0_CLIP)
        t = eng.ledger.tranches[sub]
        self.assertEqual((t["kind"], t["state"]), ("dump", "open"))
        self.assertAlmostEqual(t["avg"], bid, places=4)
        xs = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dumpexit"]
        self.assertEqual(len(xs), 1, xs)
        self.assertAlmostEqual(1 - xs[0]["price"], bots.tick_up(round(bid + a_config.DUMP_SELL_MIN, 6)), places=3, msg="entry + 1.5c")
        refills = [o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9]
        self.assertEqual(len(refills), 1, "sweep follow: the level is back at the best bid while the tranche is open")
        t["ts"] -= a_config.DUMP_STEP2_S + 60
        self.settle(eng, fake, favs, 2)
        xs = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dumpexit"]
        self.assertAlmostEqual(1 - xs[0]["price"], bots.tick_up(round(bid + a_config.DUMP_STEP2_MIN, 6)), places=3, msg="after 60 min: entry + 0.5c")
        for _ in range(3):                                # 3 refills in 10 min: the fourth waits
            o = [o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9][0]
            fake.fill(o["id"], o["qty"])
            self.settle(eng, fake, favs, 2)
        self.assertEqual([o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9], [], "no 4th refill within 10 min")

    def test_race_cap_counts_every_engine_layer_and_ri_gets_its_own_10k(self):
        fake, eng, favs = self.rig(state="full")
        ex = favs[0]
        cap = next(c for lo, c in a_config.DUMP_CONC_CAPS if eng.fair[ex] >= lo - 1e-9)
        other = next((e_ for e_, m in eng.meta.items() if m.get("race") == eng.meta[ex]["race"] and e_ != ex), None)
        if other:                                         # a scalper holding on the other market of the race counts
            eng.ledger.subbooks[other] = {"scalp:%s:no" % other: -int(cap / 0.95) - 100}
            eng.ledger.tranches["scalp:%s:no" % other] = dict(ex=other, side="no", kind="scalp", state="open", ts=time.time(), t="x", avg=0.95)
            self.assertLessEqual(eng.race_room(ex, "yes", eng.fair[ex]), 0)
        ri = next((e_ for e_, m in eng.meta.items() if m.get("race") == "Rhode Island Senate" and m.get("party") == "R"), None)
        if ri:
            eng.ledger.subbooks[ri] = {"inventory": -14_401.0}       # the old RI Senate R NO book does not count
            self.assertAlmostEqual(eng.race_room(ri, "no", 0.99), a_config.RI_ENGINE_CAP, delta=1)
            eng.ledger.subbooks[ri]["dump:%s:no:L0" % ri] = -4_000.0
            eng.ledger.tranches["dump:%s:no:L0" % ri] = dict(ex=ri, side="no", kind="dump", state="open", ts=time.time(), t="x", avg=0.9)
            self.assertAlmostEqual(eng.race_room(ri, "no", 0.99), a_config.RI_ENGINE_CAP - 3_600, delta=1)

    def test_touch_book_becomes_l0_tranches_and_the_ratchet_replays_its_history(self):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full"), touch=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        with open(d_ledger.LEDGER_FILE, "w") as fh:        # a touch holding from last night (the path the Ledger reads in this process)
            json.dump(dict(orders={}, fills=[], baskets=[], subbooks={"1074": {"touch:1074:no": -2058.0}},
                           tranches={"touch:1074:no": dict(ex="1074", side="no", kind="touch", state="open", ts=time.time() - 3600, t="x", avg=0.875)},
                           ladder_seq={}, moves=[]), fh)
        with open(a_config.OVERRIDES_FILE) as fh:
            ov = json.load(fh)
        now = time.time()
        ov["ratchet"] = {"dump": dict(level=0, since=now, hold_until=0.0, history=[dict(t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 4 * 3600)), level=1, ok=True)])}
        ov["cash_target"] = dict(T=55_000.0, hour=int(now // 3600), idle=0, hist=[])
        with open(a_config.OVERRIDES_FILE, "w") as fh:
            json.dump(ov, fh)
        eng = engine(fake)
        self.assertEqual(eng.ledger.subbooks["1074"], {"dump:1074:no:L0": -2058.0})
        self.assertEqual(eng.ledger.tranches["dump:1074:no:L0"]["avg"], 0.875)
        self.assertEqual(eng.ledger.tranches["dump:1074:no:L0"]["state"], "open")
        self.assertEqual(eng.controls.state("touch"), "off")
        self.assertEqual(eng.controls.d["cash_target"]["T"], 25_000.0)
        self.assertEqual(eng.controls.d["ratchet"]["dump"]["level"], 0, "no fills in the replayed hours: held at 0")
        self.assertTrue(any("re-evaluated" in e["text"] for e in eng.events))
        self.assertTrue(eng.controls.d["day7"]["touch_to_l0"])

    def test_ratchet_up_on_a_passing_gate_and_back_on_a_failure(self):
        fake, eng, favs = self.rig(state="full")
        now = time.time()
        c = eng.controls
        c.d["ratchet"] = {"dump": dict(level=0, since=now - 3600 - 10, hold_until=0.0, history=[])}
        eng.ratchet("dump", now)                          # day 6 §1.1: too few fills in the hour -> the level is held, not failed
        self.assertEqual(c.d["ratchet"]["dump"]["level"], 0)
        self.assertEqual(c.d["ratchet"]["dump"]["hold_until"], 0.0)
        self.assertIsNone(c.d["ratchet"]["dump"]["history"][-1]["ok"])
        self.assertTrue(any("ratchet held" in e["text"] for e in eng.events))
        from unittest import mock
        c.d["ratchet"]["dump"].update(since=now - 3600 - 10, hold_until=0.0)
        with mock.patch.object(eng, "dump_gate", return_value=dict(ok=True, why="test pass", stats=dict(fills=20))):
            eng.ratchet("dump", now)
        self.assertEqual(c.d["ratchet"]["dump"]["level"], 1)
        self.assertAlmostEqual(eng.ratchet_mult("dump"), 1.25)
        c.d["ratchet"]["dump"].update(since=now - 3600 - 10)
        with mock.patch.object(eng, "dump_gate", return_value=dict(ok=False, why="test fail", stats=dict(fills=20))):
            eng.ratchet("dump", now)
        self.assertEqual(c.d["ratchet"]["dump"]["level"], 0, "one notch back on the first failure")
        self.assertGreater(c.d["ratchet"]["dump"]["hold_until"], now, "and a 1 h hold")
        # sizes follow the level but never pass 15k / funded / 2
        c.d["ratchet"]["dump"]["level"] = 10
        self.settle(eng, fake, favs, 3)
        for ex in favs[:3]:
            cost = sum(o["qty"] * o["price"] for o in self.dump_orders(eng, fake, ex))
            self.assertLessEqual(cost, min(a_config.DUMP_SIDE_HARD_MAX, a_config.OPPORTUNITY_RESERVE / 2) + 60)

    def test_party_exposure_cap(self):
        fake, eng, favs = self.rig(state="full")
        ex = favs[0]
        d = eng.pol(ex, "yes")
        other = next(e_ for e_ in favs[1:] if eng.pol(e_, "yes") == d)
        eng.ledger.subbooks[other] = {f"dump:{other}:yes:0": 50_000.0}        # 50k shares held at ~0.9 on that party
        eng.ledger.tranches[f"dump:{other}:yes:0"] = dict(ex=other, side="yes", kind="dump", avg=0.9, state="open", ts=time.time(), t="x")
        self.assertGreaterEqual(f_engine.dash4.party_exposure(eng)[d], 40_000)
        self.settle(eng, fake, favs, 3)
        self.assertFalse(self.dump_orders(eng, fake, ex), "no new levels on a party side at its cap")
        opp = next((e_ for e_ in favs if eng.pol(e_, "yes") != d), None)
        if opp:
            self.assertTrue(self.dump_orders(eng, fake, opp), "the other party still gets levels")

    def test_rebound_survives_a_pruned_entry_order(self):
        fake, eng, favs = self.rig()
        self.settle(eng, fake, favs, 6)
        ex = favs[0]
        top = max(self.dump_orders(eng, fake, ex), key=lambda o: o["price"])
        sub = eng.ledger.orders[top["id"]]["sub"]
        fake.fill(top["id"], top["qty"])
        self.settle(eng, fake, favs, 2)
        self.assertTrue(self.dump_orders(eng, fake, ex, "dumpexit"))
        eng.ledger.tranches[sub].pop("avg", None)
        del eng.ledger.orders[top["id"]]                  # pruned
        eng.track_dumps(time.time())
        self.assertTrue(eng.dump_view.get(ex), "sell-back still planned (entry price from the fills)")
        self.assertAlmostEqual(eng.dump_view[ex][0]["avg"], top["price"], places=4)

    def test_self_cross_guard(self):
        fake, eng, favs = self.rig()
        self.settle(eng, fake, favs, 6)
        ex = favs[0]
        top = max(self.dump_orders(eng, fake, ex), key=lambda o: o["price"])     # our YES bid
        b = eng.owner[ex]
        cross = dict(side="no", action="buy", px=round(1 - top["price"], 3), qty=100, layer="mm")   # = a YES ask at our bid
        self.assertIsNotNone(eng.self_cross(cross, [dict(top)]))
        self.assertEqual(eng.no_self_cross(b, [cross], []), [])
        ok = dict(cross, px=round(1 - top["price"] - 0.02, 3))                   # a YES ask 2c above our bid
        self.assertEqual(eng.no_self_cross(b, [ok], []), [ok])

    def test_subbook_fix_moves_shares_closes_tranche_and_cancels(self):
        fake, eng, favs = self.rig()
        ex = favs[0]
        led = eng.ledger
        fake.pos[ex] = 181
        led.subbooks[ex] = {"inventory": 797.0, "mm": 612.0, f"dump:{ex}:yes:0": -1228.0}
        led.tranches[f"dump:{ex}:yes:0"] = dict(ex=ex, side="no", kind="dump", pre_ref_side=0.15, t="x", ts=time.time(), state="open")
        c = f_engine.EngineControls(eng)
        with self.assertRaises(ValueError):
            c._apply("subbook_fix", dict(ex=ex, moves={f"dump:{ex}:yes:0": 1228, "inventory": -797}))
        c._apply("subbook_fix", dict(ex=ex, moves={f"dump:{ex}:yes:0": 1228, "inventory": -797, "mm": -431}, reason="test"))
        loop(eng, fake)
        self.assertEqual({k: v for k, v in led.subbooks[ex].items() if abs(v) >= 1e-6}, {"mm": 181.0})
        self.assertEqual(led.tranches[f"dump:{ex}:yes:0"]["state"], "closed")
        self.assertTrue(any(e["kind"] == "FIX" for e in eng.events))

    def test_release_cost_cap_is_5_percent(self):
        fake, eng, favs = self.rig()
        self.assertEqual(eng.release_max_cost(), 0.05)    # day 4 §3: both branches of release on demand stay within 5%
        ex = favs[0]
        bid, f = eng.px[ex][0], eng.fair[ex]
        eng.fair[ex] = bid * 1.08
        self.assertFalse(eng.release_ok(ex, "yes", "inventory", eng.cost_to_free(ex, "yes")))
        eng.fair[ex] = bid * 1.04
        self.assertTrue(eng.release_ok(ex, "yes", "inventory", eng.cost_to_free(ex, "yes")))
        eng.fair[ex] = f

    def test_release_without_a_spare_read_sells_at_most_500_at_the_bid(self):
        fake, eng, favs = self.rig()
        fake.spare = False
        eng.depth_cache.clear()
        self.assertEqual(eng.bid_depth(favs[0], "yes", time.time()), float(a_config.RELEASE_FALLBACK_QTY))


class OrderTruthTests(EngineCase):
    """Evening §1.2-1.4 + Piero's unattended rules: SIG's fills projection stalls; fills come from our own order records
    (an order that shrank or left the book without our cancel, before its TTL) when the position moved the same way."""



    def rig(self, mode="book"):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 40_000)
        eng = engine(fake)
        eng.controls.d["truth_mode"] = mode
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None
                and eng.fair[b.ex] >= 0.93]
        for _ in range(3):
            for ex in favs:
                seed_ref(eng, ex, mid=eng.fair[ex] - 0.01)
            eng.ex.capacity = 6000.0
            loop(eng, fake)
        self.assertTrue(self.dump_orders(eng, fake))
        fake.stall_fills()                                # SIG's fills list stops here (Oct 2, 15:25:55)
        loop(eng, fake)
        return fake, eng, favs

    def dump_orders(self, eng, fake):
        return [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dump"]

    def test_shadow_logs_and_books_nothing(self):
        fake, eng, favs = self.rig(mode="shadow")
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        n0 = len(eng.ledger.fills)
        fake.fill(o["id"], o["qty"])
        loop(eng, fake)
        loop(eng, fake)
        self.assertEqual(len(eng.ledger.fills), n0, "shadow books nothing")
        rows = [json.loads(l) for l in open(os.path.join(TMP, "intel", "truth_shadow.jsonl"))]
        hit = [r for x in rows for r in x["rows"] if r["ex"] == o["ex"]]
        self.assertTrue(hit and hit[0]["match"], rows[-3:])

    def test_book_mode_books_to_the_owner_and_nothing_lands_in_inventory(self):
        fake, eng, favs = self.rig()
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        sub = eng.ledger.orders[o["id"]]["sub"]
        inv0 = eng.ledger.sub(o["ex"], "inventory")
        fake.fill(o["id"], 300)                           # a partial fill: the order shrinks
        loop(eng, fake)
        fs = [f for f in eng.ledger.fills if f["order"] == o["id"]]
        self.assertEqual([(f["source"], f["qty"], f["price"]) for f in fs], [("order_shrink", 300, o["price"])])
        self.assertAlmostEqual(abs(eng.ledger.sub(o["ex"], sub)), 300, msg="owner's sub-book")
        self.assertAlmostEqual(eng.ledger.sub(o["ex"], "inventory"), inv0, msg="nothing pushed into inventory")
        self.assertIsNotNone(eng.truth_t)
        loop(eng, fake)                                   # v3: the level's remainder is pulled while its tranche is sold back
        self.assertFalse([x for x in fake.open_orders() if x["id"] == o["id"]])
        self.assertAlmostEqual(abs(eng.ledger.sub(o["ex"], sub)), 300)
        # SIG's projection catches up later with the real fill: not booked twice
        fake.fill_stalled = False
        loop(eng, fake)
        loop(eng, fake)
        self.assertAlmostEqual(sum(f["qty"] for f in eng.ledger.fills if f["order"] == o["id"]), 300)
        self.assertAlmostEqual(eng.ledger.sub(o["ex"], "inventory"), inv0)
        self.assertAlmostEqual(eng.ledger.sub(o["ex"], "inventory"), inv0)

    def test_carried_move_plus_sig_fill_in_the_same_loop_is_not_booked_again(self):
        # the exact 13:24 state: an order left the book, this loop's sync already booked SIG's fill for it, and a move of
        # the same size was carried from the last loop: the carry branch matched and booked it a second time
        fake, eng, favs = self.rig()
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        oid, ex, q = o["id"], o["ex"], o["qty"]
        rec = eng.ledger.orders[oid]
        sgn = (1 if rec["side"] == "yes" else -1) * (1 if rec["action"] == "buy" else -1)
        eng.truth_prev = {oid: q}                         # last snapshot: open, nothing booked
        eng.truth_filled = {oid: 0.0}
        eng.truth_carry = {ex: sgn * q}                   # the move was seen (and carried) last loop
        rec["filled"] = float(q)                          # this loop's sync booked SIG's fill for it
        eng.open = [x for x in eng.open if x["id"] != oid]   # and it's gone from the book
        p0 = eng.book.get(ex, {}).get("pos", 0)
        prev = {e_: (b_["pos"], b_["row"]) for e_, b_ in eng.book.items()}
        prev[ex] = (p0, (prev.get(ex) or (0, None))[1])
        pos = {e_: v[0] for e_, v in prev.items()}
        pos[ex] = p0 + sgn * q                            # positions show the move this loop
        proj = collections.Counter({ex: sgn * q})         # ... explained by SIG's fill booked this loop
        n0 = len(eng.ledger.fills)
        booked = eng.order_truth(pos, prev, proj, time.time(), True)
        self.assertEqual([r for r in booked if r["order"] == oid], [], "already booked from SIG: not again")
        self.assertEqual(len(eng.ledger.fills), n0)

    def test_sig_fill_and_vanish_in_the_same_loop_is_booked_once(self):
        # Oct 3 13:24 (840, order 6287642): the trade fell between our orders read and our positions read (position moved,
        # order still listed); next loop SIG's fill arrived AND the order was gone: it was booked twice
        fake, eng, favs = self.rig()
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        stale = fake.open_orders()                        # the snapshot before the trade
        real_oo = fake.open_orders
        fake.fill(o["id"], o["qty"])                      # the trade: position moves, the order leaves the book
        fake.open_orders = lambda: stale                  # ... but this loop's orders read still lists it
        loop(eng, fake)                                   # (projection still stalled: no SIG fill yet)
        fake.open_orders = real_oo
        fake.fill_stalled = False                         # next loop: SIG's fill arrives, the order is gone
        loop(eng, fake)
        loop(eng, fake)
        fs = [f for f in eng.ledger.fills if f["order"] == o["id"]]
        self.assertAlmostEqual(sum(f["qty"] for f in fs), o["qty"], msg=[(f["qty"], f.get("source")) for f in fs])
        self.assertLessEqual(eng.ledger.orders[o["id"]]["filled"], o["qty"] + 1e-6)

    def test_order_gone_without_position_move_is_not_a_fill(self):
        fake, eng, favs = self.rig()
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        with fake.lock:
            fake.book.pop(next(k for k in fake.book if str(k) == o["id"]))   # gone, but nothing traded
        loop(eng, fake)
        loop(eng, fake)
        self.assertFalse([f for f in eng.ledger.fills if f["order"] == o["id"]])

    def test_unexplained_moves_raise_fills_blind_in_book_mode(self):
        fake, eng, favs = self.rig()
        ex = favs[0]
        for _ in range(3):
            fake.pos[ex] = fake.pos.get(ex, 0) + 500      # positions move with no order of ours leaving the book
            loop(eng, fake)
        eng.last_booked_t = time.time() - 400
        fake.pos[ex] = fake.pos.get(ex, 0) + 500
        loop(eng, fake)
        fake.pos[ex] = fake.pos.get(ex, 0) + 500
        loop(eng, fake)
        self.assertIsNotNone(eng.fills_blind)
        self.assertFalse(eng.fills_health(time.time())[0])

    def test_targeted_backfill_moves_the_known_dump_orders_out_of_inventory(self):
        fake, eng, favs = self.rig(mode="shadow")
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        ex, sub = o["ex"], eng.ledger.orders[o["id"]]["sub"]
        t0 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 5))
        fake.fill(o["id"], o["qty"])                      # the catch (unbooked: projection stalled, shadow mode)
        rec = eng.ledger.orders[o["id"]]
        rec["ts"] -= 600                                  # it rested 10 min (well inside its TTL)
        for _ in range(3):
            loop(eng, fake)
        inv = eng.ledger.sub(ex, "inventory")
        signed = -o["qty"] if o["side"] == "no" else o["qty"]
        now = time.strftime("%H:%M:%S")
        with open(os.path.join(TMP, "engine.log"), "w") as fh:     # what the audit logged
            fh.write(f"{now} loop 1 | x\n  AUDIT  ex {ex}: sub-books were off by {signed:+,.0f} shares -> inventory\n")
        eng.truth_live_t = time.time() + 5
        eng.pending_backfill = dict(since=t0, dry_run=False, orders=[o["id"]])
        loop(eng, fake)
        self.assertEqual(eng.backfill_report["mismatched"], [])
        self.assertAlmostEqual(eng.ledger.sub(ex, sub), signed)
        self.assertAlmostEqual(eng.ledger.sub(ex, "inventory"), inv - signed)
        t_ = eng.ledger.tranches.get(sub)
        self.assertTrue(t_ and t_["kind"] == "dump", "the catch is a dump tranche: its rebound goes out")
        rb = [x for x in fake.open_orders() if x["id"] in eng.ledger.orders and eng.ledger.orders[x["id"]]["layer"] == "dumpexit"]
        self.assertTrue(rb)

    def test_exit_never_sells_past_its_sub_book_and_never_makes_a_tranche(self):
        # night §1.1 (ex 840): a dumpexit still resting when its tranche completed kept filling and sold inventory/mm shares
        fake, eng, favs = self.rig()
        led = eng.ledger
        ex = favs[0]
        led.subbooks[ex] = {"inventory": 797.0, "mm": 612.0, f"dump:{ex}:yes:0": 307.0}
        oid = "x-exit"
        led.orders[oid] = dict(id=oid, bot="b", layer="dumpexit", sub=f"dump:{ex}:yes:0", ex=ex, side="no", action="buy", price=0.13,
                               qty=1535, filled=0.0, cost=0.0, left=1535, placed="2026-10-02 21:23:00", ts=time.time(), status="open")
        exc = led.apply_sub(ex, led.orders[oid], -307.0)    # the tranche's own 307: fine
        self.assertEqual(exc, 0.0)
        exc = led.apply_sub(ex, led.orders[oid], -1228.0)   # 4 more fills of 307 after it completed: clipped
        self.assertAlmostEqual(exc, -1228.0)
        self.assertEqual(led.subbooks[ex][f"dump:{ex}:yes:0"], 0.0)
        self.assertAlmostEqual(led.subbooks[ex]["inventory"], 0.0)          # out of inventory first ...
        self.assertAlmostEqual(led.subbooks[ex]["mm"], 612.0 - 431.0)       # ... then mm
        self.assertAlmostEqual(sum(led.subbooks[ex].values()), 797 + 612 + 307 - 307 - 1228)

    def test_completed_tranche_cancels_its_resting_exit(self):
        fake, eng, favs = self.rig()
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        sub = eng.ledger.orders[o["id"]]["sub"]
        eng.controls.d["truth_mode"] = "book"
        fake.fill(o["id"], o["qty"])                      # the catch
        loop(eng, fake)
        loop(eng, fake)
        ex_o = [x for x in fake.open_orders() if x["id"] in eng.ledger.orders and eng.ledger.orders[x["id"]]["layer"] == "dumpexit"]
        self.assertTrue(ex_o)
        e1 = ex_o[0]
        fake.fill(e1["id"], e1["qty"] // 2)              # half the rebound sells ...
        loop(eng, fake)
        eng.ledger.subbooks[o["ex"]][sub] = 0.0           # ... and the tranche completes elsewhere (e.g. a late fill)
        eng.ledger.subbooks[o["ex"]]["inventory"] = eng.ledger.subbooks[o["ex"]].get("inventory", 0) + \
            (o["qty"] - e1["qty"] // 2) * (1 if o["side"] == "no" else -1) * -1
        loop(eng, fake)
        self.assertEqual(eng.ledger.tranches[sub]["state"], "closed")
        self.assertIn(e1["id"], set(eng.ex.cancelling) | {i for i in eng.ex.gone}, "its exit is cancelled at once")
        self.assertFalse([s for s, t_ in eng.ledger.tranches.items() if t_.get("state") == "open" and s.startswith("dump:")
                          and s.split(":")[2] != ("yes" if eng.ledger.subbooks[t_["ex"]].get(s, 0) > 0 else "no")])

    def test_late_fill_is_carried_not_pushed_into_inventory(self):
        # night §1.2: an order vanished; SIG's (late) fill lands a loop later: the audit waits instead of using inventory
        fake, eng, favs = self.rig(mode="shadow")
        o = max(self.dump_orders(eng, fake), key=lambda o: o["price"])
        sub = eng.ledger.orders[o["id"]]["sub"]
        inv0 = eng.ledger.sub(o["ex"], "inventory")
        fake.fill(o["id"], o["qty"])                      # the projection is stalled: the fill is late
        loop(eng, fake)
        self.assertAlmostEqual(eng.ledger.sub(o["ex"], "inventory"), inv0, msg="carried, not pushed into inventory")
        fake.fill_stalled = False                         # the late fill arrives
        loop(eng, fake)
        self.assertAlmostEqual(abs(eng.ledger.sub(o["ex"], sub)), o["qty"])
        self.assertAlmostEqual(eng.ledger.sub(o["ex"], "inventory"), inv0)
        # a difference with no order behind it still goes to inventory, after 2 min
        ex = favs[1]
        inv1 = eng.ledger.sub(ex, "inventory")
        fake.pos[ex] = fake.pos.get(ex, 0) + 400          # no order of ours moved here: nothing to wait for
        loop(eng, fake)
        self.assertAlmostEqual(eng.ledger.sub(ex, "inventory") - inv1, 400)

    def test_stop_and_pull_needs_10_writes_at_50_percent_and_resumes(self):
        # day 4 §1.2: 15:44 paused on 5 writes at 60%; now >= 10 writes in 10 min and >= 50% failed
        fake, eng, favs = self.rig()
        from unittest import mock
        with mock.patch.object(fake, "health", return_value=dict(read=dict(n=50, bad_rate=0.0), write=dict(n=5, bad_rate=0.6), throttled=0), create=True):
            eng.controller(time.time())
        self.assertFalse(eng.controls.paused, "5 failed writes don't trip it")
        with mock.patch.object(fake, "health", return_value=dict(read=dict(n=50, bad_rate=0.0), write=dict(n=12, bad_rate=0.5), throttled=0), create=True):
            eng.controller(time.time())
        self.assertTrue(eng.controls.paused)
        self.assertEqual(eng.controls.d["auto_pause"]["kind"], "stop_pull")
        self.assertTrue(any(e["kind"] == "STOP" for e in eng.events))
        self.assertEqual(eng.controls.d.get("decisions"), [])
        fake.writes_ok = 0.9                              # SIG writes 90% OK over the last 20 -> lifts itself
        eng.controls.d["auto_pause"]["t"] -= 400
        eng.controller(time.time())
        self.assertFalse(eng.controls.paused)
        # the same kind 3x in 2 h stays paused
        for _ in range(2):
            eng.faults_pulled = None
            eng.auto_pause("stop_pull", "again", time.time())
            eng.controls.set_paused(False, "t")
        eng.faults_pulled = None
        eng.auto_pause("stop_pull", "third", time.time())
        self.assertTrue(eng.controls.d["auto_pause"]["stuck"])
        eng.controls.d["auto_pause"]["t"] -= 400
        eng.controller(time.time())
        self.assertTrue(eng.controls.paused, "3 in 2 h: waits for Piero")


class LongshotTests(EngineCase):
    """Day 3 §2: 0.5-1c bids on sides Polymarket prices <= 5%, exits at 3x entry / the SIG 6-h median; off by default."""

    def rig(self, state="pilot", cash_above_floor=40_000):
        layers = dict(position=dict(state="full"))
        if state:
            layers["longshot"] = dict(state=state)
        fresh_data(layers=layers)
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        for _ in range(4):
            eng.ex.capacity = 6000.0
            loop(eng, fake)
        return fake, eng

    def long_orders(self, eng, fake, layer="longshot"):
        return [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer]

    def test_off_by_default(self):
        fake, eng = self.rig(state=None)
        self.assertEqual(self.long_orders(eng, fake), [])

    def test_levels_sides_and_caps(self):
        fake, eng = self.rig()
        mine = self.long_orders(eng, fake)
        self.assertTrue(mine)
        by_side = {}
        for o in mine:
            fs = eng.side_fair(o["ex"], o["side"])
            self.assertLessEqual(fs, 0.05 + 1e-9, o)
            self.assertNotIn(o["ex"], (AK, RI), "never manual")
            by_side.setdefault((o["ex"], o["side"]), []).append((o["price"], o["qty"]))
        for k, lv in by_side.items():
            self.assertEqual(sorted(lv), [(0.005, 20000.0), (0.01, 10000.0)], k)
            self.assertLessEqual(sum(p * q for p, q in lv), 200 + 1e-9)
        self.assertLessEqual(sum(o["qty"] * o["price"] for o in mine), 20_000 + 1e-6)
        self.assertEqual(eng.longshot_levels(0.003), [(0.005, 10_000)], "fair_side < 0.005: only the 0.005 level, 10k shares")

    def test_not_where_we_hold_the_other_side(self):
        fake, eng = self.rig()
        ex, side = next((o["ex"], o["side"]) for o in self.long_orders(eng, fake))
        eng.book[ex] = dict(eng.book.get(ex, {}), pos=500 if side == "no" else -500)
        self.assertEqual([s for s, _ in eng.longshot_eligible(ex) if s == side], [])

    def test_fill_exits_at_3x_and_the_6h_median(self):
        fake, eng = self.rig()
        o = next(o for o in self.long_orders(eng, fake) if o["price"] == 0.01)
        ex, side = o["ex"], o["side"]
        now = time.time()
        med_yes = 0.06 if side == "yes" else 0.94           # the side's 6-h median: 6c
        eng.mid24[ex] = collections.deque([(now - 300 * k, med_yes) for k in range(20)][::-1], maxlen=288)
        fake.fill(o["id"], o["qty"])
        for _ in range(2):
            eng.mid24[ex] = collections.deque([(now - 300 * k, med_yes) for k in range(20)][::-1], maxlen=288)
            loop(eng, fake)
        ex_o = self.long_orders(eng, fake, "longexit")
        sells = sorted((round(1 - x["price"], 3), x["qty"]) for x in ex_o if x["ex"] == ex)
        self.assertEqual([p for p, _ in sells], [0.03, 0.06], "3x entry and the 6-h median")
        self.assertEqual(sum(q for _, q in sells), o["qty"])
        self.assertTrue(any(e["kind"] == "LONGSHOT" for e in eng.events))

    def test_news_guard_and_breaker(self):
        fake, eng = self.rig()
        ex, side = next((o["ex"], o["side"]) for o in self.long_orders(eng, fake))
        f = eng.fair[ex]
        FAIR[ex] = f + 0.04 if side == "yes" else f - 0.04  # the longshot side rises 4c within 10 min
        try:
            for _ in range(3):
                loop(eng, fake)
            self.assertGreater(eng.long_block.get((ex, side), 0), time.time())
            self.assertFalse([o for o in self.long_orders(eng, fake) if o["ex"] == ex and o["side"] == side])
        finally:
            FAIR.pop(ex, None)
        fake2, eng2 = self.rig()
        picks, seen = [], set()
        for o in self.long_orders(eng2, fake2):
            if (o["ex"], o["side"]) not in seen:
                seen.add((o["ex"], o["side"]))
                picks.append(o)
        if len(picks) < 5:
            self.skipTest("fixture has fewer than 5 longshot sides")
        for o in picks[:5]:
            fake2.fill(o["id"], 100)
        loop(eng2, fake2)
        self.assertGreater(eng2.breaker.get(("long", "all"), 0), time.time())


class Day4Tests(EngineCase):
    """Day 4 §3 (release on demand, the AK sale) and §4 (reads)."""

    def rig(self, cash_above_floor=40_000):
        fresh_data(layers=dict(position=dict(state="full"), release=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        return fake, eng

    def test_ak_sale_three_slices_above_fair_and_sig_bid(self):
        f_engine.LIQUIDATE = {"AK-GOV-D": dict(slices=3, over_fair=0.01)}
        fake, eng = self.rig()
        loop(eng, fake)
        sells = sorted((o["price"], o["qty"]) for o in fake.open_orders() if o["ex"] == AK and o["action"] == "sell"
                       and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "exit")
        self.assertEqual(len(sells), 3, sells)
        held = abs(eng.ledger.sub(AK, "manual"))
        self.assertEqual(sum(q for _, q in sells), held)
        fair_no = 1 - eng.fair[AK]
        bid_no = bots.side_book(*eng.px[AK][:2], "no")[0]
        self.assertTrue(all(abs(p - bots.tick_up(max(fair_no + 0.01, bid_no))) < 1e-9 for p, _ in sells), (sells, fair_no, bid_no))
        self.assertGreater(sells[0][0], fair_no, "sold above Polymarket fair: +EV")
        b, a_, l = fake.px[AK]
        fake.px[AK] = (b - 0.005, a_ - 0.005, l)           # the SIG bid ticks up 0.5c on NO: the slices are kept (no churn)
        fake.calls.clear()
        loop(eng, fake)
        self.assertFalse([c for c in fake.calls if c[0] == "cancel"], "a 1-tick move doesn't re-price the AK slices")

    def test_release_tiers_under_rule_0(self):
        """Day 6 §9: tier 0 (bid >= fair - 2c) sells any day-1 lot and engine lots above entry + 0.5c; tiers 1-2 only fund
        the cash target T (<= 10k/h, >= 24 h old, tier 2 needs the trading layers at >= 10%/day); never > 7c under fair;
        never while paused; not in the quiet hour."""
        fake, eng = self.rig()
        self.addCleanup(setattr, f_engine, "RELEASE_QUIET_HOURS", f_engine.RELEASE_QUIET_HOURS)
        f_engine.RELEASE_QUIET_HOURS = (0, 0)              # the test may run during Piero's 07:00-08:00 review hour
        ex = mid_market(eng)
        fake.pos[ex] = 2000
        fake.avg_cost[ex] = 0.30
        loop(eng, fake)
        bid = fake.px[ex][0]
        fair = eng.fair[ex]
        self.assertLessEqual(fair - bid, 0.02 + 1e-9, "the fixture's mid market sits within 2c of fair at the bid")
        # a day-1 lot (Oct 1) bought above the bid: tier 0 sells it anyway (rule 0 lets day-1 lots go below entry)
        eng.ledger.fills.append(dict(key="d0", order="x0", ex=ex, side="yes", qty=2000.0, price=0.60, action="buy", realized=0.0,
                                     t="2026-10-01 12:00:00", bot="b", layer="", before_engine=False, fair=None, edge=None, sig_edge=None, sig_mid=None))
        eng.release_t = 0
        plan = eng.release_plan(eng.cash_hist[-1], time.time(), eng.bucket_usage())
        self.assertIn(ex, plan)
        self.assertEqual(plan[ex]["why"], "tier0")
        self.assertEqual(sum(plan[ex]["subs"].values()), min(2000, eng.bid_depth(ex, "yes", time.time())), "sized to the visible bid depth")
        # the same lot bought by the engine (Oct 2+) 1 tick above the bid: rule 0 blocks the sale in every layer
        eng.ledger.fills[-1].update(t="2026-10-03 12:00:00", price=round(bid + 0.005, 3))
        eng.lots_cache = (None, {})
        eng.release_t = 0
        self.assertNotIn(ex, eng.release_plan(eng.cash_hist[-1], time.time(), eng.bucket_usage()), "an engine lot under entry + 0.5c never sells")
        self.assertEqual(eng.rule0_allowed(ex, "yes", "inventory", bid, 2000), 0)
        self.assertEqual(eng.rule0_allowed(ex, "yes", "inventory", round(bid + 0.01, 3), 2000), 2000, "it sells once the bid clears entry + 0.5c")
        hsub = f"dump:{ex}:yes:scalp"                          # a hand-over tranche with no lot history of its own: its entry is the tranche's avg
        eng.ledger.subbooks[ex][hsub] = 500.0
        eng.ledger.tranches[hsub] = dict(ex=ex, side="yes", kind="dump", state="open", ts=time.time(), t="x", avg=0.805, from_="scalp")
        self.assertEqual(eng.rule0_allowed(ex, "yes", hsub, 0.81, 500), 500, "entry 0.805 + 0.5c = 0.81: allowed")
        self.assertEqual(eng.rule0_allowed(ex, "yes", hsub, 0.805, 500), 0)
        eng.ledger.subbooks[ex].pop(hsub); eng.ledger.tranches.pop(hsub)
        loop(eng, fake)
        sells = [o for o in fake.open_orders() if o["ex"] == ex and jx.reduces(o["side"], o["action"], fake.pos.get(ex, 0))]
        self.assertEqual(sells, [], "no layer (release, recycler) placed a sale of the engine lot under entry")
        # tiers 1-2: a holding 4c under fair is sold only while free cash is below T, from holdings >= 24 h old
        eng.ledger.fills[-1].update(t="2026-10-01 12:00:00", price=0.60)
        eng.lots_cache = (None, {})
        FAIR[ex] = bid + 0.04
        try:
            loop(eng, fake)
            eng.release_t = 0
            self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 60_000, time.time(), eng.bucket_usage()), "free cash above T: tier 1 not needed")
            eng.release_t = 0
            plan = eng.release_plan(a_config.CASH_RESERVE + 20_000, time.time(), eng.bucket_usage())   # free 20k < T 25k
            self.assertEqual(plan.get(ex, {}).get("why"), "tier1", plan)
            self.assertLessEqual(sum(plan[ex]["subs"].values()) * plan[ex]["px"], 5_000 + 1, "only up to T")
            FAIR[ex] = bid + 0.06                              # tier 2: only with the trading layers earning >= 10%/day
            loop(eng, fake)
            eng.release_t = 0
            self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 20_000, time.time(), eng.bucket_usage()))
            eng.trading_return_24h = lambda now: 0.25
            eng.release_t = 0
            self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 20_000, time.time(), eng.bucket_usage()), "day 8 §1.4: tier 2 needs Piero's flag")
            eng.controls.d["release_tier2"] = True
            eng.release_t = 0
            self.assertEqual(eng.release_plan(a_config.CASH_RESERVE + 20_000, time.time(), eng.bucket_usage()).get(ex, {}).get("why"), "tier2")
            eng.controls.d.pop("release_tier2")
            FAIR[ex] = bid + 0.08                              # > 7c under fair: never for funding
            loop(eng, fake)
            eng.release_t = 0
            self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 20_000, time.time(), eng.bucket_usage()))
        finally:
            FAIR.pop(ex, None)
        eng.controls.set_paused(True, "t")
        eng.release_t = 0
        self.assertEqual(eng.release_plan(eng.cash_hist[-1], time.time(), eng.bucket_usage()), {})
        eng.controls.set_paused(False, "t")
        from unittest import mock
        eng.release_t = 0
        with mock.patch.object(f_engine, "RELEASE_QUIET_HOURS", (0, 24)):
            self.assertEqual(eng.release_plan(eng.cash_hist[-1], time.time(), eng.bucket_usage()), {})

    def test_cash_target_ratchet(self):
        fake, eng = self.rig()
        loop(eng, fake)
        now = time.time()
        c = eng.controls
        c.d["cash_target"] = dict(T=25_000, hour=int(now // 3600) - 1, idle=0, hist=[])
        eng.use_peak = 0.75                                   # the trading layers peaked at 75% of free cash last hour
        self.assertEqual(eng.cash_target(now), 35_000)
        c.d["cash_target"]["hour"] -= 1
        for i in range(20):
            eng.constrained_min.append(now - 60 * i)          # 20 constrained minutes: at the cap (day 8 §1.1: 35k) nothing changes
        self.assertEqual(eng.cash_target(now), 35_000, "capped at 35k")
        c.d["cash_target"]["hour"] -= 1                       # day 8 §1.2: an hour with peak 65% and constrained minutes is not idle
        eng.use_peak = 0.65
        eng.cash_target(now)
        self.assertEqual(c.d["cash_target"]["idle"], 0)
        eng.constrained_min.clear()
        c.d["cash_target"]["hour"] -= 1                       # peak 65% but no constrained minute: idle hour 1
        eng.use_peak = 0.65
        eng.cash_target(now)
        self.assertEqual((c.d["cash_target"]["T"], c.d["cash_target"]["idle"]), (35_000, 1))
        c.d["cash_target"]["hour"] -= 1                       # peak 10%: idle hour 2 -> -10k
        eng.use_peak = 0.1
        eng.cash_target(now)
        self.assertEqual(c.d["cash_target"]["T"], 25_000)
        for _ in range(4):
            c.d["cash_target"]["hour"] -= 1
            eng.use_peak = 0.1
            eng.cash_target(now)
        self.assertEqual(c.d["cash_target"]["T"], 25_000, "never below 25k")
        c.d["cash_target"].update(T=55_000, hour=c.d["cash_target"]["hour"] - 1)   # an old 55k target is clamped on read
        eng.use_peak = 0.9
        self.assertEqual(eng.cash_target(now), 35_000, "never above 35k")
        self.assertTrue(any(e["kind"] == "CASH" and "CASH TARGET" in e["text"] for e in eng.events))

    def test_reads_pnl_60s_orders_30s_prices_rotated(self):
        f_engine.PNL_READ_S, f_engine.ORDERS_READ_S = 60, 30
        fake, eng = self.rig()
        real_pnl = fake.pnl
        fake.pnl = lambda: (fake._count("pnl"), real_pnl())[1]   # the rig's lambda doesn't count: count here
        for _ in range(3):                                # settle: the first loops place orders (a write -> re-read)
            loop(eng, fake)
        fake.reads = {}
        n_all = len(eng.px)
        loop(eng, fake)                                   # no writes, no fills: no open-orders read, no pnl read
        self.assertEqual(fake.reads.get("pnl", 0), 0)
        self.assertEqual(fake.reads.get("orders", 0), 0)
        self.assertEqual(fake.reads.get("positions"), 1)
        self.assertLess(fake.last_prices_n, n_all, "prices: core markets + one rotated chunk, not every market")
        self.assertEqual(len(eng.px), n_all, "rotated markets keep their last price")
        eng.pnl_read_t -= 61
        eng.open_read_t -= 31
        fake.reads = {}
        loop(eng, fake)
        self.assertEqual(fake.reads.get("pnl"), 1)
        self.assertEqual(fake.reads.get("orders"), 1)
        fake.reads = {}
        eng.rt_event("fills", set(), time.time())         # our own fill -> open orders re-read at once
        loop(eng, fake)
        self.assertEqual(fake.reads.get("orders"), 1)

    def test_loop_backs_off_to_20s_after_a_429(self):
        self.assertEqual(f_engine.loop_gap(50, True, time.time()), 2.0)
        self.assertEqual(f_engine.loop_gap(80, True, time.time()), 15.0)
        f_engine.api.LAST_429 = [time.time()]
        try:
            self.assertEqual(f_engine.loop_gap(50, True, time.time()), 20.0)
            self.assertEqual(f_engine.loop_gap(50, True, time.time() + 130), 2.0)
        finally:
            f_engine.api.LAST_429 = [0.0]


class IntelTests(EngineCase):
    def test_tape_sweep_becomes_a_big_event_with_missed_value(self):
        fresh_data(layers=dict(position=dict(state="full"), leaders=dict(state="full")))
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and eng.side_fair(b.ex, "yes") and eng.side_fair(b.ex, "yes") > 0.6)
        now = time.time()
        for b_ in eng.bots:                               # visited recently: only a price change brings a market back
            eng.intel_t[b_.ex], eng.intel_px[b_.ex] = now, (fake.px.get(b_.ex) or (None, None, None))[2]
        stamp_ = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        fake.tape = {ex: [dict(id="900", createdAt=stamp_(now - 2), price=0.05, size=60_000),
                          dict(id="901", createdAt=stamp_(now - 1), price=0.04, size=20_000)]}
        b, a, _ = fake.px[ex]
        fake.px[ex] = eng.px[ex] = (b, a, 0.04)           # the loop read a new last price: the collector reads this market first
        eng.fat_now = {ex: [dict(side="yes", px=0.12, qty=5_000), dict(side="yes", px=0.08, qty=10_000)]}
        eng.intel_step(time.time())
        evs = [json.loads(l) for l in open(os.path.join(TMP, "intel", "events.jsonl"))]
        self.assertEqual(len(evs), 1)
        e = evs[0]
        self.assertIn("Max sweep", e["kinds"])
        self.assertEqual((e["ex"], e["gainer_side"], e["shares"]), (ex, "yes", 80_000))
        f = eng.side_fair(ex, "yes")
        self.assertAlmostEqual(e["missed_value"], 5_000 * (f - 0.12) + 10_000 * (f - 0.08), places=0)
        self.assertTrue(any(x["kind"] == "EVENT" and "BIG EVENT" in x["text"] for x in eng.events))
        rows = open(os.path.join(TMP, "intel", "trades", datetime.datetime.fromtimestamp(now).strftime("%Y-%m-%d") + ".jsonl")).read()
        self.assertIn('"id": "901"', rows)
        n = fake.tape_reads
        eng.intel_step(time.time())                       # nothing changed: no reads
        self.assertEqual(fake.tape_reads, n)
        # dump tracker (afternoon §4.1): the event is followed for 2 h, then written with its price path
        self.assertEqual(len(eng.dump_track), 1)
        d = next(iter(eng.dump_track.values()))
        self.assertEqual((d["side"], d["extreme"]), ("yes", 0.04))
        self.assertTrue(d["path"])
        d["t"] -= 2 * 3600 + 60
        eng.intel_step(time.time())
        self.assertEqual(eng.dump_track, {})
        rows = [json.loads(l) for l in open(os.path.join(TMP, "intel", "dumps.jsonl"))]
        self.assertEqual(rows[0]["market"], e["market"])

    def test_daily_report_and_leaders_study_from_local_files(self):
        fresh_data(layers=dict(position=dict(state="full"), leaders=dict(state="full")))
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        eng.intel_step(time.time())
        docs = os.path.join(TMP, "docs")
        dpath, lpath, _ = l_intel.run_daily(TMP, docs, time.time())
        md = open(dpath).read()
        for h in ("## 1. Scoreboard", "## 2. Where the money went", "## 3. What we captured vs missed", "## 4. Leaders",
                  "## 5. Coverage", "## 6. Recommendations"):
            self.assertIn(h, md)
        self.assertIn("## Style rules", open(lpath).read())


class LeadersTests(EngineCase):
    def test_logger_and_marks_check(self):
        fresh_data(layers=dict(position=dict(state="full"), leaders=dict(state="full")))
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        with open(os.path.join(TMP, "leaders.csv")) as fh:
            rows = list(csv.DictReader(fh))
        self.assertEqual(len(rows), 32, "the fake's 30 leaders, #100 and our own row")
        self.assertEqual(sum(r["ours"] == "1" for r in rows), 1)
        self.assertTrue(eng.dash_state()["leaders"]["series"])
        with open(os.path.join(TMP, "leaders.csv"), "a", newline="") as fh:   # a sample 6 h later
            w = csv.writer(fh)
            later = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 6 * 3600 + 60))
            for r in rows:
                v = float(r["value"]) + (5000 if r["name"] == "leader-1" else 0)
                n = int(r["trades"]) + (0 if r["name"] == "leader-1" else 3)
                w.writerow([later, r["rank"], r["name"], r["profile"], v, n, r["volume"], r["roi"], r["ours"]])
        eng.leaders_marks_check(time.time())
        with open(a_config.NIGHT_LOG_FILE) as fh:
            log = fh.read()
        self.assertIn("leaders marks check", log)
        self.assertIn("leader-1 +5,000 on 0 trades", log)


class PromotionTests(EngineCase):
    def test_mm_pilot_promoted_after_its_gate(self):
        classic_capital(self)
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.pilot_sets = {}                               # the whole fixture is the pilot
        loop(eng, fake)
        loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
        for o in mm[:22]:                                 # turnover MM: entries fill ...
            fake.fill(o["id"], o["qty"])
        for _ in range(2):
            loop(eng, fake)
        exits = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
                 and jx.reduces(o["side"], "buy", fake.pos.get(o["ex"], 0))]
        self.assertTrue(exits, "the turnover MM offers its inventory back")
        for o in exits:                                   # ... and the exits fill: round trips with realized P&L
            fake.fill(o["id"], o["qty"])
        old = f_engine.PILOT_S
        f_engine.PILOT_S = 0
        try:
            loop(eng, fake)
        finally:
            f_engine.PILOT_S = old
        self.assertEqual(eng.controls.state("mm"), "full", eng.layer_stats({"mm": 0}).get("mm"))
        with open(a_config.NIGHT_LOG_FILE) as fh:
            self.assertIn("layer mm: pilot -> full (engine: pilot gate passed", fh.read())


class RealtimeTests(EngineCase):
    def test_fill_event_wakes_the_planner_and_is_timed(self):
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        for _ in range(3):
            loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"
              and o["ex"] not in POS]
        fake.fill(mm[0]["id"], mm[0]["qty"])              # an MM entry fills: its reducing quote must go out
        eng.rt_event("fills", {mm[0]["ex"]}, time.time())
        self.assertTrue(eng.wake.is_set(), "the planner is woken")
        loop(eng, fake)
        lat = eng.latency_view()
        self.assertEqual(lat["fill_to_queued"]["n"], 1)
        self.assertLess(lat["fill_to_queued"]["p50"], 3.0, "A7: fill -> exit queued within 3 s")
        self.assertGreaterEqual(lat["plan_cycle"]["n"], 4)
        self.assertIn("latency", eng.dash_state()["health"])


class DashV3PayloadTests(EngineCase):
    def test_payload_follows_the_v3_contract(self):
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.leaders_t = 0
        for _ in range(3):
            loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
        fake.fill(mm[0]["id"], mm[0]["qty"])
        loop(eng, fake)
        s = eng.dash_state()
        for k in ("money", "daily", "series", "capital", "strategies", "holdings", "activity"):
            self.assertIn(k, s)
        m = s["money"]
        for k in ("account_sig", "start", "vs_start", "rank", "n_traders", "to_rank", "today", "total", "fair"):
            self.assertIn(k, m)
        self.assertEqual(m["rank"], 585)
        self.assertEqual(m["n_traders"], 700)
        t = m["total"]
        self.assertAlmostEqual(t["sum"], t["realized"] + t["unrealized"], places=2)
        self.assertEqual(t["ok"], abs(t["diff"]) <= 50)
        self.assertEqual(s["daily"][-1]["partial"], True)
        cap = s["capital"]
        self.assertEqual([b["key"] for b in cap["buckets"]][-1], "cash")
        self.assertTrue(all(b["sig"] >= 0 for b in cap["buckets"]))
        self.assertEqual([r["key"] for r in s["strategies"]],
                         ["position", "recycler", "release", "mm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "leaders"])
        self.assertTrue(all(h["side"] in ("yes", "no") and h["shares"] > 0 for h in s["holdings"]))
        mm_row = next(r for r in s["strategies"] if r["key"] == "mm")
        self.assertEqual(mm_row["fills_today"], 1)
        self.assertTrue(s["activity"] and s["activity"][0]["text"].startswith("Market maker bought"), s["activity"][:1])
        self.assertIn("thresholds", s["leaders"])
        import g_dashboard as dashboard
        self.assertLess(len(dashboard.dumps(s)), 300_000)


class DashV4PayloadTests(EngineCase):
    """dashboard_v4.md 'Done when': today = Δ account since 17:00 PT; Σ strategies + drift = today ± 25; the Strategies total
    is the Live 'today'; the Holdings total is the Capital 'in holdings'. DASH_FIXTURE=1 also writes tests/fixtures/dash_v4.json."""
    def test_v4_payload_adds_up_on_one_clock(self):
        import csv as _csv
        import m_dash4
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.leaders_t = 0
        for _ in range(2):
            loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
        fake.fill(mm[0]["id"], mm[0]["qty"])
        loop(eng, fake)
        # one pnl.csv sample at the SIG day start (17:00 PT), 200 below the fake's fixed account value and unrealized
        start = m_dash4.sig_day_start(time.time())
        with open(a_config.PNL_FILE, "w", newline="") as fh:
            w = _csv.writer(fh)
            w.writerow(["time", "account_value", "account_book", "account_pm", "unrealized", "roi_pct"])
            w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)), 92800.0, 92800.0, 92800.0, -6200.0, -7.2])
        eng.acct = fake.pnl()
        snap_path = m_dash4.snapshot_path(eng, start)
        if os.path.exists(snap_path):
            os.remove(snap_path)                            # the loops above took a late snapshot: replace it with an on-time one
        s = eng.dash_state()
        for k in ("sigday", "status", "daily4", "feed", "strategies4", "capital4", "holdings4", "leaders4", "admin4"):
            self.assertTrue(s.get(k) not in (None, [], {}), k)
        sd = s["sigday"]
        self.assertEqual(sd["since"], start)
        self.assertAlmostEqual(sd["today"], 93000.0 - 92800.0, places=2, msg="today = Δ account since 17:00 PT")
        self.assertAlmostEqual(sd["our_trades"] + sd["drift"], sd["today"], delta=25)
        self.assertIsNone(sd["check"], "no on-time snapshot: no check yet")
        self.assertIn("snapshot", sd["note"])
        self.assertTrue(sd["ok"])
        # an on-time 17:00 snapshot: every position held then, SIG marks 1c lower than now -> drift estimate = Σ q0 x 0.01
        pos = {ex: [b["pos"], eng.sig_mark(ex, "yes") - 0.01] for ex, b in eng.book.items() if abs(b.get("pos", 0)) >= 1}
        est = sum(q * 0.01 for q, _ in pos.values())
        os.makedirs(os.path.dirname(snap_path), exist_ok=True)
        with open(snap_path, "w") as fh:
            json.dump(dict(start=start, t=start + 20, late=False, account=93000.0 - (sd["our_trades"] + est), cash=0, positions=pos), fh)
        sd = eng.dash_state()["sigday"]
        self.assertAlmostEqual(sd["drift_estimate"], est, places=2)
        self.assertAlmostEqual(sd["check"], 0.0, places=1, msg="drift (residual) = the snapshot estimate")
        self.assertTrue(sd["ok"])
        with open(snap_path) as fh:
            snap = json.load(fh)
        snap["account"] -= 300                               # a 300 gap between the two -> red on Admin
        with open(snap_path, "w") as fh:
            json.dump(snap, fh)
        sd = eng.dash_state()["sigday"]
        self.assertFalse(sd["ok"])
        self.assertAlmostEqual(abs(sd["check"]), 300.0, places=1)
        snap["account"] += 300                               # back to the consistent snapshot for the rest of the checks
        with open(snap_path, "w") as fh:
            json.dump(snap, fh)
        s = eng.dash_state()
        sd = s["sigday"]
        self.assertTrue(sd["ok"])
        rows = s["strategies4"]
        self.assertEqual([r["key"] for r in rows], ["dump", "wall", "touch", "scalp", "mm", "sniper", "fatfinger", "longshot", "pairs", "recycler", "release", "position"])
        self.assertEqual(next(r for r in rows if r["key"] == "mm")["trades_today"], 1)
        self.assertAlmostEqual(sum(r["pnl_today"] for r in rows) + sd["drift"], sd["today"], delta=25, msg="Strategies total = Live today")
        self.assertAlmostEqual(sum(h["value_sig"] for h in s["holdings4"]), s["capital4"]["holdings"], places=2, msg="Holdings total = Capital in holdings")
        self.assertGreater(s["capital4"]["holdings"], 0)
        for h in s["holdings4"]:
            self.assertIn("exit", h)
            self.assertIn("age_h", h)
        c4 = s["capital4"]
        self.assertAlmostEqual(c4["in_use"], c4["tranches"] + c4["scalper"] + c4["takes"], places=2)
        self.assertEqual([b["key"] for b in c4["bar"]], ["floor", "reserve", "free", "tranches", "settle"])
        self.assertEqual(s["status"]["kind"], "TRADING")
        self.assertTrue(s["feed"] and s["feed"][0]["kind"] in ("trade", "alert", "market") and s["feed"][0]["id"])
        self.assertEqual(s["daily4"][-1]["partial"], True)
        self.assertAlmostEqual(s["daily4"][-1]["total"], sd["today"], places=2)
        a4 = s["admin4"]
        self.assertEqual(a4["recon"]["ok"], True)
        self.assertIn("dump", a4["coverage"])
        import g_dashboard as dashboard
        self.assertLess(len(dashboard.dumps(s)), 300_000)
        if os.environ.get("DASH_FIXTURE"):
            with open(os.path.join(os.path.dirname(__file__), "fixtures", "dash_v4.json"), "w") as fh:
                fh.write(dashboard.dumps(s))


class ScalperTests(EngineCase):
    """Day 5 §2 (Mode A): on the busiest markets join the best bid (improve by a tick when the spread is >= 2 ticks) only
    at <= fair_side - 2c, clips 300-500, <= 2 buys per market, 3k per market / the slice in total; exits a tick inside the
    ask >= entry + 0.5c, entry + 0 after 30 min, the dump tranche rules after 2 h; yields to the dump catcher; 6 writes/min;
    the 2 h gate; the §3 idle-cash rule."""

    def rig(self, state="pilot", cash_above_floor=60_000, n_markets=12, spread_ticks=2):
        fresh_data(layers=dict(position=dict(state="full"), scalp=dict(state=state)))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        mids = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None
                and 0.20 <= eng.fair[b.ex] <= 0.80][:n_markets]
        self.assertGreaterEqual(len(mids), n_markets)
        now = time.time()
        for i, ex in enumerate(mids):                      # traded value ranks them: mids[0] busiest
            eng.vol24[ex] = collections.deque([(now - 600, 100_000.0 - i * 1_000)])
            f = eng.fair[ex]
            bid = bots.tick_down(round(f - 0.05, 3))
            fake.px[ex] = (bid, round(bid + spread_ticks * a_config.TICK, 3), bid)
        return fake, eng, mids

    def scalp_orders(self, eng, fake, ex=None, layer="scalp"):
        return [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer
                and (ex is None or o["ex"] == ex)]

    def age_orders(self, eng, seconds=61):
        for o in eng.ledger.orders.values():
            if o.get("layer") in ("scalp", "scalpexit"):
                o["ts"] = (o.get("ts") or time.time()) - seconds

    def test_pilot_bids_on_the_10_busiest_markets_within_the_write_budget(self):
        fake, eng, mids = self.rig()
        loop(eng, fake)
        mine = self.scalp_orders(eng, fake)
        self.assertEqual(len(mine), a_config.SCALP_WRITES_PER_MIN, "6 placements in the first minute")
        for _ in range(3):                                 # the ledger's placements age out of the minute: 6 more each time
            self.age_orders(eng)
            loop(eng, fake)
        mine = self.scalp_orders(eng, fake)
        exs = {o["ex"] for o in mine}
        self.assertTrue(exs <= set(mids[:a_config.SCALP_PILOT_MARKETS]), "pilot: only the 10 busiest")
        self.assertNotIn(mids[11], exs)
        per = collections.Counter(o["ex"] for o in mine)
        self.assertTrue(all(n <= a_config.SCALP_MAX_BUYS for n in per.values()), per)
        for o in mine:
            f = eng.fair[o["ex"]]
            fs = f if o["side"] == "yes" else 1 - f
            bid_y, ask_y, _ = fake.px[o["ex"]]
            best = bid_y if o["side"] == "yes" else round(1 - ask_y, 3)
            self.assertAlmostEqual(o["price"], round(best + a_config.TICK, 3), places=3, msg="spread 2 ticks: improve by a tick")
            self.assertLessEqual(o["price"], fs - a_config.SCALP_BUY_EDGE + 1e-9)
            self.assertEqual(o["qty"], a_config.SCALP_CLIP_MAX)
            self.assertEqual(eng.ledger.orders[o["id"]]["sub"], f"scalp:{o['ex']}:{o['side']}")
        self.assertLessEqual(sum(o["qty"] * o["price"] for o in mine), eng.scalp_slice() + 1e-6)
        self.assertEqual(engine_order_problems(fake, eng.ledger), [])

    def test_join_when_the_spread_is_one_tick_and_never_above_fair_minus_2c(self):
        fake, eng, mids = self.rig(spread_ticks=1)
        ex = mids[0]
        fake.px[ex] = (bots.tick_down(round(eng.fair[ex] - 0.015, 3)), bots.tick_down(round(eng.fair[ex] - 0.010, 3)), 0.5)   # bid 1.5c under fair
        loop(eng, fake)
        mine = self.scalp_orders(eng, fake)
        self.assertTrue(mine)
        self.assertFalse([o for o in mine if o["ex"] == ex and o["side"] == "yes"], "YES bid would be above fair - 2c")
        for o in mine:
            bid_y, ask_y, _ = fake.px[o["ex"]]
            best = bid_y if o["side"] == "yes" else round(1 - ask_y, 3)
            self.assertAlmostEqual(o["price"], best, places=3, msg="spread 1 tick: join")

    def test_fill_exit_steps_and_handover_to_the_dump_rules(self):
        fake, eng, mids = self.rig()
        loop(eng, fake)
        o = next(o for o in self.scalp_orders(eng, fake) if o["side"] == "yes")
        ex, px = o["ex"], o["price"]
        fake.fill(o["id"], o["qty"])
        loop(eng, fake)
        sub = f"scalp:{ex}:yes"
        self.assertEqual(eng.ledger.subbooks[ex].get(sub), o["qty"])
        ex_orders = self.scalp_orders(eng, fake, ex, layer="scalpexit")
        self.assertEqual(len(ex_orders), 1, ex_orders)
        ask_y = fake.px[ex][1]
        want_px = bots.tick_up(round(max(ask_y - a_config.TICK, px + a_config.SCALP_SELL_MIN), 6))
        self.assertAlmostEqual(1 - ex_orders[0]["price"], want_px, places=3, msg="ask - 1 tick, never below entry + 0.5c")
        self.assertEqual(ex_orders[0]["qty"], o["qty"])
        self.assertEqual(engine_order_problems(fake, eng.ledger), [])
        t = eng.ledger.tranches[sub]
        self.assertEqual((t["kind"], t["state"]), ("scalp", "open"))
        self.assertAlmostEqual(t["avg"], px, places=4)
        t["ts"] -= a_config.SCALP_STEP1_S + 60                 # 30 min later: still entry + 0.5c (day 6 rule 0: never at a loss, never at entry + 0)
        loop(eng, fake)
        loop(eng, fake)
        ex_orders = self.scalp_orders(eng, fake, ex, layer="scalpexit")
        self.assertEqual(len(ex_orders), 1, [e["text"] for e in eng.events[-5:]])
        self.assertAlmostEqual(1 - ex_orders[0]["price"], want_px, places=3, msg="entry + 0.5c holds after 30 min")
        self.assertFalse(any(e["kind"] == "RULE0" for e in eng.events), "the scalper's exits never trip rule 0")
        t["ts"] -= a_config.SCALP_HAND_S                       # 2 h later: the dump catcher's tranche rules
        loop(eng, fake)
        self.assertNotIn(sub, {k for k, v in eng.ledger.subbooks[ex].items() if abs(v) >= 1})
        dsub = f"dump:{ex}:yes:scalp"
        self.assertEqual(eng.ledger.subbooks[ex].get(dsub), o["qty"])
        self.assertEqual(eng.ledger.tranches[dsub]["state"], "open")
        self.assertEqual(eng.ledger.tranches[sub]["state"], "handed")
        loop(eng, fake)
        self.assertTrue(eng.dump_view.get(ex), "the dump tracker now owns the sell-back")
        self.assertFalse(self.scalp_orders(eng, fake, ex, layer="scalpexit"))
        self.assertTrue(self.scalp_orders(eng, fake, ex, layer="dumpexit"))

    def test_round_trip_closes_the_tranche_and_counts_realized(self):
        fake, eng, mids = self.rig()
        loop(eng, fake)
        o = next(o for o in self.scalp_orders(eng, fake) if o["side"] == "yes")
        fake.fill(o["id"], o["qty"])
        loop(eng, fake)
        x = self.scalp_orders(eng, fake, o["ex"], layer="scalpexit")[0]
        fake.fill(x["id"], x["qty"])
        loop(eng, fake)
        t = eng.ledger.tranches[f"scalp:{o['ex']}:yes"]
        self.assertEqual(t["state"], "closed")
        s = eng.scalp_stats(time.time() - 3600)
        self.assertEqual((s["buys"], s["sells"], s["trades"]), (1, 1, 2))
        self.assertGreater(s["realized"], 0)
        self.assertGreater(s["volume"], 0)

    def test_runs_all_day(self):
        """Day 6 §10.3: no 04:30 / 05:45 / 06:00-17:00 schedule; the scalper only yields to the catcher and hands off after 2 h."""
        self.assertIsNone(a_config.SCALP_DAY_OFF)
        fake, eng, mids = self.rig()
        loop(eng, fake)                                       # the engine reads the rig's books
        noon = time.mktime(time.strptime(time.strftime("%Y-%m-%d") + " 12:00:00", "%Y-%m-%d %H:%M:%S"))
        for o in eng.ledger.orders.values():                  # the write budget counts placements within 60 s of `now`
            if o.get("layer") == "scalp":
                o["ts"] = noon - 120
        plan = eng.scalp_plan(noon, 60_000 + a_config.CASH_RESERVE, {})
        self.assertTrue(plan, "bids at noon too")

    def test_yields_to_the_dump_catcher_and_pauses(self):
        fake, eng, mids = self.rig(cash_above_floor=12_000)        # funded reserve 12k < 15k
        loop(eng, fake)
        self.assertEqual(self.scalp_orders(eng, fake), [])
        self.assertTrue(any("yields to the dump catcher" in e["text"] for e in eng.events))
        fake, eng, mids = self.rig()
        eng.controls.set_paused(True, "test")
        loop(eng, fake)
        self.assertEqual(self.scalp_orders(eng, fake), [])

    def test_inventory_caps_per_market_and_slice(self):
        fake, eng, mids = self.rig()
        eng.scalp_cost = lambda ex=None: 9_990.0 if ex is None else 0.0        # the slice is nearly full (room 10 < a 300-share clip)
        loop(eng, fake)
        self.assertEqual(self.scalp_orders(eng, fake), [], "no room for a 300-share clip")
        eng.scalp_cost = lambda ex=None: 0.0 if ex is None else (2_960.0 if ex == mids[0] else 0.0)
        loop(eng, fake)
        self.assertEqual(self.scalp_orders(eng, fake, mids[0]), [], "market at its 3k cap")
        self.assertTrue(self.scalp_orders(eng, fake, mids[1]))

    def test_requotes_only_with_budget_and_keeps_resting_orders(self):
        fake, eng, mids = self.rig()
        loop(eng, fake)
        before = {o["id"]: o["price"] for o in self.scalp_orders(eng, fake)}
        for ex in mids:                                    # the touch moves a tick everywhere
            b, a, l = fake.px[ex]
            fake.px[ex] = (round(b - a_config.TICK, 3), round(a - a_config.TICK, 3), l)
        loop(eng, fake)
        after = {o["id"]: o["price"] for o in self.scalp_orders(eng, fake)}
        self.assertEqual(set(after), set(before), "no budget this minute: the resting orders stay as they are")
        self.age_orders(eng)
        loop(eng, fake)
        after = {o["id"]: o["price"] for o in self.scalp_orders(eng, fake)}
        self.assertTrue(set(after) != set(before), "with budget the touch move is requoted")
        self.assertLessEqual(len(after), len(before) + a_config.SCALP_WRITES_PER_MIN)

    def test_gate_and_promotion(self):
        fake, eng, mids = self.rig()
        g = eng.scalp_gate(time.time() - 7200, time.time())
        self.assertFalse(g["ok"])
        eng.scalp_stats = lambda since: dict(trades=80, buys=40, sells=40, volume=60_000.0, realized=120.0, markout30=0.001, n_mo=40, avg_inv=8_000, pace=4.5)
        self.assertTrue(eng.scalp_gate(time.time() - 7200, time.time())["ok"])
        eng.scalp_stats = lambda since: dict(trades=80, buys=40, sells=40, volume=60_000.0, realized=120.0, markout30=0.001, n_mo=40, avg_inv=8_000, pace=2.0)
        self.assertFalse(eng.scalp_gate(time.time() - 7200, time.time())["ok"], "turnover below 3x/day")
        eng.scalp_stats = lambda since: dict(trades=80, buys=40, sells=40, volume=60_000.0, realized=120.0, markout30=0.001, n_mo=40, avg_inv=8_000, pace=4.5)
        with eng.controls.lock:
            eng.controls.d["layers"]["scalp"]["since"] = time.time() - a_config.SCALP_PILOT_S - 10
            eng.controls.save()
        loop(eng, fake)
        self.assertEqual(eng.controls.state("scalp"), "full")
        self.assertEqual(eng.scalp_slice(), a_config.LAYER_CAPS["scalp"]["inventory"][1])
        with eng.controls.lock:                           # the ratchet: a passing gate 2 h later raises the slice x1.25
            eng.controls.d["ratchet"] = {"scalp": dict(level=0, since=time.time() - 2 * 3600 - 1, hold_until=0.0, history=[])}   # 2-h cadence
        loop(eng, fake)
        self.assertEqual(eng.controls.d["ratchet"]["scalp"]["level"], 1)
        self.assertAlmostEqual(eng.scalp_slice(), 12_500.0)

    def test_idle_cash_rule_steps_and_reverses(self):
        fake, eng, mids = self.rig(state="full")
        now = time.time()
        eng.cap_use_hist.clear()
        for i in range(25):                                # 2 h of 20% capital use
            eng.cap_use_hist.append((now - a_config.IDLE_HOLD_S + i * 300, 0.20))
        eng.idle_rule(now)
        self.assertEqual(eng.controls.d["idle"]["scalp_bonus"], 5_000)
        self.assertAlmostEqual(eng.scalp_slice(), 15_000.0)
        eng.idle_rule(now)
        self.assertEqual(eng.controls.d["idle"]["scalp_bonus"], 5_000, "one step per 2 h")
        eng.controls.d["idle"]["t"] = now - a_config.IDLE_HOLD_S
        eng.idle_rule(now)
        self.assertAlmostEqual(eng.scalp_slice(), 20_000.0, msg="capped at 20k")
        eng.controls.d["idle"]["t"] = now - a_config.IDLE_HOLD_S
        eng.idle_rule(now)
        self.assertGreater(eng.controls.d["idle"]["roc_until"], now)
        self.assertEqual(eng.roc_hold(now), a_config.IDLE_ROC_MIN)
        self.assertEqual(len([e for e in eng.events if e["kind"] == "IDLE"]), 3)
        eng.cap_use_hist.append((now, 0.75))                # use > 70%: reversed
        eng.idle_rule(now)
        self.assertEqual(eng.controls.d["idle"]["scalp_bonus"], 0)
        self.assertEqual(eng.roc_hold(now), a_config.ROC_HOLD_MIN)
        self.assertAlmostEqual(eng.scalp_slice(), 10_000.0)
        eng.cap_use_hist.clear()                            # a pause in the last 2 h: no step
        for i in range(25):
            eng.cap_use_hist.append((now - a_config.IDLE_HOLD_S + i * 300, 0.20))
        eng.controls.d["idle"]["t"] = 0
        eng.pause_t = now - 600
        eng.idle_rule(now)
        self.assertEqual(eng.controls.d["idle"]["scalp_bonus"], 0)


class LiveDataGuardTests(EngineCase):
    def test_an_engine_on_the_live_data_dir_refuses_without_live(self):
        """Day 6 §5.2: the 19:52 incident can't repeat: Engine() on the repo's data/ raises unless started with --live."""
        self.addCleanup(setattr, f_engine, "DATA", f_engine.DATA)
        f_engine.DATA = f_engine.Engine.LIVE_DATA
        self.assertNotIn("--live", sys.argv)
        with self.assertRaises(RuntimeError):
            engine(new_fake())
        self.assertNotEqual(os.path.realpath(os.environ.get("SIG_DATA", "")), os.path.realpath(f_engine.Engine.LIVE_DATA))


class TravelTests(EngineCase):
    def test_travel_pulls_entries_keeps_exits_and_longshots_and_resumes_after_a_healthy_wake(self):
        """Day 6 §4.2: ./travel.sh pauses new risk and pulls engine entries (not position exits, not longshot bids, not
        sell-backs); the pause lifts by itself 2 min after a wake (loop gap > 5 min) once SIG and Polymarket answer."""
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full"), longshot=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90]
        for _ in range(4):                                # like DumpV3Tests.settle: SIG trades 6c under fair on the favourites
            eng.ex.capacity = 6000.0
            for ex in favs:
                seed_ref(eng, ex, mid=eng.fair[ex] - 0.06)
            loop(eng, fake)
        layers = lambda: collections.Counter(eng.ledger.orders[o["id"]]["layer"] for o in fake.open_orders() if o["id"] in eng.ledger.orders)
        before = layers()
        self.assertTrue(before.get("dump") and before.get("longshot"), before)
        pos_exits = lambda: sum(1 for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["bot"] in eng.position_names
                                and jx.reduces(o["side"], o["action"], fake.pos.get(o["ex"], 0)))
        n_pos = pos_exits()
        self.assertGreaterEqual(n_pos, 4)
        msg = eng.controls_api.apply("travel", {"on": True}) if hasattr(eng, "controls_api") else f_engine.EngineControls(eng).apply("travel", {"on": True})
        self.assertIn("TRAVEL on", msg)
        self.assertTrue(eng.controls.paused)
        loop(eng, fake)
        loop(eng, fake)
        after = layers()
        self.assertEqual(after.get("dump", 0), 0, after)
        self.assertEqual(after.get("longshot"), before.get("longshot"), "longshot bids stay")
        self.assertEqual(pos_exits(), n_pos, "position exits stay")
        self.assertTrue(all(int(o["id"]) in set(fake.manual.values()) or o["id"] in eng.ledger.orders for o in fake.open_orders()), "manual orders untouched")
        now = time.time()
        self.assertFalse(eng.travel_ok(now), "no wake yet: stays paused")
        eng.wake_t = now - 60
        self.assertFalse(eng.travel_ok(now), "a wake 1 min ago: not yet")
        eng.wake_t = now - 130
        self.assertTrue(eng.travel_ok(now), "2 healthy minutes after the wake")
        with eng.controls.lock:                           # the auto-resume lifts it on the next loop (5 min after the pause)
            eng.controls.d["auto_pause"]["t"] = now - 400
            eng.controls.save()
        loop(eng, fake)
        self.assertFalse(eng.controls.paused)
        f_engine.EngineControls(eng).apply("travel", {"on": True})
        self.assertTrue(eng.controls.paused)
        f_engine.EngineControls(eng).apply("travel", {"on": False})
        self.assertFalse(eng.controls.paused, "./travel.sh off resumes by hand")
        self.assertTrue(any(e["kind"] == "TRAVEL" for e in eng.events))


class DoubleBookingTests(EngineCase):
    """Oct 5 02:30: SIG's fills page and its positions lag each other by seconds either way. A synthetic order-shrink fill
    must never exceed the order's own arithmetic (shares gone from the book minus shares already booked), and the ledger
    can undo the duplicates it already holds."""

    def test_order_truth_never_books_what_the_projection_already_booked(self):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.controls.d["truth_mode"] = "book"
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS)
        oid = "4242"
        eng.ledger.orders[oid] = dict(bot=eng.owner[ex].name, layer="dump", ex=ex, side="no", action="buy", price=0.815, qty=389, filled=389.0,
                                      cost=389 * 0.815, left=0, placed=stamp_(), ts=time.time() - 30, closed=None, status="open", sub=f"dump:{ex}:no:0", note="")
        now = time.time()
        eng.truth_prev = {oid: 389.0}                       # the last snapshot still showed the order resting (stale by a few seconds)
        eng.truth_filled = {}
        eng.truth_read_t = now - 10
        eng.truth_live_t = now - 100
        eng.open = [o for o in eng.open if o["id"] != oid]
        prev = {ex: (0, {})}
        booked = eng.order_truth({ex: -389}, prev, collections.Counter(), now, True)   # the position moved by the fill SIG already reported
        self.assertEqual(booked, [], "the shrink is fully explained by the fill already booked: no synthetic fill")
        eng.ledger.orders[oid]["filled"] = 0.0                # the other way round: nothing booked yet -> the synthetic fill is right
        eng.truth_prev = {oid: 389.0}
        eng.truth_read_t = now - 10
        booked = eng.order_truth({ex: -389}, prev, collections.Counter(), now + 1, False)
        self.assertEqual(len(booked), 1)
        self.assertEqual(booked[0]["qty"], 389)

    def test_an_order_we_cancelled_that_vanished_is_not_a_fill(self):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS)
        now = time.time()
        rec = dict(id="4343", bot=eng.owner[ex].name, layer="dump", ex=ex, side="yes", action="buy", price=0.84, qty=4763, filled=1891.0, cost=0.0,
                   left=2872, placed=stamp_(), ts=now - 3000, closed=None, status="open", sub=f"dump:{ex}:yes:0", note="", ttl=3600)
        eng.ledger.orders["4343"] = rec
        self.assertEqual(eng.vanished_fill("4343", rec, 2872.0, now), 2872.0, "no cancel asked: a vanished order is a fill")
        eng.ex.recent_cancels["4343"] = now - 5                 # we asked for its DELETE 5 s ago (the 50-min refresh); the ack lags
        self.assertEqual(eng.vanished_fill("4343", rec, 2872.0, now), 0.0)
        eng.ex.recent_cancels["4343"] = now - 700
        self.assertEqual(eng.vanished_fill("4343", rec, 2872.0, now), 2872.0, "10 min later the memory has expired")

    def test_dedup_removes_duplicates_and_nets_phantom_pairs(self):
        fresh_data()
        fake = new_fake()
        eng = engine(fake)
        L = eng.ledger
        ex = "1043"
        L.orders["9001"] = dict(id="9001", bot="MKT x", layer="dump", ex=ex, side="no", action="buy", price=0.815, qty=389, filled=778.0, cost=0.0, left=0,
                                placed="x", ts=time.time(), closed=None, status="done", sub=f"dump:{ex}:no:0", note="", truth_q=389.0)
        base = dict(order="9001", ex=ex, side="no", qty=389.0, price=0.815, action="buy", realized=0.0, bot="MKT x", layer="dump", before_engine=False,
                    fair=0.9, sig_mid=0.82, edge=0.085, sig_edge=0.005)
        L.fills.append(dict(base, key="49074377:9001", t="2026-10-05 01:01:24"))
        L.fills.append(dict(base, key="order_shrink:9001:52", t="2026-10-05 01:01:27", source="order_shrink"))
        L.orders["9004"] = dict(L.orders["9001"], id="9004", filled=389.0, truth_q=389.0)
        L.fills.append(dict(base, order="9004", key="order_shrink:9004:99", t="2026-10-05 01:40:00", source="order_shrink"))   # genuine: no SIG fill, fits its order
        L.subbooks[ex] = {"inventory": 389.0, f"dump:{ex}:no:0": -389.0}
        L.tranches[f"dump:{ex}:no:0"] = dict(ex=ex, side="no", kind="dump", state="open", ts=time.time(), t="x", avg=0.815)
        L.subbooks["975"] = {"inventory": 1450.0, "pairset:Oklahoma:1": -1396.0}
        dry = L.dedup_synthetic(dry_run=True, window_s=180)
        self.assertEqual(dry["removed"], ["order_shrink:9001:52"])
        self.assertEqual(len(L.fills), 3, "dry run changes nothing")
        r = L.dedup_synthetic(dry_run=False, window_s=180)
        self.assertEqual(r["removed"], ["order_shrink:9001:52"])
        self.assertEqual([f["key"] for f in L.fills if f["order"] in ("9001", "9004")], ["49074377:9001", "order_shrink:9004:99"])
        self.assertEqual(L.orders["9001"]["filled"], 389.0)
        self.assertEqual(L.subbooks.get(ex), {}, "the phantom tranche and the audit's inventory offset cancel")
        self.assertEqual(L.tranches[f"dump:{ex}:no:0"]["state"], "voided")
        self.assertEqual(L.subbooks["975"], {"inventory": 1450.0, "pairset:Oklahoma:1": -1396.0}, "pair-set legs are never netted")
        self.assertEqual(L.dedup_synthetic(dry_run=False), dict(removed=[], netted=[]), "idempotent")
        # an order booked twice with the pair further apart than the window: the synthetic fill goes anyway (booked <= quantity)
        L.orders["9003"] = dict(id="9003", bot="MKT x", layer="release", ex="1037", side="yes", action="buy", price=0.2, qty=2587, filled=5174.0, cost=0.0, left=0,
                                placed="x", ts=time.time(), closed=None, status="done", sub="inventory", note="")
        b3 = dict(order="9003", ex="1037", side="yes", qty=2587.0, price=0.2, action="buy", realized=0.0, bot="MKT x", layer="release", before_engine=False,
                  fair=0.2, sig_mid=0.2, edge=0.0, sig_edge=0.0)
        L.fills.append(dict(b3, key="500:9003", t="2026-10-04 10:00:00"))
        L.fills.append(dict(b3, key="order_shrink:9003:7", t="2026-10-04 10:09:00", source="order_shrink"))
        r = L.dedup_synthetic(dry_run=False)
        self.assertEqual(r["removed"], ["order_shrink:9003:7"])
        self.assertEqual(L.orders["9003"]["filled"], 2587.0)
        # a SIG fill replayed for an order already booked in full (truth_q lost across a restart) is not booked again
        L.orders["9002"] = dict(id="9002", bot="MKT x", layer="scalp", ex="990", side="yes", action="buy", price=0.5, qty=500, filled=500.0, cost=250.0, left=0,
                                placed="x", ts=time.time(), closed=None, status="done", sub="scalp:990:yes", note="")
        L.subbooks["990"] = {"scalp:990:yes": 500.0}
        n0 = len(L.fills)
        L.sync([], [dict(id="77", orderId=9002, exchangeId="990", side="YES", price=0.5, quantity=500, filledAt="2026-10-05T09:15:48Z")],
               lambda ex: "MKT x", lambda ex, side: 0.5, lambda ex, side: 0.5, as_of=time.time())
        self.assertEqual(len(L.fills), n0, "nothing booked beyond the order's quantity")
        self.assertEqual(L.subbooks["990"], {"scalp:990:yes": 500.0})


class CashGuardTests(EngineCase):
    """Oct 5 22:50 (Piero), after the floor breach: resting entries <= 2 x free cash (near-touch and L0 at 3x), none under 2k
    free; L0 holds <= 8k; near-touch levels have a 20-min TTL; a rebuild sells engine lots at the bid to 15k free (rule 0,
    with the -1c exception for lots <= 5c under fair); both sides vanishing at once are never booked as fills."""

    def rig(self, cash_above_floor):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90]
        for _ in range(4):
            eng.ex.capacity = 6000.0
            for ex in favs:
                seed_ref(eng, ex, mid=eng.fair[ex] - 0.06)
            loop(eng, fake)
        return fake, eng, favs

    def entries(self, eng, fake):
        return [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] in ("dump", "fatfinger", "scalp")]

    @staticmethod
    def weight_of(rec):
        """Day 8 §2 weight of a resting entry from its order record."""
        if rec["layer"] == "dump":
            tail = str(rec.get("sub", "")).rsplit(":", 1)[-1]
            return a_config.ENTRY_WEIGHTS["L0" if tail == "L0" else f"L{int(tail) + 1}"]
        return a_config.ENTRY_WEIGHTS[rec["layer"]]

    def test_resting_entries_capped_at_1_5x_free_cash_weighted_and_none_under_2k(self):
        fake, eng, favs = self.rig(cash_above_floor=6_000)
        ents = self.entries(eng, fake)
        self.assertTrue(ents)
        weighted = sum(o["qty"] * o["price"] * self.weight_of(eng.ledger.orders[o["id"]]) for o in ents)
        self.assertLessEqual(weighted, a_config.BUDGET_K_MAX * 6_000 + 1e-6, f"weighted resting entries {weighted:.0f} > k x free")
        self.assertGreater(len([o for o in ents if eng.ledger.orders[o["id"]]["layer"] == "dump"]), 7, "the weighted budget funds more than the 7 levels of the 2x rule")
        cov = eng.dump_cov
        self.assertTrue(cov and cov["eligible"] > 0 and cov["covered"] > 0 and cov["weighted"] <= cov["budget"], cov)
        fake, eng, favs = self.rig(cash_above_floor=1_500)
        self.assertEqual(self.entries(eng, fake), [], "under 2k free: no entry bids at all")
        self.assertTrue(any("no entry bids" in e["text"] for e in eng.events))

    def test_dump_levels_get_the_budget_before_the_fat_finger_and_the_scalper(self):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full"), fatfinger=dict(state="full"), scalp=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 6_000)   # budget 12k
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90]
        for _ in range(4):
            eng.ex.capacity = 6000.0
            for ex in favs:
                seed_ref(eng, ex, mid=eng.fair[ex] - 0.06)
            loop(eng, fake)
        by = collections.Counter(eng.ledger.orders[o["id"]]["layer"] for o in fake.open_orders() if o["id"] in eng.ledger.orders)
        self.assertGreater(by.get("dump", 0), 0, by)
        cost = {l: sum(o["qty"] * o["price"] * self.weight_of(eng.ledger.orders[o["id"]]) for o in fake.open_orders()
                       if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == l) for l in ("dump", "fatfinger", "scalp")}
        k = a_config.BUDGET_K_MAX
        self.assertLessEqual(cost["fatfinger"], max(0.0, k * 6_000 - cost["dump"] - cost["scalp"]) + 1e-6, cost)   # weighted: dump first, fat-finger last
        self.assertLessEqual(sum(cost.values()), k * 6_000 + 1e-6, cost)

    def test_piero_walls_come_off_the_top_of_the_entry_budget(self):
        """C70: the dump levels may use only 1.5 x free minus the resting walls' cost (weight 1.0); the walls are never cut."""
        fake, eng, favs = self.rig(cash_above_floor=20_000)
        cov0 = dict(eng.dump_cov)
        self.assertLessEqual(cov0["weighted"], cov0["budget"] + 1e-6)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and b.ex not in favs and eng.fair.get(b.ex) is not None and eng.fair[b.ex] <= 0.10)
        fake.px[ex] = (0.10, 0.105, 0.10)
        eng.controls.d["layers"]["wall"] = dict(state="full", caps={}, killed=None)
        eng.controls.d["walls"] = [dict(id="wt", ex=ex, side="no", px=0.88, qty=12_000, exit_px=0.105, t=time.time(), ttl_s=86400, hold_s=21600, news_c=0.02, state="active", filled=0.0, by="Piero")]
        for _ in range(3):
            for e_ in favs:
                seed_ref(eng, e_, mid=eng.fair[e_] - 0.06)
            loop(eng, fake)
        wall = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "wall"]
        self.assertEqual(sum(o["qty"] for o in wall), 12_000, "the wall rests in full")
        self.assertAlmostEqual(eng.wall_resting(), 12_000 * 0.88, delta=1.0)
        cov = eng.dump_cov
        k = a_config.BUDGET_K_MAX
        self.assertLessEqual(cov["weighted"], k * 20_000 - 12_000 * 0.88 + 1e-6, cov)
        ents = self.entries(eng, fake)
        weighted = sum(o["qty"] * o["price"] * self.weight_of(eng.ledger.orders[o["id"]]) for o in ents) + eng.wall_resting()
        self.assertLessEqual(weighted, k * 20_000 + 1e-6, f"dump levels + wall weighted {weighted:.0f} > k x free")
        self.assertLess(cov["weighted"], cov0["weighted"], "the wall took budget from the dump levels")

    def test_budget_k_auto_adjust(self):
        """Day 9 §2: k starts at k_max; -0.5 after 30 min with peak use > 70% or a constrained minute; +0.5 after 2 h with
        peak use < 40%, never above k_max; a new k_max in the config resets k."""
        fake, eng, favs = self.rig(cash_above_floor=20_000)
        now = time.time()
        c = eng.controls
        self.assertEqual(eng.budget_k(), a_config.BUDGET_K_MAX)
        self.assertEqual(c.d["budget_k"]["k_max"], a_config.BUDGET_K_MAX)
        k0 = eng.budget_k()
        eng.cap_use_hist.append((round(now), 0.80))              # peak use 80% in the last 30 min
        c.d["budget_k"]["t"] = now - 1000                        # only 17 min since the last step: no change yet
        eng.adjust_budget_k(now)
        self.assertEqual(eng.budget_k(), k0)
        c.d["budget_k"]["t"] = now - 1900
        eng.adjust_budget_k(now)
        self.assertEqual(eng.budget_k(), k0 - 0.5, "peak use > 70% for 30 min: k - 0.5")
        self.assertTrue(any("BUDGET k" in e["text"] for e in eng.events))
        eng.cap_use_hist.clear()
        eng.cap_use_hist.append((round(now), 0.10))
        c.d["budget_k"]["t"] = now - 1900
        eng.constrained_min.append(now - 100)                    # a constrained minute in the last 30 min: another -0.5
        eng.adjust_budget_k(now)
        self.assertEqual(eng.budget_k(), max(a_config.BUDGET_K_MIN, k0 - 1.0))
        eng.constrained_min.clear()
        c.d["budget_k"]["t"] = now - 7300                        # 2 h quiet at 10%: +0.5
        eng.adjust_budget_k(now)
        self.assertEqual(eng.budget_k(), max(a_config.BUDGET_K_MIN, k0 - 1.0) + 0.5)
        for _ in range(5):
            c.d["budget_k"]["t"] = now - 7300
            eng.adjust_budget_k(now)
        self.assertEqual(eng.budget_k(), a_config.BUDGET_K_MAX, "never above k_max")
        self.assertLessEqual(eng.entry_budget(a_config.CASH_RESERVE + 10_000), a_config.BUDGET_K_MAX * 10_000 + 1e-6)

    def test_levels_ranked_by_expected_edge_l0_first_resting_kept(self):
        """Day 9 §3-§4: L0 first within its 8k cap, then (fair - price) x reach / weight; a resting level keeps its slot
        before any new level is funded."""
        fake, eng, favs = self.rig(cash_above_floor=20_000)
        def row(lname, edge, resting=False, px=0.90, qty=1000, ex="x"):
            lvl = 4 if lname == "L0" else int(lname[1]) - 1
            return (lvl, resting, ex, dict(side="yes", px=px, qty=qty, sub=f"dump:{ex}:yes:{lname}", level=lvl, edge=edge, arena=False, l0=lname == "L0",
                                           ttl=3600, weight=a_config.ENTRY_WEIGHTS[lname], lname=lname))
        rows = [row("L3", 0.05, ex="a"), row("L1", 0.02, ex="b"), row("L0", 0.06, ex="c"), row("L2", 0.03, ex="d"), row("L0", 0.055, ex="e")]
        out, cut, spent = eng.allocate_levels(rows, budget=1e9, l0_room=1e9)
        names = [(r[3]["lname"], r[2]) for r in out]
        self.assertEqual(names[:2], [("L0", "c"), ("L0", "e")], "L0 first")
        xe = lambda w: w["edge"] * a_config.DUMP_REACH[w["lname"]] / w["weight"]
        rest = [xe(r[3]) for r in out[2:]]
        self.assertEqual(rest, sorted(rest, reverse=True), "then by expected edge")
        self.assertEqual([r[3]["lname"] for r in out[2:]], ["L3", "L2", "L1"], "5c x 0.10 / 0.15 > 3c x 0.25 / 0.3 > 2c x 0.40 / 0.5")
        out, cut, spent = eng.allocate_levels(rows, budget=1e9, l0_room=1_500)   # 8k cap: one L0 (900) fits, the second (900 more) does not
        self.assertEqual(sum(1 for r in out if r[3]["l0"]), 1)
        self.assertEqual(sum(1 for r in cut if r[3]["l0"]), 1)
        # hysteresis: a resting L3 with a small edge keeps its slot ahead of a better new L1 when only one fits
        rows = [row("L3", 0.03, resting=True, ex="r"), row("L1", 0.03, ex="n")]
        out, cut, spent = eng.allocate_levels(rows, budget=0.90 * 1000 * 0.15 + 1.0, l0_room=1e9)
        self.assertEqual([r[2] for r in out], ["r"], out)
        self.assertEqual([r[2] for r in cut], ["n"])
        self.assertIn("k", eng.dump_cov)
        self.assertIn("by_l", eng.dump_cov)

    def test_near_touch_levels_have_a_20_min_ttl_and_l0_stops_at_8k(self):
        fake, eng, favs = self.rig(cash_above_floor=60_000)
        ttls = {}
        for o in self.entries(eng, fake):
            rec = eng.ledger.orders[o["id"]]
            if rec["layer"] != "dump":
                continue
            best = bots.side_book(*eng.px[o["ex"]][:2], o["side"])[0]
            near = str(rec.get("sub", "")).endswith(":L0") or (best is not None and o["price"] >= best - 0.02 - 1e-9)
            ttls.setdefault(near, set()).add(rec.get("ttl"))
        self.assertEqual(ttls.get(True, {a_config.DUMP_NEAR_TTL_S}), {a_config.DUMP_NEAR_TTL_S})
        self.assertEqual(ttls.get(False, {a_config.DUMP_TTL_S}), {a_config.DUMP_TTL_S})
        ex = favs[0]
        eng.ledger.subbooks.setdefault(ex, {})[f"dump:{ex}:yes:L0"] = 9_000.0
        eng.ledger.tranches[f"dump:{ex}:yes:L0"] = dict(ex=ex, side="yes", kind="dump", state="open", ts=time.time(), t="x", avg=0.9)
        self.assertGreaterEqual(eng.l0_held(), a_config.DUMP_L0_BUDGET)
        for _ in range(3):
            for e_ in favs:
                seed_ref(eng, e_, mid=eng.fair[e_] - 0.06)
            loop(eng, fake)
        self.assertEqual([o for o in self.entries(eng, fake) if str(eng.ledger.orders[o["id"]].get("sub", "")).endswith(":L0")], [], "L0 at its 8k budget: no L0 bids")

    def test_both_sides_vanishing_at_once_are_not_fills(self):
        fresh_data(layers=dict(position=dict(state="full"), fatfinger=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.controls.d["truth_mode"] = "book"
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS)
        now = time.time()
        for oid, side, px in (("5001", "yes", 0.12), ("5002", "no", 0.115)):
            eng.ledger.orders[oid] = dict(id=oid, bot=eng.owner[ex].name, layer="fatfinger", ex=ex, side=side, action="buy", price=px, qty=4996, filled=0.0,
                                          cost=0.0, left=4996, placed=stamp_(), ts=now - 600, closed=None, status="open", sub="fatfinger", note="", ttl=7200)
        eng.truth_prev = {"5001": 4996.0, "5002": 4996.0}
        eng.truth_read_t, eng.truth_live_t = now - 10, now - 100
        eng.open = [o for o in eng.open if o["id"] not in ("5001", "5002")]
        n0 = len(eng.ledger.fills)
        booked = eng.order_truth({ex: 0}, {ex: (0, {})}, collections.Counter(), now, False)
        self.assertEqual(booked, [])
        self.assertEqual(len(eng.ledger.fills), n0, "SIG pulled both sides at the cash floor: nothing was filled")

    def test_rebuild_sells_engine_lots_at_the_bid_to_15k_free(self):
        fake, eng = Day4Tests.rig(self)
        self.addCleanup(setattr, f_engine, "RELEASE_QUIET_HOURS", f_engine.RELEASE_QUIET_HOURS)
        f_engine.RELEASE_QUIET_HOURS = (0, 0)
        ex = mid_market(eng)
        fake.pos[ex] = 2000
        fake.avg_cost[ex] = 0.30
        loop(eng, fake)
        bid = fake.px[ex][0]
        eng.ledger.subbooks[ex] = {f"dump:{ex}:yes:0": 2000.0}      # an engine tranche: the release never touched these before
        eng.ledger.tranches[f"dump:{ex}:yes:0"] = dict(ex=ex, side="yes", kind="dump", state="open", ts=time.time(), t="x", avg=round(bid - 0.01, 3))
        eng.ledger.fills.append(dict(key="r1", order="r1", ex=ex, side="yes", qty=2000.0, price=round(bid - 0.01, 3), action="buy", realized=0.0,
                                     t="2026-10-05 12:00:00", bot="b", layer="dump", before_engine=False, fair=None, edge=None, sig_edge=None, sig_mid=None))
        eng.ledger.orders["r1"] = dict(id="r1", bot="b", layer="dump", ex=ex, side="yes", action="buy", price=round(bid - 0.01, 3), qty=2000, filled=2000.0,
                                       cost=0.0, left=0, placed="x", ts=time.time() - 3600, closed=None, status="done", sub=f"dump:{ex}:yes:0", note="")
        eng.lots_cache = (None, {})
        eng.release_t = 0
        plan = eng.release_plan(a_config.CASH_RESERVE + 3_000, time.time(), eng.bucket_usage())    # free 3k < 15k: rebuild
        self.assertEqual(plan.get(ex, {}).get("why"), "rebuild", plan)
        self.assertEqual(sum(plan[ex]["subs"].values()), min(2000, eng.bid_depth(ex, "yes", time.time())))
        self.assertTrue(any("REBUILD sale" in e["text"] for e in eng.events))
        eng.ledger.tranches[f"dump:{ex}:yes:0"]["avg"] = round(bid + 0.005, 3)   # entry 0.5c above the bid: rule 0 blocks it...
        eng.ledger.fills[-1]["price"] = round(bid + 0.005, 3)
        eng.lots_cache = (None, {})
        eng.release_t = 0
        plan = eng.release_plan(a_config.CASH_RESERVE + 3_000, time.time(), eng.bucket_usage())
        self.assertEqual(plan.get(ex, {}).get("why"), "rebuild", "...unless the lot is <= 5c under fair: Piero's -1c exception applies")
        self.assertLess(plan[ex]["rule0_margin"], 0)
        eng.release_t = 0                                      # day 8 §1.5: the exception runs only while free < 10k
        self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 12_000, time.time(), eng.bucket_usage()), "free 12k: rule 0 holds, no exception")
        FAIR[ex] = bid + 0.06                                  # 6c under fair: the exception is off, rule 0 holds
        try:
            loop(eng, fake)
            eng.release_t = 0
            self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 3_000, time.time(), eng.bucket_usage()))
        finally:
            FAIR.pop(ex, None)
        eng.release_t = 0
        self.assertNotIn(ex, eng.release_plan(a_config.CASH_RESERVE + 20_000, time.time(), eng.bucket_usage()), "above 15k free: the tiers rule again")


class WallTests(EngineCase):
    """Piero's manual wall (Oct 5 23:40): a control places one big resting order; it is cut so a full fill leaves >= 2k free;
    what fills rests its buy-back at his YES price (rule 0 applies); after 6 h it is held; a 2c Polymarket move against it
    pulls it; everything is logged."""

    def rig(self, cash_above_floor=60_000):
        fresh_data(layers=dict(position=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] <= 0.10)
        fake.px[ex] = (0.10, 0.105, 0.10)                 # YES 0.10 / 0.105: NO bid 0.895
        return fake, eng, ex

    def walls(self, eng, fake, ex, layer="wall"):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer]

    def test_place_cut_fill_buyback_hold(self):
        fake, eng, ex = self.rig(cash_above_floor=10_000)   # a full 15,000 x 0.88 = 13.2k fill would leave < 2k: cut
        msg = f_engine.EngineControls(eng).apply("wall", dict(ex=ex, side="no", price=0.88, qty=15_000, exit_px=0.105, ttl_h=24, hold_h=6))
        self.assertIn("WALL", msg)
        loop(eng, fake)
        loop(eng, fake)
        ws = self.walls(eng, fake, ex)
        self.assertEqual(len(ws), 1, ws)
        self.assertEqual((ws[0]["side"], ws[0]["action"], ws[0]["price"]), ("no", "buy", 0.88))
        self.assertEqual(ws[0]["qty"], int((10_000 - a_config.WALL_MIN_FREE) / 0.88), "cut so a full fill leaves >= 2k free")
        self.assertTrue(any("cut to" in e["text"] for e in eng.events if e["kind"] == "WALL"))
        fake, eng, ex = self.rig(cash_above_floor=60_000)
        f_engine.EngineControls(eng).apply("wall", dict(ex=ex, side="no", price=0.88, qty=15_000, exit_px=0.105, ttl_h=24, hold_h=6))
        loop(eng, fake)
        loop(eng, fake)
        ws = self.walls(eng, fake, ex)
        self.assertEqual(ws[0]["qty"], 15_000)
        wid = eng.ledger.orders[ws[0]["id"]]["sub"].rsplit(":", 1)[-1]
        fake.fill(ws[0]["id"], 4_000)
        loop(eng, fake)
        loop(eng, fake)
        sub = f"wall:{ex}:no:{wid}"
        self.assertEqual(eng.ledger.subbooks[ex].get(sub), -4_000.0)
        xs = self.walls(eng, fake, ex, layer="wallexit")
        self.assertEqual(len(xs), 1, xs)
        self.assertEqual((xs[0]["side"], xs[0]["action"], xs[0]["price"], xs[0]["qty"]), ("yes", "buy", 0.105, 4_000), "the buy-back: YES bid 0.105 for the filled quantity")
        self.assertEqual(self.walls(eng, fake, ex)[0]["qty"], 11_000, "the wall keeps resting for the rest")
        w = next(w for w in eng.walls() if w["id"] == wid)
        self.assertEqual(w["filled"], 4_000)
        self.assertTrue(any("WALL fill" in e["text"] for e in eng.events))
        eng.ledger.tranches[sub]["ts"] -= a_config.WALL_HOLD_S + 60   # 6 h later: held to settlement, no buy-back
        loop(eng, fake)
        loop(eng, fake)
        self.assertEqual(self.walls(eng, fake, ex, layer="wallexit"), [])
        self.assertEqual(eng.ledger.tranches[sub]["state"], "held")
        self.assertTrue(any("held to settlement" in e["text"] for e in eng.events))
        self.assertEqual(engine_order_problems(fake, eng.ledger), [])

    def test_news_guard_pulls_the_wall_and_rule_0_covers_the_buyback(self):
        fake, eng, ex = self.rig()
        f_engine.EngineControls(eng).apply("wall", dict(ex=ex, side="no", price=0.88, qty=15_000, exit_px=0.105))
        loop(eng, fake)
        loop(eng, fake)
        self.assertTrue(self.walls(eng, fake, ex))
        FAIR[ex] = eng.fair[ex] + 0.025                   # Polymarket YES up 2.5c in 10 min: against a NO buyer
        try:
            for _ in range(3):
                loop(eng, fake)
            self.assertEqual(self.walls(eng, fake, ex), [], "pulled")
            self.assertEqual(next(w for w in eng.walls())["state"], "pulled")
        finally:
            FAIR.pop(ex, None)
        eng.ledger.tranches["wall:x"] = dict(ex=ex, side="no", kind="wall", state="open", ts=time.time(), t="x", avg=0.88)
        self.assertEqual(eng.rule0_allowed(ex, "no", "wall:x", 0.895, 100), 100, "selling NO at 0.895 clears the wall's entry 0.88 + 0.5c")
        self.assertEqual(eng.rule0_allowed(ex, "no", "wall:x", 0.88, 100), 0, "at entry it does not")


class ControlTests(EngineCase):
    def test_position_only_by_flags_places_only_the_exits(self):
        fresh_data(layers=dict(position=dict(state="full")))
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        mine = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders]
        self.assertEqual({o["ex"] for o in mine}, {AK}, "only the AK exits and add (RI is covered by the hand-placed sells)")
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER)
        self.assertFalse([c for c in fake.calls if c[0] == "multileg"], "no pair sets")

    def test_flags_persist_reload_from_disk_and_gate_the_layers(self):
        fresh_data(layers=dict(position=dict(state="full")))
        fake = new_fake()
        eng = engine(fake)
        loop(eng, fake)
        n = len(fake.book)
        with open(a_config.OVERRIDES_FILE) as fh:
            d = json.load(fh)
        d["layers"]["pairs"] = dict(state="pilot")
        time.sleep(0.01)
        with open(a_config.OVERRIDES_FILE, "w") as fh:          # edited by hand: picked up on the next loop
            json.dump(d, fh)
        os.utime(a_config.OVERRIDES_FILE, (time.time() + 5, time.time() + 5))
        loop(eng, fake)
        self.assertEqual(eng.controls.state("pairs"), "pilot")
        self.assertTrue([c for c in fake.calls if c[0] == "multileg"], "pairs started once flagged on")
        eng.controls.kill("pairs", "test kill")
        self.assertEqual(eng.controls.state("pairs"), "off")
        eng2 = engine(fake)                                     # a restart keeps the stage, including the kill
        self.assertEqual(eng2.controls.state("pairs"), "off")
        self.assertEqual(eng2.controls.layer("pairs")["killed"]["reason"], "test kill")
        eng2.controls.set_paused(True, "test")
        fake.calls.clear()
        loop(eng2, fake)
        self.assertFalse([c for c in fake.calls if c[0] in ("batch", "multileg")], "paused: nothing new")
        self.assertEqual(resting(fake, AK, "sell"), AK_LADDER, "exits stay")
        with open(a_config.NIGHT_LOG_FILE) as fh:
            log = fh.read()
        self.assertIn("KILL layer pairs: test kill", log)
        self.assertIn("PAUSE new risk", log)
        self.assertEqual(log.count("<!-- status:auto -->"), 1)
        self.assertIn("**Account at fair:**", log)


if __name__ == "__main__":
    unittest.main()
