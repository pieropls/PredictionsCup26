"""The whole engine (f_engine.py + j_executor.py) against the fake exchange (tests/fake_sig.py) and a fake Polymarket.
No network, nothing sent.   python3 -m unittest tests.test_engine

  scenario          position exits, foreign orders, inventory I2, atomic pair sets, repairs, UNKNOWN freeze + audit
  kill-restart      three kills mid-batch: zero duplicates, every order sent after a restart was planned after it
  stop under load   slow writes, 429, DELETE timeouts, unreadable book: exits and foreign orders survive, nothing placed
  plan-cycle        writes taking 2 s each in the executor's threads do not slow the plan cycle
  dashboard         read-only controls, bound to 127.0.0.1
"""
import os as _os
_os.environ["SIG_FM_PILOT"] = "0"
_os.environ["SIG_RESERVOIR"] = "0"   # C84c: the RI reservoir never switches itself on inside a rig (ReservoirTests set it explicitly)
_os.environ["SIG_C87"] = "0"
_os.environ["SIG_C90"] = "0"   # C90: the edge program's A/B starts only where C90Tests ask for it
_os.environ["SIG_C91"] = "0"
_os.environ["SIG_C92"] = "0"
_os.environ["SIG_C93"] = "0"   # C87: the one-time migration (TM full, FM / scalper deleted) runs only where C87Tests ask for it
_os.environ["SIG_SM_PILOT"] = "0"   # turnover_plan C84: the spread-maker pilot never switches itself on inside a rig (SMTests set the layer explicitly)
_os.environ["SIG_TM_PILOT"] = "0"   # day 11 C83: the touch-maker pilot never switches itself on inside a rig (TMTests set the layer explicitly)   # mm_plan C81: the fair-maker pilot never switches itself on inside a rig (FMTests set targets explicitly); set here because `unittest discover tests` imports this module without tests/__init__.py
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
        self.addCleanup(setattr, f_engine, "RING_WINDOWS", f_engine.RING_WINDOWS)
        f_engine.RING_WINDOWS = []                          # the rigs run on the real clock: no ring window unless a test sets one (RingWindowTests)
        self.addCleanup(setattr, f_engine, "STARTUP_HOLD_S", f_engine.STARTUP_HOLD_S)
        f_engine.STARTUP_HOLD_S = 0                         # the rigs read from their first loop (the 2-min start-up hold is a live-only guard)
        for k, v in (("TM_ENTRY_STOPS", []), ("SM_QUOTE_REGIMES", ("BOUNCE", "MIXED", "QUIET", "TREND", "NEWS", "SWEEP")), ("TM_BUFFER_GATE", False),
                     ("CORE_CAP_COST", 0.0), ("TRADING_HOLD_C", 99.0), ("CORE_LOCK_RET", -99.0), ("TRADING_INV_CAP", 1e12), ("TRADING_CASH_TARGET", 0.0),
                     ("NOEDGE", dict(ret=-99.0, low_fair=0.0, low_edge_c=0.0)), ("REPEG_AWAY_S", 0.0), ("GTC_TTL_S", 0), ("SM_REPEG_TICKS", 2), ("TM_REPEG_TICKS", 2), ("ROTATION", dict(a_config.ROTATION, on=False, sells={}, buys=[]))):   # C88 / C89 / C93 rules off in the legacy rigs (their own test classes turn them on)
            self.addCleanup(setattr, f_engine, k, getattr(f_engine, k, None))
            setattr(f_engine, k, v)                         # C87 gates (real clock, no tape in the rigs, rig cash under BUFFER): off unless C87Tests set them
        for k in ("PNL_READ_S", "ORDERS_READ_S", "FILLS_READ_S", "POSITIONS_READ_S"):   # day 4 §4 / Oct 8 read caching have their own tests; the rigs (no realtime feed) read fresh each loop
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
        self.assertLess(len(dashboard.dumps(eng.last_state)), 350_000, "initial snapshot <= 300 KB")
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

        self.assertEqual(main_with(a_config.ENGINE_HOST), "engine would start", "the Wi-Fi hostname changed; the machine did not (the configured engine host: the server since the Oct 7 cutover)")
        os.remove(a_config.PID_FILE)
        msg = main_with("someone-elses-mac")
        self.assertIn("refusing --live", msg)
        self.assertFalse(os.path.exists(a_config.PID_FILE), "no pid file, no lock taken")
        self.assertIn("refusing --live", main_with(None), "no scutil and no data/host_id: refuse")
        with open(a_config.HOST_ID_FILE, "w") as fh:
            fh.write(a_config.ENGINE_HOST + "\n")
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
        self.assertEqual(eng.controls.state("mm"), "full", "C88 (Piero 01:55): a P&L rule is an alert, not a stop")
        self.assertTrue(any(e["kind"] == "ALERT" and "P&L at fair" in e["text"] for e in eng.events), "the P&L breach raised an alert")
        bot_loss = sum(eng.fill_pnl_fair(f) for f in eng.fills_since(time.time() - 86400))
        self.assertFalse(eng.controls.paused, "the D5 loss stop never pauses")
        if bot_loss < -a_config.LOSS_STOP:
            self.assertTrue(any(e["kind"] == "ALERT" and "D5" in e["text"] for e in eng.events), "D5 is an alert")
        from unittest import mock
        bad = dict(read=dict(n=50, bad_rate=0.9), write=dict(n=0, bad_rate=0.0), throttled=0)
        with mock.patch.object(fake, "health", return_value=bad, create=True):
            loop(eng, fake)
        self.assertTrue(eng.controls.paused, "SIG API errors pause new risk (a hard pause)")
        self.assertEqual(eng.controls.d["auto_pause"]["kind"], "api_errors")
        good = dict(read=dict(n=50, bad_rate=0.0), write=dict(n=0, bad_rate=0.0), throttled=0)
        eng.controls.d["auto_pause"]["t"] = time.time() - 301
        with mock.patch.object(fake, "health", return_value=good, create=True):
            loop(eng, fake)
        self.assertFalse(eng.controls.paused, "and lifts itself once the API is clean")
        with open(a_config.NIGHT_LOG_FILE) as fh:
            log = fh.read()
        self.assertIn("PAUSE (api_errors)", log)
        self.assertIn("AUTO-RESUME after api_errors", log)


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
        self.addCleanup(setattr, f_engine, "RELEASE_QUIET_HOURS", f_engine.RELEASE_QUIET_HOURS)
        f_engine.RELEASE_QUIET_HOURS = (25, 25)            # the rig runs on the real clock: no 07:00-08:00 quiet hour here (the rotation's touch ask would show instead)
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
        self.addCleanup(setattr, f_engine, "RULES_V2", f_engine.RULES_V2)
        f_engine.RULES_V2 = False                              # day 11 rules v2 add the rotation, which rides the release path by design (RulesV2Tests)
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
        self.assertIn(eng.controls.d["auto_pause"]["kind"], ("cash_short", "cash_floor"), "C88: cash under the floor is a hard pause too")
        self.assertEqual(eng.controls.d.get("decisions"), [], "safety stops don't queue decisions")
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 1_500)   # C93e: floor + 1,000 is enough (was + 5,000)
        eng.controls.d["auto_pause"]["t"] -= 400         # 5 min after the pause
        for _ in range(3):
            loop(eng, fake)
        self.assertFalse(eng.controls.paused, "cash >= floor + 1k for 2 loops: resumed by itself (C93e, Piero 14:58 Oct 13)")
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
                and eng.fair[b.ex] >= 0.90 and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE]   # C74: RI Senate has its own 15c rule
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
        n_lv = min(len(offs), 2) if a_config.DUMP_DEEP_OFF else len(offs)          # C87 catcher fix #1: L3 / L4 off
        self.assertEqual(len(mine), n_lv + (1 if l0 else 0), mine)
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
        self.assertGreater(shares[0] * mine[0]["price"], shares[-1] * mine[-1]["price"], "L1 weighs more than the deeper level")
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
        ask_s = bots.side_book(eng.px[ex][0], eng.px[ex][1], "yes")[1]
        want = bots.tick_up(round(max(top["price"] + 0.005, ask_s), 6))   # C88 (Piero 01:55): the exit rests at max(entry + 0.5c, the touch)
        self.assertAlmostEqual(sell, want, places=3)
        t["ts"] -= 1300                                   # time does not step it down any more: still max(entry + 0.5c, touch)
        self.settle(eng, fake, favs, 2)
        rb2 = self.dump_orders(eng, fake, ex, "dumpexit")
        self.assertAlmostEqual(round(1 - rb2[0]["price"], 3), want, places=3)
        t["ts"] -= 2500                                   # never under entry + 0.5c (rule 0)
        self.settle(eng, fake, favs, 2)
        rb2 = self.dump_orders(eng, fake, ex, "dumpexit")
        self.assertGreaterEqual(round(1 - rb2[0]["price"], 3), bots.tick_up(round(top["price"] + 0.005, 6)) - 1e-9)
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
        self.addCleanup(setattr, f_engine, "HOLD_IDLE_GAP", f_engine.HOLD_IDLE_GAP)
        f_engine.HOLD_IDLE_GAP = 0.50                          # day 11 §1.2 would hold an 8c lot while cash is idle (RulesV2Tests); here the ladder
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
        ask_s = bots.side_book(eng.px[ex][0], eng.px[ex][1], "yes")[1]
        self.assertAlmostEqual(1 - xs[0]["price"], bots.tick_up(round(max(bid + 0.005, ask_s), 6)), places=3, msg="C88: the exit at max(entry + 0.5c, the touch)")
        refills = [o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9]
        self.assertEqual(len(refills), 1, "sweep follow: the level is back at the best bid while the tranche is open")
        t["ts"] -= a_config.DUMP_STEP2_S + 60
        self.settle(eng, fake, favs, 2)
        xs = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dumpexit"]
        self.assertAlmostEqual(1 - xs[0]["price"], bots.tick_up(round(bid + a_config.DUMP_STEP2_MIN, 6)), places=3, msg="after 60 min: entry + 0.5c")
        for _ in range(a_config.DUMP_L0_REFILLS):          # DUMP_L0_REFILLS refills in 10 min (5 since C82): the next one waits
            o = [o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9][0]
            fake.fill(o["id"], o["qty"])
            self.settle(eng, fake, favs, 2)
        self.assertEqual([o for o in self.dump_orders(eng, fake, ex) if abs(o["price"] - bid) < 1e-9], [], f"no refill beyond {a_config.DUMP_L0_REFILLS} within 10 min")

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
            eng.ri_senate_base = None                             # day 7 rule (before the C73 base is recorded): the engine's own 10k
            eng.ri_held_cap = None                                # day 11 §1.4 turnover cap (RulesV2Tests); here the day-7 / day-9 rules
            eng.ledger.subbooks[ri] = {"inventory": -14_401.0}       # the old RI Senate R NO book does not count
            self.assertAlmostEqual(eng.race_room(ri, "no", 0.99), a_config.RI_ENGINE_CAP, delta=1)
            eng.ledger.subbooks[ri]["dump:%s:no:L0" % ri] = -4_000.0
            eng.ledger.tranches["dump:%s:no:L0" % ri] = dict(ex=ri, side="no", kind="dump", state="open", ts=time.time(), t="x", avg=0.9)
            self.assertAlmostEqual(eng.race_room(ri, "no", 0.99), a_config.RI_ENGINE_CAP - 3_600, delta=1)
            eng.ri_senate_base = 15_000.0                         # day 9 §9.4: base + 10k, every sub-book counts (the inventory at its SIG cost)
            inv_avg = ((eng.book.get(ri) or {}).get("row") or {}).get("avgCost") or eng.sig_mark(ri, "no")
            self.assertAlmostEqual(eng.race_room(ri, "no", 0.99), 25_000 - 14_401 * inv_avg - 3_600, delta=1)

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
                and eng.fair[b.ex] >= 0.93 and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE]   # C74: RI Senate has its own 15c rule
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
        self.assertLess(len(dashboard.dumps(s)), 350_000)


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
        self.assertEqual([r["key"] for r in rows], ["dump", "tm", "sm", "fm", "swing", "sleeve", "wall", "touch", "scalp", "mm", "sniper", "fatfinger", "longshot", "pairs", "recycler", "release", "position"])   # day 11: the touch maker; turnover_plan: the spread maker
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
        self.assertLess(len(dashboard.dumps(s)), 350_000)
        if os.environ.get("DASH_FIXTURE"):
            with open(os.path.join(os.path.dirname(__file__), "fixtures", "dash_v4.json"), "w") as fh:
                fh.write(dashboard.dumps(s))


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
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90
                and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE]   # C74: RI Senate has its own 15c rule
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
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90
                and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE]   # C74: RI Senate has its own 15c rule
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
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90
                and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE]   # C74: RI Senate has its own 15c rule
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
        k = eng.budget_k()
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
        kmax = c.d["budget_k"]["k_max"]
        self.assertEqual(eng.budget_k(), kmax)
        self.assertEqual(kmax, eng.k_max_for(20_000)[1])
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
        self.assertEqual(eng.budget_k(), kmax, "never above k_max")
        self.assertLessEqual(eng.entry_budget(a_config.CASH_RESERVE + 10_000), kmax * 10_000 + 1e-6)

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
        exp = sorted(["L3", "L2", "L1"], key=lambda n: -{"L3": 0.05, "L2": 0.03, "L1": 0.02}[n] * a_config.DUMP_REACH[n] / a_config.ENTRY_WEIGHTS[n])
        self.assertEqual([r[3]["lname"] for r in out[2:]], exp, "ranked by edge x reach / weight with the configured reach")
        out, cut, spent = eng.allocate_levels(rows, budget=1e9, l0_room=1_500)   # 8k cap: one L0 (900) fits, the second (900 more) does not
        self.assertEqual(sum(1 for r in out if r[3]["l0"]), 1)
        self.assertEqual(sum(1 for r in cut if r[3]["l0"]), 1)
        # hysteresis: a resting L2 with a small edge keeps its slot ahead of a better new L1 when only one fits
        # (an L2, not an L3: day 11 §1.5 caps L3 / L4 at 20% of the budget — RulesV2Tests)
        rows = [row("L2", 0.03, resting=True, ex="r"), row("L1", 0.03, ex="n")]
        out, cut, spent = eng.allocate_levels(rows, budget=0.90 * 1000 * 0.30 + 1.0, l0_room=1e9)
        self.assertEqual([r[2] for r in out], ["r"], out)
        self.assertEqual([r[2] for r in cut], ["n"])
        self.assertIn("k", eng.dump_cov)
        self.assertIn("by_l", eng.dump_cov)
        # C72 breadth-first: side A has two strong levels, side B one weak level; with room for two, B's level comes before A's second
        rows = [row("L1", 0.04, ex="A"), row("L2", 0.04, ex="A"), row("L1", 0.01, ex="B")]
        rows[1][3]["sub"] = "dump:A:yes:L2"
        out, cut, spent = eng.allocate_levels(rows, budget=2 * 0.90 * 1000 * 0.5 + 1.0, l0_room=1e9)
        self.assertEqual(sorted(r[2] for r in out), ["A", "B"], out)
        self.assertEqual([r[2] for r in cut], ["A"])

    def test_k_max_table_by_free_cash(self):
        """Day 9 §9.2: k_max 3.0 at >= 20k free, 3.0 at 15-20k, 2.0 below 15k; a tier is entered upward only 1k past its
        line; a lower tier caps k at once and every change is logged."""
        fake, eng, favs = self.rig(cash_above_floor=20_000)
        c = eng.controls
        now = time.time()
        c.d["buffer"] = dict(value=15_000, day=time.strftime("%Y-%m-%d"))   # day 11: the table hangs off BUFFER; at 15k it is the day-9 table
        self.assertEqual(eng.k_max_for(25_000)[1], 3.0)
        self.assertEqual(eng.k_max_for(17_000)[1], 3.0)
        self.assertEqual(eng.k_max_for(12_000)[1], 2.0)
        self.assertEqual(eng.k_max_for(20_500, cur_tier=15_000)[1], 3.0)
        self.assertEqual(eng.k_max_for(15_500, cur_tier=0)[1], 2.0, "15.5k from below: not yet 1k past the 15k line")
        self.assertEqual(eng.k_max_for(16_100, cur_tier=0)[1], 3.0)
        c.d["budget_k"] = dict(k=3.0, k_max=3.0, tier=20_000, t=now, hist=[])
        eng.adjust_budget_k(now, free=12_000)
        self.assertEqual((c.d["budget_k"]["k"], c.d["budget_k"]["k_max"], c.d["budget_k"]["tier"]), (2.0, 2.0, 7_500), "a lower tier caps k at once (day 11: the BUFFER/2 tier)")
        self.assertTrue(any("BUDGET k_max -> 2" in e["text"] for e in eng.events))
        eng.adjust_budget_k(now, free=21_500)
        self.assertEqual((c.d["budget_k"]["k"], c.d["budget_k"]["k_max"]), (2.0, 3.0), "a higher tier raises k_max; k climbs by the +0.5 rule")

    def test_ri_senate_adds_10k_only_on_levels_15c_under_fair(self):
        """Day 9 §9.4 (Piero): on the RI Senate race the engine may add 10k at cost beyond the recorded base (every sub-book
        counts), and only levels >= 15c under fair are placed."""
        fake, eng, favs = self.rig(cash_above_floor=60_000)
        ex = favs[0]
        eng.meta[ex]["race"] = a_config.RI_SENATE_RACE
        eng.ri_senate_base = None                             # C74: the base is recorded on a priced loop from every sub-book at cost
        eng.ledger.subbooks.setdefault(ex, {})["inventory"] = 2_000.0
        eng.record_ri_base()
        self.assertIsNotNone(eng.ri_senate_base)
        self.assertGreater(eng.ri_senate_base, 1_000, "2,000 shares at a real price, not 0")
        self.assertEqual(eng.controls.d["day9"]["ri_senate_base"], round(eng.ri_senate_base, 2))
        eng.ledger.subbooks[ex].pop("inventory")
        eng.ri_senate_base = 15_000.0
        fs = eng.fair[ex]
        room0 = eng.race_room(ex, "yes", fs)
        self.assertLessEqual(room0, 15_000 + a_config.RI_SENATE_ADD + 1e-6)
        eng.ledger.subbooks.setdefault(ex, {})["inventory"] = 4_000.0   # old inventory counts on RI Senate
        self.assertLess(eng.race_room(ex, "yes", fs), room0)
        eng.ledger.subbooks[ex].pop("inventory")
        for _ in range(3):
            seed_ref(eng, ex, mid=fs - 0.06)                  # levels 7-10c under fair: none allowed on RI Senate
            for e_ in favs[1:]:
                seed_ref(eng, e_, mid=eng.fair[e_] - 0.06)
            loop(eng, fake)
        self.assertEqual([o for o in self.entries(eng, fake) if o["ex"] == ex and eng.ledger.orders[o["id"]]["layer"] == "dump"], [], "no level < 15c under fair on RI Senate")
        for _ in range(3):
            seed_ref(eng, ex, mid=fs - 0.17)                  # levels >= 15c under fair: allowed
            loop(eng, fake)
        lv = [o for o in self.entries(eng, fake) if o["ex"] == ex and eng.ledger.orders[o["id"]]["layer"] == "dump"]
        self.assertTrue(lv)
        self.assertTrue(all(o["price"] <= fs - 0.15 + 1e-9 for o in lv), [o["price"] for o in lv])

    def test_resting_levels_are_not_resized_for_small_cash_moves(self):
        """C72: a resting level keeps its quantity while the fresh size stays within x0.5..x2 of it (a fill or a sell-back
        moving the per-side cost must not cancel and re-place the whole book)."""
        fake, eng, favs = self.rig(cash_above_floor=60_000)
        before = {o["id"]: (o["price"], o["qty"]) for o in self.entries(eng, fake) if eng.ledger.orders[o["id"]]["layer"] == "dump"}
        self.assertTrue(before)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 45_000)   # cash -25%: sizes would shrink ~25%
        fake.calls.clear()
        for _ in range(2):
            for e_ in favs:
                seed_ref(eng, e_, mid=eng.fair[e_] - 0.06)
            loop(eng, fake)
        after = {o["id"]: (o["price"], o["qty"]) for o in self.entries(eng, fake) if eng.ledger.orders[o["id"]]["layer"] == "dump"}
        kept = set(before) & set(after)
        self.assertGreaterEqual(len(kept), 0.5 * len(before), f"{len(kept)} of {len(before)} resting levels kept after a 25% cash move (the smaller budget cuts some; "
                                                             f"with L3 / L4 off since C87 the cheap deep levels that used to survive are gone)")
        same = sum(1 for i in kept if before[i] == after[i])
        self.assertGreaterEqual(same, 0.95 * len(kept), f"{same} of {len(kept)} surviving levels kept their price and size (no re-size churn)")

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
        eng.ledger.subbooks.setdefault(ex, {})[f"dump:{ex}:yes:L0"] = float(int(a_config.DUMP_L0_BUDGET / 0.9) + 1_000)   # above the L0 held cap (15k since C82)
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
        self.addCleanup(setattr, f_engine, "RULES_V2", f_engine.RULES_V2)
        f_engine.RULES_V2 = False                              # day 11 rules v2 retire the rebuild (RulesV2Tests); this covers the v1 code path
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


class WallRulesC76Tests(EngineCase):
    """C76 (Piero's RI Senate R wall): a per-wall cut (full fill leaves >= min_free), an edge floor (rests only while
    fair_side - price >= min_edge, re-placed when it returns), and a resting wall uses the race's cap room."""

    def rig(self, cash_above_floor=20_000):
        fresh_data(layers=dict(position=dict(state="full"), wall=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] <= 0.10
                  and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE)
        FAIR[ex] = 0.005                                  # fair NO 0.995
        self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = (0.13, 0.135, 0.13)
        loop(eng, fake)
        return fake, eng, ex

    def walls(self, eng, fake, ex):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "wall"]

    def test_min_free_cut_edge_floor_and_race_room(self):
        fake, eng, ex = self.rig(cash_above_floor=20_000)
        ctl = f_engine.EngineControls(eng)
        ctl._apply("wall", dict(ex=ex, side="no", price=0.85, qty=11_900, exit_px=0.14, ttl_h=72, hold_h=6, news_c=0.02, min_free=15_000, min_edge=0.14))   # a recycling wall (exit +1c): the day-10 30k rule does not apply
        loop(eng, fake); loop(eng, fake)
        w = self.walls(eng, fake, ex)
        self.assertEqual(len(w), 1, w)
        self.assertEqual(w[0]["qty"], int(5_000 / 0.85), "cut so a full fill leaves >= 15k free (free 20k)")
        room0 = eng.race_room(ex, "no", 0.995)
        self.assertLessEqual(room0, eng.race_room(ex, "yes", 0.995) + 1e-6)
        self.assertTrue(any("cut to" in e["text"] for e in eng.events))
        # the resting wall uses the race room
        used_before = eng.race_room(ex, "no", 0.995)
        for o in w:
            fake.open.pop(o["id"], None) if hasattr(fake, "open") else None
        FAIR[ex] = 0.03                                    # fair NO 0.97: edge 12c < 14c floor -> pulled
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.walls(eng, fake, ex), [], "edge under the floor: not resting")
        self.assertTrue(any("floor: pulled" in e["text"] for e in eng.events))
        FAIR[ex] = 0.005                                   # edge back to 14.5c: re-placed
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(len(self.walls(eng, fake, ex)), 1)
        self.assertTrue(any("again: re-placed" in e["text"] for e in eng.events))
        self.assertLess(eng.race_room(ex, "no", 0.995), eng.race_room(ex, "no", 0.995) + self.walls(eng, fake, ex)[0]["qty"] * 0.85 - 1, "a resting wall counts")


class WallC77Tests(WallRulesC76Tests):
    """C77 (Piero 20:10): a wall whose fills are held to settlement while free >= exit_min_free (the buy-back rests only
    below it, never under entry + 0.5c) and that is never re-rested after its first fill (once)."""

    def test_conditional_buyback_and_one_fill(self):
        fake, eng, ex = self.rig(cash_above_floor=40_000)
        ctl = f_engine.EngineControls(eng)
        ctl._apply("wall", dict(ex=ex, side="no", price=0.85, qty=4_000, exit_px=0.13, ttl_h=72, hold_h=72, news_c=0.02, min_free=15_000, min_edge=0.14, exit_min_free=15_000, once=True))
        loop(eng, fake); loop(eng, fake)
        w = self.walls(eng, fake, ex)
        self.assertEqual(len(w), 1)
        fake.fill(w[0]["id"], 1_000)                      # a partial fill: one fill only -> the rest is not re-rested
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.walls(eng, fake, ex), [], "once: not re-rested after a fill")
        self.assertTrue(any("one fill only" in e["text"] for e in eng.events))
        exits = lambda: [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "wallexit"]
        self.assertEqual(exits(), [], "free 40k >= 15k: held, no buy-back")
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 12_000)   # free 12k < 15k
        for _ in range(3):
            loop(eng, fake)
        x = exits()
        self.assertEqual(len(x), 1, "free under 15k: the buy-back rests")
        self.assertAlmostEqual(1 - x[0]["price"], 0.87, places=3, msg="sell the NO at 0.870 (YES bid 0.130)")
        self.assertTrue(any("buy-back at YES 0.130 rests" in e["text"] for e in eng.events))
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 20_000)
        for _ in range(3):
            loop(eng, fake)
        self.assertEqual(exits(), [], "free back above 15k: buy-back pulled, held again")

    def test_hold_wall_needs_30k_free_recycling_wall_does_not(self):
        fake, eng, ex = self.rig(cash_above_floor=20_000)
        ctl = f_engine.EngineControls(eng)
        with self.assertRaises(ValueError):               # day 10 §4.1: a settlement hold needs free >= 30k after it
            ctl._apply("wall", dict(ex=ex, side="no", price=0.85, qty=5_000, exit_px=0.05, ttl_h=72, hold_h=72, exit_min_free=15_000, once=True))
        ctl._apply("wall", dict(ex=ex, side="no", price=0.85, qty=5_000, exit_px=0.14, ttl_h=72, hold_h=6, once=True))   # recycling: exit NO 0.86 = +1c
        self.assertEqual(len(eng.walls()), 1)

    def test_rebuild_never_sells_wall_lots_and_the_cut_is_netted(self):
        fake, eng, ex = self.rig(cash_above_floor=30_000)
        ctl = f_engine.EngineControls(eng)
        ctl._apply("wall", dict(ex=ex, side="no", price=0.85, qty=10_000, exit_px=0.14, ttl_h=72, hold_h=6, news_c=0.02, min_free=15_000, min_edge=0.14, once=True))   # recycling walls (exit +1c)
        time.sleep(1.1)
        ctl._apply("wall", dict(ex=ex, side="no", price=0.85, qty=10_000, exit_px=0.14, ttl_h=72, hold_h=6, news_c=0.02, min_free=15_000, min_edge=0.14, once=True))
        loop(eng, fake); loop(eng, fake)
        w = sorted(self.walls(eng, fake, ex), key=lambda o: -o["qty"])
        self.assertEqual(len(w), 2, w)
        self.assertEqual(w[0]["qty"], 10_000, "the first wall keeps its size (free 30k - 15k = 15k > 8.5k)")
        self.assertEqual(w[1]["qty"], int((30_000 - 15_000 - 8_500) / 0.85), "the second is cut by what the first could still fill")
        # a filled wall lot is never sold by the rebuild (it has Piero's own exit)
        sub = f"wall:{ex}:no:{eng.walls()[0]['id']}"
        eng.ledger.subbooks.setdefault(ex, {})[sub] = -4_000.0
        eng.ledger.tranches[sub] = dict(ex=ex, side="no", kind="wall", state="open", ts=time.time(), t="x", avg=0.85)
        fake.pos[ex] = -4_000
        self.addCleanup(setattr, f_engine, "RELEASE_QUIET_HOURS", f_engine.RELEASE_QUIET_HOURS)
        f_engine.RELEASE_QUIET_HOURS = (0, 0)
        loop(eng, fake)
        eng.release_t = 0
        plan = eng.release_plan(a_config.CASH_RESERVE + 3_000, time.time(), eng.bucket_usage())
        self.assertNotIn(ex, plan, plan)


class RulesV2Tests(EngineCase):
    """Day 11 §1 (C81c) rules v2: no forced sales at any cash level (the 15k rebuild line is gone); while free cash >= BUFFER a
    catch lot >= 8c under fair is held off the ladder, under BUFFER the ladder offers such lots lowest-edge first; BUFFER = the
    catcher's p95 rolling-1-h entry cost over 72 h + 5k and the k table hangs off it; RI unfrozen for turnover (held capped at
    the deploy level, +10k in flight, levels >= 15c under fair); L3 / L4 levels <= 20% of the weighted budget."""

    def rig(self, cash_above_floor=60_000):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full"), release=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        favs = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.90
                and eng.meta[b.ex].get("race") not in a_config.RI_RACES]
        self.assertTrue(favs)
        return fake, eng, favs

    def tranche(self, eng, ex, sub, qty, avg, side="yes"):
        eng.ledger.subbooks.setdefault(ex, {})[sub] = float(qty if side == "yes" else -qty)
        eng.ledger.tranches[sub] = dict(ex=ex, side=side, kind="dump", state="open", ts=time.time() - 60, t="x", avg=avg)

    def test_migration_unfreezes_ri_and_logs_the_rules(self):
        fake, eng, favs = self.rig()
        self.assertTrue(a_config.RULES_V2)
        self.assertFalse(eng.controls.d.get("ri_freeze"))
        self.assertTrue(eng.controls.d["day11"]["rules_v2"])
        self.assertTrue(any("rules v2" in e["text"] for e in eng.events))
        self.assertTrue(any(e["text"].startswith("BUFFER ") for e in eng.events), "BUFFER logged with its derivation")

    def test_no_forced_sales_at_any_cash_level(self):
        fake, eng = Day4Tests.rig(self)
        self.addCleanup(setattr, f_engine, "RELEASE_QUIET_HOURS", f_engine.RELEASE_QUIET_HOURS)
        f_engine.RELEASE_QUIET_HOURS = (0, 0)
        ex = mid_market(eng)
        fake.pos[ex] = 2000
        fake.avg_cost[ex] = 0.30
        loop(eng, fake)
        bid = fake.px[ex][0]
        eng.ledger.subbooks[ex] = {f"dump:{ex}:yes:0": 2000.0}
        eng.ledger.tranches[f"dump:{ex}:yes:0"] = dict(ex=ex, side="yes", kind="dump", state="open", ts=time.time(), t="x", avg=round(bid - 0.01, 3))
        eng.ledger.fills.append(dict(key="r1", order="r1", ex=ex, side="yes", qty=2000.0, price=round(bid - 0.01, 3), action="buy", realized=0.0,
                                     t="2026-10-05 12:00:00", bot="b", layer="dump", before_engine=False, fair=None, edge=None, sig_edge=None, sig_mid=None))
        eng.ledger.orders["r1"] = dict(id="r1", bot="b", layer="dump", ex=ex, side="yes", action="buy", price=round(bid - 0.01, 3), qty=2000, filled=2000.0,
                                       cost=0.0, left=0, placed="x", ts=time.time() - 3600, closed=None, status="done", sub=f"dump:{ex}:yes:0", note="")
        eng.lots_cache = (None, {})
        for free in (3_000, 8_000, 12_000):
            eng.release_t = 0
            plan = eng.release_plan(a_config.CASH_RESERVE + free, time.time(), eng.bucket_usage())
            self.assertNotEqual(plan.get(ex, {}).get("why"), "rebuild", (free, plan))
        self.assertFalse(any("REBUILD sale" in e["text"] for e in eng.events), "nothing is ever sold to raise cash")

    def test_hold_when_idle_and_the_ladder_under_buffer(self):
        """C88 (Piero 01:55): a trading lot >= 3c under fair is held whatever the cash (no +0.5c exit); under 3c it exits at
        max(entry + 0.5c, the touch); a CORE lot is held even under 3c; the held time still does not count towards the 6-h ageing."""
        f_engine.TRADING_HOLD_C = 0.03
        fake, eng, favs = self.rig(cash_above_floor=60_000)
        ex = favs[0]
        fs = eng.fair[ex]
        a, b, c = f"dump:{ex}:yes:1", f"dump:{ex}:yes:2", f"dump:{ex}:yes:3"
        self.tranche(eng, ex, a, 2000, round(fs - 0.10, 3))       # 10c under fair: held
        self.tranche(eng, ex, b, 2000, round(fs - 0.02, 3))       # 2c under fair: exits
        self.tranche(eng, ex, c, 2000, round(fs - 0.015, 3))      # 1.5c under fair but CORE: held
        eng.core_set = {(ex, c)}
        now = time.time()
        eng.cash_hist = [a_config.CASH_RESERVE + 60_000]
        eng.track_dumps(now)
        view = {x["sub"]: x for x in eng.dump_view.get(ex, [])}
        self.assertNotIn(a, view, "a lot >= 3c under fair is held (no +0.5c exit)")
        self.assertIn(b, view, "a lot < 3c under fair exits")
        self.assertNotIn(c, view, "a core lot is held")
        ask_s = bots.side_book(eng.px[ex][0], eng.px[ex][1], "yes")[1]
        self.assertAlmostEqual(view[b]["rebound"], bots.tick_up(round(max(round(fs - 0.02, 3) + 0.005, ask_s), 6)), places=3, msg="at max(entry + 0.5c, the touch)")
        self.assertTrue(eng.ledger.tranches[a].get("held"))
        eng.cash_hist = [a_config.CASH_RESERVE + 1_000]            # cash short: the hold does not depend on cash (the rotation is the cash mechanism)
        eng.track_dumps(now + 600)
        view = {x["sub"] for x in eng.dump_view.get(ex, [])}
        self.assertNotIn(a, view)
        self.assertEqual(eng.lot_policy(ex, "yes", a, round(fs - 0.10, 3), fs), "hold")
        self.assertEqual(eng.lot_policy(ex, "yes", b, round(fs - 0.02, 3), fs), "exit")
        self.assertEqual(eng.lot_policy(ex, "yes", c, round(fs - 0.015, 3), fs), "core")
        eng.controls.d["core"] = dict(swap_out={f"{ex}|{a}": dict(t=now, why="test")})
        self.assertEqual(eng.lot_policy(ex, "yes", a, round(fs - 0.10, 3), fs), "exit", "a swapped-out core lot exits at the touch")

    def test_buffer_is_the_catchers_p95_hourly_cost_plus_5k_and_sets_the_k_table(self):
        fake, eng, favs = self.rig()
        now = time.time()
        eng.ledger.fills = [f for f in eng.ledger.fills if f["layer"] != "dump"]
        v0 = eng.buffer_calc(now, force=True)
        self.assertEqual(v0, a_config.BUFFER_MIN, "no catches in 72 h: the 10k floor")
        for i in range(72):                                       # 2,000 at cost every 5 min for 6 h: a 24k rolling hour
            t = now - 6 * 3600 + i * 300
            eng.ledger.fills.append(dict(key=f"bf{i}", order=f"bf{i}", ex=favs[0], side="yes", qty=2500.0, price=0.80, action="buy", realized=0.0,
                                         t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)), bot="b", layer="dump", before_engine=False, fair=0.9, edge=0.1, sig_edge=0, sig_mid=0.8))
        v = eng.buffer_calc(now, force=True)
        bf = eng.controls.d["buffer"]
        self.assertEqual(bf["fills"], 72)
        self.assertTrue(20_000 <= bf["p95"] <= 24_000, bf)
        self.assertEqual(v, min(a_config.BUFFER_MAX, bf["p95"] + a_config.BUFFER_ADD))
        self.assertEqual(eng.buffer(), v)
        self.assertTrue(any(e["text"].startswith("BUFFER ") and "p95" in e["text"] for e in eng.events))
        # recomputed once a day: a second call the same day keeps the value
        eng.ledger.fills = eng.ledger.fills[:-72]
        self.assertEqual(eng.buffer_calc(now), v)
        # the k table hangs off BUFFER
        eng.controls.d["buffer"] = dict(value=20_000, day=bf["day"])
        self.assertEqual(eng.k_max_for(26_000)[1], 3.0)
        self.assertEqual(eng.k_max_for(21_500)[1], 3.0)
        self.assertEqual(eng.k_max_for(15_000)[1], 2.0, "under BUFFER entries shrink")
        self.assertEqual(eng.k_max_for(5_000)[1], a_config.BUDGET_K_MIN)

    def test_l3_levels_take_at_most_20pct_of_the_budget(self):
        fake, eng, favs = self.rig()
        def row(i, lname, px, qty, w, l0=False):
            return (i, False, favs[0], dict(side="yes", px=px, qty=qty, sub=f"dump:{favs[0]}:yes:{lname}{i}", level=i, edge=0.05, arena=False, l0=l0,
                                            ttl=600, weight=w, lname=lname, px_rest=None))
        rows = [row(i, "L3", 0.80, 1000, a_config.ENTRY_WEIGHTS["L3"]) for i in range(10)] + [row(4, "L0", 0.85, 2000, 1.0, l0=True)]
        out, cut, spent = eng.allocate_levels(rows, 3_000, 15_000)
        deep = [w for _, _, _, w in out if w["lname"] == "L3"]
        self.assertEqual(len(deep), 5, "5 x 120 = 600 = 20% of 3,000")
        self.assertTrue(any(w["l0"] for _, _, _, w in out), "the L0 takes the rest")
        self.assertLessEqual(sum(w["px"] * w["qty"] * w["weight"] for w in deep), a_config.L3_BUDGET_SHARE * 3_000 + 1e-6)

    def test_ri_unfrozen_for_turnover_with_a_held_cap_and_the_15c_rule(self):
        fake, eng, favs = self.rig(cash_above_floor=60_000)
        ex = favs[0]
        eng.meta[ex]["race"] = a_config.RI_SENATE_RACE
        fs = eng.fair[ex]
        eng.ri_held_cap = None
        eng.ledger.subbooks.setdefault(ex, {})["inventory"] = 2_000.0
        eng.record_ri_cap()
        self.assertIsNotNone(eng.ri_held_cap)
        self.assertGreater(eng.ri_held_cap, 1_000)
        self.assertEqual(eng.controls.d["day11"]["ri_held_cap"], round(eng.ri_held_cap, 2))
        self.assertAlmostEqual(eng.race_room(ex, "yes", fs), a_config.RI_TURNOVER, delta=1.0, msg="at the cap: the 10k turnover room")
        eng.ledger.subbooks[ex]["inventory"] = 4_000.0
        self.assertLess(eng.race_room(ex, "yes", fs), a_config.RI_TURNOVER - 1_000, "above the cap: the room shrinks")
        self.assertTrue(eng.ri_edge_short(ex, fs - 0.10, fs))
        self.assertFalse(eng.ri_edge_short(ex, fs - 0.16, fs))
        # an RI lot above the cap is never held, whatever the cash
        sub = f"dump:{ex}:yes:1"
        t = dict(ex=ex, side="yes", kind="dump", state="open", ts=time.time(), t="x", avg=round(fs - 0.16, 3))
        eng.cash_hist = [a_config.CASH_RESERVE + 60_000]
        eng.controls.d["buffer"] = dict(value=20_000, day=time.strftime("%Y-%m-%d"))
        f_engine.TRADING_HOLD_C = 0.03                             # C88: the 3c hold on
        self.assertEqual(eng.hold_when_idle([(sub, t, ex, 1000, fs - 0.16, fs)], time.time()), set())
        eng.ledger.subbooks[ex]["inventory"] = 1_000.0           # back under the cap: holdable
        self.assertEqual(eng.hold_when_idle([(sub, t, ex, 1000, fs - 0.16, fs)], time.time()), {sub})
        f_engine.TRADING_HOLD_C = 99.0
        # the 15c rule on the levels
        for _ in range(3):
            seed_ref(eng, ex, mid=fs - 0.06)
            for e_ in favs[1:]:
                seed_ref(eng, e_, mid=eng.fair[e_] - 0.06)
            loop(eng, fake)
        mine = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dump"]
        self.assertEqual(mine, [], "no RI level < 15c under fair")
        for _ in range(3):
            seed_ref(eng, ex, mid=fs - 0.17)
            loop(eng, fake)
        mine = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dump"]
        self.assertTrue(mine, "levels >= 15c under fair are placed on RI again (unfrozen)")
        self.assertTrue(all(o["price"] <= fs - 0.15 + 1e-9 for o in mine))


class TMTests(EngineCase):
    """Day 11 §2 (C83): the touch maker (scalper v2) joins the SIG best bid on the side that is cheap vs fair (improves by a tick
    when the spread is >= 2 ticks), only at <= fair - 2c, never the rich side; keeps its queue place while at the touch or a
    tick behind and re-pegs only >= 2 ticks behind; pulls when the edge is gone; clip 500 in the pilot; 1.5k shares per
    market-side and 8k at cost; exits at fill + 0.5c (fill + 1c after 10 min), never below entry + 0.5c, held while cash is
    idle when >= 8c under fair; pulled on a 2c / 10 min Polymarket move; <= 8 writes/min; ranked by the 24-h value printed
    >= 2c off fair; its cap is reserved off the top of the entry budget; the day-5 scalper goes off with it."""

    def rig(self, cash_above_floor=60_000):
        fresh_data(layers=dict(position=dict(state="full"), tm=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.controls.d["tm_autofull"] = False                  # Piero 16:10 §3 promotes the TM at free >= BUFFER: keep the pilot sizes under test
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.50
        self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)        # SIG 0.46 / 0.47: the bid 4c under fair, 2 ticks wide -> improve to 0.465
        eng.off24[ex] = collections.deque([(time.time(), 1e9)])   # the busiest off-fair market: first in the ranking
        eng.tm_depth[ex] = (time.time() + 1e9, [(0.46, 50_000.0)], [(0.47, 50_000.0)])   # a deep touch (the 20% depth rule has its own check)
        loop(eng, fake)
        return fake, eng, ex

    def tm_orders(self, eng, fake, ex, layer="tm"):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer]

    def test_bids_the_cheap_side_at_the_touch_improving_wide_spreads(self):
        fake, eng, ex = self.rig()
        loop(eng, fake)
        q = self.tm_orders(eng, fake, ex)
        self.assertEqual(len(q), 1, q)
        self.assertEqual(q[0]["side"], "yes")
        self.assertAlmostEqual(q[0]["price"], 0.465, places=3, msg="improve by a tick: the spread is 2 ticks")
        self.assertEqual(q[0]["qty"], a_config.TM_CLIP_PILOT)
        self.assertEqual(eng.ledger.orders[q[0]["id"]].get("ttl"), a_config.TM_TTL_S)
        self.assertEqual(eng.ledger.orders[q[0]["id"]]["sub"], f"tm:{ex}:yes")
        self.assertEqual([o for o in q if o["side"] == "no"], [], "the NO side is rich vs fair (ask 0.47 < fair + 2c): never quoted")
        # a 1-tick book: join, never cross
        fake.px[ex] = eng.px[ex] = (0.46, 0.465, 0.4625)
        for o in q:
            fake.cancel(o["id"]) if hasattr(fake, "cancel") else None
        loop(eng, fake); loop(eng, fake)
        q = self.tm_orders(eng, fake, ex)
        self.assertTrue(q and all(abs(o["price"] - 0.46) < 1e-9 for o in q), q)
        self.assertIn("quoted", eng.tm_cov)

    def test_queue_priority_repeg_two_ticks_behind_and_pull_without_edge(self):
        fake, eng, ex = self.rig()
        loop(eng, fake)
        o0 = self.tm_orders(eng, fake, ex)[0]
        fake.px[ex] = eng.px[ex] = (0.47, 0.475, 0.4725)       # the touch moved 1 tick past us: keep the queue place
        loop(eng, fake); loop(eng, fake)
        q = self.tm_orders(eng, fake, ex)
        self.assertEqual([o["id"] for o in q], [o0["id"]], "one tick behind the touch: the order stays (queue priority)")
        fake.px[ex] = eng.px[ex] = (0.475, 0.48, 0.4775)       # 2 ticks behind: re-pegged to the new touch (join: 1 tick wide)
        loop(eng, fake); loop(eng, fake)
        q = self.tm_orders(eng, fake, ex)
        self.assertEqual(len(q), 1, q)
        self.assertNotEqual(q[0]["id"], o0["id"])
        self.assertAlmostEqual(q[0]["price"], 0.475, places=3)
        fake.px[ex] = eng.px[ex] = (0.485, 0.49, 0.4875)       # the bid is 1.5c under fair: no edge -> pulled
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.tm_orders(eng, fake, ex), [], "edge < 2c: pulled")

    def test_exit_ladder_rule0_round_trip_and_hold_when_idle(self):
        fake, eng, ex = self.rig()
        loop(eng, fake)
        o = self.tm_orders(eng, fake, ex)[0]
        fake.fill(o["id"], o["qty"])
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(eng.ledger.subbooks[ex].get(f"tm:{ex}:yes"), float(a_config.TM_CLIP_PILOT))
        t = eng.ledger.tranches[f"tm:{ex}:yes"]
        self.assertEqual((t["kind"], t["state"]), ("tm", "open"))
        xs = self.tm_orders(eng, fake, ex, "tmexit")
        self.assertEqual(len(xs), 1, xs)
        self.assertAlmostEqual(1 - xs[0]["price"], 0.47, places=3, msg="exit at the touch ask 0.47 (= fill + 1 tick here), turnover_plan §2.4")
        self.assertEqual(eng.controls.d["tm_stats"]["buys"], 1)
        self.assertTrue(any("TM bought" in e["text"] for e in eng.events))
        fake.px[ex] = eng.px[ex] = (0.47, 0.48, 0.475)          # the ask moves up: the exit joins the new touch ask
        loop(eng, fake); loop(eng, fake)
        xs = self.tm_orders(eng, fake, ex, "tmexit")
        self.assertEqual(len(xs), 1, xs)
        self.assertAlmostEqual(1 - xs[0]["price"], 0.48, places=3)
        fake.px[ex] = eng.px[ex] = (0.455, 0.46, 0.4575)        # the ask under entry + 0.5c: rule 0 holds the exit at 0.47
        loop(eng, fake); loop(eng, fake)
        xs = self.tm_orders(eng, fake, ex, "tmexit")
        self.assertAlmostEqual(1 - xs[0]["price"], 0.47, places=3)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)
        loop(eng, fake); loop(eng, fake)
        xs = self.tm_orders(eng, fake, ex, "tmexit")
        # rule 0: never below entry + 0.5c
        self.assertGreaterEqual(1 - xs[0]["price"], t["avg"] + a_config.RULE0_MARGIN - 1e-9)
        # C88: a TM lot >= 3c under fair is held (no +0.5c exit), whatever the cash; under 3c it is offered again
        f_engine.TRADING_HOLD_C = 0.03
        FAIR[ex] = 0.60
        eng.controls.d["buffer"] = dict(value=20_000, day=time.strftime("%Y-%m-%d"))
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.tm_orders(eng, fake, ex, "tmexit"), [], "13.5c under fair: held")
        self.assertTrue(t.get("held"))
        eng.cash_hist = [a_config.CASH_RESERVE + 10_000]
        eng.track_tm(time.time())
        self.assertTrue(t.get("held"), "cash does not change the hold (the rotation is the cash mechanism)")
        FAIR[ex] = 0.49                                        # 2.5c under fair: under the 3c hold -> offered again
        loop(eng, fake); loop(eng, fake)
        self.assertFalse(t.get("held"))
        self.assertTrue(eng.tm_view.get(ex))
        FAIR[ex] = 0.50
        f_engine.TRADING_HOLD_C = 99.0
        loop(eng, fake); loop(eng, fake)
        # the round trip
        xs = self.tm_orders(eng, fake, ex, "tmexit")
        self.assertTrue(xs)
        fake.fill(xs[0]["id"], xs[0]["qty"])
        loop(eng, fake); loop(eng, fake)
        self.assertLess(abs(eng.ledger.subbooks[ex].get(f"tm:{ex}:yes", 0)), 1)
        self.assertEqual(eng.ledger.tranches[f"tm:{ex}:yes"]["state"], "closed")
        self.assertEqual(eng.controls.d["tm_stats"]["round_trips"], 1)
        self.assertTrue(any("round trip done" in e["text"] and "TM" in e["text"] for e in eng.events))

    def test_caps_news_guard_write_budget_and_reserve(self):
        fake, eng, ex = self.rig()
        loop(eng, fake)
        self.assertAlmostEqual(eng.tm_reserve(), min(eng.tm_cap(time.time()) - eng.tm_cost(), eng.free_cash() - a_config.TM_PAUSE_FREE), delta=1.0,
                               msg="the cap (base + free above BUFFER) is reserved off the top, never the catcher's last 10k (Piero 21:37)")
        self.assertGreaterEqual(eng.tm_cap(time.time()), a_config.TM_CAP_COST_PILOT)
        self.assertGreater(a_config.ENTRY_WEIGHTS["tm"], 0.99)
        # per market-side inventory cap: only the exit quotes
        fake.pos[ex] = a_config.TM_INV_MARKET_PILOT; fake.avg_cost[ex] = 0.46   # SIG holds them (else the audit nets the phantom sub)
        eng.ledger.subbooks.setdefault(ex, {})[f"tm:{ex}:yes"] = float(a_config.TM_INV_MARKET_PILOT)
        eng.ledger.tranches[f"tm:{ex}:yes"] = dict(ex=ex, side="yes", kind="tm", state="open", ts=time.time(), t="x", avg=0.46)
        for o in self.tm_orders(eng, fake, ex):
            fake.cancel(o["id"]) if hasattr(fake, "cancel") else None
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.tm_orders(eng, fake, ex), [], "at the 1.5k cap: no entry bid")
        self.assertTrue(self.tm_orders(eng, fake, ex, "tmexit"))
        eng.ledger.subbooks.get(ex, {}).pop(f"tm:{ex}:yes", None); eng.ledger.tranches.pop(f"tm:{ex}:yes", None)
        fake.pos[ex] = 0
        # the cost cap: 8k at cost held elsewhere -> no new entry
        other = next(b.ex for b in eng.bots if b.kind == "market" and b.ex != ex and b.ex not in POS)
        fake.pos[other] = 10_000; fake.avg_cost[other] = 0.85
        eng.ledger.subbooks.setdefault(other, {})[f"tm:{other}:yes"] = 10_000.0
        eng.ledger.tranches[f"tm:{other}:yes"] = dict(ex=other, side="yes", kind="tm", state="open", ts=time.time(), t="x", avg=0.85)
        cash_was = eng.cash_hist
        eng.cash_hist = [a_config.CASH_RESERVE + 5_000]       # under BUFFER: the cap is the 8k base (Piero 22:15 item 2)
        self.assertEqual(eng.tm_reserve(), 0.0)
        self.assertNotIn(ex, eng.tm_plan(time.time(), eng.cash_hist[-1], eng.bucket_usage()))
        eng.cash_hist = cash_was
        eng.ledger.subbooks.get(other, {}).pop(f"tm:{other}:yes", None); eng.ledger.tranches.pop(f"tm:{other}:yes", None)
        fake.pos[other] = 0
        # the touch depth rule: a 1,000-share touch -> at most 200 shares
        eng.tm_depth[ex] = (time.time() + 1e9, [(0.46, 1_000.0)], [(0.47, 1_000.0)])
        plan = eng.tm_plan(time.time(), eng.cash_hist[-1], eng.bucket_usage())
        new = [w for w in plan.get(ex, []) if w["qty"] != a_config.TM_CLIP_PILOT]
        self.assertTrue(all(w["qty"] <= int(a_config.TM_DEPTH_SHARE * 1_000) for w in plan.get(ex, [])) and plan.get(ex), plan.get(ex))
        eng.tm_depth[ex] = (time.time() + 1e9, [(0.46, 50_000.0)], [(0.47, 50_000.0)])
        # a 2c Polymarket move in 10 min pulls the market's TM orders for 5 min
        loop(eng, fake); loop(eng, fake)
        self.assertTrue(self.tm_orders(eng, fake, ex))
        eng.fair_hist[ex] = [(time.time() - 300, 0.50)]
        FAIR[ex] = 0.53
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.tm_orders(eng, fake, ex), [], "pulled on the move")
        self.assertGreater(eng.tm_block.get(ex, 0), time.time())
        self.assertTrue(any("TM " in e["text"] and "Polymarket moved" in e["text"] for e in eng.events))
        # the write budget: 8 writes in the last minute -> no new placement this loop
        FAIR[ex] = 0.50
        eng.tm_block.pop(ex, None); eng.fair_hist[ex] = []
        now = time.time()
        for i in range(a_config.TM_WRITES_PER_MIN):
            eng.ledger.orders[f"w{i}"] = dict(id=f"w{i}", bot="b", layer="tm", ex=other, side="yes", action="buy", price=0.3, qty=100, filled=0, cost=0.0, left=100,
                                                placed=stamp_(), ts=now - 30 + i, closed=None, status="open", sub=f"tm:{other}:yes", note="")
        self.assertGreaterEqual(eng.tm_writes_last_min(now), a_config.TM_WRITES_PER_MIN)   # (the rig's own placements count too)
        self.assertNotIn(ex, eng.tm_plan(now, eng.cash_hist[-1], eng.bucket_usage()), "no write budget: the new side waits")
        self.assertLessEqual(eng.tm_cov["writes_left"], 0)

    def test_ranking_by_off_fair_value_and_the_pilots_top_n(self):
        fake, eng, ex = self.rig()
        self.addCleanup(setattr, f_engine, "TM_SIDES_PILOT", f_engine.TM_SIDES_PILOT)
        f_engine.TM_SIDES_PILOT = 1
        cands = eng.tm_candidates()
        self.assertTrue(cands)
        self.assertEqual([r[0] for r in cands], sorted([r[0] for r in cands], reverse=True), "ranked by edge / price (Piero 22:15)")
        top_ex = cands[0][1]
        resting = {o["ex"] for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "tm"}
        plan = eng.tm_plan(time.time(), eng.cash_hist[-1], eng.bucket_usage())
        self.assertTrue(set(plan) <= {top_ex} | resting, "the pilot takes the top N market-sides (N = 1 here); sides already resting are kept while they rest")
        self.assertEqual(eng.tm_cov["top"], 1)

    def test_e_min_by_cash_and_ranking_by_edge_over_price(self):
        """Piero 22:15 Oct 7 (C82 item 1): e_min 2c while free >= BUFFER, 4c below; TM entries ranked by edge / price."""
        fake, eng, ex = self.rig()
        eng.controls.d["buffer"] = dict(value=20_000, day=time.strftime("%Y-%m-%d"))
        eng.cash_hist = [a_config.CASH_RESERVE + 60_000]
        self.assertEqual(eng.tm_e_min(), a_config.TM_E_MIN)
        cands = eng.tm_candidates()
        self.assertEqual([r[0] for r in cands], sorted([r[0] for r in cands], reverse=True))
        self.assertAlmostEqual(cands[0][0], (cands[0][3] - cands[0][4]) / cands[0][4], places=6, msg="score = edge / price")
        eng.cash_hist = [a_config.CASH_RESERVE + 10_000]       # under BUFFER: 4c
        self.assertEqual(eng.tm_e_min(), a_config.TM_E_MIN, "C87: every side >= 2c whatever the cash (entries stop under 10k free instead)")
        fake.px[ex] = eng.px[ex] = (0.47, 0.475, 0.4725)       # 3c under fair: since C87 enough at any cash (entries stop under 10k free instead)
        self.assertIn((ex, "yes"), {(r[1], r[2]) for r in eng.tm_candidates()})
        eng.cash_hist = [a_config.CASH_RESERVE + 60_000]
        self.assertIn((ex, "yes"), {(r[1], r[2]) for r in eng.tm_candidates()})

    def test_cap_grows_with_free_cash_above_buffer_and_logs_on_change(self):
        """Piero 22:15 Oct 7 (C82 item 2): TM cap = base + max(0, free - BUFFER), recomputed each loop, logged on change."""
        fake, eng, ex = self.rig()
        eng.controls.d["buffer"] = dict(value=20_000, day=time.strftime("%Y-%m-%d"))
        eng.cash_hist = [a_config.CASH_RESERVE + 10_000]
        self.assertEqual(eng.tm_cap(time.time()), a_config.TM_CAP_COST_PILOT, "under BUFFER: the base cap")
        eng.cash_hist = [a_config.CASH_RESERVE + 32_000]
        n0 = sum(1 for e in eng.events if e["text"].startswith("TM cap ->"))
        self.assertEqual(eng.tm_cap(time.time()), a_config.TM_CAP_COST_PILOT + 12_000)
        self.assertEqual(sum(1 for e in eng.events if e["text"].startswith("TM cap ->")), n0 + 1, "logged on change")
        self.assertEqual(eng.tm_cap(time.time()), a_config.TM_CAP_COST_PILOT + 12_000)
        self.assertEqual(sum(1 for e in eng.events if e["text"].startswith("TM cap ->")), n0 + 1, "no change, no line")
        self.assertAlmostEqual(eng.tm_reserve(), a_config.TM_CAP_COST_PILOT + 12_000 - eng.tm_cost(), delta=1.0)

    def test_per_market_round_trip_share_sets_clip_and_cap(self):
        """Piero 22:15 Oct 7 (C82 item 3): exits >= 50% of buys over 24 h -> clip 1k and cap 3k; < 20% -> cap 1k."""
        fake, eng, ex = self.rig()
        now = time.time()
        def fill(e_, layer, qty, t):
            eng.ledger.fills.append(dict(key=f"{e_}{layer}{t}", order="x", ex=e_, side="yes", qty=float(qty), price=0.46, action="buy", realized=0.0,
                                         t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)), bot="b", layer=layer, before_engine=False, fair=0.5, edge=0.04, sig_edge=0, sig_mid=0.46))
        a, b, c = "a1", "b1", "c1"
        fill(a, "tm", 1000, now - 7200); fill(a, "tmexit", 600, now - 3600)      # 60% -> clip 1k, cap 3k
        fill(b, "tm", 1000, now - 7200); fill(b, "tmexit", 100, now - 3600)      # 10% -> cap 1k
        fill(c, "tm", 1000, now - 7200); fill(c, "tmexit", 300, now - 3600)      # 30% -> defaults
        fill("d1", "tm", 1000, now - 600)                                        # bought 10 min ago: grace, no profile
        prof = eng.tm_market_profile(now)
        self.assertEqual(prof[a][:2], (a_config.TM_CLIP_HI, a_config.TM_INV_HI))
        self.assertEqual(prof[b][:2], (a_config.TM_CLIP_PILOT, a_config.TM_INV_LO))
        self.assertEqual(prof[c][:2], (a_config.TM_CLIP_PILOT, a_config.TM_INV_MARKET_PILOT))
        self.assertNotIn("d1", prof)
        # in the plan: a 60% market bids the 1k clip
        fill(ex, "tm", 1000, now - 7200); fill(ex, "tmexit", 600, now - 3600)
        for o in self.tm_orders(eng, fake, ex):
            fake.cancel(o["id"]) if hasattr(fake, "cancel") else None
        loop(eng, fake); loop(eng, fake)
        q = self.tm_orders(eng, fake, ex)
        self.assertTrue(q and q[0]["qty"] == a_config.TM_CLIP_HI, q)

    def test_migration_turns_the_pilot_on_and_the_scalper_off(self):
        fresh_data(layers=dict(position=dict(state="full"), scalp=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        _os.environ["SIG_TM_PILOT"] = "1"
        self.addCleanup(_os.environ.__setitem__, "SIG_TM_PILOT", "0")
        eng = engine(fake)
        self.assertEqual(eng.controls.state("tm"), "pilot")
        self.assertEqual(eng.controls.state("scalp"), "off")
        self.assertTrue(eng.controls.d["day11"]["tm_pilot"])
        self.assertTrue(any("touch maker pilot ON" in e["text"] for e in eng.events))
        self.assertTrue(eng.tm_pilot())
        self.assertEqual(eng.tm_sizes()["clip"], a_config.TM_CLIP_PILOT)


class SMTests(EngineCase):
    """turnover_plan §1 (C84): the spread maker joins the best bid and the best ask on the bounce universe, keeps each order while it
    is at the touch or a tick behind, re-pegs >= 2 ticks away, refills a filled side, skews once long (bid a tick lower, the ask
    an exit under rule 0), holds the rich side at one clip, pulls on PM moves and sweeps, respects +/- 1k, 10k and 10 writes/min;
    the universe is the bounce ranking from our own tape."""

    def rig(self, fair=0.50, book=(0.48, 0.49, 0.485)):
        fresh_data(layers=dict(position=dict(state="full"), sm=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 60_000)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = fair
        self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = book
        eng.controls.d["sm_markets"] = [ex]
        eng.sm_depth[ex] = (time.time() + 1e9, [(book[0], 50_000.0)], [(book[1], 50_000.0)])
        loop(eng, fake); loop(eng, fake)
        return fake, eng, ex

    def quotes(self, eng, fake, ex, layer=None):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] in ((layer,) if layer else ("sm", "smexit"))]

    def test_two_sided_at_the_touch_queue_kept_and_repegged(self):
        fake, eng, ex = self.rig()
        q = self.quotes(eng, fake, ex, "sm")
        self.assertEqual(len(q), 2, q)
        bid = next(o for o in q if o["side"] == "yes"); ask = next(o for o in q if o["side"] == "no")
        self.assertAlmostEqual(bid["price"], 0.48, places=3); self.assertAlmostEqual(1 - ask["price"], 0.49, places=3)
        self.assertEqual((bid["qty"], ask["qty"]), (a_config.SM_CLIP_PILOT, a_config.SM_CLIP_PILOT))
        self.assertEqual(eng.ledger.orders[bid["id"]].get("ttl"), a_config.SM_TTL_S)
        self.assertLess(bid["price"], 1 - ask["price"], "never crossing")
        fake.px[ex] = eng.px[ex] = (0.485, 0.49, 0.4875)      # the bid moved up one tick (our ask is the touch): both orders keep their place
        loop(eng, fake); loop(eng, fake)
        self.assertEqual({o["id"] for o in self.quotes(eng, fake, ex, "sm")}, {bid["id"], ask["id"]}, "one tick behind the touch: kept (queue)")
        fake.px[ex] = eng.px[ex] = (0.49, 0.50, 0.495)        # two ticks: re-pegged
        loop(eng, fake); loop(eng, fake)
        q = self.quotes(eng, fake, ex, "sm")
        self.assertEqual(len(q), 2, q)
        self.assertFalse({o["id"] for o in q} & {bid["id"], ask["id"]})
        self.assertAlmostEqual(next(o for o in q if o["side"] == "yes")["price"], 0.49, places=3)
        self.assertAlmostEqual(1 - next(o for o in q if o["side"] == "no")["price"], 0.50, places=3)
        self.assertIn("quoted", eng.sm_cov)

    def test_fill_skews_the_bid_exits_under_rule0_and_counts_the_round_trip(self):
        fake, eng, ex = self.rig()
        bid = next(o for o in self.quotes(eng, fake, ex, "sm") if o["side"] == "yes")
        fake.fill(bid["id"], bid["qty"])
        loop(eng, fake); loop(eng, fake); loop(eng, fake)
        self.assertEqual(eng.sm_inventory(ex), 500.0)
        self.assertEqual(eng.controls.d["sm_stats"]["entries"], 1)
        xs = self.quotes(eng, fake, ex, "smexit")
        self.assertEqual(len(xs), 1, xs)
        self.assertAlmostEqual(1 - xs[0]["price"], 0.49, places=3, msg="the ask stays at the touch (>= entry 0.48 + 0.5c)")
        self.assertEqual(xs[0]["qty"], 500)
        nb = [o for o in self.quotes(eng, fake, ex, "sm") if o["side"] == "yes"]
        self.assertEqual(len(nb), 1, nb)
        self.assertAlmostEqual(nb[0]["price"], 0.475, places=3, msg="once long the bid steps a tick lower")
        # rule 0: the touch ask under entry + 0.5c -> the exit rests at entry + 0.5c
        fake.px[ex] = eng.px[ex] = (0.47, 0.475, 0.4725)
        loop(eng, fake); loop(eng, fake)
        xs = self.quotes(eng, fake, ex, "smexit")
        self.assertTrue(xs and all(1 - o["price"] >= 0.485 - 1e-9 for o in xs), xs)
        fake.px[ex] = eng.px[ex] = (0.48, 0.49, 0.485)
        loop(eng, fake); loop(eng, fake)
        xs = self.quotes(eng, fake, ex, "smexit")
        fake.fill(xs[0]["id"], xs[0]["qty"])                   # the round trip
        loop(eng, fake); loop(eng, fake)
        self.assertLess(abs(eng.sm_inventory(ex)), 1)
        st = eng.controls.d["sm_stats"]
        self.assertEqual((st["round_trips"], st["filled_sh"], st["rt_sh"]), (1, 500.0, 500.0))
        g = eng.sm_gate(time.time())
        self.assertIn("round-trip share 100%", g["why"])
        # the rich side holds at most one clip: long 500 with YES rich vs fair -> no bid
        FAIR[ex] = 0.47
        eng.ledger.subbooks.setdefault(ex, {})[f"sm:{ex}"] = 500.0; fake.pos[ex] = 500; fake.avg_cost[ex] = 0.48
        eng.ledger.tranches[f"sm:{ex}"] = dict(ex=ex, side="yes", kind="sm", state="open", ts=time.time(), t="x", avg=0.48)
        loop(eng, fake); loop(eng, fake)
        self.assertEqual([o for o in self.quotes(eng, fake, ex, "sm") if o["side"] == "yes"], [], "rich side at one clip: no more bids")
        self.assertTrue(self.quotes(eng, fake, ex, "smexit"))

    def test_guards_caps_and_write_budget(self):
        fake, eng, ex = self.rig()
        self.assertEqual(len(self.quotes(eng, fake, ex)), 2)
        eng.fair_hist[ex] = [(time.time() - 300, 0.50)]
        FAIR[ex] = 0.53                                        # a 3c Polymarket move: pulled 5 min
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.quotes(eng, fake, ex), [], "pulled on the move")
        self.assertGreater(eng.sm_block.get(ex, 0), time.time())
        FAIR[ex] = 0.50; eng.sm_block.pop(ex, None); eng.fair_hist[ex] = []
        fake.px[ex] = eng.px[ex] = (0.48, 0.49, 0.55)          # a print 6.5c off the touch: a sweep -> pulled 10 min
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.quotes(eng, fake, ex), [])
        self.assertTrue(any("a sweep" in e["text"] for e in eng.events))
        eng.sm_block.pop(ex, None)
        fake.px[ex] = eng.px[ex] = (0.48, 0.49, 0.485)
        # inventory cap: +1k held -> no bid, the exit still quotes
        eng.ledger.subbooks.setdefault(ex, {})[f"sm:{ex}"] = float(a_config.SM_CAP_SH_PILOT); fake.pos[ex] = a_config.SM_CAP_SH_PILOT; fake.avg_cost[ex] = 0.48
        eng.ledger.tranches[f"sm:{ex}"] = dict(ex=ex, side="yes", kind="sm", state="open", ts=time.time(), t="x", avg=0.48)
        loop(eng, fake); loop(eng, fake)
        self.assertEqual([o for o in self.quotes(eng, fake, ex, "sm") if o["side"] == "yes"], [])
        self.assertTrue(self.quotes(eng, fake, ex, "smexit"))
        eng.ledger.subbooks.get(ex, {}).pop(f"sm:{ex}", None); eng.ledger.tranches.pop(f"sm:{ex}", None); fake.pos[ex] = 0
        # the reserve off the top and the write budget
        self.assertAlmostEqual(eng.sm_reserve(), a_config.SM_CAP_COST_PILOT - eng.sm_cost(), delta=1.0)
        now = time.time()
        for i in range(a_config.SM_WRITES_PER_MIN):
            eng.ledger.orders[f"sw{i}"] = dict(id=f"sw{i}", bot="b", layer="sm", ex="x", side="yes", action="buy", price=0.3, qty=100, filled=0, cost=0.0, left=100,
                                                 placed=stamp_(), ts=now - 30 + i, closed=None, status="open", sub="sm:x", note="")
        for o in self.quotes(eng, fake, ex):
            fake.cancel(o["id"]) if hasattr(fake, "cancel") else None
        loop(eng, fake)
        self.assertNotIn(ex, eng.sm_plan(now, eng.cash_hist[-1]), "no write budget: no new quotes")

    def test_touch_maker_steps_aside_in_sm_markets(self):
        """ideas_backlog #4 (C84): no two of our quotes on the same side and level — the TM never quotes an SM market."""
        fresh_data(layers=dict(position=dict(state="full"), sm=dict(state="pilot"), tm=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 60_000)
        eng = engine(fake); eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.50; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)            # 4c under fair: a TM candidate
        eng.off24[ex] = collections.deque([(time.time(), 1e9)])
        eng.controls.d["sm_universe"] = dict(day=time.strftime("%Y-%m-%d"), markets=[], stats={})   # no SM market yet (the rig's tape may hold another test's file)
        loop(eng, fake)                                            # the fair and the book reach the engine
        self.assertIn((ex, "yes"), {(r[1], r[2]) for r in eng.tm_candidates()})
        eng.controls.d["sm_markets"] = [ex]
        self.assertNotIn((ex, "yes"), {(r[1], r[2]) for r in eng.tm_candidates()}, "an SM market: the TM steps aside")
        eng.controls.d["layers"]["sm"]["state"] = "off"
        self.assertIn((ex, "yes"), {(r[1], r[2]) for r in eng.tm_candidates()}, "SM off: the TM may quote it again")

    def test_universe_is_the_bounce_ranking_from_the_tape(self):
        fake, eng, ex = self.rig()
        import os as os_
        mk = [b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.2 <= eng.fair[b.ex] <= 0.8]
        a, b = mk[0], mk[1]
        ri = next((e for e, m in eng.meta.items() if m.get("race") in a_config.RI_RACES), None)
        path = os_.path.join(f_engine.DATA, "intel", "trades", time.strftime("%Y-%m-%d") + ".jsonl")
        os_.makedirs(os_.path.dirname(path), exist_ok=True)
        now = time.time()
        with open(path, "w") as fh:
            for i in range(60):                                # a: 30 bounces of 1k shares a leg; b: 5; ri: 100 (excluded)
                for e_, n in ((a, 30), (b, 5), (ri, 100)):
                    if e_ and i < 2 * n:
                        fh.write(json.dumps(dict(ex=e_, ts=now - 3600 + i * 10, p=0.50 if i % 2 == 0 else 0.505, q=1000.0, fair=0.5)) + "\n")
        u = eng.sm_universe_calc(now)
        self.assertEqual(u["markets"][:2], [a, b], u)
        self.assertNotIn(ri, u["markets"])
        self.assertGreaterEqual(u["stats"][a]["bounces"], 25)
        eng.controls.d.pop("sm_markets", None); eng.controls.d.pop("sm_universe", None)
        self.assertEqual(eng.sm_universe(now)[:2], [a, b])
        self.assertTrue(any("SM universe refreshed" in e["text"] for e in eng.events))


class ReadBudgetTests(EngineCase):
    """ideas_backlog (Oct 8): fills are read on a realtime fill / order notification or every 60 s, positions after fills or order
    reads or every 120 s (prices every loop); optional reads run up to READ_BUDGET - SPARE_KEEP; reads are counted by purpose;
    the book sampler covers the SM universe + RI Senate R every ~60 s and feeds the depth caches; the order-lifecycle log."""

    def rig(self):
        fresh_data(layers=dict(position=dict(state="full"), sm=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 60_000)
        eng = engine(fake); eng.ex.capacity = 6000.0
        loop(eng, fake)
        return fake, eng

    def test_fills_and_positions_are_polled_on_events_or_fallback(self):
        fake, eng = self.rig()
        for k, v in (("FILLS_READ_S", a_config.FILLS_READ_S), ("POSITIONS_READ_S", a_config.POSITIONS_READ_S), ("ORDERS_READ_S", a_config.ORDERS_READ_S), ("PNL_READ_S", a_config.PNL_READ_S)):
            self.addCleanup(setattr, f_engine, k, getattr(f_engine, k)); setattr(f_engine, k, v)
        eng.open_wsig = (eng.ex.sent_orders, len(eng.ex.gone), len(eng.ex.cancelling)); eng.open_read_t = time.time()
        n_f, n_p = [0], [0]
        real_f, real_p = fake.fills, fake.positions_raw
        fake.fills = lambda *a, **k: (n_f.__setitem__(0, n_f[0] + 1), real_f(*a, **k))[1]
        fake.positions_raw = lambda *a, **k: (n_p.__setitem__(0, n_p[0] + 1), real_p(*a, **k))[1]
        loop(eng, fake)                                        # the first loop after the switch: reads (fallback timer starts)
        f0, p0 = n_f[0], n_p[0]
        eng.last_fill_exs = set()
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(n_f[0], f0, "no notification, under 60 s: fills not polled")
        self.assertEqual(n_p[0], p0, "positions not polled either (no fills, no order read)")
        eng.rt_event("fills", set(), time.time())              # a fill notification wakes the fills read, then positions
        loop(eng, fake)
        self.assertEqual((n_f[0], n_p[0]), (f0 + 1, p0 + 1))
        eng.fills_read_t -= a_config.FILLS_READ_S + 1          # the 60-s fallback
        loop(eng, fake)
        self.assertEqual(n_f[0], f0 + 2)
        self.assertEqual(a_config.READ_BUDGET, 95)

    def test_reads_by_purpose_and_the_hourly_line(self):
        fake, eng = self.rig()
        import importlib.util as ilu
        spec = ilu.spec_from_file_location("b_sig_real", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "b_sig.py")); b_sig = ilu.module_from_spec(spec); spec.loader.exec_module(b_sig)
        b_sig._calls.append((time.time(), "read", 0.1, 200, "prices")); b_sig._calls.append((time.time(), "read", 0.1, 200, "book"))
        rb = b_sig.reads_by_purpose(60)
        self.assertGreaterEqual(rb.get("prices", 0), 1); self.assertGreaterEqual(rb.get("book", 0), 1)
        self.assertEqual(b_sig.spare_reads.__defaults__, (None,), "optional reads run up to READ_BUDGET - SPARE_KEEP")
        self.assertEqual(b_sig._purpose("/tournaments/x/portfolio/fills"), "fills"); self.assertEqual(b_sig._purpose("/exchanges/12/orderbook"), "book")
        eng.reads_report_t = time.time() - 3601
        loop(eng, fake)
        self.assertTrue(any(e["text"].startswith("READS last 10 min") for e in eng.events))

    def test_book_sampler_writes_samples_and_feeds_the_depth_caches(self):
        fake, eng = self.rig()
        import os as os_
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        eng.controls.d["sm_markets"] = [ex]
        eng.sample_t = {}; eng.boot_t = 0.0                    # past the 2-min start-up hold on optional reads
        n = eng.book_sampler(time.time())
        self.assertGreaterEqual(n, 1)
        self.assertIn(ex, eng.sm_depth); self.assertIn(ex, eng.tm_depth)
        path = os_.path.join(f_engine.DATA, "intel", "books", time.strftime("%Y-%m-%d") + ".jsonl")
        rows = [json.loads(l) for l in open(path)]
        self.assertTrue(any(r["ex"] == ex and r["bids"] and r["asks"] for r in rows), rows[-1])
        self.assertEqual(eng.book_sampler(time.time()), 0, "sampled within the last 60 s: nothing due")

    def test_order_lifecycle_log(self):
        fake, eng = self.rig()
        import os as os_
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.50; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.48, 0.49, 0.485)
        eng.controls.d["sm_markets"] = [ex]
        eng.sm_depth[ex] = eng.tm_depth[ex] = (time.time(), [(0.48, 1_234.0)], [(0.49, 2_000.0)])
        loop(eng, fake); loop(eng, fake)
        q = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "sm"]
        self.assertTrue(q)
        bid = next(o for o in q if o["side"] == "yes")
        r = eng.lifecycle.get(bid["id"])
        self.assertIsNotNone(r, "an SM order is tracked from placement")
        self.assertEqual(r["queue_ahead"], 1_000.0, "shares at our level from the latest book sample (the sampler read the fake's 1,000-share touch)")
        self.assertIsNotNone(r["mid0"])
        fake.fill(bid["id"], bid["qty"])
        r["t_place"] -= 1801                                   # all three marks are due
        loop(eng, fake); loop(eng, fake)
        errs = [e["text"] for e in eng.events if e["text"].startswith("lifecycle")]
        self.assertEqual(errs, [], errs)
        path = os_.path.join(f_engine.DATA, "intel", "orders", time.strftime("%Y-%m-%d", time.localtime(r["t_place"])) + ".jsonl")
        rows = [json.loads(l) for l in open(path)]
        row = next(x for x in rows if x["id"] == bid["id"])
        self.assertEqual(set(row["marks"]), {"60", "300", "1800"})
        self.assertIn(row["outcome"], ("done", "filled", "closed", "cancelled", "expired"))
        self.assertNotIn(bid["id"], eng.lifecycle)


class ReservoirTests(EngineCase):
    """Piero 10:10 / 16:10 Oct 8 (C84g): reservoirs — a core held to settlement is never offered; every offerable share rests at its
    sub's price in clips <= 5k (rule 0 per lot); rebuys at three levels for 40/30/30 of what was sold, never above the last sell - 3c,
    sized to free cash - the 2k guard; rebuild triggers stop the selling and rebuy to the full size; several markets at once."""

    def rig(self, cash_above_floor=30_000):
        fresh_data(layers=dict(position=dict(state="full"), swing=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake); eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] <= 0.10
                  and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE)
        FAIR[ex] = 0.0065; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = (0.16, 0.165, 0.1625)                # NO 0.835 / 0.84: the asks rest above the touch, the rebuys below it
        subs = [f"swing:{ex}:no", "inventory", f"wall:{ex}:no:wA", f"wall:{ex}:no:wB"]
        eng.ledger.subbooks[ex] = {"inventory": -20_144.0, subs[2]: -8_900.0, subs[3]: -3_100.0, subs[0]: -14_787.0}
        for s_, avg in ((subs[0], 0.834), (subs[2], 0.85), (subs[3], 0.85)):
            eng.ledger.tranches[s_] = dict(ex=ex, side="no", kind="wall" if "wall" in s_ else "swing", state="open", ts=time.time(), t="x", avg=avg)
        fake.pos[ex] = -46_931
        eng.meta[ex]["race"] = a_config.RI_SENATE_RACE; eng.ri_held_cap = 43_539.0
        r = json.loads(json.dumps(a_config.RESERVOIRS["1077"]))
        r.update(ex=ex, offers=[[subs[0], 0.850], ["inventory", 0.850], [subs[2], 0.855], [subs[3], 0.855]], max_out=None, rest_qty=None, join_touch=False, hold_subs=[], core_qty=10_000, sell_caps={}, avg_floor=False, no_rebuild=False, catcher_cycle_c=None,
                 rebuy=[[0.820, 0.40], [0.815, 0.30], [0.810, 0.30]], rebuy_under_sell=0.03, full_qty=46_949, rebuild_px=0.85,
                 mode="trade", fair0=0.0065, since=stamp_(),
                 stats=dict(sold_sh=0.0, bought_sh=0.0, realized=0.0, cycles=0, sold_val=0.0, bought_val=0.0, legs=[]))
        eng.controls.d["reservoirs"] = {ex: r}
        eng.controls.d["swing"] = dict(a_config.SWING, ex=ex, wall_subs=subs[2:], hold_qty=0.0, stats=dict(sold=0.0, bought=0.0, realized=0.0, cycles=0, legs=[]))
        eng.res_p10, eng.res_p10_t = {ex: 0.80}, {ex: time.time() + 1e9}   # the tape's p10 pinned under the trigger
        self.addCleanup(setattr, eng, "rule0_allowed", eng.rule0_allowed)
        loop(eng, fake); loop(eng, fake)
        return fake, eng, ex, subs

    def legs(self, eng, fake, ex, layer):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer]

    def test_core_never_offered_clips_per_sub_and_rule0_per_lot(self):
        fake, eng, ex, subs = self.rig()
        r = eng.controls.d["reservoirs"][ex]
        plan = eng.reservoir_plan(time.time(), r)
        by = collections.defaultdict(int)
        for x in plan["sells"]:
            by[(x["sub"], round(x["px"], 3))] += x["qty"]
            self.assertLessEqual(x["qty"], 5_000, "clips <= 5k")
        self.assertEqual(by[(subs[0], 0.85)], 14_787, "the swing lots at 0.850")
        self.assertLessEqual(by[("inventory", 0.85)], 20_144 - 10_000, "the core (10,000) is never offered")
        self.assertEqual(by[(subs[2], 0.855)], 8_900); self.assertEqual(by[(subs[3], 0.855)], 3_100)
        self.assertGreater(sum(1 for x in plan["sells"] if x["sub"] == subs[0]), 1, "several clips resting at once")
        sells = self.legs(eng, fake, ex, "swingsell")
        self.assertTrue(sells)
        self.assertFalse(any(eng.ledger.orders[o["id"]].get("sub") == "inventory" and o["qty"] > 10_144 for o in sells))
        self.assertEqual(self.legs(eng, fake, ex, "wallexit"), [], "the walls' buy-backs retire")
        # rule 0 per lot: a wall lot with entry 0.852 (+0.5c = 0.857 > 0.855) waits
        eng.ledger.tranches[subs[2]]["avg"] = 0.852
        plan = eng.reservoir_plan(time.time(), r)
        self.assertFalse(any(x["sub"] == subs[2] for x in plan["sells"]))
        # inventory lots are counted by rule0_allowed (only the cheap ones go)
        eng.rule0_allowed = lambda ex_, side, sub, px, q, margin=a_config.RULE0_MARGIN: min(q, 4_000) if sub == "inventory" else q
        plan = eng.reservoir_plan(time.time(), r)
        self.assertEqual(sum(x["qty"] for x in plan["sells"] if x["sub"] == "inventory"), 4_000)

    def test_partial_unwind_20k_at_0845_joining_the_touch_rest_held(self):
        """Piero 16:50: 20,000 NO offered at 0.845 (swing lots first, then cheap inventory lots), joining the touch, clips <= 5k; the walls
        and the rest held (no wall buy-back); after a sale only (20,000 - out) is offered."""
        fake, eng, ex, subs = self.rig()
        r = eng.controls.d["reservoirs"][ex]
        r.update(offers=[[subs[0], 0.845], ["inventory", 0.845]], hold_subs=subs[2:], max_out=20_000, rest_qty=None, join_touch=True, core_qty=0)
        fake.px[ex] = eng.px[ex] = (0.155, 0.16, 0.1575)       # NO bid 0.84 / ask 0.845: the level is the touch
        plan = eng.reservoir_plan(time.time(), r)
        self.assertEqual(sum(x["qty"] for x in plan["sells"]), 20_000)
        self.assertTrue(all(abs(x["px"] - 0.845) < 1e-9 and x["qty"] <= 5_000 for x in plan["sells"]))
        self.assertEqual(sum(x["qty"] for x in plan["sells"] if x["sub"] == subs[0]), 14_787, "the swing lots first")
        self.assertFalse(any(x["sub"] in subs[2:] for x in plan["sells"]), "the walls are held")
        self.assertEqual(eng.reservoir_subs(ex), {subs[0], "inventory", subs[2], subs[3]}, "held walls stay owned: no wall buy-back")
        fake.px[ex] = eng.px[ex] = (0.145, 0.15, 0.1475)       # NO touch ask 0.855 above the level: join it
        plan = eng.reservoir_plan(time.time(), r)
        self.assertTrue(all(abs(x["px"] - 0.855) < 1e-9 for x in plan["sells"]), [x["px"] for x in plan["sells"]])
        # an existing 0.845 order stays when the touch moves up one tick (queue kept); a 0.850 order above a 0.845 touch re-pegs down (tested above)
        r["stats"]["sold_sh"] = 6_000.0                         # 6,000 out: only 14,000 more offered
        plan = eng.reservoir_plan(time.time(), r)
        self.assertEqual(sum(x["qty"] for x in plan["sells"]), 14_000)

    def test_sell_now_fill_or_cancel_and_the_15k_resting_cap(self):
        """Piero 17:15: 20,000 NO at 0.840 now (limit buy YES 0.160, clips <= 5k), the unfilled rest cancelled after its TTL; the resting
        asks pulled first and paused while it runs; then at most 15,000 rest at 0.845; the fills logged with the average before / after."""
        fake, eng, ex, subs = self.rig()
        r = eng.controls.d["reservoirs"][ex]
        r.update(offers=[[subs[0], 0.845], ["inventory", 0.845]], hold_subs=subs[2:], rest_qty=15_000, max_out=None, join_touch=True, core_qty=0, rebuy_under_sell=0.02)
        fake.px[ex] = eng.px[ex] = (0.155, 0.16, 0.1575)
        loop(eng, fake); loop(eng, fake)
        resting = [o for o in self.legs(eng, fake, ex, "swingsell")]
        self.assertEqual(sum(o["qty"] for o in resting), 15_000, "at most 15,000 resting")
        ctl = f_engine.EngineControls(eng)
        ctl._apply("sell_now", dict(ex=ex, side="no", px=0.840, qty=20_000, clip=5_000, ttl_s=10))
        loop(eng, fake)
        sn = eng.controls.d["sell_now"]
        self.assertEqual(sn["state"], "sent")
        new = [o for o in self.legs(eng, fake, ex, "swingsell") if abs(o["price"] - 0.16) < 1e-9]
        self.assertEqual(sum(o["qty"] for o in new), 20_000)
        self.assertTrue(all(o["qty"] <= 5_000 and o["side"] == "yes" for o in new), "limit buy YES at 0.160, clips <= 5k")
        self.assertFalse([o for o in self.legs(eng, fake, ex, "swingsell") if abs(o["price"] - 0.155) < 1e-9], "the 0.845 asks are pulled while it runs")
        fake.fill(new[0]["id"], new[0]["qty"]); fake.fill(new[1]["id"], 3_000)   # 8,000 fill at once
        loop(eng, fake)
        sn["sent_t"] -= 20                                    # the TTL passed
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(sn["state"], "done")
        self.assertEqual(sn["filled"], 8_000.0)
        self.assertFalse([o for o in self.legs(eng, fake, ex, "swingsell") if abs(o["price"] - 0.16) < 1e-9], "the unfilled rest is cancelled, not left resting")
        line = next(e["text"] for e in eng.events if e["text"].startswith("SELL NOW") and "done" in e["text"])
        self.assertIn("filled 8,000 of 20,000", line); self.assertIn("SIG average", line); self.assertIn("free cash", line)
        loop(eng, fake); loop(eng, fake)
        self.assertLessEqual(sum(o["qty"] for o in self.legs(eng, fake, ex, "swingsell")), 15_000, "back to at most 15,000 resting at 0.845")
        buys = sorted(self.legs(eng, fake, ex, "swing"), key=lambda o: -o["price"])
        self.assertEqual([round(o["price"], 3) for o in buys], [0.82, 0.815, 0.81], "rebuys unchanged (last sell 0.840 - 2c = 0.820)")

    def test_1740_hold_11900_rest_at_the_average_floor_no_rebuys(self):
        """Piero 17:40: hold 11,900 (one wall + 3,000 of the other); everything else rests at 0.840 (the position-average floor replaces the
        per-lot rule 0 on his order), clips <= 5k; no rebuys; no rebuild; the TM never buys the market; catcher lots cycle at entry + 2c."""
        fake, eng, ex, subs = self.rig()
        r = eng.controls.d["reservoirs"][ex]
        r.update(offers=[[subs[0], 0.840], ["inventory", 0.840], [subs[3], 0.840]], sell_caps={subs[3]: 100}, hold_subs=[subs[2]], avg_floor=True,
                 join_touch=False, no_rebuild=True, catcher_cycle_c=0.02, rebuy=[], max_out=None, rest_qty=None, core_qty=0)
        r["stats"]["sold_sh"] = 7_644.0; r["last_sell_px"] = 0.84
        fake.px[ex] = eng.px[ex] = (0.16, 0.165, 0.1625)        # NO bid 0.835 / ask 0.840
        eng.book[ex]["row"] = dict(eng.book[ex].get("row") or {}, avgCost=0.8397)
        plan = eng.reservoir_plan(time.time(), r)
        by = collections.defaultdict(int)
        for x in plan["sells"]:
            by[x["sub"]] += x["qty"]
            self.assertAlmostEqual(x["px"], 0.840, places=3); self.assertLessEqual(x["qty"], 5_000); self.assertEqual(x["rule0_margin"], -1.0)
        self.assertEqual((by[subs[0]], by["inventory"], by[subs[3]]), (14_787, 20_144, 100), "swing + inventory + 100 of the second wall")
        self.assertNotIn(subs[2], by, "the 8,900 wall is held")
        self.assertEqual(plan["buys"], [], "no rebuy ladder")
        FAIR[ex] = 0.05
        eng.reservoir_check_triggers(time.time(), r, 0.05)
        self.assertEqual(r["mode"], "trade", "no rebuild on RI any more")
        FAIR[ex] = 0.0065
        self.assertNotIn(ex, {c_[1] for c_ in eng.tm_candidates()}, "the TM never buys a reservoir market")
        # a catcher tranche on the market: offered back at >= entry + 2c and never held while cash is idle
        sub = f"dump:{ex}:no:1"
        eng.ledger.subbooks[ex][sub] = -2_000.0
        eng.ledger.tranches[sub] = dict(ex=ex, side="no", kind="dump", state="open", ts=time.time() - 60, t="x", avg=0.80)
        eng.controls.d["buffer"] = dict(value=1_000, day=time.strftime("%Y-%m-%d"))
        eng.track_dumps(time.time())
        v = [x for x in eng.dump_view.get(ex, []) if x["sub"] == sub]
        self.assertTrue(v, "not held")
        self.assertGreaterEqual(1 - v[0]["rebound"] if v[0]["rebound"] < 0.5 else v[0]["rebound"], 0.82 - 1e-9)

    def test_an_ask_never_crosses_the_bid(self):
        fake, eng, ex, subs = self.rig()
        fake.px[ex] = eng.px[ex] = (0.10, 0.105, 0.1025)       # NO bid 0.895 / ask 0.90: the 0.850 / 0.855 levels would cross
        r = eng.controls.d["reservoirs"][ex]
        plan = eng.reservoir_plan(time.time(), r)
        self.assertTrue(plan["sells"])
        self.assertTrue(all(x["px"] >= 0.90 - 1e-9 for x in plan["sells"]), [x["px"] for x in plan["sells"]])

    def test_rebuy_only_what_was_sold_3c_under_and_the_2k_guard(self):
        fake, eng, ex, subs = self.rig()
        sell = next(o for o in self.legs(eng, fake, ex, "swingsell") if eng.ledger.orders[o["id"]]["sub"] == subs[0])
        fake.fill(sell["id"], 3_000)
        loop(eng, fake); loop(eng, fake)
        r = eng.controls.d["reservoirs"][ex]
        self.assertEqual(r["stats"]["sold_sh"], 3000.0); self.assertAlmostEqual(r["last_sell_px"], 0.850, places=3)
        self.assertTrue(any("RESERVOIR" in e["text"] and "sold 3,000" in e["text"] for e in eng.events))
        buys = sorted(self.legs(eng, fake, ex, "swing"), key=lambda o: -o["price"])
        self.assertEqual([round(o["price"], 3) for o in buys], [0.82, 0.815, 0.81])
        self.assertEqual([o["qty"] for o in buys], [1200, 900, 900], "40 / 30 / 30 of the 3,000 sold")
        self.assertTrue(all(o["price"] <= 0.850 - 0.03 + 1e-9 for o in buys), "never above the last sell - 3c")
        cash_was = eng.cash_hist; eng.cash_hist = [a_config.CASH_RESERVE + 1_500]
        self.assertEqual((eng.reservoir_plan(time.time(), r) or {}).get("buys", []), [], "free < 2k: no rebuy")
        eng.cash_hist = cash_was
        for o in buys:
            fake.fill(o["id"], o["qty"])
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(r["stats"]["cycles"], 1)
        self.assertTrue(any("cycle 1:" in e["text"] and "c per share" in e["text"] for e in eng.events))
        # the rebought lots (swing sub, ~0.817) are offered again at 0.850 (>= entry + 3c)
        plan = eng.reservoir_plan(time.time(), r)
        self.assertTrue(any(x["sub"] == subs[0] and abs(x["px"] - 0.85) < 1e-9 for x in plan["sells"]))

    def test_rebuild_triggers_stop_selling_and_rebuy_to_full(self):
        fake, eng, ex, subs = self.rig()
        self.assertTrue(self.legs(eng, fake, ex, "swingsell"))
        FAIR[ex] = 0.03
        loop(eng, fake); loop(eng, fake)
        r = eng.controls.d["reservoirs"][ex]
        self.assertEqual(r["mode"], "rebuild")
        self.assertTrue(any("-> REBUILD" in e["text"] for e in eng.events))
        self.assertEqual(self.legs(eng, fake, ex, "swingsell"), [], "selling stops")
        eng.ledger.subbooks[ex][subs[0]] = -8_805.0; fake.pos[ex] = -40_949
        loop(eng, fake); loop(eng, fake)
        buys = self.legs(eng, fake, ex, "swing")
        self.assertEqual(len(buys), 1, buys)
        self.assertLessEqual(buys[0]["price"], r["rebuild_px"] + 1e-9)
        self.assertEqual(buys[0]["qty"], r["full_qty"] - 40_949)
        fake, eng, ex, subs = self.rig()
        eng.res_p10[ex] = 0.86
        loop(eng, fake)
        self.assertEqual(eng.controls.d["reservoirs"][ex]["mode"], "rebuild")

    def test_migration_builds_three_reservoirs_and_carries_the_ri_state(self):
        fake, eng, ex, subs = self.rig()
        eng.controls.d.pop("reservoirs", None)
        eng.controls.d["reservoir"] = dict(ex="1077", side="no", mode="trade", fair0=0.0065, last_sell_px=0.86, stats=dict(sold_sh=6000.0, bought_sh=2982.0, realized=55.5, cycles=0, sold_val=5160.0, bought_val=2412.0, legs=[]))
        _os.environ["SIG_RESERVOIR"] = "1"; self.addCleanup(_os.environ.__setitem__, "SIG_RESERVOIR", "0")
        eng.reservoir_migration()
        rs = eng.controls.d["reservoirs"]
        self.assertEqual(sorted(rs), sorted(a_config.RESERVOIRS))
        self.assertEqual(rs["1077"]["stats"]["sold_sh"], 6000.0, "the live RI state carries over (shares out)")
        self.assertAlmostEqual(rs["1077"]["last_sell_px"], 0.86, places=3)
        self.assertNotIn("reservoir", eng.controls.d)
        self.assertEqual((rs["1077"]["ver"], rs["1077"]["offers"][0][1], rs["1077"]["rebuy"]), ("17:40", 0.84, []), "Piero 17:40: 0.840, no rebuys")

    def test_fair_reference_pinned_on_the_first_priced_loop(self):
        fake, eng, ex, subs = self.rig()
        eng.controls.d["reservoirs"][ex]["fair0"] = None
        loop(eng, fake)
        self.assertAlmostEqual(eng.controls.d["reservoirs"][ex]["fair0"], eng.fair[ex], places=6)
        self.assertTrue(any("fair reference pinned" in e["text"] for e in eng.events))

    def test_entry_guard_hysteresis_and_tm_auto_full(self):
        fake, eng, ex, subs = self.rig()
        self.assertEqual(eng.entry_budget(a_config.CASH_RESERVE + 1_900), 0.0)
        self.assertTrue(eng.entries_off)
        self.assertEqual(eng.entry_budget(a_config.CASH_RESERVE + 2_500), 0.0, "between 2k and 3k: still off")
        self.assertGreater(eng.entry_budget(a_config.CASH_RESERVE + 3_000), 0.0, "resumes at 3k")
        # Piero 16:10 §3: the TM goes to full size as soon as free >= BUFFER
        eng.controls.d["layers"]["tm"] = dict(state="pilot", caps={}, killed=None)
        eng.controls.d["buffer"] = dict(value=20_000, day=time.strftime("%Y-%m-%d"))
        eng.cash_hist = [a_config.CASH_RESERVE + 30_000]
        eng.tm_plan(time.time(), eng.cash_hist[-1], eng.bucket_usage())
        self.assertEqual(eng.controls.state("tm"), "full")
        self.assertTrue(any("TM -> FULL SIZE" in e["text"] for e in eng.events))


class RingWindowTests(EngineCase):
    """Piero 15:35 Oct 8 (1c): inside a ring window no new TM / SM entries (exits keep running), their reserves drop to what they
    hold, the allocator puts L0 / L1 first; the TM / SM exit floor is the newest held lot's entry + 0.5c (item 2); a pair-set
    leg is judged with the pair's net lock."""

    def rig(self):
        fresh_data(layers=dict(position=dict(state="full"), sm=dict(state="pilot"), tm=dict(state="pilot"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 60_000)
        eng = engine(fake); eng.ex.capacity = 6000.0
        loop(eng, fake)
        return fake, eng

    def in_window(self, on):
        self.addCleanup(setattr, f_engine, "RING_WINDOWS", f_engine.RING_WINDOWS)
        f_engine.RING_WINDOWS = [("00:00", "23:59")] if on else [("00:00", "00:00")]

    def test_no_new_tm_sm_entries_reserves_zero_l0_l1_first(self):
        fake, eng = self.rig()
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.50; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)
        eng.controls.d["sm_markets"] = [ex]; eng.sm_depth[ex] = eng.tm_depth[ex] = (time.time() + 1e9, [(0.46, 50_000.0)], [(0.47, 50_000.0)])
        self.in_window(False)
        loop(eng, fake); loop(eng, fake)
        self.assertGreater(eng.sm_reserve() + eng.tm_reserve(), 0)
        self.in_window(True)
        self.assertTrue(eng.ring_window(time.time()))
        self.assertTrue(any("RING WINDOW on" in e["text"] for e in eng.events))
        self.assertEqual(eng.tm_plan(time.time(), eng.cash_hist[-1], eng.bucket_usage()), {}, "no new TM entries")
        self.assertEqual([q for qs in eng.sm_plan(time.time(), eng.cash_hist[-1]).values() for q in qs if not q.get("reduce")], [], "no new SM entries")
        self.assertEqual((eng.tm_reserve(), eng.sm_reserve()), (0.0, 0.0), "reserves drop to what they hold")
        # an SM lot still exits inside the window
        eng.ledger.subbooks.setdefault(ex, {})[f"sm:{ex}"] = 500.0; fake.pos[ex] = 500; fake.avg_cost[ex] = 0.46
        eng.ledger.tranches[f"sm:{ex}"] = dict(ex=ex, side="yes", kind="sm", state="open", ts=time.time(), t="x", avg=0.46)
        plan = eng.sm_plan(time.time(), eng.cash_hist[-1])
        self.assertTrue(any(q.get("reduce") for q in plan.get(ex, [])), plan)
        # the allocator: L0 / L1 before deeper levels in the window
        def row(i, lname, px, qty, w, edge, l0=False):
            return (i, False, "x", dict(side="yes", px=px, qty=qty, sub=f"dump:x:yes:{lname}{i}", level=i, edge=edge, arena=False, l0=l0, ttl=600, weight=w, lname=lname, px_rest=None))
        rows = [row(2, "L3", 0.80, 1000, a_config.ENTRY_WEIGHTS["L3"], 0.09), row(0, "L1", 0.85, 1000, a_config.ENTRY_WEIGHTS["L1"], 0.03)]
        out, cut, spent = eng.allocate_levels(rows, 1e9, 1e9)
        self.assertEqual([w["lname"] for _, _, _, w in out], ["L1", "L3"], "L1 ahead of the better-edge L3 inside the window")
        self.in_window(False)
        out, cut, spent = eng.allocate_levels(rows, 1e9, 1e9)
        self.assertEqual([w["lname"] for _, _, _, w in out], ["L3", "L1"], "by expected edge outside it")

    def test_tm_exit_floor_is_the_newest_lot_plus_half_a_cent(self):
        fake, eng = self.rig()
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.50; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)
        sub = f"tm:{ex}:yes"
        eng.ledger.subbooks.setdefault(ex, {})[sub] = 500.0; fake.pos[ex] = 500; fake.avg_cost[ex] = 0.47
        eng.ledger.tranches[sub] = dict(ex=ex, side="yes", kind="tm", state="open", ts=time.time(), t="x", avg=0.465, last_px=0.485)   # the newest lot was bought at 0.485
        eng.track_tm(time.time())
        xs = eng.tm_view.get(ex, [])
        self.assertTrue(xs and xs[0]["px"] >= 0.485 + 0.005 - 1e-9, xs)
        # the fill event records the newest entry
        eng.tm_fill_events([dict(ex=ex, side="yes", action="buy", qty=100.0, price=0.49, layer="tm", realized=0.0, t=stamp_())])
        self.assertAlmostEqual(eng.ledger.tranches[sub]["last_px"], 0.49, places=3)

    def test_pair_set_leg_judged_with_the_pairs_lock(self):
        fake, eng = self.rig()
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.1 <= eng.fair[b.ex] <= 0.3)
        eng.ledger.orders["pA"] = dict(id="pA", bot="pair", layer="set", ex=ex, side="yes", action="buy", price=0.18, qty=100, filled=100.0, cost=18.0, left=0, placed="x", ts=time.time() - 60, closed="x", status="done", sub="pairset:77", note="")
        eng.ledger.orders["pB"] = dict(id="pB", bot="pair", layer="set", ex="other", side="no", action="buy", price=0.79, qty=100, filled=100.0, cost=79.0, left=0, placed="x", ts=time.time() - 60, closed="x", status="done", sub="pairset:77", note="")
        eng.ledger.baskets.append(dict(id="77", orders=["pA", "pB"], legs=2, repairs={}))
        lock = eng.pair_lock("77")
        self.assertIsNotNone(lock); self.assertAlmostEqual(lock["lock"], 0.03, places=6, msg="1 - (0.18 + 0.79) = +3c per share")
        self.assertIn("@ 0.180", lock["legs"]); self.assertIn("@ 0.790", lock["legs"])


class FeedTests(EngineCase):
    """C86 (Piero 15:35 Oct 8): SIG's tournament markets feed — books (full, versioned) and sweeps from the trades; per-market
    revisions (duplicates dropped, gaps counted); a reconnect makes every book stale until a fresh batch (the election-night drill);
    log mode compares with REST and changes nothing; live mode serves the depth, the queue-ahead and the fast path from the feed."""
    TID = "tid-1"

    def batch(self, mid, rev, prev, books=(), trades=(), at="2026-10-08T23:11:45.000+00:00"):
        return {"event": "broadcast", "topic": f"realtime:tournament:{self.TID}:markets", "payload": {"event": "markets_batch", "type": "broadcast", "payload": {"markets": [
            {"marketId": mid, "batch": {"delivery": {"previousRevision": prev, "revision": rev, "model": "best-effort-authoritative-resync"},
                                        "books": [{"exchangeId": ex, "asOf": {"at": at, "sequence": seq}, "bids": [{"price": p, "quantity": q} for p, q in bids],
                                                   "asks": [{"price": p, "quantity": q} for p, q in asks], "nextExpiryAt": None} for ex, seq, bids, asks in books],
                                        "trades": [{"exchangeId": ex, "executedAt": t_at, "id": str(i), "price": p, "quantity": q, "sequence": i} for i, (ex, t_at, p, q) in enumerate(trades)],
                                        "bookDirty": [], "marketSettled": []}}]}}}

    def feed(self, t0):
        import i_realtime
        clock = [t0]
        f = i_realtime.SigMarketFeed(lambda: {}, self.TID, clock=lambda: clock[0], log=lambda *a: None)
        f.connected_t = t0
        return f, clock

    def test_parser_books_lag_duplicates_and_gaps(self):
        from datetime import datetime, timezone
        t0 = datetime(2026, 10, 8, 23, 11, 46, tzinfo=timezone.utc).timestamp()
        f, clock = self.feed(t0)
        f.handle(self.batch("270", 10, 9, books=[("959", 100, [(0.48, 500), (0.475, 900)], [(0.49, 700)])]))
        self.assertEqual(f.book("959"), ([(0.48, 500.0), (0.475, 900.0)], [(0.49, 700.0)]))
        self.assertAlmostEqual(f.lags[-1], 1.0, places=2, msg="asOf -> received")
        f.handle(self.batch("270", 10, 9, books=[("959", 101, [(0.47, 1)], [(0.5, 1)])]))
        self.assertEqual(f.stats["duplicates"], 1, "a revision at or below the last one is a duplicate")
        self.assertEqual(f.book("959")[0][0], (0.48, 500.0))
        f.handle(self.batch("270", 14, 12, books=[("959", 102, [(0.485, 300)], [(0.49, 700)])]))
        self.assertEqual(f.stats["gaps"], 1, "previousRevision 12 > last 10: a gap")
        self.assertEqual(f.book("959")[0][0], (0.485, 300.0), "the gap batch's book is a full snapshot: used")
        f.handle(self.batch("270", 15, 14, books=[("959", 90, [(0.40, 1)], [(0.6, 1)])]))
        self.assertEqual(f.book("959")[0][0], (0.485, 300.0), "an older book version is ignored")

    def test_reconnect_resync_drill_and_staleness(self):
        from datetime import datetime, timezone
        t0 = datetime(2026, 10, 8, 23, 11, 46, tzinfo=timezone.utc).timestamp()
        f, clock = self.feed(t0)
        f.handle(self.batch("270", 10, 9, books=[("959", 100, [(0.48, 500)], [(0.49, 700)])]))
        self.assertIsNotNone(f.book("959"))
        f.connection_lost()                                  # the drill: the socket drops (election night: SIG / network blip)
        self.assertIsNone(f.book("959"), "after a reconnect every book is stale")
        self.assertEqual(f.stats["reconnects"], 1)
        f.connected_t = clock[0]
        f.handle(self.batch("270", 3, 2, books=[("959", 50, [(0.47, 100)], [(0.48, 100)])]))   # revisions restart after a reconnect
        self.assertEqual(f.book("959"), ([(0.47, 100.0)], [(0.48, 100.0)]), "a fresh batch on the new connection is used, whatever its sequence")
        clock[0] += 31                                       # no message for 31 s: not connected -> no book
        self.assertIsNone(f.book("959"))
        clock[0] -= 31
        f.books["959"]["at"] = clock[0] - 20; f.books["959"]["recv"] = clock[0] - 20
        self.assertIsNone(f.book("959", 10.0), "older than 10 s: stale")

    def test_sweep_detection(self):
        from datetime import datetime, timezone
        t0 = datetime(2026, 10, 8, 23, 11, 46, tzinfo=timezone.utc).timestamp()
        f, clock = self.feed(t0)
        got = []
        f.on_sweep = got.append
        small = [("986", "2026-10-08T23:11:45.1+00:00", 0.90, 300)]
        f.handle(self.batch("297", 1, 0, trades=small))
        self.assertEqual(got, [])
        burst = [("986", "2026-10-08T23:11:45.2+00:00", 0.90, 2000), ("986", "2026-10-08T23:11:45.6+00:00", 0.905, 2500), ("986", "2026-10-08T23:11:46.0+00:00", 0.915, 2000)]
        f.handle(self.batch("297", 2, 1, trades=burst))
        self.assertEqual(len(got), 1, got)
        self.assertEqual((got[0]["ex"], got[0]["direction"], got[0]["shares"]), ("986", "up", 6800.0))
        f.handle(self.batch("297", 3, 2, trades=[]))
        self.assertEqual(len(got), 1, "reported once")

    def rig(self):
        fresh_data(layers=dict(position=dict(state="full"), sm=dict(state="pilot"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 60_000)
        eng = engine(fake); eng.ex.capacity = 6000.0; fake.DRY_RUN = False
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        import i_realtime
        f = i_realtime.SigMarketFeed(lambda: {}, self.TID, log=lambda *a: None)
        f.connected_t = time.time(); f.last_msg_t = time.time()
        f.books[ex] = dict(seq=1, at=time.time(), recv=time.time(), epoch=f.epoch, bids=[(0.48, 4321.0), (0.475, 100.0)], asks=[(0.49, 1234.0)])
        eng.mkt_feed = f
        eng.boot_t = 0.0                                     # past the 2-min start-up hold on optional reads
        n = [0]; real = fake.orderbook
        fake.orderbook = lambda *a, **k: (n.__setitem__(0, n[0] + 1), real(*a, **k))[1]
        return fake, eng, ex, n

    def test_log_mode_changes_nothing_and_compares_with_rest(self):
        fake, eng, ex, n = self.rig()
        eng.controls.d["feed_mode"] = "log"
        self.assertIsNone(eng.feed_book(ex), "log mode: the engine never uses the feed's book")
        eng.sm_depth.pop(ex, None)
        eng.sm_touch_size(ex, time.time())
        self.assertEqual(n[0], 1, "the depth still comes from REST")
        eng.feed_compare(ex, [(0.48, 4321.0)], [(0.49, 1234.0)], time.time())
        eng.feed_compare(ex, [(0.48, 4000.0)], [(0.49, 1234.0)], time.time())
        self.assertEqual([(x["px_ok"], x["size_ok"]) for x in eng.feed_cmp], [(True, True), (True, False)])
        eng.feed_report_t = time.time() - a_config.FEED_REPORT_S - 1
        eng.feed_report(time.time())
        self.assertTrue(any(e["text"].startswith("FEED (log)") and "touch prices equal 2" in e["text"] for e in eng.events))

    def test_live_mode_serves_depth_queue_and_the_fast_path_and_wakes_on_sweeps(self):
        fake, eng, ex, n = self.rig()
        eng.controls.d["feed_mode"] = "live"
        eng.sm_depth.pop(ex, None); eng.tm_depth.pop(ex, None)
        self.assertEqual(eng.sm_touch_size(ex, time.time()), (4321.0, 1234.0))
        self.assertEqual(eng.tm_touch_depth(ex, "yes", time.time()), 4321.0)
        self.assertEqual(eng.queue_ahead(ex, "yes", 0.48), 4321.0)
        self.assertEqual(eng.queue_ahead(ex, "no", round(1 - 0.49, 3)), 1234.0)
        self.assertEqual(n[0], 0, "no REST book read in live mode")
        eng.mkt_feed.connection_lost()                       # the drill: stale -> REST fallback
        eng.sm_touch_size(ex, time.time())
        self.assertEqual(n[0], 1, "after a reconnect the depth falls back to REST")
        eng.mkt_feed.connected_t = time.time(); eng.mkt_feed.last_msg_t = time.time()
        eng.mkt_feed.books[ex]["epoch"] = eng.mkt_feed.epoch
        with eng.rt_lock:
            eng.rt_pending = []
        eng.on_feed_sweep(dict(ex=ex, t=time.time(), recv=time.time(), shares=9000.0, lo=0.48, hi=0.50, direction="up", n=4))
        self.assertTrue(any(k == "sweep" and ex in exs for _, k, exs in eng.rt_pending), "live: a sweep wakes the loop for that market")


class C87Tests(EngineCase):
    """C87 (Piero 19:56 Oct 8): the touch maker's entries stop 60 min before each ring and pause while free < BUFFER (resume at
    BUFFER + 1k), exits always run; the one-time migration (TM full, SM on, FM / scalper deleted, the scalper's leftover to held,
    a deleted layer can never be switched back on); the FM's lots offered at max(entry + 0.5c, touch); the live regime classifier
    (mm_playbook v0 rules); SM entries only in BOUNCE markets, a market under -50 today excluded, the 24-h stop; the nightly
    re-tune applied only when it beats the current setting by enough, within bounds."""

    def tm_rig(self, cash_above_floor=60_000):
        return TMTests.rig(self, cash_above_floor)

    def test_tm_entries_stop_before_each_ring_and_exits_keep_running(self):
        fake, eng, ex = self.tm_rig()
        loop(eng, fake)
        self.assertTrue(TMTests.tm_orders(self, eng, fake, ex), "a TM bid rests outside the stops")
        hm = time.strftime("%H:%M", time.localtime(time.time() - 60)); hm2 = time.strftime("%H:%M", time.localtime(time.time() + 3600))
        f_engine.TM_ENTRY_STOPS = [(hm, hm2)]
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(TMTests.tm_orders(self, eng, fake, ex), [], "inside the stop: the resting TM bid is cancelled")
        self.assertEqual(eng.tm_reserve(), 0.0, "nothing reserved for TM entries")
        self.assertTrue(any("TM entries STOP" in e["text"] for e in eng.events))
        self.assertTrue(eng.tm_entry_stop(time.time()))
        f_engine.TM_ENTRY_STOPS = [("23:59", "00:01")]
        self.assertEqual(eng.tm_entry_stop(time.mktime(time.strptime("2026-10-09 00:00:30", "%Y-%m-%d %H:%M:%S"))), True, "a stop across midnight")
        self.assertEqual(eng.tm_entry_stop(time.mktime(time.strptime("2026-10-09 12:00:00", "%Y-%m-%d %H:%M:%S"))), False)

    def test_tm_pauses_under_buffer_and_resumes_at_buffer_plus_1k(self):
        f_engine.TM_BUFFER_GATE = True
        fake, eng, ex = self.tm_rig()
        buf = eng.buffer()
        eng.cash_hist = collections.deque([a_config.CASH_RESERVE + buf - 100.0])
        self.assertTrue(eng.tm_buffer_gate(time.time()), "free < BUFFER: paused")
        eng.cash_hist = collections.deque([a_config.CASH_RESERVE + buf + 500.0])
        self.assertTrue(eng.tm_buffer_gate(time.time()), "BUFFER <= free < BUFFER + 1k: still paused (no flapping)")
        eng.cash_hist = collections.deque([a_config.CASH_RESERVE + buf + 1_000.0])
        self.assertFalse(eng.tm_buffer_gate(time.time()), "free >= BUFFER + 1k: resumes")
        texts = [e["text"] for e in eng.events]
        self.assertTrue(any("TM entries PAUSED" in t for t in texts) and any("TM entries resume" in t for t in texts))

    def test_tm_resting_bids_stay_inside_the_cap_and_the_catchers_last_10k(self):
        """C87b: resting TM bids count: held + resting <= the cap and <= free - 10k (SIG locks no cash for a resting bid)."""
        for above, expect in ((12_000, 2_000.0), (60_000, None)):
            fake, eng, ex = self.tm_rig(cash_above_floor=above)
            eng.controls.set_layer("tm", "full", "test")
            for e_ in [b.ex for b in eng.bots if b.kind == "market"][:40]:
                if eng.fair.get(e_) is not None:
                    eng.tm_depth[e_] = (time.time() + 1e9, [(0.1, 50_000.0)], [(0.9, 50_000.0)])
            for _ in range(4):
                loop(eng, fake)
            rest = sum(o["qty"] * o["price"] for o in fake.open_orders() if (eng.ledger.orders.get(o["id"]) or {}).get("layer") == "tm")
            allow = min(eng.tm_cap(time.time(), log=False) - eng.tm_cost(), eng.free_cash() - a_config.TM_PAUSE_FREE)
            self.assertLessEqual(rest, max(0.0, allow) + 1.0, f"free {eng.free_cash():,.0f}: resting TM {rest:,.0f} vs allowance {allow:,.0f}")
            self.assertEqual(eng.tm_cov.get("allowance"), round(max(0.0, allow)))

    def test_migration_tm_full_sm_on_fm_and_scalper_deleted(self):
        fresh_data(layers=dict(position=dict(state="full"), tm=dict(state="pilot"), sm=dict(state="off"), fm=dict(state="pilot"), scalp=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS)
        eng.ledger.subbooks.setdefault(ex, {})[f"scalp:{ex}:yes"] = 5.0
        os.environ["SIG_C87"] = "1"
        self.addCleanup(os.environ.__setitem__, "SIG_C87", "0")
        eng.c87_migrations()
        c = eng.controls
        self.assertEqual((c.state("tm"), c.state("sm"), c.state("fm"), c.state("scalp")), ("full", "pilot", "off", "off"))
        self.assertNotIn(f"scalp:{ex}:yes", eng.ledger.subbooks[ex])
        self.assertEqual(eng.ledger.subbooks[ex].get("inventory"), 5.0, "the scalper's leftover goes to held (inventory)")
        self.assertTrue(any("C87" in e["text"] and "DELETED" in e["text"] for e in eng.events))
        c.set_layer("tm", "pilot", "test")
        eng.c87_migrations()
        self.assertEqual(c.state("tm"), "pilot", "once only")
        with self.assertRaises(ValueError):
            f_engine.EngineControls(eng).apply("layer_state", dict(layer="fm", state="pilot"))
        self.assertEqual(eng.fm_targets(), [], "no FM targets: its markets are open to the TM and the SM")
        self.assertEqual(eng.scalp_plan(time.time(), 1e6, {}), {})

    def test_fm_lots_offered_at_max_entry_plus_half_cent_and_touch(self):
        fake, eng, ex = self.tm_rig()
        eng.ledger.subbooks.setdefault(ex, {})[f"fm:{ex}"] = 1000.0
        eng.ledger.tranches[f"fm:{ex}"] = dict(ex=ex, side="yes", kind="fm", ts=time.time() - 9000, state="open", avg=0.6225)
        eng.px[ex] = (0.60, 0.61, 0.605)                       # touch ask 0.61 < entry 0.6225: the floor wins
        x = eng.fm_plan(time.time(), 1e6)[ex][0]
        self.assertTrue(x["reduce"]); self.assertEqual(x["held"], "yes"); self.assertEqual(x["qty"], 1000)
        self.assertAlmostEqual(x["sell_px"], 0.63, places=3, msg="never below entry + 0.5c (0.6275 -> the 0.630 tick)")
        eng.px[ex] = (0.66, 0.67, 0.665)                       # touch ask 0.67 above the floor: offered at the touch
        self.assertAlmostEqual(eng.fm_plan(time.time(), 1e6)[ex][0]["sell_px"], 0.67, places=3)
        eng.ledger.subbooks[ex][f"fm:{ex}"] = -1000.0
        eng.ledger.tranches[f"fm:{ex}"].update(side="no")
        eng.px[ex] = (0.36, 0.37, 0.365)                       # NO ask = 1 - 0.36 = 0.64
        x = eng.fm_plan(time.time(), 1e6)[ex][0]
        self.assertEqual((x["held"], x["side"]), ("no", "yes"))
        self.assertAlmostEqual(x["sell_px"], 0.64, places=3)
        self.assertAlmostEqual(x["px"], 0.36, places=3, msg="the NO lot sells as a YES buy at 1 - its price")

    def test_regime_classifier_matches_the_playbook_rules(self):
        E = f_engine.Engine
        mk = lambda ps, fair=None: [(i * 10.0, p, 100.0, fair if fair is None else fair) for i, p in enumerate(ps)]
        self.assertEqual(E.classify_regime(mk([0.50, 0.505] * 12))[0], "BOUNCE")
        self.assertEqual(E.classify_regime(mk([0.50, 0.505, 0.51, 0.515, 0.52, 0.525]))[0], "TREND")
        self.assertEqual(E.classify_regime(mk([0.50, 0.505, 0.45, 0.50]))[0], "SWEEP", "a print 10 ticks from the window's first")
        self.assertEqual(E.classify_regime(mk([0.50, 0.505, 0.50]))[0], "QUIET")
        news = [(i * 10.0, p, 100.0, 0.50 + 0.003 * i) for i, p in enumerate([0.50, 0.505] * 6)]
        self.assertEqual(E.classify_regime(news)[0], "NEWS")
        self.assertEqual(E.classify_regime(mk([0.50, 0.505, 0.50, 0.51, 0.505, 0.505, 0.50, 0.51]))[0], "MIXED")
        self.assertEqual(E.classify_regime([])[0], "QUIET")

    def sm_rig(self):
        f_engine.SM_QUOTE_REGIMES = ("BOUNCE",)
        return SMTests.rig(self)

    def test_sm_quotes_only_in_bounce_and_the_loss_stops(self):
        fake, eng, ex = self.sm_rig()
        loop(eng, fake)
        self.assertEqual(SMTests.quotes(self, eng, fake, ex, "sm"), [], "no prints -> QUIET: no new SM quotes")
        self.assertNotEqual(eng.sm_regime[ex]["regime"], "BOUNCE")
        now = time.time()
        for i in range(24):
            eng.note_print(ex, f"t{i}", now - 600 + i * 20, 0.485 if i % 2 else 0.48, 200, eng.fair.get(ex))
        eng.sm_regime[ex]["t"] = 0                             # due for its minute recompute
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(eng.sm_regime[ex]["regime"], "BOUNCE")
        self.assertEqual(len(SMTests.quotes(self, eng, fake, ex, "sm")), 2, "BOUNCE: both sides quote")
        eng.sm_pnl = lambda e=None, since=0.0: -60.0 if e == ex else -60.0
        eng.sm_regime_t = 0
        loop(eng, fake); loop(eng, fake)
        self.assertIn(ex, eng.controls.d["sm_excluded"]["markets"], "under -50 today: excluded for the SIG day")
        self.assertEqual(SMTests.quotes(self, eng, fake, ex, "sm"), [], "excluded: the entry quotes are cancelled")
        eng.sm_pnl = lambda e=None, since=0.0: -10.0 if e is not None else -200.0
        eng.controls.d["sm_excluded"] = {}
        eng.sm_regime_t = 0
        loop(eng, fake)
        self.assertTrue(eng.sm_exits_only, "rolling 24 h under -150: exits only")
        self.assertTrue(any("SM EXITS ONLY" in e["text"] for e in eng.events))

    def test_sm_retune_applies_only_a_winning_bounded_change(self):
        fake, eng, ex = self.sm_rig()
        path = os.path.join(TMP, "intel", "sm_tune_proposal.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        old = f_engine.DATA
        def write(prop):
            with open(path, "w") as fh:
                json.dump(prop, fh)
            os.utime(path, (time.time(), time.time() + len(json.dumps(prop))))
        write(dict(id="n1", markets={ex: dict(new=dict(clip=300, repeg=3, skew=1), gain_day=5.0, fills=40),
                                     "x2": dict(new=dict(clip=5000), gain_day=9.0, fills=99),        # out of bounds: ignored
                                     "x3": dict(new=dict(clip=200), gain_day=1.0, fills=99)}))     # too small a gain: ignored
        eng.sm_tune_apply(time.time())
        t = eng.controls.d["sm_tune"]
        self.assertEqual(t["markets"], {ex: dict(clip=300, repeg=3, skew=1)})
        self.assertEqual(eng.sm_params(ex, eng.sm_sizes()), (300, 3, 1))
        self.assertEqual(eng.sm_params("other", eng.sm_sizes())[0], eng.sm_sizes()["clip"], "untuned markets keep the defaults")
        eng.sm_tune_apply(time.time())
        self.assertEqual(sum(1 for e in eng.events if "re-tune n1" in e["text"]), 1, "applied once per proposal")
        self.assertEqual(f_engine.DATA, old)


class CatcherFix1Tests(EngineCase):
    """Piero 21:20 (catcher fix #1, docs/catcher_diagnosis.md): L3 / L4 off; the L0 cap counts only resting L0 bids (held L0 lots
    no longer block new L0 bids); on the top-20 thin sides L1 joins the touch as its own order (next to L0) with L3 / L4's weight
    and L0's reach in the allocation."""

    def rig(self):
        return DumpV3Tests.rig(self)

    def test_no_deep_levels_and_thin_side_l1_at_the_touch(self):
        fake, eng, favs = self.rig()
        DumpV3Tests.settle(self, eng, fake, favs, 6)
        ex = favs[0]
        fs = eng.fair[ex]; side = "yes"
        bid = eng.px[ex][0]
        levels = eng.dump_levels(ex, side, fs, eng.ref(ex), 6_000.0, room=50_000.0)
        self.assertFalse([l for l in levels if l[0] in (2, 3)], f"no L3 / L4: {levels}")
        eng.controls.d["thin_sides"] = [[ex, side]]
        levels = eng.dump_levels(ex, side, fs, eng.ref(ex), 6_000.0, room=50_000.0)
        l1 = [l for l in levels if l[0] == 0]
        self.assertTrue(l1, levels)
        self.assertAlmostEqual(l1[0][1], bots.tick_down(min(bid, fs - a_config.DUMP_FAIR_GAP)), places=3, msg="L1 joins the touch on a thin side")
        day = eng.day_window(time.time())
        ws = a_config.DUMP_V3_WEIGHTS_DAY if day else a_config.DUMP_V3_WEIGHTS
        self.assertEqual(l1[0][2], int(6_000.0 * (ws[0] + sum(ws[2:])) / l1[0][1]), "L1 carries L3 / L4's weight")
        if any(l[0] == 4 for l in levels):
            self.assertEqual(sum(1 for l in levels if abs(l[1] - l1[0][1]) < 1e-9), 2, "L0 and L1 both rest at the touch")

    def test_l0_cap_counts_resting_bids_not_held_lots(self):
        fake, eng, favs = self.rig()
        DumpV3Tests.settle(self, eng, fake, favs, 3)
        ex = favs[0]
        eng.ledger.subbooks.setdefault(ex, {})[f"dump:{ex}:yes:L0"] = 40_000.0      # far more held L0 than the cap
        eng.ledger.tranches[f"dump:{ex}:yes:L0"] = dict(ex=ex, side="yes", kind="dump", ts=time.time(), state="open", avg=0.9)
        self.assertGreater(eng.l0_held(), a_config.DUMP_L0_BUDGET)
        self.assertLess(eng.l0_cap_used(), a_config.DUMP_L0_BUDGET, "held lots do not count")
        self.assertEqual(eng.l0_cap_used(), eng.l0_resting())

    def test_allocation_ranks_a_touch_l1_like_l0(self):
        fake, eng, favs = self.rig()
        w_touch = dict(edge=0.05, lname="L1", weight=0.5, touch=True, l0=False, px=0.9, qty=1000, side="yes")
        w_deep = dict(edge=0.05, lname="L1", weight=0.5, touch=False, l0=False, px=0.88, qty=1000, side="yes")
        out, cut, spent = eng.allocate_levels([(0, False, "a", w_deep), (0, False, "b", w_touch)], 0.9 * 1000 * 0.5 + 1, 15_000)
        self.assertEqual([x[2] for x in out], ["b"], "with room for one, the touch L1 wins")


class C88Tests(EngineCase):
    """C88 (Piero 01:55 Oct 9): the two books (CORE <= 50k at cost by edge-%, sticky, swaps at +3c, leaves within 1c of fair; TRADING
    lots >= 3c held, under 3c exit at max(entry + 0.5c, touch)); the hard pauses (pm_stale, engine_errors, i2_breach, cash_floor,
    api_errors) that lift themselves; the feed book scan; the recycle-depth snapshot; the scoreboard."""

    def rig(self):
        f_engine.CORE_CAP_COST, f_engine.TRADING_HOLD_C = 50_000.0, 0.03
        return DumpV3Tests.rig(self)

    def lot(self, eng, ex, sub, qty, avg, side="yes"):
        eng.ledger.subbooks.setdefault(ex, {})[sub] = float(qty if side == "yes" else -qty)
        eng.ledger.tranches[sub] = dict(ex=ex, side=side, kind="dump", state="open", ts=time.time() - 60, t="x", avg=avg)

    def test_core_picks_the_highest_edge_pct_within_50k_and_swaps_at_3c_more(self):
        f_engine.CORE_LOCK_RET = -99.0                         # the rig's touch sits above fair: the C89 gain lock has its own test
        fake, eng, favs = self.rig()
        exs = [e for e in favs[:4]]
        fs = {e: eng.fair[e] for e in exs}
        self.lot(eng, exs[0], f"dump:{exs[0]}:yes:1", 20_000, round(fs[exs[0]] - 0.10, 3))   # 20k x ~0.85 = 17k, ~12% edge
        self.lot(eng, exs[1], f"dump:{exs[1]}:yes:1", 30_000, round(fs[exs[1]] - 0.06, 3))   # ~27k, ~7%
        self.lot(eng, exs[2], f"dump:{exs[2]}:yes:1", 30_000, round(fs[exs[2]] - 0.04, 3))   # ~27k, ~4%: does not fit the cap
        self.lot(eng, exs[3], f"dump:{exs[3]}:yes:1", 1_000, round(fs[exs[3]] - 0.02, 3))    # 2c: not a candidate
        eng.core_t = 0
        eng.core_update(time.time())
        core = eng.controls.d["core"]
        keys = set(core["lots"])
        self.assertIn(f"{exs[0]}|dump:{exs[0]}:yes:1", keys); self.assertIn(f"{exs[1]}|dump:{exs[1]}:yes:1", keys)
        self.assertNotIn(f"{exs[2]}|dump:{exs[2]}:yes:1", keys, "over the 50k cap"); self.assertNotIn(f"{exs[3]}|dump:{exs[3]}:yes:1", keys, "< 3c edge")
        self.assertLessEqual(core["cost"], 50_000 + 1)
        self.assertEqual(eng.lot_policy(exs[0], "yes", f"dump:{exs[0]}:yes:1", 0.8, fs[exs[0]]), "core")
        self.assertEqual(eng.lot_policy(exs[2], "yes", f"dump:{exs[2]}:yes:1", round(fs[exs[2]] - 0.04, 3), fs[exs[2]]), "hold", "a trading lot >= 3c under fair is held")
        # a new lot with >= 3c more edge than the weakest core lot swaps it out (one swap per minute); the weakest exits at the touch
        self.lot(eng, exs[2], f"dump:{exs[2]}:yes:2", 10_000, round(fs[exs[2]] - 0.12, 3))
        eng.core_t = 0
        eng.core_update(time.time())
        core = eng.controls.d["core"]
        self.assertIn(f"{exs[2]}|dump:{exs[2]}:yes:2", core["lots"])
        self.assertNotIn(f"{exs[1]}|dump:{exs[1]}:yes:1", core["lots"], "the weakest (6c) swapped out for a 12c lot")
        self.assertIn(f"{exs[1]}|dump:{exs[1]}:yes:1", core["swap_out"])
        self.assertEqual(eng.lot_policy(exs[1], "yes", f"dump:{exs[1]}:yes:1", 0.8, fs[exs[1]]), "exit")
        self.assertTrue(any("SWAP" in e["text"] for e in eng.events))
        # within 1c of fair: leaves core
        FAIR[exs[0]] = round(eng.ledger.tranches[f"dump:{exs[0]}:yes:1"]["avg"] + 0.005, 3)
        self.addCleanup(FAIR.pop, exs[0], None)
        loop(eng, fake)
        eng.core_t = 0
        eng.core_update(time.time())
        self.assertNotIn(f"{exs[0]}|dump:{exs[0]}:yes:1", eng.controls.d["core"]["lots"], "within 1c of fair: out of core")
        # the rotation and the release never touch a core lot
        self.assertTrue(eng.core_set)
        for ex_, sub_ in eng.core_set:
            self.assertEqual(eng.lot_policy(ex_, "yes", sub_, 0.5, 0.9), "core")

    def test_hard_pauses_lift_themselves_and_pnl_stops_only_alert(self):
        fake, eng, favs = self.rig()
        loop(eng, fake)
        self.assertFalse(eng.controls.paused)
        eng.fair_t = time.time() - 200                       # Polymarket fair 200 s old
        eng.hard_pauses(time.time())
        self.assertTrue(eng.controls.paused); self.assertEqual(eng.controls.d["auto_pause"]["kind"], "pm_stale")
        eng.controls.d["auto_pause"]["t"] = time.time() - 301
        eng.fair_t = time.time()
        eng.auto_resume(time.time())
        self.assertFalse(eng.controls.paused, "lifts itself once the fair is fresh")
        for _ in range(ENGINE_ERRORS := a_config.ENGINE_ERRORS_10M):
            eng.event("ERR", "test error")
        eng.hard_pauses(time.time())
        self.assertEqual(eng.controls.d["auto_pause"]["kind"], "engine_errors")
        eng.controls.d["auto_pause"]["t"] = time.time() - 301
        eng.events = [e for e in eng.events if e["kind"] != "ERR"]
        eng.auto_resume(time.time())
        self.assertFalse(eng.controls.paused)
        eng.ex.trims["tm"] = eng.ex.trims.get("tm", 0) + 1   # an I2 trim
        eng.hard_pauses(time.time())
        self.assertEqual(eng.controls.d["auto_pause"]["kind"], "i2_breach")
        eng.controls.d["auto_pause"]["t"] = time.time() - 301
        eng.auto_resume(time.time())
        self.assertTrue(eng.controls.paused, "30 min without another trim first")
        eng.trim_t = time.time() - a_config.I2_RESUME_S - 1
        eng.auto_resume(time.time())
        self.assertFalse(eng.controls.paused)
        self.assertFalse(any(e["kind"] == "PAUSE" and "D5" in e["text"] for e in eng.events))

    def test_book_scan_and_recycle_depth_from_the_feed(self):
        fake, eng, favs = self.rig()
        class F:
            def __init__(self, b): self.b = b
            def book(self, ex, max_age_s=10.0): return self.b.get(str(ex))
        books = {}
        for b in eng.bots:
            if b.kind == "market" and eng.fair.get(b.ex) is not None:
                f = eng.fair[b.ex]
                books[b.ex] = ([(round(f - 0.03, 3), 1500.0), (round(f - 0.035, 3), 500.0), (0.985, 9000.0)], [(round(f - 0.02, 3), 800.0), (0.015, 3000.0)])
        eng.mkt_feed = F(books)
        eng.controls.d["feed_mode"] = "live"
        eng.book_scan_t = 0
        eng.book_scan(time.time())
        rows = eng.book_scan_rows
        self.assertGreaterEqual(len(rows), 100)
        self.assertTrue(all(abs(rows[i]["gap_c"]) >= abs(rows[i + 1]["gap_c"]) for i in range(len(rows) - 1)), "ranked by |gap|")
        self.assertEqual(rows[0]["bid_q3"], 2000.0)
        self.assertTrue(os.path.exists(os.path.join(TMP, "intel", "book_scan", "latest.json")))
        self.assertEqual(sum(1 for r in rows if r["cheap"] == "yes"), len(rows), "every bid sits 3c under fair: the YES side is cheap")
        now = time.time()
        with_hour = time.mktime(time.localtime(now)[:3] + (13, 0, 0, 0, 0, -1))
        eng.recycle_depth_snapshot(with_hour)
        day = time.strftime("%Y-%m-%d", time.localtime(with_hour))
        snap = json.load(open(os.path.join(TMP, "intel", "recycle_depth", day + ".json")))
        m = next(iter(snap["markets"].values()))
        self.assertEqual((m["yes_98"], m["yes_99"], m["no_98"], m["no_99"]), (9000.0, 0.0, 3000.0, 0.0))

    def test_scoreboard_edge_bought_given_back_and_books(self):
        fake, eng, favs = self.rig()
        loop(eng, fake)
        ex = favs[0]
        t = time.strftime("%Y-%m-%d %H:%M:%S")
        eng.ledger.orders["o1"] = dict(id="o1", ex=ex, side="yes", action="buy", layer="dump", sub=f"dump:{ex}:yes:1", bot="MKT x", price=0.8, qty=1000, status="done", placed=t, closed=t, ts=time.time(), filled=1000.0, left=0, cost=800.0, note="")
        eng.ledger.orders["o2"] = dict(id="o2", ex=ex, side="no", action="buy", layer="dumpexit", sub=f"dump:{ex}:yes:1", bot="MKT x", price=0.18, qty=400, status="done", placed=t, closed=t, ts=time.time(), filled=400.0, left=0, cost=72.0, note="")
        eng.ledger.fills.extend([dict(key="s1", order="o1", ex=ex, side="yes", qty=1000.0, price=0.80, action="buy", realized=0.0, t=t, layer="dump", before_engine=False, fair=0.90, edge=0.10, bot="MKT x", sig_mid=None, sig_edge=None, mo5=None, mo30=None),
                                 dict(key="s2", order="o2", ex=ex, side="no", qty=400.0, price=0.18, action="buy", realized=8.0, t=t, layer="dumpexit", before_engine=False, fair=0.10, edge=-0.08, bot="MKT x", sig_mid=None, sig_edge=None, mo5=None, mo30=None)])
        self.lot(eng, ex, f"dump:{ex}:yes:1", 600, 0.80)
        FAIR[ex] = 0.92; self.addCleanup(FAIR.pop, ex, None)
        loop(eng, fake)
        eng.scoreboard_t = 0
        eng.scoreboard_update(time.time())
        sb = eng.scoreboard
        c = sb["today"]["strategy"]["catcher"]
        self.assertAlmostEqual(c["bought"], 100.0, places=2, msg="edge bought = 0.10 x 1,000")
        self.assertAlmostEqual(c["given"], 32.0, places=2, msg="given back = (fair 0.90 - sale 0.82) x 400")
        self.assertAlmostEqual(c["realized"], 8.0, places=2)
        self.assertAlmostEqual(c["drift"], (0.92 - 0.90) * 600, places=1, msg="fair drift on the 600 still held")
        self.assertAlmostEqual(c["net"], 68.0, places=2)
        self.assertIn("trading", sb["today"]["book"])
        self.assertEqual(sb["target_day"], a_config.SCOREBOARD_TARGET_DAY)


class C89Tests(EngineCase):
    """C89 (Piero 16:55 Oct 9): free < 20k -> the trading book (never core) sells lowest return-left first at the touch, >= entry + 0.5c,
    until free >= 20k, overriding the 3c hold; a core lot within 3% return of fair at the touch is sold at the touch (gain lock);
    no new TM / catcher / SM buys while trading inventory at cost > 30k."""

    def rig(self):
        f_engine.CORE_CAP_COST, f_engine.TRADING_HOLD_C = 50_000.0, 0.03
        f_engine.CORE_LOCK_RET, f_engine.TRADING_INV_CAP, f_engine.TRADING_CASH_TARGET = 0.03, 30_000.0, 20_000.0
        return DumpV3Tests.rig(self)

    def lot(self, eng, ex, sub, qty, avg, side="yes"):
        eng.ledger.subbooks.setdefault(ex, {})[sub] = float(qty if side == "yes" else -qty)
        eng.ledger.tranches[sub] = dict(ex=ex, side=side, kind="dump", state="open", ts=time.time() - 60, t="x", avg=avg)

    def test_core_gain_lock_and_the_30k_inventory_stop(self):
        fake, eng, favs = self.rig()
        ex = favs[0]
        fs = eng.fair[ex]
        bid, ask = eng.px[ex][0], eng.px[ex][1]
        self.lot(eng, ex, f"dump:{ex}:yes:1", 2000, round(ask - 0.10, 3))          # bought 10c under the touch
        FAIR[ex] = round(ask * 1.02, 3); self.addCleanup(FAIR.pop, ex, None)          # fair 2% above the touch: within the 3% lock
        loop(eng, fake)
        eng.core_set = {(ex, f"dump:{ex}:yes:1")}
        eng.controls.d["core"] = dict(lots={f"{ex}|dump:{ex}:yes:1": dict(ex=ex, side="yes", sub=f"dump:{ex}:yes:1", qty=2000, avg=round(ask - 0.10, 3), cost=2000 * (ask - 0.10), edge_c=0.1)}, swap_out={})
        eng.core_t = 0
        eng.core_update(time.time())
        self.assertIn(f"{ex}|dump:{ex}:yes:1", eng.controls.d["core"]["swap_out"], "within 3% return at the touch: out of core")
        self.assertTrue(any("CORE LOCK" in e["text"] for e in eng.events))
        self.assertEqual(eng.lot_policy(ex, "yes", f"dump:{ex}:yes:1", round(ask - 0.10, 3), eng.fair[ex]), "exit")
        eng.book.setdefault(ex, {})["pos"] = 2000; fake.pos[ex] = 2000; fake.avg_cost[ex] = round(ask - 0.10, 3)
        eng.lots_cache = (None, {})
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 60_000, time.time())
        self.assertIn(ex, plan, "sold at the touch whatever the cash"); self.assertEqual(plan[ex]["why"], "core lock")
        # the 30k stop
        self.lot(eng, favs[1], f"dump:{favs[1]}:yes:1", 40_000, 0.9)
        self.assertTrue(eng.trading_inv_gate(time.time()))
        self.assertEqual(eng.tm_plan(time.time(), a_config.CASH_RESERVE + 60_000, eng.bucket_usage()), {}, "no TM entries")
        self.assertEqual(eng.dump_plan(time.time(), a_config.CASH_RESERVE + 60_000, eng.bucket_usage()), {}, "no catcher bids")
        self.assertTrue(any("TRADING INVENTORY" in e["text"] for e in eng.events))
        eng.ledger.subbooks[favs[1]].pop(f"dump:{favs[1]}:yes:1")
        self.assertFalse(eng.trading_inv_gate(time.time()))


class C90Tests(EngineCase):
    """docs/edge_program.md (C90): H1 GTC quotes on arm A (24-h TTL, queue kept within 1 tick and for 30 min at >= 2 ticks), control on
    the 20-min TTL with paper fills; the A/B controller promotes after 12 winning hours and rolls back after 6 losing ones; H4 pair
    locks on two-candidate races with idle cash, held to settlement; a release ask and a layer's own exit never both rest for one lot."""

    def test_arms_are_stable_halves_and_h1_quotes_rest_long_on_arm_a(self):
        fake, eng, ex = TMTests.rig(self)
        eng.controls.d["edge"] = {"H1": dict(state="ab", since="x", hist=[])}
        arms = {b.ex: eng.edge_arm("H1", b.ex) for b in eng.bots if b.kind == "market"}
        n_a = sum(1 for a in arms.values() if a == "A")
        self.assertTrue(0.35 * len(arms) <= n_a <= 0.65 * len(arms), f"half the universe by hash: {n_a} of {len(arms)}")
        self.assertEqual(arms, {b.ex: eng.edge_arm("H1", b.ex) for b in eng.bots if b.kind == "market"}, "stable")
        eng.controls.d["edge"]["H1"]["state"] = "full"
        self.assertTrue(all(eng.edge_arm("H1", e) == "A" for e in arms))
        eng.controls.d["edge"]["H1"]["state"] = "off"
        self.assertTrue(all(eng.edge_arm("H1", e) == "B" for e in arms))
        # arm A: a 24-h TTL on the TM bid; control: 20 min (the rig's old 20-min bid is pulled first: no edge, then the book comes back)
        eng.controls.d["edge"]["H1"]["state"] = "full"
        fake.px[ex] = eng.px[ex] = (0.485, 0.49, 0.4875)
        loop(eng, fake); loop(eng, fake)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)
        loop(eng, fake); loop(eng, fake)
        q = TMTests.tm_orders(self, eng, fake, ex)
        self.assertEqual(len(q), 1, q)
        self.assertEqual(eng.ledger.orders[q[0]["id"]].get("ttl"), a_config.H1_TTL_S)
        # the touch moves 2 ticks up: arm A keeps its place for 30 min, then re-pegs
        fake.px[ex] = eng.px[ex] = (0.475, 0.48, 0.4775)
        loop(eng, fake); loop(eng, fake)
        q2 = TMTests.tm_orders(self, eng, fake, ex)
        self.assertEqual(q2[0]["id"], q[0]["id"], "kept: 2 ticks behind for < 30 min")
        eng.tm_behind[(ex, "yes")] = time.time() - a_config.H1_BEHIND_S - 1
        loop(eng, fake); loop(eng, fake)
        q3 = TMTests.tm_orders(self, eng, fake, ex)
        self.assertNotEqual(q3[0]["id"], q[0]["id"], "re-pegged after 30 min")
        self.assertAlmostEqual(q3[0]["price"], 0.475, places=3, msg="joins the new touch (a 1-tick spread: no improving)")

    def test_paper_fill_from_the_tape_and_the_controller_verdicts(self):
        fake, eng, ex = TMTests.rig(self)
        eng.controls.d["edge"] = {"H1": dict(state="ab", since="x", hist=[])}
        ex_b = next(b.ex for b in eng.bots if b.kind == "market" and eng.edge_arm("H1", b.ex) == "B")
        now = time.time()
        eng.ledger.orders["p1"] = dict(id="p1", ex=ex_b, side="yes", action="buy", layer="tm", sub=f"tm:{ex_b}:yes", bot="MKT x", price=0.40, qty=500, filled=0.0,
                                      status="done", placed="x", closed="x", ts=now - 1300, ttl=1200, left=0, cost=0.0, note="")
        eng.paper_scan(now)
        self.assertIn("p1", eng.paper, "an expired, unfilled control-arm quote becomes a paper order")
        po = eng.paper["p1"]
        eng.note_print(ex_b, "a", now + 10, 0.40, 600, None)     # at our price: half counts (300)
        eng.paper_scan(now + 20)
        self.assertIsNone(po["fill_t"]); self.assertAlmostEqual(po["cum"], 300.0)
        eng.note_print(ex_b, "b", now + 30, 0.395, 300, None)    # under our price: in full -> 600 >= 500: filled
        eng.paper_scan(now + 40)
        self.assertIsNotNone(po["fill_t"])
        self.assertTrue(os.path.exists(os.path.join(TMP, "intel", "paper", f"H1-{time.strftime('%Y-%m-%d', time.localtime(now))}.jsonl")))
        st = eng.h1_arm_stats(now + 40)
        self.assertEqual(st["B"]["paper_fills"], 1)
        # controller: 12 winning hours promote, 6 losing hours roll back
        eng.edge_hour = None
        h1 = eng.controls.d["edge"]["H1"]
        h1["hist"] = [dict(h=str(i), v="win", A={}, B={}) for i in range(12)]
        eng.h1_arm_stats = lambda now_, hours=12: {"A": dict(rest_h=10, fills=30, shares=1, per_h=3.0, mo30=0.001, n_mo=30, tm_edge=0.05, paper_fills=0),
                                                   "B": dict(rest_h=10, fills=5, shares=1, per_h=0.5, mo30=0.0, n_mo=5, tm_edge=0.05, paper_fills=2)}
        eng.edge_t = 0
        eng.edge_controller(now)
        self.assertEqual(eng.controls.d["edge"]["H1"]["state"], "full", "promoted after 12 winning hours")
        self.assertTrue(any("H1 PROMOTED" in e["text"] for e in eng.events))
        h1 = eng.controls.d["edge"]["H1"]; h1["state"] = "ab"; h1["hist"] = [dict(h=str(i), v="lose", A={}, B={}) for i in range(6)]
        eng.h1_arm_stats = lambda now_, hours=12: {"A": dict(rest_h=10, fills=4, shares=1, per_h=0.4, mo30=-0.01, n_mo=4, tm_edge=0.05, paper_fills=0),
                                                   "B": dict(rest_h=10, fills=8, shares=1, per_h=0.8, mo30=0.0, n_mo=8, tm_edge=0.05, paper_fills=2)}
        eng.edge_hour = None; eng.edge_t = 0
        eng.edge_controller(now + 3600)
        self.assertEqual(eng.controls.d["edge"]["H1"]["state"], "off", "rolled back after 6 losing hours")

    def test_h4_two_candidate_races_and_the_idle_cash_budget(self):
        fresh_data(layers=dict(position=dict(state="full"), pairs=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        two = [pb for pb in eng.pairs if eng.h4_race_ok(pb)]
        self.assertTrue(two, "two-candidate races exist in the fixture")
        for pb in two:
            self.assertEqual(len(pb.legs), 2)
            self.assertEqual({(eng.meta.get(e) or {}).get("party") for e in pb.legs}, {"D", "R"})
        eng.cash_hist = [a_config.CASH_RESERVE + 25_000]
        eng.dump_cut_rows, eng.tm_unfunded = [], []
        self.assertAlmostEqual(eng.h4_budget(time.time()), min(a_config.H4["cap"], 25_000 - a_config.H4["idle_free"]), delta=1)
        eng.dump_cut_rows = [(0.07, 1000.0, 0.9)]
        self.assertEqual(eng.h4_budget(time.time()), 0.0, "an entry with >= 6% edge waits: no lock")
        eng.dump_cut_rows = []
        eng.cash_hist = [a_config.CASH_RESERVE + 9_000]
        self.assertEqual(eng.h4_budget(time.time()), 0.0, "free under the catcher's 10k reserve: no lock")
        eng.ledger.baskets.append(dict(id="h4x", t="x", ts=time.time(), race=two[0].race, side="no", qty=1000, margin=0.04, legs=2, orders=[], repaired=False, state="held", h4=True))
        eng.cash_hist = [a_config.CASH_RESERVE + 25_000]
        self.assertAlmostEqual(eng.h4_budget(time.time()), a_config.H4["cap"] - 1000 * 1.96, delta=1)

    def test_dedup_vs_sig_drops_only_the_over_booked_synthetic_fills(self):
        fake, eng, favs = DumpV3Tests.rig(self)
        ex = favs[0]; t = time.strftime("%Y-%m-%d %H:%M:%S")
        base = dict(ex=ex, side="yes", action="buy", layer="dump", bot="MKT x", before_engine=False, fair=0.9, edge=0.05, sig_mid=None, sig_edge=None, mo5=None, mo30=None, realized=0.0, price=0.85, t=t)
        eng.ledger.orders["d1"] = dict(id="d1", ex=ex, side="yes", action="buy", layer="dump", sub=f"dump:{ex}:yes:1", bot="MKT x", price=0.85, qty=1000, filled=1000.0, status="done", placed=t, closed=t, ts=time.time(), left=0, cost=850.0, note="")
        eng.ledger.orders["d2"] = dict(id="d2", ex=ex, side="yes", action="buy", layer="dump", sub=f"dump:{ex}:yes:2", bot="MKT x", price=0.85, qty=1000, filled=1000.0, status="done", placed=t, closed=t, ts=time.time(), left=0, cost=850.0, note="")
        eng.ledger.fills.extend([dict(base, key="s1", order="d1", qty=500.0), dict(base, key="s2", order="d1", qty=500.0, source="order_shrink"),   # SIG filled 500: the synthetic 500 is a duplicate
                                 dict(base, key="s3", order="d2", qty=500.0), dict(base, key="s4", order="d2", qty=500.0, source="order_shrink")])  # SIG filled 1,000: both genuine
        truth = {"d1": 500.0, "d2": 1000.0}
        r = eng.ledger.dedup_vs_truth(truth, dry_run=True)
        self.assertEqual((r["removed"], r["shares"], r["orders"]), (["s2"], 500.0, 1))
        r = eng.ledger.dedup_vs_truth(truth)
        self.assertEqual(r["removed"], ["s2"])
        self.assertEqual({f["key"] for f in eng.ledger.fills if f.get("order") in ("d1", "d2")}, {"s1", "s3", "s4"})
        self.assertEqual(eng.ledger.orders["d1"]["filled"], 500.0)

    def test_h1_morning_verdict_fires_only_in_the_hour_after_the_window(self):
        fake, eng, ex = TMTests.rig(self)
        eng.controls.d["edge"] = {"H1": dict(state="ab", since="x", hist=[]), "h4_paused": True}
        lt = time.localtime()
        at = lambda hh, mm: time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hh, mm, 0, 0, 0, -1))
        eng.h1_morning_verdict(at(18, 4))
        self.assertIsNone(eng.controls.d["edge"]["H1"].get("judged"), "18:04: not the verdict hour (the C92 defect)")
        self.assertTrue(eng.controls.d["edge"]["h4_paused"])
        eng.h1_morning_verdict(at(8, 10))
        self.assertEqual(eng.controls.d["edge"]["H1"].get("judged"), time.strftime("%Y-%m-%d"))
        self.assertFalse(eng.controls.d["edge"]["h4_paused"], "H4 resumes with the verdict")
        self.assertTrue(any("H1 MORNING VERDICT" in e["text"] for e in eng.events))

    def test_release_ask_and_own_exit_never_both_rest(self):
        f_engine.TRADING_HOLD_C = 0.03
        fake, eng, favs = DumpV3Tests.rig(self)
        ex = favs[0]
        fs = eng.fair[ex]
        sub = f"dump:{ex}:yes:1"
        eng.ledger.subbooks.setdefault(ex, {})[sub] = 2000.0
        eng.ledger.tranches[sub] = dict(ex=ex, side="yes", kind="dump", state="open", ts=time.time() - 60, t="x", avg=round(fs - 0.02, 3))
        eng.open.append(dict(id="r1", ex=ex, side="no", action="buy", price=round(1 - (fs - 0.01), 3), qty=2000, layer="release"))
        eng.ledger.orders["r1"] = dict(id="r1", ex=ex, side="no", action="buy", layer="release", sub=sub, bot="MKT x", price=0.1, qty=2000, status="live", placed="x", ts=time.time())
        self.assertTrue(eng.release_resting(ex, sub))
        eng.track_dumps(time.time())
        self.assertNotIn(sub, {x["sub"] for x in eng.dump_view.get(ex, [])}, "the catcher's own exit stands down while the release ask rests")
        eng.open.pop(); eng.ledger.orders.pop("r1")
        eng.track_dumps(time.time())
        self.assertIn(sub, {x["sub"] for x in eng.dump_view.get(ex, [])})


class C93Tests(EngineCase):
    """Piero 15:30 Oct 12 (C93): no seller touches a lot with >= 6% return left; a no-edge lot (< 6% return at its sale price, or a
    fair < 0.85 side with < 3c of edge) gets one GTC ask at max(lot average + 0.5c, touch ask); core never; a re-peg waits 10 min; the
    H1 test's sleeve, window, stop and 03:15 shortfall sale."""

    def rig(self):
        f_engine.CORE_CAP_COST, f_engine.TRADING_HOLD_C = 50_000.0, 0.03
        f_engine.NOEDGE, f_engine.REPEG_AWAY_S, f_engine.GTC_TTL_S = dict(a_config.NOEDGE), a_config.REPEG_AWAY_S, a_config.GTC_TTL_S
        f_engine.SM_REPEG_TICKS, f_engine.TM_REPEG_TICKS = a_config.SM_REPEG_TICKS, a_config.TM_REPEG_TICKS
        return DumpV3Tests.rig(self)

    def lot(self, eng, fake, ex, sub, qty, avg, px3, fair, side="yes"):
        fake.px[ex] = eng.px[ex] = px3
        FAIR[ex] = fair; self.addCleanup(FAIR.pop, ex, None)
        fake.pos[ex] = qty; fake.avg_cost[ex] = avg
        eng.ledger.subbooks[ex] = {sub: float(qty)}
        eng.ledger.tranches[sub] = dict(ex=ex, side=side, kind="dump", state="open", ts=time.time() - 60, t="x", avg=avg)

    def test_hold_above_6pct_release_no_edge_lots_gtc_at_max_avg_touch(self):
        fake, eng, favs = self.rig()
        a, b, c, d = favs[:4]
        self.lot(eng, fake, a, f"dump:{a}:yes:1", 2000, 0.85, (0.90, 0.905, 0.9025), 0.97)   # 7.2% left at the touch 0.905: HELD
        self.lot(eng, fake, b, f"dump:{b}:yes:1", 2000, 0.85, (0.90, 0.905, 0.9025), 0.94)   # 3.9% left: released at the touch 0.905
        self.lot(eng, fake, c, f"dump:{c}:yes:1", 2000, 0.92, (0.90, 0.905, 0.9025), 0.94)   # lot average 0.92 > touch: ask at 0.925 (avg + 0.5c), 1.6% left
        self.lot(eng, fake, d, f"dump:{d}:yes:1", 2000, 0.40, (0.78, 0.79, 0.785), 0.81)     # fair 0.81 < 0.85 and 2c of edge left: released (even at 2.5% return)
        loop(eng, fake); loop(eng, fake)
        eng.lots_cache = (None, {})
        for e_, sub_ in ((a, f"dump:{a}:yes:1"), (b, f"dump:{b}:yes:1"), (c, f"dump:{c}:yes:1"), (d, f"dump:{d}:yes:1")):
            eng.ledger.subbooks[e_] = {sub_: 2000.0}
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertNotIn(a, plan, "7% left: held whatever the cash")
        self.assertIn(b, plan); self.assertAlmostEqual(plan[b]["px"], 0.905, places=3, msg="at the touch ask")
        self.assertIn(c, plan); self.assertAlmostEqual(plan[c]["px"], 0.925, places=3, msg="max(lot average + 0.5c, touch)")
        self.assertIn(d, plan); self.assertEqual(plan[d]["why"], "no-edge")
        eng.core_set = {(b, f"dump:{b}:yes:1")}
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertNotIn(b, plan, "core: never")
        self.assertTrue(any(e["text"].startswith("POSTED NO-EDGE ask") for e in eng.events))
        self.assertFalse(any("TRADING SALE" in e["text"] or "H3 SWAP" in e["text"] for e in eng.events))

    def test_release_asks_are_gtc_and_a_repeg_waits_ten_minutes(self):
        fake, eng, favs = self.rig()
        eng.controls.d["layers"].setdefault("release", dict(state="off", caps={}, killed=None))   # the release asks are reducing orders: they pass with the layer off, as live
        b = favs[0]
        self.lot(eng, fake, b, f"dump:{b}:yes:1", 2000, 0.85, (0.90, 0.905, 0.9025), 0.94)
        loop(eng, fake); loop(eng, fake)
        eng.ledger.subbooks[b] = {f"dump:{b}:yes:1": 2000.0}; eng.lots_cache = (None, {})
        loop(eng, fake); loop(eng, fake)
        rel = [o for o in fake.open_orders() if o["ex"] == b and (eng.ledger.orders.get(o["id"]) or {}).get("layer") == "release"]
        self.assertEqual(len(rel), 1, rel)
        self.assertEqual(eng.ledger.orders[rel[0]["id"]].get("ttl"), a_config.GTC_TTL_S, "GTC")
        oid = rel[0]["id"]
        fake.px[b] = eng.px[b] = (0.91, 0.915, 0.9125)        # the touch moves 2 ticks up
        loop(eng, fake); loop(eng, fake)
        rel2 = [o for o in fake.open_orders() if o["ex"] == b and (eng.ledger.orders.get(o["id"]) or {}).get("layer") == "release"]
        self.assertEqual([o["id"] for o in rel2], [oid], "kept: the re-peg waits")
        eng.drift_since[oid] = time.time() - a_config.REPEG_AWAY_S - 1
        loop(eng, fake); loop(eng, fake)
        rel3 = [o for o in fake.open_orders() if o["ex"] == b and (eng.ledger.orders.get(o["id"]) or {}).get("layer") == "release"]
        self.assertEqual(len(rel3), 1); self.assertNotEqual(rel3[0]["id"], oid, "re-pegged after 10 min")
        self.assertAlmostEqual(1 - rel3[0]["price"], 0.915, places=3)

    def test_h1_test_window_sleeve_stop_and_shortfall(self):
        fake, eng, favs = self.rig()
        day = a_config.H1_TEST["day"]
        at = lambda hh, mm: time.mktime(time.strptime(f"{day} {hh:02d}:{mm:02d}:00", "%Y-%m-%d %H:%M:%S"))
        self.assertFalse(eng.h1_test_active(at(3, 50))); self.assertTrue(eng.h1_test_active(at(9, 0))); self.assertFalse(eng.h1_test_active(at(18, 0)))
        self.assertEqual(eng.h1_sleeve_reserve(at(3, 30)), 0.0)
        self.assertAlmostEqual(eng.h1_sleeve_reserve(at(3, 50)), a_config.H1_TEST["sleeve"], places=1)
        self.assertEqual(eng.h1_sleeve_reserve(at(18, 1)), 0.0)
        eng.h1_test_pnl_fair = lambda now_: -160.0
        eng.h1_test_check(at(9, 0))
        self.assertTrue(eng.h1_test_stopped); self.assertFalse(eng.h1_test_active(at(9, 1)))
        self.assertTrue(any("H1 TEST STOPPED" in e["text"] for e in eng.events))
        # the 03:15 shortfall: lowest return first, at the bid, >= avg + 0.5c, capped by the edge given up
        eng.h1_test_stopped = False
        a, b = favs[:2]
        self.lot(eng, fake, a, f"dump:{a}:yes:1", 3000, 0.80, (0.90, 0.905, 0.9025), 0.93)   # 3.3% left: first
        self.lot(eng, fake, b, f"dump:{b}:yes:1", 3000, 0.80, (0.90, 0.905, 0.9025), 0.99)   # 10% left: later
        loop(eng, fake); loop(eng, fake)
        eng.ledger.subbooks[a] = {f"dump:{a}:yes:1": 3000.0}; eng.ledger.subbooks[b] = {f"dump:{b}:yes:1": 3000.0}; eng.lots_cache = (None, {})
        eng.cash_hist = [a_config.CASH_RESERVE + 1_000]
        eng.h1_shortfall_done = False                        # the loops above run on the real clock: on the test day between 03:15 and 03:45 they consume the one-shot
        plan = eng.h1_shortfall_plan(at(3, 16))
        self.assertIn(a, plan); self.assertGreaterEqual(plan[a], 10)
        self.assertLessEqual(sum(plan.values()) * 0.90, a_config.H1_TEST["sleeve"] + 1)
        self.assertTrue(eng.h1_shortfall_done)
        self.assertEqual(eng.h1_shortfall_plan(at(3, 20)), {}, "once")
        # C93e: two sub-books on one market share the market's shortfall (Oct 13 03:15: 4,891 Oklahoma NO sold for a 2,505 shortfall)
        eng.ledger.subbooks[a] = {f"dump:{a}:yes:1": 3000.0, f"dump:{a}:yes:2": 3000.0}; fake.pos[a] = 6000; eng.lots_cache = (None, {})
        eng.ledger.tranches[f"dump:{a}:yes:2"] = dict(eng.ledger.tranches[f"dump:{a}:yes:1"])
        loop(eng, fake); loop(eng, fake)
        eng.h1_shortfall_done = False; eng.ledger.subbooks[a] = {f"dump:{a}:yes:1": 3000.0, f"dump:{a}:yes:2": 3000.0}; eng.lots_cache = (None, {})
        eng.h1_shortfall_plan = lambda now_, _p=eng.h1_shortfall_plan: {a: 2500}
        plan2 = eng.rotation_plan(a_config.CASH_RESERVE + 1_000, at(3, 16))
        self.assertIn(a, plan2, ([e["text"][:120] for e in eng.events[-8:]], eng.ledger.subbooks[a], eng.book[a]["pos"], [(eng.ledger.orders.get(o["id"], {}).get("layer"), o["side"], o["price"], o["qty"]) for o in eng.open if o["ex"] == a]))
        self.assertLessEqual(sum(plan2[a]["subs"].values()), 2500, plan2[a])

    def test_release_ask_uses_sig_average_when_the_lot_is_the_whole_position(self):
        """C93b (Piero 16:15 item 4): U.S. House D / CO-03 D / Kansas D asked at a tranche average that mixed both sides' entries."""
        fake, eng, favs = self.rig()
        a = favs[0]
        self.lot(eng, fake, a, f"sm:{a}", 492, 0.545, (0.315, 0.32, 0.3175), 0.275)   # the tranche says 0.545; SIG's position average is 0.33
        fake.avg_cost[a] = 0.33
        loop(eng, fake); loop(eng, fake)
        eng.lots_cache = (None, {}); eng.ledger.subbooks[a] = {f"sm:{a}": 492.0}
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertIn(a, plan); self.assertAlmostEqual(plan[a]["px"], 0.335, places=3, msg="max(SIG avg 0.33 + 0.5c, touch 0.32)")
        eng.ledger.subbooks[a] = {f"sm:{a}": 200.0, "inventory": 292.0}     # the lot is less than the position: the tranche average stands
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertIn(a, plan); self.assertAlmostEqual(plan[a]["px"], 0.55, places=3)

    def test_own_exit_guard_counts_an_exit_being_repegged_or_planned(self):
        """C93b (item 3): the 15:59 I2 trim — the release ask was posted while the TM exit for the same lot was in 'cancelling'."""
        fake, eng, favs = self.rig()
        a = favs[0]; sub = f"tm:{a}:yes"
        eng.open.append(dict(id="z1", ex=a, side="no", action="buy", price=0.04, qty=201))
        eng.ledger.orders["z1"] = dict(layer="tmexit", sub=sub, ex=a, side="no", action="buy", price=0.04, qty=201)
        self.assertTrue(eng.own_exit_resting(a, sub))
        eng.ex.cancelling.add("z1")
        self.assertTrue(eng.own_exit_resting(a, sub), "being re-pegged: still covers the lot")
        eng.open[:] = [o for o in eng.open if o["id"] != "z1"]
        self.assertFalse(eng.own_exit_resting(a, sub))
        eng.tm_view = {a: [dict(sub=sub, side="yes", px=0.96)]}
        self.assertTrue(eng.own_exit_resting(a, sub), "planned this loop: covers the lot")

    def test_h1_sleeve_is_touch_maker_quotes_only_inside_the_window(self):
        """C93b (item 1): under the 10k pause the TM quotes only the test markets, ~600 a clip, within the sleeve; outside the window nothing."""
        f_engine.TM_BUFFER_GATE = True
        fake, eng, ex = TMTests.rig(self, cash_above_floor=1_000)   # free 1k < 10k: TM entries paused
        tm = lambda: [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "tm"]
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(tm(), [], "paused under 10k")
        eng.h1_test_markets = lambda: [ex]
        eng.h1_test_active = lambda now_: True
        loop(eng, fake); loop(eng, fake)
        q = tm()
        self.assertEqual(len(q), 1, q)
        self.assertEqual(q[0]["side"], "yes"); self.assertEqual(q[0]["qty"], a_config.H1_TEST["clip"])
        self.assertAlmostEqual(q[0]["price"], 0.465, places=3, msg="the side below fair, at the touch (improved by a tick on a 2-tick spread)")
        self.assertLessEqual(eng.h1_cost(), a_config.H1_TEST["sleeve"])
        self.assertAlmostEqual(eng.h1_cost(), q[0]["qty"] * q[0]["price"], places=2)
        sm = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "sm"]
        self.assertEqual(sm, [], "the spread maker stays exits-only")
        # C93c item 1: the ring stops are lifted inside the sleeve only
        f_engine.RING_WINDOWS = [("00:00", "23:59")]; f_engine.TM_ENTRY_STOPS = [("00:00", "23:59")]
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(len(tm()), 1, "ring window: the sleeve keeps quoting")
        # C93c item 2: never at or through our own ask (a release ask YES at 0.46 = a resting buy NO at 0.54): a tick below is behind the touch -> skipped, logged
        eng.open.append(dict(id="z9", ex=ex, side="no", action="buy", price=0.54, qty=300, status="open", filled=0))
        eng.ledger.orders["z9"] = dict(layer="release", sub=f"sm:{ex}", ex=ex, side="no", action="buy", price=0.54, qty=300, status="open", bot=f"MKT {ex}", filled=0)
        n0 = len(eng.events)
        plan = eng.tm_plan(time.time(), a_config.CASH_RESERVE + 1_000, 0.0)
        self.assertEqual(plan.get(ex, []), [], "a sleeve bid at 0.465 would sit through our own ask at 0.46: skipped")
        self.assertTrue(any(e["text"].startswith("H1 SKIP") for e in eng.events[n0:]), [e["text"] for e in eng.events[n0:]])
        eng.open[:] = [o for o in eng.open if o["id"] != "z9"]; eng.ledger.orders.pop("z9", None)
        eng.h1_test_active = lambda now_: False
        f_engine.RING_WINDOWS = []; f_engine.TM_ENTRY_STOPS = []
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(tm(), [], "outside the window: the pause holds")

class C94Tests(EngineCase):
    """Piero 20:30 Oct 13 (C94): the coin-flip rotation — named favourites sold as GTC asks at max(lot avg + 0.5c, touch) whatever
    their return left (the market's target shared by its sub-books); bids at min(touch, cap) only while the gap to PM fair >= 4c and
    only from the cash those sales freed; a target closed when PM moves >= 2c against it; exits at max(entry + 0.5c, fair - 1.5c);
    rotation lots exempt from the no-edge release above 2% left."""

    def rig(self):
        fake, eng, favs = C93Tests.rig(self)
        return fake, eng, favs

    def test_rotation_sale_sells_the_named_favourite_across_its_subs_up_to_the_target(self):
        fake, eng, favs = self.rig()
        a = favs[0]
        C93Tests.lot(self, eng, fake, a, f"dump:{a}:yes:1", 3000, 0.80, (0.90, 0.905, 0.9025), 0.99)   # 9% left: HELD by C93
        loop(eng, fake); loop(eng, fake)
        eng.ledger.subbooks[a] = {f"dump:{a}:yes:1": 3000.0, f"dump:{a}:yes:2": 3000.0}; eng.ledger.tranches[f"dump:{a}:yes:2"] = dict(eng.ledger.tranches[f"dump:{a}:yes:1"])
        fake.pos[a] = 6000; eng.lots_cache = (None, {})
        self.assertNotIn(a, eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time()), "a favourite is held")
        f_engine.ROTATION = dict(a_config.ROTATION, on=True, sells={a: 4000}, buys=[])
        eng.controls.d["rotation"] = dict(since="x", sell_left={a: 4000}, cash_freed=0.0, sold=0.0, skipped={})
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertIn(a, plan); self.assertEqual(plan[a]["why"], "rotation sale")
        self.assertAlmostEqual(plan[a]["px"], 0.905, places=3, msg="max(lot avg + 0.5c = 0.805, touch ask 0.905)")
        self.assertEqual(sum(plan[a]["subs"].values()), 4000, "the market's target, shared by its two sub-books")
        eng.core_set = {(a, f"dump:{a}:yes:1"), (a, f"dump:{a}:yes:2")}
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertIn(a, plan); self.assertEqual(sum(plan[a]["subs"].values()), 4000, "C94b: a core lot Piero named for the rotation is sold too")
        eng.core_set = set()
        # a fill shrinks the target and feeds the pool
        tag = eng.rot_sale_fill(dict(ex=a, qty=1500.0, price=0.095, side="no", action="buy", layer="release"), 0.905)
        self.assertEqual(tag, "rotation "); self.assertEqual(eng.controls.d["rotation"]["sell_left"][a], 2500)
        self.assertAlmostEqual(eng.controls.d["rotation"]["cash_freed"], 1500 * 0.905, places=1)
        self.assertAlmostEqual(eng.rot_reserve(), 1500 * 0.905, places=1, msg="freed cash is reserved for the rotation's bids")

    def test_rotation_bids_only_with_gap_and_pool_and_close_on_a_pm_move(self):
        fake, eng, favs = self.rig()
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.52; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465)
        f_engine.ROTATION = dict(a_config.ROTATION, on=True, sells={}, buys=[dict(key="t", name="T", legs=[(ex, "yes", 0.47)], cap=4_000.0)])
        eng.controls.d["layers"].setdefault("rot", dict(state="off", caps={}, killed=None))["state"] = "full"
        eng.controls.d["rotation"] = dict(since="x", sell_left={}, cash_freed=0.0, sold=0.0, skipped={})
        loop(eng, fake)
        self.assertEqual(eng.rot_plan(time.time(), a_config.CASH_RESERVE + 5_000), {}, "no cash freed yet: no bid")
        eng.controls.d["rotation"]["cash_freed"] = 1_000.0
        plan = eng.rot_plan(time.time(), a_config.CASH_RESERVE + 5_000)
        self.assertIn(ex, plan); w = plan[ex][0]
        self.assertAlmostEqual(w["px"], 0.46, places=3, msg="at the touch (<= the 0.47 cap)"); self.assertEqual(w["qty"], int(1_000 / 0.46), "sized to the pool")
        self.assertTrue(any(e["text"].startswith("ROT POSTED") for e in eng.events))
        fake.px[ex] = eng.px[ex] = (0.49, 0.50, 0.495)        # the touch is above the cap: the bid sits at the cap 0.47 (gap 5c)
        plan = eng.rot_plan(time.time(), a_config.CASH_RESERVE + 5_000)
        self.assertAlmostEqual(plan[ex][0]["px"], 0.47, places=3, msg="never above the cap")
        FAIR[ex] = 0.505; loop(eng, fake)                      # fair down 1.5c (under the 2c skip): the gap at the cap is 3.5c -> no bid, not a skip
        self.assertEqual(eng.rot_plan(time.time(), a_config.CASH_RESERVE + 5_000), {})
        self.assertNotIn("t", eng.controls.d["rotation"]["skipped"])
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465); FAIR[ex] = 0.495   # PM fair moved 2.5c against us after posting: closed
        loop(eng, fake)
        self.assertEqual(eng.rot_plan(time.time(), a_config.CASH_RESERVE + 5_000), {})
        self.assertIn("t", eng.controls.d["rotation"]["skipped"]); self.assertTrue(any(e["text"].startswith("ROT SKIP") for e in eng.events))

    def test_rotation_exit_at_max_entry_half_cent_fair_less_1_5c_and_release_exemption(self):
        fake, eng, favs = self.rig()
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and 0.3 <= eng.fair[b.ex] <= 0.7)
        FAIR[ex] = 0.52; self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = eng.px[ex] = (0.46, 0.47, 0.465); fake.pos[ex] = 2000; fake.avg_cost[ex] = 0.46
        f_engine.ROTATION = dict(a_config.ROTATION, on=True, sells={}, buys=[dict(key="t", name="T", legs=[(ex, "yes", 0.47)], cap=4_000.0)])
        eng.controls.d["rotation"] = dict(since="x", sell_left={}, cash_freed=0.0, sold=0.0, skipped={})
        loop(eng, fake)
        eng.ledger.subbooks[ex] = {f"rot:{ex}:yes": 2000.0}; eng.ledger.tranches[f"rot:{ex}:yes"] = dict(ex=ex, side="yes", kind="rot", state="open", ts=time.time(), t="x", avg=0.46, qty=2000.0)
        eng.lots_cache = (None, {})
        eng.track_rot(time.time())
        v = eng.rot_view.get(ex, [])
        self.assertEqual(len(v), 1); self.assertAlmostEqual(v[0]["px"], 0.505, places=3, msg="max(0.465, fair 0.52 - 1.5c)")
        self.assertNotIn(ex, eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time()), "a rotation lot with 10% left is not released")
        FAIR[ex] = 0.475; loop(eng, fake); eng.lots_cache = (None, {}); eng.ledger.subbooks[ex] = {f"rot:{ex}:yes": 2000.0}
        plan = eng.rotation_plan(a_config.CASH_RESERVE + 2_000, time.time())
        self.assertIn(ex, plan, "under 2% left (fair 0.475 vs the 0.47 ask): the no-edge release takes it")

class Dash5Tests(EngineCase):
    """Dashboard v5 (docs/dashboard_v5.md): §0 P&L by SIG day (00:00 UTC; realized at SIG's average cost; realized + open = the day
    exactly; past days fixed), the strategy registry and the cash-flow buckets that add up to account - 100,000, the status turning
    amber on rate-limit errors, and the Markets rows (every contract, holdings = positions, a reason wherever we have no order)."""

    def test_replay_realized_cash_and_netting_like_sig(self):
        import q_dash5
        def f(i, side, q, p, t):
            return dict(id=str(i), orderId=f"o{i}", exchangeId="1", side=side, quantity=q if side == "yes" else -q, price=p, filledAt=t, _ts=q_dash5.iso_ts(t))
        rows = [f(1, "yes", 1000, 0.40, "2026-10-02T10:00:00Z"), f(2, "no", 400, 0.55, "2026-10-02T11:00:00Z"), f(3, "no", 900, 0.50, "2026-10-03T01:00:00Z")]
        evs, cash_ev, info = q_dash5.events(rows, [], {"o1": "catcher", "o2": "catcher", "o3": "tm"})
        r = q_dash5.replay(evs, cash_ev)
        self.assertAlmostEqual(r["realized_day"]["2026-10-02"], 400 * ((1 - 0.55) - 0.40), places=6, msg="a NO buy against YES sells YES at 1 - p vs the average")
        self.assertAlmostEqual(r["realized_day"]["2026-10-03"], 600 * ((1 - 0.50) - 0.40), places=6)
        self.assertAlmostEqual(r["pos"]["1"], -300.0, msg="flipped: 300 NO open")
        self.assertAlmostEqual(r["avg"]["1"], 0.50)
        cash = -400 + 0.45 * 400 + 0.50 * 600 - 0.50 * 300
        self.assertAlmostEqual(r["cash"], cash, places=6)
        self.assertAlmostEqual(sum(r["cash_b"].values()), r["cash"], places=6, msg="every fill's cash flow belongs to exactly one bucket")
        # P&L at a NO mark of 0.52: cash + value = realized + open, by construction
        value = 300 * 0.52
        realized = sum(r["realized_day"].values())
        self.assertAlmostEqual(r["cash"] + value, realized + 300 * (0.52 - 0.50), places=6)

    def test_transactions_give_the_direction_and_the_fills_the_strategy(self):
        """The fills feed has no BUY / SELL: the transactions (one per order event, at its average price) do. A SELL of YES at p is
        booked like a NO buy at 1 - p; a transaction with no fills on file goes to 'held' at its price; fills after the last
        transaction take SELL from the engine's own order records; the opening balance is not a cash event."""
        import q_dash5
        T = q_dash5.iso_ts
        def f(i, oid, side, q, p, t):
            return dict(id=str(i), orderId=oid, exchangeId="1", side=side, quantity=q if side == "yes" else -q, price=p, filledAt=t, _ts=T(t))
        def tx(i, typ, q, p, t, ex="1"):
            return dict(event_id=f"e{i}", event_type="trade", orderType=typ, exchangeId=ex, quantity=q, price=p, createdAt=t, _ts=T(t))
        fills = [f(1, "a", "yes", 600, 0.40, "2026-10-02T10:00:00.100Z"), f(2, "a", "yes", 400, 0.45, "2026-10-02T10:00:00.100Z"),
                 f(3, "b", "yes", 500, 0.60, "2026-10-02T12:00:00.000Z"),                       # a SELL of 500 YES at 0.60
                 f(4, "c", "yes", 200, 0.62, "2026-10-03T12:00:00.000Z")]                       # after the last transaction: rotation sells 200
        txs = [dict(event_id="e0", event_type="deposit", transactionType="OPENING_BALANCE", amount=100000, createdAt="2026-09-29T23:00:00Z", _ts=T("2026-09-29T23:00:00Z")),
               tx(1, "BUY", 1000, 0.42, "2026-10-02T10:00:00.100Z"), tx(2, "SELL", 500, 0.60, "2026-10-02T12:00:00.000Z"),
               tx(3, "BUY", -100, 0.30, "2026-10-02T13:00:00.000Z", ex="2")]                     # no fills on file
        evs, cash_ev, info = q_dash5.events(fills, txs, {"a": "catcher", "b": "catcher", "c": "rotation"}, {"c"}, {"c": "catcher"}, {"4:c": 0.65})
        self.assertEqual((info["matched"], info["unmatched"], info["after_tx"], info["sells"]), (2, 1, 1, 2))
        self.assertEqual(cash_ev, [], "the opening balance is INITIAL, not a cash event")
        r = q_dash5.replay(evs, cash_ev)
        self.assertAlmostEqual(r["pos"]["1"], 300.0)
        self.assertAlmostEqual(r["pos"]["2"], -100.0)
        self.assertAlmostEqual(r["realized_day"]["2026-10-02"], 500 * (0.60 - 0.42), places=6)
        self.assertAlmostEqual(r["realized_day"]["2026-10-03"], 200 * (0.62 - 0.42), places=6)
        self.assertAlmostEqual(r["cash"], -420 + 300 - 30 + 124, places=6)
        self.assertAlmostEqual(r["cash_b"]["catcher"], -420 + 300 + 0.42 * 200, places=6, msg="the owner gets its cost back (SIG average)")
        self.assertAlmostEqual(r["cash_b"]["rotation"], (0.62 - 0.42) * 200, places=6, msg="rotation keeps its own realized (Piero 21:20)")
        self.assertAlmostEqual(r["giveup"]["rotation"], (0.65 - 0.62) * 200, places=6, msg="given up vs fair, for the hover")
        self.assertAlmostEqual(r["cash_b"]["held"], -30, places=6)
        self.assertEqual(r["lots"], {"1": {"catcher": 300.0}, "2": {"held": 100.0}})
        self.assertAlmostEqual(sum(r["cash_b"].values()), r["cash"], places=6)

    def test_activity_phrases_exits_as_sales_and_groups_tiny_fills(self):
        import q_dash5
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS)
        t = lambda m: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - m * 60))
        base = dict(before_engine=False, fair=0.10)
        eng.ledger.fills.extend([dict(base, key="a", order="o1", ex=ex, side="yes", qty=450.0, price=0.115, action="buy", realized=4.5, t=t(3), layer="dumpexit"),
                                 dict(base, key="b", order="o2", ex=ex, side="no", qty=1.0, price=0.9, action="buy", realized=0.0, t=t(20), layer="dump"),
                                 dict(base, key="c", order="o3", ex=ex, side="no", qty=1.0, price=0.905, action="buy", realized=0.0, t=t(25), layer="dump"),
                                 dict(base, key="d", order="o4", ex=ex, side="no", qty=300.0, price=0.89, action="buy", realized=0.0, t=t(22), layer="dump")])
        eng.ledger.fills.sort(key=lambda f: f["t"])
        acts = [a["text"] for a in q_dash5.activity(eng) if a["kind"] == "trade"]
        self.assertTrue(any(a.startswith(f"Catcher sold 450 NO {eng.name(ex)} at 0.885, +1.0c vs entry") for a in acts), acts[:4])
        self.assertTrue(any("in 2 tiny fills" in a for a in acts), "two 1-share catches 5 min apart (not adjacent) are one row")

    def test_daily_table_adds_up_past_days_fixed_and_the_nov_1_boundary(self):
        import q_dash5
        old = q_dash5.DATA; q_dash5.DATA = TMP; self.addCleanup(setattr, q_dash5, "DATA", old)
        q_dash5._cache.clear()
        b1, b2 = q_dash5.day_start("2026-10-02"), q_dash5.day_start("2026-10-03")
        with open(os.path.join(TMP, "pnl.csv"), "w") as fh:
            fh.write("time,account_value,account_book,account_pm,unrealized,roi_pct\n")
            for t, a, f in ((b1 - 600, 99000.0, 101000.0), (b1 + 60, 99100.0, 101100.0), (b2 - 30, 100500.0, 102500.0), (b2 + 900, 100400.0, 102400.0)):
                fh.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))},{a},,{f},0,0\n")
        now = b2 + 3600
        real = {"2026-10-01": -500.0, "2026-10-02": 300.0, "2026-10-03": 50.0}
        days = q_dash5.daily_table(now, 100600.0, 102700.0, real)
        by = {d["day"]: d for d in days}
        self.assertEqual(by["2026-10-01"]["start"], 100_000.0, "Oct 1 starts at 100,000")
        self.assertAlmostEqual(by["2026-10-02"]["board"], 100500.0 - 99100.0, msg="end - start at the samples nearest 00:00 UTC")
        for d in days:
            self.assertAlmostEqual(d["realized"] + d["open"], d["board"], places=6, msg=d["day"])
            self.assertAlmostEqual(d["realized"] + d["fair_open"], d["fair"], places=6)
        self.assertTrue(by["2026-10-03"]["partial"])
        days2 = q_dash5.daily_table(now, 90000.0, 95000.0, real)        # today's prices move: past days do not
        self.assertEqual([d["board"] for d in days2[:-1]], [d["board"] for d in days[:-1]])
        self.assertNotEqual(days2[-1]["board"], days[-1]["board"])
        self.assertEqual(time.localtime(q_dash5.day_start("2026-11-02")).tm_hour, 16, "after DST ends the boundary is 16:00 PT")
        self.assertEqual(time.localtime(q_dash5.day_start("2026-10-09")).tm_hour, 17)

    def test_registry_maps_layers_and_subs(self):
        import q_dash5
        b = q_dash5.bucket_of_layer
        self.assertEqual((b("dump"), b("dumpexit"), b("tm"), b("tmexit"), b("sm"), b("smexit"), b("release"), b("recycle")),
                         ("catcher", "catcher", "tm", "tm", "sm", "sm", "rotation", "rotation"))
        self.assertEqual((b("pairs"), b("unwind"), b("sniper"), b("fatfinger")), ("pairs", "pairs", "sniper", "sniper"))
        self.assertEqual((b("swing"), b("swingsell"), b("wall", "wall:1077:no:w1", "1077"), b("wall", "wall:974:no:w2", "974")), ("ri", "ri", "ri", "walls"))
        self.assertEqual((b("fm"), b("fmexit"), b("scalp"), b("mm"), b("take"), b("touch"), b("set"), b("longshot")),
                         ("fm", "fm", "scalp", "mm", "take", "touch", "set", "longshot"))
        self.assertEqual([x["key"] for x in q_dash5.RETIRED], ["fm", "scalp", "mm", "take", "touch", "set", "longshot"])
        self.assertEqual([x["key"] for x in q_dash5.REGISTRY], ["tm", "sm", "catcher", "sniper", "rotation", "pairs"])
        self.assertEqual((q_dash5.bucket_of_sub("inventory"), q_dash5.bucket_of_sub("manual"), b("", "dump:1:yes:L0")), ("held", "held", "catcher"))

    def test_payload_status_markets_and_the_check(self):
        import q_dash5
        q_dash5._cache.clear()
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake); eng.ex.capacity = 6000.0
        for _ in range(3):
            loop(eng, fake)
        st = eng.dash_state()
        for k in ("v5hdr", "v5ov", "v5lv", "v5mk", "v5mk_rows", "v5ad"):
            self.assertIn(k, st, st.get("v5err"))
        ov, lv = st["v5ov"], st["v5lv"]
        acct = float(eng.acct.get("totalAccountValue") or 0)
        self.assertAlmostEqual(ov["board"]["pnl"], acct - 100_000.0, places=2)
        self.assertTrue(ov["days"] and ov["days"][-1]["partial"])
        self.assertAlmostEqual(lv["check"]["sum"], round(sum(c["pnl"] for c in lv["cards"] if c["pnl"] is not None), 2), places=1)
        self.assertAlmostEqual(lv["check"]["gap"], round(lv["check"]["sum"] - lv["check"]["board"], 2), places=2)
        self.assertEqual({c["key"] for c in lv["cards"] if c["group"] == "strategy"}, {b["key"] for b in q_dash5.REGISTRY}, "every strategy has a card")
        self.assertAlmostEqual(ov["port"]["invested"] + ov["port"]["available"], ov["port"]["account"], delta=1.0, msg="donut + cash = account")
        rows = st["v5mk_rows"]
        self.assertEqual(len(rows), len({b.ex for b in eng.bots if b.kind in ("market", "position")}), "every contract listed (market and position bots)")
        for r in rows:
            p = (eng.book.get(r["ex"]) or {}).get("pos", 0)
            self.assertEqual(bool(r["hold"]), abs(p) >= 1, r["ex"])
            if r["hold"]:
                self.assertEqual(r["hold"]["shares"], round(abs(p)))
            if not r["orders"]:
                self.assertTrue(r["why"], f"{r['ex']} has no order and no reason")
        api_ = types.SimpleNamespace(_throttled=[time.time() - 30])
        self.assertEqual(q_dash5.status_of(eng, 1, 3)[0], "degraded", "a rate-limit error turns the status amber")
        self.assertEqual(q_dash5.status_of(eng, 0, 3)[0], "trading")
        eng.controls.d["paused"] = True
        self.assertEqual(q_dash5.status_of(eng, 0, 3)[0], "paused")
        eng.controls.d["paused"] = False


class FastPathTests(EngineCase):
    """mm_plan §2 / §8.1: on a SIG fill notification the fast step reads the fills once, re-bids a filled L0 level at the new
    best bid while it is >= 5c under fair (<= 3 per side per 10 min), re-quotes a filled FM side, and pulls a market's FM
    quotes on a Polymarket move >= 2c; a fast order neither resting nor filled sends the market to the slow path 10 min."""

    def rig(self):
        fresh_data(layers=dict(position=dict(state="full"), dump=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 60_000)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        fake.DRY_RUN = False
        self.addCleanup(setattr, f_engine, "FAST_PATH_ON", f_engine.FAST_PATH_ON)
        f_engine.FAST_PATH_ON = True                       # the switch ships with C82 (Oct 8 15:00); the tests cover the path itself
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] >= 0.95
                  and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE)
        return fake, eng, ex

    def test_l0_sweep_follow_within_the_fast_step(self):
        fake, eng, ex = self.rig()
        f = eng.fair[ex]
        fake.px[ex] = eng.px[ex] = (round(f - 0.08, 3), round(f - 0.075, 3), f - 0.08)   # the bid 8c under fair: L0 joins it
        seed_ref(eng, ex, mid=f - 0.08)
        loop(eng, fake); loop(eng, fake)
        l0 = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and str(eng.ledger.orders[o["id"]].get("sub", "")).endswith(":L0")]
        self.assertEqual(len(l0), 1, l0)
        fake.fill(l0[0]["id"], l0[0]["qty"])               # the sweep takes our L0 level
        fake.px[ex] = (round(f - 0.09, 3), round(f - 0.085, 3), f - 0.09)   # the new best bid, lower
        eng.rt_event("fills", set(), time.time() - 0.3)    # the SIG notification (no market in it)
        before = len(fake.open_orders())
        n = eng.fast_step(time.time())
        self.assertGreaterEqual(n, 1, "the fast step placed the re-bid")
        new = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and str(eng.ledger.orders[o["id"]].get("sub", "")).endswith(":L0")]
        self.assertEqual(len(new), 1, new)
        self.assertAlmostEqual(new[0]["price"], round(f - 0.09, 3), places=3, msg="re-bid at the NEW best bid")
        self.assertTrue(eng.latency.get("fast_fill_to_sent"), "latency sampled")
        self.assertLess(eng.latency["fast_fill_to_sent"][-1], 2.0)
        self.assertTrue(any("FAST L0 re-bid" in e["text"] for e in eng.events))
        # guard: no more than 3 fast re-arms per side per 10 min
        eng.fast_rearms[(ex, "yes")] = [time.time()] * 3
        eng.rt_event("fills", set(), time.time())
        fake.fill(new[0]["id"], new[0]["qty"])
        self.assertEqual(eng.fast_step(time.time()), 0)
        self.assertGreaterEqual(eng.fast_stats["l0_capped"], 1)
        # reconcile: a fast order that vanished without a fill -> slow path 10 min
        eng.fast_orders[(ex, f"dump:{ex}:yes:L0")] = time.time() - 30
        for o in list(fake.open_orders()):
            if o["ex"] == ex:
                fake.open.pop(o["id"], None) if hasattr(fake, "open") else None
        loop(eng, fake)
        eng.fast_reconcile(time.time())
        self.assertTrue(eng.fast_slow.get(ex, 0) > time.time() or eng.fast_stats["mismatch"] == 0)

    def test_wv_senate_miss_an_8c_under_touch_carries_an_l0_and_the_sweep_follow_rebids(self):
        """Day 11 §4.3 (West Virginia Senate R, Oct 7 19:1x): 429k shares printed at 0.875-0.905 under a 0.982 fair while our only level
        sat at 0.860 and the side was uncovered through a refresh. With the fast path: an >= 0.95 favourite whose touch is 8c under
        fair carries an L0 at the bid (the L0 budget has room, the race room allows), the sweep that takes it is followed by a
        re-bid at the new touch within the fast step, and the slow loop never leaves the side without its L0 while the tranche sells back."""
        fake, eng, ex = self.rig()
        f = eng.fair[ex]
        bid = round(f - 0.08, 3)
        fake.px[ex] = eng.px[ex] = (bid, round(bid + 0.005, 3), bid)
        seed_ref(eng, ex, mid=f - 0.08)
        loop(eng, fake); loop(eng, fake)
        l0 = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and str(eng.ledger.orders[o["id"]].get("sub", "")).endswith(":L0")]
        self.assertEqual(len(l0), 1, "an L0 rests at the 8c-under touch")
        self.assertAlmostEqual(l0[0]["price"], bid, places=3)
        self.assertLess(eng.l0_held(), a_config.DUMP_L0_BUDGET, "the L0 budget is not binding (WV: 487 held of the cap)")
        # the sweep prints 1-2 ticks above the old touch: it takes our L0; the fast step re-bids at the new (higher) best bid
        fake.fill(l0[0]["id"], l0[0]["qty"])
        new_bid = round(bid + 0.01, 3)
        fake.px[ex] = (new_bid, round(new_bid + 0.005, 3), new_bid)
        eng.rt_event("fills", set(), time.time() - 0.3)
        self.assertGreaterEqual(eng.fast_step(time.time()), 1)
        new = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and str(eng.ledger.orders[o["id"]].get("sub", "")).endswith(":L0")]
        self.assertEqual(len(new), 1, new)
        self.assertAlmostEqual(new[0]["price"], new_bid, places=3, msg="re-bid at the new touch, still >= 5c under fair")
        # the slow loop keeps the L0 while the tranche sells back (the refill rule, not a gap): the side is never uncovered
        for _ in range(3):
            loop(eng, fake)
            mine = [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "dump"]
            self.assertTrue(mine, "the side keeps a dump level across the loops after the fill")

class SwingTests(EngineCase):
    """Day 10 §3 (Piero D2): a 15k slice of the RI Senate R wall lots cycles: a sell leg at NO 0.865 for what is held, a
    buy-back at NO 0.845 for what was sold (only >= 14c under fair, never more than sold); the other wall lots are never
    sold; both legs pulled while fair YES > 0.03 or after a 2c rise in 10 min; rule 0 on every sale."""

    def rig(self):
        fresh_data(layers=dict(position=dict(state="full"), swing=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 30_000)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] <= 0.10
                  and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE)
        FAIR[ex] = 0.0065
        self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = (0.135, 0.14, 0.135)                # NO bid/ask 0.86 / 0.865
        subs = [f"wall:{ex}:no:wA", f"wall:{ex}:no:wB"]
        eng.ledger.subbooks[ex] = {subs[0]: -11_900.0, subs[1]: -10_000.0}
        for s_ in subs:
            eng.ledger.tranches[s_] = dict(ex=ex, side="no", kind="wall", state="open", ts=time.time(), t="x", avg=0.85)   # open (not "held"): the under-15k buy-back may rest
        fake.pos[ex] = -21_900
        eng.controls.d["swing"] = dict(a_config.SWING, ex=ex, wall_subs=subs, hold_qty=6_900.0, stats=dict(sold=0.0, bought=0.0, realized=0.0, cycles=0, legs=[]))
        loop(eng, fake)
        return fake, eng, ex

    def legs(self, eng, fake, ex, layer):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == layer]

    def test_band_trades_the_no_inventory_at_the_touch(self):
        """turnover_plan §2.1: with the band on, a NO ask rests at the NO touch ask while held > lo (>= entry + 1c), a NO bid at the
        NO touch bid while held < hi and >= 14c under fair; clips 1k; the queue is kept within a tick."""
        fake, eng, ex = self.rig()
        eng.controls.d["swing"]["band"] = dict(a_config.SWING_BAND, lo=15_000, hi=25_000)
        loop(eng, fake); loop(eng, fake)
        sells = self.legs(eng, fake, ex, "swingsell")
        self.assertEqual(len(sells), 1, sells)
        self.assertAlmostEqual(1 - sells[0]["price"], 0.865, places=3, msg="NO ask at the NO touch ask (0.865 >= 0.85 + 1c)")
        self.assertEqual(sells[0]["qty"], a_config.SWING_BAND["clip"])
        self.assertEqual(self.legs(eng, fake, ex, "swing"), [], "NO bid 0.86 is 13.35c under fair NO: no buy")
        FAIR[ex] = 0.0
        band = eng.controls.d["swing"]["band"]
        band.update(credit=0.0, sold_q=0.0, sold_val=0.0, bought_q=0.0, bought_val=0.0)
        eng.controls.d["buffer"] = dict(value=40_000, day=time.strftime("%Y-%m-%d"))   # C84b: free 30k < BUFFER 40k and credit 0 -> no band buys (the sells stay)
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.legs(eng, fake, ex, "swing"), [], "credit 0 and no idle cash: the band does not buy")
        self.assertTrue(self.legs(eng, fake, ex, "swingsell"))
        # a sell fills: the credit funds a buy-back at <= the sell price - 1c, sized within the credit
        sells = self.legs(eng, fake, ex, "swingsell")
        fake.fill(sells[0]["id"], sells[0]["qty"])
        loop(eng, fake); loop(eng, fake)
        self.assertAlmostEqual(band["credit"], 1000 * 0.865, delta=1.0, msg="the sale's proceeds are the band's credit")
        buys = self.legs(eng, fake, ex, "swing")
        self.assertEqual(len(buys), 1, buys)
        self.assertLessEqual(buys[0]["price"], 0.865 - 0.01 + 1e-9, "at most the sell price - 1c")
        self.assertLessEqual(buys[0]["price"] * buys[0]["qty"], band["credit"] + 1.0, "within the credit")
        # the 2k guard stops every entry, credit or not
        cash_was = eng.cash_hist; eng.cash_hist = [a_config.CASH_RESERVE + 1_000]
        plan = eng.swing_plan(time.time())
        self.assertNotIn("buy", plan.get(ex, {}), "free < 2k: no band buy")
        eng.cash_hist = cash_was
        # idle cash (free >= BUFFER): buys beyond the credit
        eng.controls.d["buffer"] = dict(value=10_000, day=time.strftime("%Y-%m-%d"))
        band.update(credit=0.0, sold_q=0.0, sold_val=0.0, bought_q=0.0, bought_val=0.0)
        loop(eng, fake); loop(eng, fake)
        buys = self.legs(eng, fake, ex, "swing")
        self.assertEqual(len(buys), 1, (buys, eng.swing_plan(time.time()), dict(band), eng.free_cash(), eng.buffer(), eng.race_room(ex, "no", eng.side_fair(ex, "no")), eng.ledger.subbooks.get(ex), fake.pos.get(ex)))
        self.assertIn(round(buys[0]["price"], 3), (0.855, 0.86), "at the NO touch bid (0.86), or the kept 0.855 order one tick behind it")
        sells = self.legs(eng, fake, ex, "swingsell")
        self.assertTrue(sells)
        self.assertLess(buys[0]["price"], 1 - sells[0]["price"], "the bid stays under our ask")
        fake.px[ex] = (0.14, 0.145, 0.1425)                   # NO touch 0.855 / 0.86, one tick down: the sell keeps its place (one tick behind), the buy re-pegs off our own ask
        loop(eng, fake); loop(eng, fake)
        self.assertEqual({o["id"] for o in self.legs(eng, fake, ex, "swingsell")}, {sells[0]["id"]})
        buys = self.legs(eng, fake, ex, "swing")
        self.assertEqual(len(buys), 1, buys)
        self.assertAlmostEqual(buys[0]["price"], 0.855, places=3, msg="the kept 0.86 would meet our own 0.86 ask: re-pegged to the touch bid")
        # under lo: no sells; at hi: no buys
        eng.controls.d["swing"]["band"] = dict(a_config.SWING_BAND, lo=25_000, hi=25_000)
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.legs(eng, fake, ex, "swingsell"), [])

    def test_sell_leg_buy_leg_hold_part_and_guards(self):
        fake, eng, ex = self.rig()
        loop(eng, fake)
        sells = self.legs(eng, fake, ex, "swingsell")
        self.assertEqual(sum(o["qty"] for o in sells), 15_000, sells)
        self.assertTrue(all(abs(1 - o["price"] - 0.865) < 1e-9 for o in sells), "sell NO at 0.865 = buy YES at 0.135")
        self.assertEqual(self.legs(eng, fake, ex, "swing"), [], "nothing sold yet: no buy-back")
        big = max(sells, key=lambda o: o["qty"])
        fake.fill(big["id"], 5_000)
        loop(eng, fake); loop(eng, fake)
        buy = self.legs(eng, fake, ex, "swing")
        self.assertEqual(len(buy), 1, buy)
        self.assertEqual((buy[0]["qty"], buy[0]["side"], buy[0]["action"]), (5_000, "no", "buy"))
        self.assertAlmostEqual(buy[0]["price"], 0.845, places=3)
        self.assertEqual(sum(o["qty"] for o in self.legs(eng, fake, ex, "swingsell")), 10_000, "the sell leg shrinks to what is still held of the slice")
        self.assertTrue(any("SWING sell leg" in e["text"] for e in eng.events))
        fake.fill(buy[0]["id"], 5_000)
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(eng.ledger.subbooks[ex].get(f"swing:{ex}:no"), -5_000.0)
        sells = self.legs(eng, fake, ex, "swingsell")
        self.assertEqual(sum(o["qty"] for o in sells), 15_000, "bought back: the whole slice is for sale again (swing lots first)")
        self.assertEqual(self.legs(eng, fake, ex, "swing"), [])
        self.assertEqual(eng.swing_view()["cycles"], 1)
        # the hold part (6,900) is never sold: sell everything of the slice, nothing more rests
        for o in sells:
            fake.fill(o["id"], o["qty"])
        loop(eng, fake); loop(eng, fake)
        held_walls = sum(abs(q) for s_, q in eng.ledger.subbooks[ex].items() if s_.startswith("wall:"))
        self.assertEqual(held_walls, 6_900.0)
        self.assertEqual(self.legs(eng, fake, ex, "swingsell"), [])
        self.assertEqual(self.legs(eng, fake, ex, "swing")[0]["qty"], 15_000, "all sold: the buy-back rests for the whole slice")
        # the wall's own under-15k buy-back covers only the hold part (6,900), never the slice the swing owns
        eng.ledger.subbooks[ex] = {f"wall:{ex}:no:wA": -11_900.0, f"wall:{ex}:no:wB": -10_000.0}
        fake.pos[ex] = -21_900
        eng.controls.d["walls"] = [dict(id="wA", ex=ex, side="no", px=0.85, qty=11_900, exit_px=0.13, ttl_s=259200, hold_s=259200, news_c=0.02, t=time.time(), state="active", filled=11_900.0, exit_min_free=15_000, once=True, by="Piero"),
                                   dict(id="wB", ex=ex, side="no", px=0.85, qty=10_000, exit_px=0.13, ttl_s=259200, hold_s=259200, news_c=0.02, t=time.time(), state="active", filled=10_000.0, exit_min_free=15_000, once=True, by="Piero")]
        eng.controls.d["layers"]["wall"] = dict(state="full", caps={}, killed=None)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + 12_000)   # free 12k < 15k: the wall buy-back rests
        for _ in range(3):
            loop(eng, fake)
        wexits = self.legs(eng, fake, ex, "wallexit")
        self.assertEqual(sum(o["qty"] for o in wexits), 6_900, wexits)
        self.assertEqual(sum(o["qty"] for o in self.legs(eng, fake, ex, "swingsell")), 15_000)
        FAIR[ex] = 0.04                                   # fair YES > 0.03: both legs pulled
        loop(eng, fake); loop(eng, fake)
        self.assertEqual(self.legs(eng, fake, ex, "swing"), [])
        self.assertTrue(any("legs pulled" in e["text"] for e in eng.events))


class SleeveTests(EngineCase):
    """Day 9 (Piero 18:50): the sleeve takes resting liquidity with limit clips (<= 1,500 shares, >= 2 min apart, never past
    the limit) only when our side's fair >= 0.98 and the edge >= 10c, only with cash above 20k free, 10k at cost in total
    and per-target caps; a 2c / 10 min fall of fair pauses it; the lots are held to settlement (never sold by the rebuild)."""

    def rig(self, cash_above_floor=30_000):
        fresh_data(layers=dict(position=dict(state="full"), sleeve=dict(state="full")))
        fake = new_fake(writes_per_min=10_000)
        base = fake.pnl()
        fake.pnl = lambda: dict(base, totalAccountValue=base["totalHoldingsValue"] + a_config.CASH_RESERVE + cash_above_floor)
        eng = engine(fake)
        eng.ex.capacity = 6000.0
        loop(eng, fake)
        ex = next(b.ex for b in eng.bots if b.kind == "market" and b.ex not in POS and eng.fair.get(b.ex) is not None and eng.fair[b.ex] <= 0.10
                  and eng.meta[b.ex].get("race") != a_config.RI_SENATE_RACE)
        FAIR[ex] = 0.01                                   # fair NO 0.99 (the fixture's fairs; the test pins it)
        self.addCleanup(FAIR.pop, ex, None)
        fake.px[ex] = (0.14, 0.145, 0.14)                 # YES 0.14 / 0.145 -> the NO ask is 0.86 (<= the 0.865 limit; fair NO >= 0.985)
        loop(eng, fake)
        eng.controls.d["sleeve_targets"] = [dict(ex=ex, side="no", limit=0.865, cap=5_000, name="target")]
        return fake, eng, ex

    def clips(self, eng, fake, ex):
        return [o for o in fake.open_orders() if o["ex"] == ex and o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "sleeve"]

    def test_clip_rules_caps_and_hold(self):
        fake, eng, ex = self.rig()
        loop(eng, fake)
        c = self.clips(eng, fake, ex)
        self.assertEqual(len(c), 1, c)
        self.assertEqual((c[0]["side"], c[0]["action"]), ("no", "buy"))
        self.assertAlmostEqual(c[0]["price"], 0.86, places=3, msg="a limit at the touch, never past 0.865")
        self.assertLessEqual(c[0]["qty"], a_config.SLEEVE_CLIP)
        self.assertTrue(any(e["kind"] == "SLEEVE" and "clip" in e["text"] for e in eng.events))
        fake.fill(c[0]["id"], c[0]["qty"])
        loop(eng, fake)                                   # within 2 min of the first clip: no second one
        self.assertEqual(self.clips(eng, fake, ex), [])
        self.assertEqual(eng.ledger.subbooks[ex].get(f"sleeve:{ex}:no"), -float(c[0]["qty"]))
        self.assertTrue(any("SLEEVE FILL" in e["text"] for e in eng.events))
        eng.sleeve_last[(ex, "no")] -= 130
        loop(eng, fake)
        self.assertEqual(len(self.clips(eng, fake, ex)), 1, "2 min later: the next clip")
        view = eng.sleeve_view()[0]
        self.assertEqual(view["shares"], c[0]["qty"])
        self.assertGreater(view["ev"], 0)
        # the per-target cap: 5k at cost held -> no clip
        for o in self.clips(eng, fake, ex):
            fake.cancel(o["id"]) if hasattr(fake, "cancel") else None
        eng.ledger.subbooks[ex][f"sleeve:{ex}:no"] = -6_000.0
        eng.ledger.tranches[f"sleeve:{ex}:no"] = dict(ex=ex, side="no", kind="sleeve", state="held", ts=time.time(), t="x", avg=0.86)
        eng.sleeve_last[(ex, "no")] -= 130
        loop(eng, fake)
        self.assertEqual([o for o in self.clips(eng, fake, ex) if eng.ledger.orders[o["id"]].get("ts", 0) >= time.time() - 2], [], "5k at cost: capped")
        # settlement hold: the rebuild never sells sleeve lots, even far under 15k free
        eng.release_t = 0
        self.addCleanup(setattr, f_engine, "RELEASE_QUIET_HOURS", f_engine.RELEASE_QUIET_HOURS)
        f_engine.RELEASE_QUIET_HOURS = (0, 0)
        fake.pos[ex] = -6000
        plan = eng.release_plan(a_config.CASH_RESERVE + 3_000, time.time(), eng.bucket_usage())
        self.assertNotIn(ex, plan, plan)

    def test_only_cash_above_20k_free_and_the_gates(self):
        fake, eng, ex = self.rig(cash_above_floor=21_000)
        loop(eng, fake)
        c = self.clips(eng, fake, ex)
        self.assertEqual(len(c), 1)
        self.assertLessEqual(c[0]["qty"] * c[0]["price"], 1_000 + 1, "only the 1k above 20k free")
        fake, eng, ex = self.rig(cash_above_floor=19_000)
        loop(eng, fake)
        self.assertEqual(self.clips(eng, fake, ex), [], "under 20k free: nothing")
        fake, eng, ex = self.rig()
        fake.px[ex] = (0.13, 0.135, 0.13)                 # NO ask 0.87 > the 0.865 limit: wait, never walk past the limit
        loop(eng, fake)
        self.assertEqual(self.clips(eng, fake, ex), [])
        fake.px[ex] = (0.14, 0.145, 0.14)
        FAIR[ex] = 0.03                                   # fair NO 0.97 < 0.98: no clip
        try:
            loop(eng, fake)
            loop(eng, fake)
            self.assertEqual(self.clips(eng, fake, ex), [])
        finally:
            FAIR.pop(ex, None)
        loop(eng, fake)
        now = time.time()
        eng.fair_hist[ex] = [(now - 300, 0.005)]          # our side's fair fell from 0.995 toward 0.97+: >= 2c in 10 min -> paused
        FAIR[ex] = 0.03
        try:
            loop(eng, fake)
        finally:
            FAIR.pop(ex, None)
        self.assertGreater(eng.sleeve_block.get((ex, "no"), 0), now)


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
