"""The whole engine (f_engine.py + j_executor.py) against the fake exchange (tests/fake_sig.py) and a fake Polymarket.
No network, nothing sent.   python3 -m unittest tests.test_engine

  scenario          position exits, foreign orders, inventory I2, atomic pair sets, repairs, UNKNOWN freeze + audit
  kill-restart      three kills mid-batch: zero duplicates, every order sent after a restart was planned after it
  stop under load   slow writes, 429, DELETE timeouts, unreadable book: exits and foreign orders survive, nothing placed
  plan-cycle        writes taking 2 s each in the executor's threads do not slow the plan cycle
  dashboard         read-only controls, bound to 127.0.0.1
"""
import contextlib
import csv
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

d_ledger.LEDGER_FILE = a_config.LEDGER_FILE               # whichever test module imported d_ledger first


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


ALL_ON = {l: dict(state="full") for l in ("position", "mm", "ladder", "pairs", "sniper", "leaders")}   # recycler: own tests (D1)
RECYCLER_ON = dict(position=dict(state="full"), recycler=dict(state="full"))


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
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        loop(eng, fake)
        loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
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
        rel = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "release"]
        self.assertEqual([(o["ex"], o["side"], o["price"]) for o in rel], [(ex, "no", round(1 - bid, 3))], "sell YES at the bid")
        self.assertLessEqual(sum(o["qty"] for o in rel), 1000, "<= top-of-book depth (fake: 1,000)")
        self.assertFalse([o for o in fake.open_orders() if o["ex"] in (AK, RI, MA) and o["id"] in eng.ledger.orders
                          and eng.ledger.orders[o["id"]]["layer"] == "release"], "manual and > 3% holdings are kept")
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

    def rig(self, cash_above_floor=20_000):
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
        # queue step-up: someone already bids 0.05 on a side we cover -> our 0.05 level goes to 0.055, not above caps
        ex, side = mine[0]["ex"], mine[0]["side"]
        yb = [(0.05, 500.0)] if side == "yes" else []
        ya = [(0.95, 500.0)] if side == "no" else []
        eng.fat_book[ex] = (time.time(), yb, ya)
        lv = eng.fat_levels(ex, side, eng.side_fair(ex, side), 1.0, "A")
        self.assertIn(0.055, [px for _, px, _ in lv])
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
        self.assertEqual(sorted(px for _, px, _ in eng.fat_levels(ex, side, eng.side_fair(ex, side), 1.0, "A")), [0.055, 0.08, 0.12])

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
        fake, eng = self.rig(cash_above_floor=500)        # reserve unfunded: nothing armed
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
        eng.fat_fills.append((now - 600, 0.80, 0.60))                   # filled 20c from fair: the gate holds
        eng.controller(now)
        self.assertEqual(c.state("fatfinger"), "pilot")
        eng.fat_fills[:] = [(now - 600, 0.80, 0.05)]                    # a real fat-finger catch: promote
        eng.controller(now)
        self.assertEqual(c.state("fatfinger"), "full")

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
        if cov["uncovered"]:
            rest = sum(o["qty"] * min(o["price"], 1 - o["price"]) for o in self.fat_orders(eng, fake))
            self.assertGreater(rest, eng.cap("fatfinger", "resting") - 2_400, "cap is used before sides go uncovered")


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
        fresh_data(layers=dict(position=dict(state="full"), mm=dict(state="pilot")))
        fake = new_fake(writes_per_min=10_000)
        eng = engine(fake)
        eng.pilot_sets = {}                               # the whole fixture is the pilot
        loop(eng, fake)
        loop(eng, fake)
        mm = [o for o in fake.open_orders() if o["id"] in eng.ledger.orders and eng.ledger.orders[o["id"]]["layer"] == "mm"]
        for o in mm[:22]:
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
                         ["position", "recycler", "release", "mm", "ladder", "pairs", "sniper", "fatfinger", "leaders"])
        self.assertTrue(all(h["side"] in ("yes", "no") and h["shares"] > 0 for h in s["holdings"]))
        mm_row = next(r for r in s["strategies"] if r["key"] == "mm")
        self.assertEqual(mm_row["fills_today"], 1)
        self.assertTrue(s["activity"] and s["activity"][0]["text"].startswith("Market maker bought"), s["activity"][:1])
        self.assertIn("thresholds", s["leaders"])
        import g_dashboard as dashboard
        self.assertLess(len(dashboard.dumps(s)), 300_000)


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
