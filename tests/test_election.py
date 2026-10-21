"""n_election.Election v2 rules (docs/night_reconcile.md §3 C1-C21, Piero 01:05 Oct 18; C8: no post-call bids): a call is the PM side bid
>= 0.95 held 20 s or >= 0.98 once, after the state's last poll close, with the PM spread <= 3c and the race's other books <= 0.04; the
sniper pays <= min(0.97, PM bid - 2c) walking the book, min(15k shares a race, the wave share), <= 20k a state, 120-s orders, cash floor
5k + 2k; a reversal stops the race and holds; pulls at the first close - 10 min keep exits and foreign orders; the kill switch.
python3 -m unittest tests.test_election"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import n_election as el  # noqa: E402

META = {"1": dict(race="Ohio Senate", party="R", title="R Ohio Senate", state="Ohio"), "2": dict(race="Ohio Senate", party="D", title="D Ohio Senate", state="Ohio"),
        "3": dict(race="Utah Governor", party="R", title="R Utah Governor", state="Utah"), "4": dict(race="OH-09 House race", party="R", state="Ohio")}
LAST = {"Ohio Senate": 10_000.0, "Utah Governor": 20_000.0, "OH-09 House race": 10_000.0}
FIRST = {"Ohio Senate": 9_000.0, "Utah Governor": 19_000.0, "OH-09 House race": 9_000.0}


class Rig:
    def __init__(self, levels=None, fail=0, book=(0.80, 0.82, 5_000)):
        self.placed, self.cancelled, self.logs, self.dry, self.fail = [], [], [], [], fail
        self.t = [0.0]
        self.bk = book
        self.lv = levels if levels is not None else {"yes": [(0.90, 2_000), (0.94, 3_000), (0.97, 50_000)], "no": [(0.95, 1_000)]}
        self.E = el.Election(META, LAST, lambda: self.t[0], self.place, self.cancel, lambda ex: self.bk, lambda k, x: self.logs.append((k, x)),
                             manual=(), first_close=FIRST, levels=lambda ex, side: self.lv.get(side), place_dry=self.place_dry)

    def place(self, items, layer, ttl):
        if self.fail > 0:
            self.fail -= 1
            return None
        self.placed.append((items, layer, ttl))
        return "k%d" % len(self.placed)

    def place_dry(self, items, layer, ttl, why):
        self.dry.append((items, layer, ttl, why))
        return "dry"

    def cancel(self, o, reason=""):
        self.cancelled.append((o["id"], reason))
        return True

    def pm(self, ex, bid, ask, t, cash=200_000, pos=None, defer=False):
        self.t[0] = t
        return self.E.on_pm(ex, (bid + ask) / 2, t, cash, pos or {}, pm_bid=bid, pm_ask=ask, defer=defer)


class CallTests(unittest.TestCase):
    def test_rule_a_needs_the_bid_held_at_095_for_20_s(self):
        r = Rig()
        self.assertIsNone(r.pm("1", 0.96, 0.97, 10_100.0))
        self.assertIsNone(r.pm("1", 0.96, 0.97, 10_119.0), "19 s: not yet")
        self.assertEqual(r.placed, [])
        self.assertIsNotNone(r.pm("1", 0.96, 0.97, 10_120.5))
        self.assertEqual(r.E.called("1"), "yes"); self.assertFalse(r.E.calls["1"]["strict"])
        self.assertEqual(r.placed[0][1:], ("callsnipe", 120), "a 120-s order (C5)")

    def test_a_dip_breaks_the_run_and_the_mid_is_not_the_bid(self):
        r = Rig()
        r.pm("1", 0.96, 0.97, 10_100.0)
        r.pm("1", 0.94, 0.99, 10_110.0)                       # mid 0.965 but the bid dipped: the run restarts
        self.assertIsNone(r.pm("1", 0.96, 0.97, 10_125.0))
        self.assertIsNone(r.pm("1", 0.96, 0.97, 10_140.0), "15 s of the new run")
        self.assertIsNotNone(r.pm("1", 0.96, 0.97, 10_146.0))

    def test_rule_b_strict_at_098_once(self):
        r = Rig()
        self.assertIsNotNone(r.pm("1", 0.98, 0.99, 10_100.0))
        self.assertTrue(r.E.calls["1"]["strict"]); self.assertTrue(r.E.strict_against("1", "no")); self.assertFalse(r.E.strict_against("1", "yes"))

    def test_before_the_last_close_it_is_a_precall_and_nothing_goes_out(self):
        r = Rig()
        r.E.precall_dry = True
        self.assertIsNone(r.pm("1", 0.99, 0.995, 9_500.0))
        self.assertEqual(r.placed, []); self.assertIsNone(r.E.called("1"))
        self.assertTrue(any(k == "PRECALL" for k, _ in r.logs))
        self.assertEqual(len(r.dry), 1, "shadow: the would-be snipe is logged, not sent")
        r.pm("1", 0.99, 0.995, 9_600.0)
        self.assertEqual(sum(1 for k, _ in r.logs if k == "PRECALL"), 1, "once per book per 30 min")

    def test_guards_spread_and_the_other_book(self):
        r2 = Rig()
        self.assertIsNone(r2.pm("1", 0.98, 1.0 + 0.02, 10_100.0))
        self.assertIsNone(r2.E.called("1"), "a 4c PM spread: no call")
        r3 = Rig()
        r3.pm("2", 0.10, 0.12, 10_050.0)                      # the D book still at 0.11
        self.assertIsNone(r3.pm("1", 0.98, 0.99, 10_100.0))
        self.assertIsNone(r3.E.called("1"), "the race's other book > 0.04: no win call")

    def test_reversal_stops_the_race_and_holds(self):
        r = Rig()
        r.pm("1", 0.98, 0.99, 10_100.0)
        n = len(r.placed)
        self.assertEqual(r.pm("1", 0.85, 0.87, 10_300.0), "reversed")
        self.assertEqual(len(r.placed), n, "no exit order: rule 0 holds (C16)")
        self.assertIn("Ohio Senate", r.E.race_stop)
        self.assertIsNone(r.pm("2", 0.98, 0.99, 10_400.0), "the race stays stopped")
        r.E.rearm("Ohio Senate")
        self.assertNotIn("Ohio Senate", r.E.race_stop)

    def test_the_deferred_snipe_goes_out_from_the_loop(self):
        r = Rig()
        self.assertEqual(r.pm("1", 0.98, 0.99, 10_100.0, defer=True), "queued")
        self.assertEqual(r.placed, [])
        self.assertEqual(r.E.run_queue(10_101.0, 200_000, {}), 1)
        self.assertEqual(len(r.placed), 1)


class SniperTests(unittest.TestCase):
    def test_price_cap_min_097_and_pm_bid_less_2c_walking_the_book(self):
        r = Rig()
        r.pm("1", 0.98, 0.99, 10_100.0)                       # cap = min(0.97, 0.98 - 0.02) = 0.96
        items = r.placed[0][0][0]
        self.assertEqual(items[:4], ("1", "yes", "buy", 0.94), "the 0.90 and 0.94 levels; 0.97 is over the cap")
        self.assertEqual(items[4], 5_000)
        r2 = Rig()
        r2.pm("1", 0.995, 0.999, 10_100.0)                    # cap 0.97
        self.assertEqual(r2.placed[0][0][0][3], 0.97)
        self.assertLessEqual(r2.placed[0][0][0][4], el.RACE_SHARES, "<= 15k shares a race")

    def test_cash_floor_7k_counts_unfilled_orders_and_the_state_cap(self):
        r = Rig(levels={"yes": [(0.95, 100_000)]})
        r.pm("1", 0.98, 0.99, 10_100.0, cash=9_000)           # usable 2k; wave share 2 x 2k / races of the wave
        q = r.placed[0][0][0][4]
        self.assertLessEqual(q * 0.95, 2_000 + 1)
        r.pm("4", 0.98, 0.99, 10_110.0, cash=9_000)           # the first order is still unfilled: it counts against the 2k
        self.assertLessEqual(r.placed[1][0][0][4] * 0.95, 2_000 - q * 0.95 + 1)
        r2 = Rig(levels={"yes": [(0.95, 100_000)]})
        r2.E.calls["9"] = dict(side="yes", t=0, bought=0, cost=19_800.0, fails=0, stopped=False, reversed=True, strict=False)
        r2.E.meta = dict(META, **{"9": dict(race="OH-01 House race", state="Ohio")})
        r2.pm("1", 0.98, 0.99, 10_100.0, cash=500_000)
        self.assertLessEqual(r2.placed[0][0][0][4] * 0.95, 200 + 1, "20k at cost a state")

    def test_three_failed_writes_stop_the_race(self):
        r = Rig(fail=5)
        for i in range(4):
            r.pm("1", 0.98, 0.99, 10_100.0 + i)
        self.assertTrue(r.E.calls["1"]["stopped"])
        self.assertIn("Ohio Senate", r.E.race_stop)


class LoopTests(unittest.TestCase):
    def test_pulls_at_the_first_close_less_10_min_keep_exits_and_foreign(self):
        r = Rig()
        orders = [dict(id="a", ex="1", side="yes", action="buy", price=0.5, qty=10), dict(id="b", ex="1", side="no", action="buy", price=0.4, qty=10),
                  dict(id="c", ex="1", side="yes", action="buy", price=0.5, qty=10), dict(id="d", ex="3", side="yes", action="buy", price=0.5, qty=10)]
        layer = {"a": "fmm", "b": "fmmexit", "c": None, "d": "carry"}
        out = r.E.on_tick(8_500.0, 50_000, {"1": 100}, orders, lambda o: layer[o["id"]])
        self.assertEqual(out["pulled"], 1); self.assertEqual(r.cancelled, [("a", "POLLS Ohio Senate")])
        self.assertFalse(r.E.entries_allowed("1", 8_500.0)); self.assertTrue(r.E.entries_allowed("3", 8_500.0))
        self.assertTrue(r.E.drift_ok("1", 9_100.0)); self.assertFalse(r.E.drift_ok("1", 8_900.0))

    def test_no_post_call_bids(self):
        r = Rig()
        r.pm("1", 0.98, 0.99, 10_100.0)
        n = len(r.placed)
        r.E.on_tick(10_200.0, 200_000, {"1": 1000}, [], lambda o: None)
        self.assertEqual(len(r.placed), n, "C8: no bids at 0.96-0.99 after a call")

    def test_kill_switch_pulls_entries_and_snipes_keeps_exits_and_foreign(self):
        r = Rig()
        orders = [dict(id="a", ex="1", side="yes", action="buy", price=0.95, qty=10), dict(id="b", ex="1", side="no", action="buy", price=0.4, qty=10),
                  dict(id="c", ex="3", side="yes", action="buy", price=0.5, qty=10)]
        layer = {"a": "callsnipe", "b": "fmmexit", "c": None}
        self.assertEqual(r.E.kill(1.0, orders, {"1": 100}, lambda o: layer[o["id"]]), 1)
        self.assertIsNone(r.pm("1", 0.99, 0.995, 10_100.0)); self.assertEqual(r.placed, [])

    def test_settlement_needs_a_strict_call_and_098_held_2h_and_never_exits_at_market(self):
        r = Rig(book=(0.975, 0.985, 4_000))
        r.pm("1", 0.98, 0.99, 10_100.0)
        n = len(r.placed)
        self.assertEqual(r.E.settlement(30_000.0, 100_000, {}, {"1": 0.985}, 0, 1e9), 0)
        self.assertEqual(r.E.settlement(30_000.0 + 7_201, 100_000, {}, {"1": 0.985}, 0, 1e9), 1)
        self.assertEqual(r.placed[-1][1], "settle")
        r.E.settlement(40_000.0, 100_000, {"1": 1000}, {"1": 0.95}, 0, 1e9)
        self.assertEqual(len(r.placed), n + 1, "PM under 0.98: no exit order (rule 0)")


if __name__ == "__main__":
    unittest.main()
