"""Intel (day 2 §5, morning_plan 4.6): the detector on the real tape, missed-value maths, the incremental collector,
style labels and the leaders study."""
import bisect
import datetime as dt
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import l_intel as li

TAPE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "tape")


def iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


@unittest.skipUnless(os.path.exists(os.path.join(TAPE, "trades.jsonl")), "no recorded tape")
class TapeReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.trades = []
        with open(os.path.join(TAPE, "trades.jsonl")) as fh:
            for i, line in enumerate(fh):
                r = json.loads(line)
                cls.trades.append(dict(id=str(i), ex=r["ex"], ts=iso(r["t"]), p=float(r["p"]), q=float(r["q"])))
        pmh = json.load(open(os.path.join(TAPE, "pm_history.json")))
        cls.pmh = {ex: [(t, f) for t, f in s] for ex, s in pmh.items()}
        cls.times = {ex: [x[0] for x in s] for ex, s in cls.pmh.items()}
        cls.names = {ex: m["title"] for ex, m in json.load(open(os.path.join(TAPE, "meta.json")))["markets"].items()}

        def fair_at(ex, t):                                 # as k_tape_study: last Polymarket point <= t, <= 30 min old
            s = cls.pmh.get(ex)
            if not s:
                return None
            i = bisect.bisect_right(cls.times[ex], t) - 1
            return None if i < 0 or t - s[i][0] > 1800 else s[i][1]
        cls.fair_at = staticmethod(fair_at)
        cls.events = li.detect(cls.trades, fair_at, cls.names)

    def test_flags_the_12_largest_trades(self):
        val = [(abs(r["p"] - f) * r["q"], r) for r in self.trades if (f := self.fair_at(r["ex"], r["ts"])) is not None]
        top = [r for _, r in sorted(val, key=lambda x: -x[0])[:12]]
        self.assertGreater(abs(top[0]["p"] - self.fair_at(top[0]["ex"], top[0]["ts"])) * top[0]["q"], 90_000)
        for r in top:
            hit = [e for e in self.events if e["ex"] == r["ex"] and e["t"] - 1e-6 <= r["ts"] <= e["t_end"] + 1e-6]
            self.assertTrue(hit, f"not flagged: {self.names.get(r['ex'])} {r['p']} x {r['q']}")

    def test_ri_senate_r_max_sweep(self):
        ri_r = next(ex for ex, n in self.names.items() if n == "Will the Republican Party win the Rhode Island Senate?")
        sw = [e for e in self.events if e["ex"] == ri_r and "Max sweep" in e["kinds"] and e["p_hi"] >= 0.99 - 1e-9
              and e["shares"] >= 99_649]
        self.assertTrue(sw, "the RI Senate R 0.99 x 99,649 print is a Max sweep")
        self.assertEqual(sw[0]["wrong"], "YES buyer")
        self.assertEqual(sw[0]["gainer_side"], "no")


class DetectorTests(unittest.TestCase):
    def test_rules(self):
        f = lambda ex, t: 0.90
        tr = [dict(id="1", ex="A", ts=0.0, p=0.89, q=50_000),                  # 1c off: 500 -> nothing
              dict(id="2", ex="B", ts=0.0, p=0.85, q=30_000),                  # 5c x 30k = 1,500 -> value >= 1k
              dict(id="3", ex="C", ts=0.0, p=0.45, q=5_000),                   # 45c, 5k shares -> both rules
              dict(id="4", ex="D", ts=0.0, p=0.20, q=40_000),                  # a burst: 2 prints 3 s apart
              dict(id="5", ex="D", ts=3.0, p=0.05, q=40_000),
              dict(id="6", ex="D", ts=30.0, p=0.88, q=10)]                     # 27 s later: its own burst, too small
        ev = {e["ex"]: e for e in li.detect(tr, f)}
        self.assertEqual(sorted(ev), ["B", "C", "D"])
        self.assertEqual(ev["B"]["kinds"], ["value >= 1k"])
        self.assertIn(">= 40c from fair, >= 5k shares", ev["C"]["kinds"])
        d = ev["D"]
        self.assertEqual((d["n"], d["shares"], d["p_lo"], d["p_hi"]), (2, 80_000, 0.05, 0.20))
        self.assertIn("Max sweep", d["kinds"])                                   # the YES seller's cost: 0.8x40k + 0.95x40k
        self.assertAlmostEqual(d["cost"], 0.80 * 40_000 + 0.95 * 40_000)
        self.assertEqual((d["wrong"], d["gainer_side"]), ("YES seller", "yes"))

    def test_no_fair_needs_a_walk(self):
        none = lambda ex, t: None                        # 14:15 live: OK Senate D 140,835 at 0.075-0.090 is big, not wrong
        flat = [dict(id=str(i), ex="A", ts=float(i), p=0.075 + 0.005 * (i % 4), q=35_000) for i in range(4)]
        self.assertEqual(li.detect(flat, none), [])
        walk = [dict(id="1", ex="B", ts=0.0, p=0.30, q=40_000), dict(id="2", ex="B", ts=1.0, p=0.05, q=60_000)]
        self.assertEqual(li.detect(walk, none)[0]["kinds"], ["Max sweep"])

    def test_same_second_prints_ordered_by_id(self):
        tr = [dict(id="12", ex="A", ts=5.0, p=0.5, q=1), dict(id="11", ex="A", ts=5.0, p=0.4, q=1)]
        self.assertEqual([r["id"] for r in li.order(tr)], ["11", "12"])


class MissedValueTests(unittest.TestCase):
    def test_synthetic_sweep_against_known_levels(self):
        ev = dict(fair=0.90, gainer_side="yes", p_lo=0.05, p_hi=0.30, shares=30_000)
        levels = [(0.12, 5_000), (0.08, 10_000), (0.05, 20_000), (0.04, 50_000)]   # 0.04: below the sweep's deepest print
        got, val = li.missed_value(ev, levels)
        self.assertEqual(got, 30_000)                                             # best price first, up to 30k shares
        self.assertAlmostEqual(val, 5_000 * 0.78 + 10_000 * 0.82 + 15_000 * 0.85)
        # a NO-side gain: YES lifted up to 0.97 -> NO bids down to 0.03 filled
        ev2 = dict(fair=0.10, gainer_side="no", p_lo=0.60, p_hi=0.97, shares=8_000)
        got, val = li.missed_value(ev2, [(0.05, 5_000), (0.02, 5_000)])
        self.assertEqual(got, 5_000)
        self.assertAlmostEqual(val, 5_000 * (0.90 - 0.05))


class CollectorTests(unittest.TestCase):
    def test_incremental_reads_only_new_pages(self):
        tape = [dict(id=str(i), createdAt=f"2026-10-02T20:{i // 60:02d}:{i % 60:02d}.000Z", price=0.5, size=10)
                for i in range(1, 451)]                                         # 450 prints, oldest first
        calls = []

        def get(path, params):
            calls.append(dict(params))
            rows = sorted((r for r in tape if r["createdAt"] >= params.get("from", "")), key=lambda r: -int(r["id"]))
            start = int(params.get("cursor", 0))
            page = rows[start:start + params["limit"]]
            return dict(data=page, pagination=dict(hasMore=start + params["limit"] < len(rows),
                                                   nextCursor=str(start + params["limit"])))
        with tempfile.TemporaryDirectory() as d:
            c = li.TapeCollector(get, "T", os.path.join(d, "state.json"), first_window_s=10 ** 9)
            new = c.visit("1000")
            self.assertEqual([r["id"] for r in new][:2], ["1", "2"])            # oldest first
            self.assertEqual(len(new), 450)
            self.assertEqual(len(calls), 3)                                     # 200 + 200 + 50
            calls.clear()
            self.assertEqual(c.visit("1000"), [])
            self.assertEqual(len(calls), 1, "nothing new: one read")
            tape += [dict(id="451", createdAt="2026-10-02T20:07:31.000Z", price=0.6, size=5)]
            calls.clear()
            c2 = li.TapeCollector(get, "T", os.path.join(d, "state.json"))     # resumable: state on disk
            self.assertEqual([r["id"] for r in c2.visit("1000")], ["451"])
            self.assertEqual(len(calls), 1)


class LeadersTests(unittest.TestCase):
    def test_style_rules(self):
        self.assertEqual(li.style(503_000, 400, 84_000), "Opening harvester")
        self.assertEqual(li.style(600_000, 1083, 11_425_000), "High-volume market maker")
        self.assertEqual(li.style(541_000, 6348, 4_066_000), "High-frequency bot")
        self.assertEqual(li.style(5_000, 300, 900_000), "Directional")

    def test_study_links_jumps_to_events(self):
        rows = []
        t0 = dt.datetime(2026, 10, 2, 10, 0)
        for k in range(12):
            t = (t0 + dt.timedelta(minutes=10 * k)).strftime("%Y-%m-%d %H:%M:%S")
            rows.append(dict(time=t, rank="1", name="kind", profile="p1", value=str(100_000 + (30_000 if k >= 6 else 0)),
                             trades="100", volume="200000", roi="1", ours="0"))
            rows.append(dict(time=t, rank="10", name="ten", profile="p10", value="50000", trades="3000", volume="1e6", roi="1", ours="0"))
            rows.append(dict(time=t, rank="700", name="us", profile="me", value="-7000", trades="900", volume="9e5", roi="0", ours="1"))
        ev_t = (t0 + dt.timedelta(minutes=55)).timestamp()
        ev = [dict(t=ev_t, t_end=ev_t, ex="1", market="NH Senate D", shares=100_000, p_lo=0.055, p_hi=0.055, fair=0.875,
                   value=82_000.0, cost=94_500.0, kinds=["Max sweep"])]
        md, acct = li.leaders_study(rows, ev, {"mm": 40.0}, now=t0.timestamp())
        self.assertIn("likely counterparty of NH Senate D", md)
        self.assertIn("High-frequency bot", md)
        self.assertIn("## Copy list", md)
        self.assertIn("| us (rank, P&L) |", md)
        k = next(a for a in acct if a["name"] == "kind")
        self.assertEqual(k["d_pnl"], 30_000)


if __name__ == "__main__":
    unittest.main()
