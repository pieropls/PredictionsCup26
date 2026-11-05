"""P2 realtime (i_realtime.py): SIG user feed revisions/resync, Polymarket feed parsing, forced disconnect, engine wake.
python3 -m unittest tests.test_realtime"""
import asyncio
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # the repo root (tests are in research/tests)
sys.path[:0] = [os.path.join(ROOT, d) for d in ("engine", "dashboard", "research/tests")]
sys.path.insert(0, ROOT)
import i_realtime as rt  # noqa: E402


def batch(rev, prev, fills=(), updates=()):
    return dict(event="broadcast", topic="realtime:user:me",
                payload=dict(event="account_batch", type="broadcast",
                             payload=dict(fills=list(fills), orderUpdates=list(updates), delivery=dict(revision=rev, previousRevision=prev))))


class SigFeedTests(unittest.TestCase):
    def setUp(self):
        self.ev = []
        self.f = rt.SigUserFeed(lambda: None, lambda k, exs, t: self.ev.append((k, set(exs))), clock=lambda: 100.0, log=lambda *a: None)

    def test_new_batches_trigger_duplicates_are_ignored(self):
        self.assertEqual(self.f.handle(batch(1, 0, fills=[dict(exchangeId=101)])), "batch")
        self.assertEqual(self.f.handle(batch(1, 0, fills=[dict(exchangeId=101)])), None, "duplicate revision")
        self.assertEqual(self.f.handle(batch(2, 1, updates=[dict(exchangeId="102")])), "batch")
        self.assertEqual(self.ev, [("fills", {"101"}), ("orders", {"102"})])
        self.assertEqual(self.f.stats["duplicates"], 1)

    def test_gap_and_reconnect_mean_resync(self):
        self.f.handle(batch(1, 0, fills=[dict(exchangeId=101)]))
        self.assertEqual(self.f.handle(batch(5, 4, fills=[dict(exchangeId=103)])), "resync", "revisions 2-4 missing")
        self.f.connection_lost()
        self.assertEqual([k for k, _ in self.ev], ["fills", "resync", "resync"])
        self.assertEqual(self.f.handle(batch(9, 8, fills=[dict(exchangeId=104)])), "batch", "after a resync: no gap check")

    def test_order_filled_notification_wakes_with_no_market(self):
        m = dict(event="broadcast", payload=dict(event="notification", payload=dict(id="x", type="order_filled")))
        self.assertEqual(self.f.handle(m), "notification")
        self.assertEqual(self.ev, [("fills", set())])

    def test_gap_only_when_previous_revision_is_ahead_and_on_resync_required(self):
        self.f.handle(batch(10, 9, fills=[dict(exchangeId=1)]))
        self.assertEqual(self.f.handle(batch(14, 8, fills=[dict(exchangeId=2)])), "batch", "a merged backlog: not a gap")
        b = batch(15, 14, fills=[dict(exchangeId=3)])
        b["payload"]["payload"]["resyncRequired"] = True
        self.assertEqual(self.f.handle(b), "resync")

    def test_other_messages_are_ignored(self):
        for m in [dict(event="phx_reply", payload={}), dict(event="broadcast", payload=dict(event="other", payload={}))]:
            self.assertIsNone(self.f.handle(m))
        self.assertEqual(self.ev, [])


class FakeWS:
    """An async websocket that replays messages, then drops the connection."""
    def __init__(self, msgs):
        self.msgs, self.sent = list(msgs), []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def send(self, m):
        self.sent.append(json.loads(m))

    async def recv(self):
        if not self.msgs:
            raise ConnectionError("dropped")
        return json.dumps(self.msgs.pop(0))


class DisconnectTests(unittest.TestCase):
    def test_forced_disconnect_resyncs_cleanly(self):
        ev = []
        socks = [FakeWS([batch(1, 0, fills=[dict(exchangeId=1)]), batch(2, 1, fills=[dict(exchangeId=2)])]),
                 FakeWS([batch(7, 6, fills=[dict(exchangeId=3)])])]
        f = rt.SigUserFeed(lambda: dict(token="t", supabaseUrl="https://x", anonKey="k", channels=dict(user="user:me")),
                           lambda k, exs, t: ev.append((k, set(exs))), connect=lambda url: socks.pop(0), log=lambda *a: None)
        f.running = True
        for _ in range(2):
            with self.assertRaises(ConnectionError):
                asyncio.run(f._run_once())
        self.assertEqual([k for k, _ in ev], ["resync", "fills", "fills", "resync", "fills"],
                         "each (re)connect resyncs first; after it, revision 7 is not a gap")
        self.assertEqual(f.stats["reconnects"], 2)


class PolyFeedTests(unittest.TestCase):
    def test_book_price_change_and_candidate_sum(self):
        ev = []
        f = rt.PolyFeed({"101": ["a"], "102": ["b", "c"]}, lambda k, exs, t: ev.append((k, set(exs))), clock=lambda: 5.0)
        f.handle([dict(event_type="book", asset_id="a", bids=[dict(price="0.40", size="10"), dict(price="0.41", size="1")],
                       asks=[dict(price="0.43", size="5")])])
        self.assertEqual(f.quotes["101"][:2], (0.41, 0.43))
        f.handle(dict(event_type="price_change", asset_id="a", price_changes=[dict(asset_id="a", best_bid="0.41", best_ask="0.425")]))
        self.assertEqual(f.quotes["101"][:2], (0.41, 0.425))
        self.assertEqual(ev, [], "a 0.25c move is not a move")
        f.handle(dict(event_type="price_change", price_changes=[dict(asset_id="a", best_bid="0.44", best_ask="0.46")]))
        self.assertEqual(ev, [("pm_move", {"101"})], ">= 1.5c: replan that market")
        f.handle(dict(event_type="book", asset_id="b", bids=[dict(price="0.20", size="1")], asks=[dict(price="0.22", size="1")]))
        self.assertNotIn("102", f.quotes, "a candidate sum needs every leg")
        f.handle(dict(event_type="book", asset_id="c", bids=[dict(price="0.30", size="1")], asks=[dict(price="0.33", size="1")]))
        self.assertEqual([round(x, 3) for x in f.quotes["102"][:2]], [0.5, 0.55])


if __name__ == "__main__":
    unittest.main()
