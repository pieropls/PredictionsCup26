"""Dashboard (g_dashboard.py): token, localhost, SSE snapshot + deltas, controls (confirm, read-only), crash isolation,
and the v4 page (Live, Strategies, Capital, Holdings, Leaders, Admin) against tests/fixtures/dash_v4.json, plus the v3-only
fixture (dash_v3.json) rendering with "not available yet".
python3 -m unittest tests.test_dashboard_server"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import g_dashboard as dashboard  # noqa: E402

try:
    from fastapi.testclient import TestClient
except ImportError:                                         # pragma: no cover
    TestClient = None
NEEDS = unittest.skipUnless(TestClient, "pip3 install --user fastapi uvicorn httpx")
TOKEN = "test-token"


def payload(n=3, loop=1, read_only=False):
    return dict(t="2026-10-02 01:00:00", loop=loop, live=True,
                overview=dict(account_fair=95000 + loop, free_cash=10000, paused=False, read_only=read_only, kappa=0.01, kappa_src="x",
                              reserve=5000, committed=0, realized_today=0, leaderboard=92000, loss_stop=dict(pnl=0, limit=2000, tripped=False)),
                health=dict(sig={}, executor={}, feeds={}), layers=[], positions=[], pairs={}, leaders={}, alerts=[],
                markets=[dict(ex=str(100 + i), title=f"m{i}", race=f"r{i}", party="D", fair=0.5, bid=0.49, ask=0.51) for i in range(n)],
                events=[dict(id=i, t="01:00:00", kind="X", text=f"e{i}") for i in range(loop, 0, -1)])


class FakeControls:
    def __init__(self):
        self.calls = []

    def apply(self, action, args):
        self.calls.append((action, args))
        if action == "boom":
            raise RuntimeError("boom")
        return f"{action}: ok"


def client(get_state, controls=None, host="127.0.0.1"):
    app = dashboard.create_app(get_state, controls or FakeControls(), market_detail=lambda ex: dict(orders=[], fills=[ex]), token=TOKEN)
    return TestClient(app, base_url=f"http://{host}:8765"), app


def sse(c, limit):
    out, ev = [], None
    with c.stream("GET", f"/events?t={TOKEN}&limit={limit}") as r:
        for line in r.iter_lines():
            if line.startswith("event: "):
                ev = line[7:]
            elif line.startswith("data: "):
                out.append((ev, json.loads(line[6:])))
    return out


@NEEDS
class ServerTests(unittest.TestCase):
    def test_every_route_needs_the_token(self):
        c, _ = client(lambda: payload())
        for path in ["/", "/events", "/api/market/100"]:
            self.assertEqual(c.get(path).status_code, 403, path)
            self.assertEqual(c.get(path + "?t=wrong").status_code, 403, path)
        self.assertEqual(c.post("/api/control", json=dict(action="pause")).status_code, 403)
        r = c.get(f"/?t={TOKEN}")
        self.assertEqual(r.status_code, 200)
        self.assertIn('"account_fair":95001', r.text)
        self.assertEqual(c.get(f"/api/market/101?t={TOKEN}").json(), dict(orders=[], fills=["101"]))

    def test_other_hosts_and_origins_are_refused(self):
        c, _ = client(lambda: payload(), host="evil.example")
        self.assertEqual(c.get(f"/?t={TOKEN}").status_code, 403)
        c, _ = client(lambda: payload())
        r = c.post(f"/api/control?t={TOKEN}", json=dict(action="pause"), headers={"Origin": "http://evil.example"})
        self.assertEqual(r.status_code, 403)

    def test_stream_sends_a_snapshot_then_only_what_changed(self):
        states = iter([payload(loop=1), payload(loop=1), payload(loop=2)] + [payload(loop=2)] * 50)
        c, _ = client(lambda: next(states))
        dashboard.SSE_PERIOD_S, old = 0.01, dashboard.SSE_PERIOD_S
        try:
            msgs = sse(c, 2)
        finally:
            dashboard.SSE_PERIOD_S = old
        self.assertEqual(msgs[0][0], "snapshot")
        self.assertEqual(msgs[1][0], "delta")
        d = msgs[1][1]
        self.assertEqual(set(d), {"loop", "overview", "events"}, "markets unchanged: not sent")
        self.assertEqual([e["id"] for e in d["events"]], [2], "only the new event")

    def test_diff_market_rows_and_removals(self):
        a, b = payload(n=3), payload(n=3)
        b["markets"][1] = dict(b["markets"][1], bid=0.40)
        b["markets"] = b["markets"][:2] + [dict(ex="999", title="new")]
        d = dashboard.diff(a, b)
        self.assertEqual([r["ex"] for r in d["markets"]["rows"]], ["101", "999"])
        self.assertEqual(d["markets"]["gone"], ["102"])

    def test_snapshot_is_small_for_a_full_payload(self):
        p = payload(n=240, loop=300)
        for m in p["markets"]:
            m.update(subs=dict(inventory=-1234, manual=0), resting=dict(mm=2, ladder=4), last_fill="01:02 buy YES 100 @ 0.400", u=0.005, mis=0.01)
        self.assertLess(len(dashboard.dumps(p)), 300_000)

    def test_controls_need_confirm_and_respect_read_only(self):
        fc = FakeControls()
        c, _ = client(lambda: payload(), fc)
        post = lambda body: c.post(f"/api/control?t={TOKEN}", json=body)
        self.assertEqual(post(dict(action="nope")).status_code, 400)
        self.assertEqual(post(dict(action="pull_quotes")).status_code, 400, "destructive: needs confirm")
        self.assertEqual(post(dict(action="pull_quotes", confirm="yes")).status_code, 400)
        self.assertEqual(fc.calls, [])
        r = post(dict(action="pull_quotes", confirm=True))
        self.assertEqual(r.json(), dict(ok=True, message="pull_quotes: ok"))
        self.assertEqual(post(dict(action="pause")).status_code, 200, "pause needs no confirm")
        self.assertEqual([a for a, _ in fc.calls], ["pull_quotes", "pause"])
        c2, _ = client(lambda: payload(read_only=True), fc)
        self.assertEqual(c2.post(f"/api/control?t={TOKEN}", json=dict(action="pause")).status_code, 400)
        self.assertEqual(c2.post(f"/api/control?t={TOKEN}", json=dict(action="read_only", args=dict(on=False))).status_code, 200)

    def test_failures_stay_inside_the_dashboard(self):
        def broken():
            raise RuntimeError("engine state exploded")
        c, _ = client(broken)
        r = c.get(f"/?t={TOKEN}")
        self.assertEqual(r.status_code, 200)
        self.assertIn("could not read the engine state", r.text)

        class Bad(FakeControls):
            def apply(self, action, args):
                raise RuntimeError("control exploded")
        c, _ = client(lambda: payload(), Bad())
        r = c.post(f"/api/control?t={TOKEN}", json=dict(action="pause"))
        self.assertEqual(r.status_code, 500)
        self.assertEqual(c.get(f"/?t={TOKEN}").status_code, 200, "still serving")

    def test_static_render_has_no_token_and_no_controls(self):
        html = dashboard.render(payload())
        self.assertIn("const LIVE = false", html)
        self.assertIn('TOKEN = ""', html)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@NEEDS
class ThreadTests(unittest.TestCase):
    def test_binds_localhost_streams_and_stops(self):
        port = free_port()
        app = dashboard.create_app(lambda: payload(), FakeControls(), token=TOKEN)
        h = dashboard.serve_in_thread(app, port=port)
        try:
            self.assertTrue(h.started)
            t = time.time()
            body = urllib.request.urlopen(f"http://127.0.0.1:{port}/?t={TOKEN}", timeout=5).read()
            self.assertLess(time.time() - t, 2.0, "page loads in under 2 s")
            self.assertIn(b"SIG engine", body)
            self.assertEqual(h.url, f"http://127.0.0.1:{port}/?t={TOKEN}")
        finally:
            h.stop()
        self.assertFalse(h.thread.is_alive())

    def test_refuses_to_bind_beyond_localhost(self):
        with self.assertRaises(ValueError):
            dashboard.serve_in_thread(dashboard.create_app(lambda: {}, FakeControls()), host="0.0.0.0")


V3_KEYS = ["money", "daily", "series", "capital", "strategies", "holdings", "activity"]
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "dash_v3.json")


def fixture():
    with open(FIXTURE) as fh:
        return json.load(fh)


def fixture4():
    """A v4 payload written by tests.test_engine.DashV4PayloadTests (DASH_FIXTURE=1): the fake exchange after an MM fill."""
    with open(os.path.join(ROOT, "tests", "fixtures", "dash_v4.json")) as fh:
        return json.load(fh)


def v2_only(p):
    """The same payload as an engine before the v3 upgrade: no v3 keys, no leader thresholds."""
    p = {k: v for k, v in p.items() if k not in V3_KEYS}
    p["leaders"] = {k: v for k, v in (p.get("leaders") or {}).items() if k != "thresholds"}
    return p


def render_views(test, html, extra=""):
    """Run the page's JavaScript in node with a minimal DOM: draw every tab and every block, sort and filter the holdings,
    filter the activity, open a market, apply a delta, then drop or null each v3 key in turn. Any exception inside a block,
    or a block that drew nothing, fails the test. `extra` runs last (more checks)."""
    if not shutil.which("node"):
        return
    import re
    js = re.findall(r"<script>(.*?)</script>", html, re.S)[0]
    data = re.findall(r'<script id="data" type="application/json">(.*?)</script>', html, re.S)[0]
    stub = """
const els = {}; const mk = id => els[id] || (els[id] = {id, textContent: id === "data" ? DATA : "", innerHTML: "", value: "", style: {},
  classList: {toggle(){}, add(){}, remove(){}, contains(){ return false; }}, focus(){}, querySelectorAll(){ return []; }, dataset: {},
  setAttribute(){}, removeAttribute(){}});
const document = {getElementById: mk, body: {classList: {toggle(){}}}, documentElement: {setAttribute(){}, removeAttribute(){}},
  addEventListener(){}, activeElement: null, querySelector(){ return null; }, querySelectorAll(){ return []; }};
const EventSource = function(){ this.addEventListener = () => {}; }; const setInterval = () => 0; const fetch = async () => ({json: async () => ({})});
"""
    run = """
const fail = what => { throw new Error(what + (ui.errors.length ? ": " + ui.errors.join(" | ") : "")); };
const check = what => { if (ui.errors.length) fail(what); };
const drawAll = what => { for (const t of Object.keys(TABS)) { ui.tab = t; draw(); for (const id of Object.keys(BLOCKS[t]))
  if (!OPTIONAL.has(id) && !(els["b-" + id] && els["b-" + id].innerHTML.length)) fail(what + ": empty " + t + "/" + id); } check(what); };
const out = id => (els["b-" + id] || {}).innerHTML || "";
drawAll("every tab");
ui.tab = "holdings";
for (const c of H4) for (const dir of [1, -1]) { ui.hsort = {k: c.k, dir}; draw(); }
ui.hsort = {k: "value_sig", dir: -1};
for (const f of ["dump", "settle", "noexit", ""]) { ui.hf = f; draw(); }
ui.hall = true; draw(); ui.hall = false; draw(); check("holdings filters");
ui.tab = "live"; for (const f of ["trade", "alert", "market", ""]) { ui.feed = f; draw(); } check("feed filters");
ui.tab = "strategies"; ui.sopen = "dump"; draw(); ui.sopen = null; draw(); check("strategy drawer");
ui.tab = "admin"; ui.q = "a"; ui.layer = "mm"; ui.haspos = true; ui.mis = "1"; draw(); ui.q = ""; ui.layer = ""; ui.haspos = false; ui.mis = "";
for (const t of ["admin", "holdings"]) {
  ui.tab = t; const ex = ((t === "admin" ? S.markets : (S.holdings4 || S.holdings)) || [])[0]; if (!ex) continue;
  ui.open = ex.ex; ui.detail = {ex: ex.ex, orders: [{placed: "x", bot: "b", price: 0.4, qty: 1}], fills: [{t: "x", price: 0.4, qty: 1}]}; draw(); ui.open = null; ui.detail = null; draw();
}
check("market detail");
apply({loop: 999, markets: {rows: [], gone: []}, holdings: {rows: [], gone: []}, holdings4: {rows: [], gone: []}, events: [{id: 1e9, t: "x", kind: "K", text: "t"}],
       feed: [{id: "f999999", t: "2026-10-02 23:59:59", kind: "trade", layer: "mm", text: "DELTA-FILL"}]}, "delta");
ui.tab = "admin"; draw();
if (!out("events").includes("K")) fail("event delta not applied");
ui.tab = "live"; draw();
if (Array.isArray(S.feed) && !out("feed").includes("DELTA-FILL")) fail("feed delta not applied");
const saved = JSON.parse(JSON.stringify(S));
for (const k of ["money", "daily", "series", "capital", "strategies", "holdings", "activity", "leaders", "overview", "health", "layers", "markets", "pairs", "positions", "events",
                 "sigday", "status", "daily4", "feed", "strategies4", "capital4", "holdings4", "leaders4", "admin4"])
  for (const v of [undefined, null, {}, [], "x", 0]) { if (v === undefined) delete S[k]; else S[k] = v; drawAll(k + " = " + JSON.stringify(v)); S = JSON.parse(JSON.stringify(saved)); }
for (const k of ["money", "capital", "sigday", "status", "capital4", "leaders4", "admin4"]) for (const f of Object.keys(saved[k] || {})) { S[k][f] = null; drawAll(k + "." + f + " = null"); S = JSON.parse(JSON.stringify(saved)); }
S = {}; drawAll("empty payload"); S = saved; drawAll("restored");
""" + extra + "\nconsole.log('ok');"
    fd, p = tempfile.mkstemp(suffix=".js")
    with os.fdopen(fd, "w") as fh:
        fh.write(f"const DATA = {json.dumps(data)};\n" + stub + js + "\n" + run)
    r = subprocess.run(["node", p], capture_output=True, text=True)
    os.remove(p)
    test.assertEqual(r.stdout.strip().splitlines()[-1:] or [""], ["ok"], r.stderr[-2500:])


class PageTests(unittest.TestCase):
    def test_every_view_renders(self):
        render_views(self, dashboard.render(payload(n=60, loop=5)))

    def test_v4_fixture_renders_every_tab_sorts_and_filters(self):
        p = fixture4()
        h = p["holdings4"]
        self.assertGreaterEqual(len(h), 3)
        by_unreal = [r["ex"] for r in sorted(h, key=lambda r: r["unreal_sig"])]
        by_market = [r["ex"] for r in sorted(h, key=lambda r: r["market"].lower())]
        n_dump = sum(r["layer"] == "dump" for r in h)
        n_noexit = sum(not r["exit"] for r in h)
        n_trade = sum(a["kind"] == "trade" for a in p["feed"][:30])
        today = p["sigday"]["today"]
        extra = f"""
const rowsOf = html => [...html.matchAll(/<tr class="click" data-ex="([^"]+)"/g)].map(m => m[1]);
ui.tab = "live"; draw();
for (const id of ["liveTiles", "daily4", "feed"]) if (out(id).includes("not available")) fail("v4 fixture: " + id + " says not available");   // the account chart needs 2+ pnl samples
if (!out("daily4").includes("<svg")) fail("daily4: no chart");
if (!out("liveTiles").includes({json.dumps(f"{p['money']['account_sig']:,.0f}")})) fail("leaderboard value tile missing");
if (!out("liveTiles").includes({json.dumps(("+" if today > 0 else "") + f"{today:,.0f}")})) fail("today tile missing: " + out("liveTiles"));
if (!out("liveTiles").includes("SIG day")) fail("today tile not labelled as the SIG day");
if (!out("feed").includes("k-trade")) fail("feed: no trade chip");
ui.feed = "trade"; draw(); const tr = [...out("feed").matchAll(/<tr data-id=/g)].length, wantTr = S.feed.filter(a => a.kind === "trade").slice(0, 30).length;
if (tr !== wantTr || wantTr < {n_trade}) fail("feed trade filter: " + tr + " rows, want " + wantTr);
if (out("feed").includes("k-alert") || out("feed").includes("k-market")) fail("feed trade filter shows other kinds");
ui.feed = ""; draw();
ui.tab = "strategies"; draw();
if (out("strat4").includes("not available")) fail("strategies say not available");
if (!out("strat4").includes("Market drift")) fail("strategies: no market drift row");
if (!out("strat4").includes("<tfoot")) fail("strategies: no total row");
const foot = out("strat4").slice(out("strat4").indexOf("<tfoot"));
if (!foot.includes({json.dumps(("+" if today > 0 else "") + f"{today:,.0f}")})) fail("strategies total is not today's P&L: " + foot.replace(/<[^>]+>/g, " "));
ui.sopen = "mm"; draw(); if (!out("strat4").includes("Last 10 trades")) fail("strategy drawer"); ui.sopen = null;
ui.tab = "capital"; draw();
if (out("cap4").includes("not available yet")) fail("capital says not available");
if (!out("cap4").includes("sbar")) fail("capital: no scale bar");
if (!out("cap4").includes({json.dumps(f"{p['capital4']['holdings']:,.0f}")})) fail("capital: holdings total missing");
ui.tab = "holdings"; ui.hall = true; draw();
if (!out("hold4").includes({json.dumps(f"{p['capital4']['holdings']:,.0f}")})) fail("holdings total differs from the capital page");
ui.hsort = {{k: "unreal_sig", dir: 1}}; draw();
if (JSON.stringify(rowsOf(out("hold4"))) !== JSON.stringify({json.dumps(by_unreal)})) fail("sort by P&L at SIG ascending");
ui.hsort = {{k: "market", dir: 1}}; draw();
if (rowsOf(out("hold4"))[0] !== {json.dumps(by_market[0])}) fail("sort by market");
ui.hsort = {{k: "value_sig", dir: -1}};
ui.hf = "dump"; draw(); if (rowsOf(out("hold4")).length !== {n_dump}) fail("dump filter: " + rowsOf(out("hold4")).length);
ui.hf = "noexit"; draw(); if (rowsOf(out("hold4")).length !== {n_noexit}) fail("no-exit filter: " + rowsOf(out("hold4")).length);
ui.hf = ""; ui.hall = false; draw();
if (rowsOf(out("hold4")).length !== Math.min(HOLD_FIRST, {len(h)})) fail("holdings: first rows");
ui.tab = "leaders"; draw(); if (!out("lead4").includes("Overall") || !out("lead4").includes("SIG day")) fail("leaders: two halves");
for (const z of Object.keys(ZOOM)) {{ ui.tab = z === "spark" ? "capital" : "live"; ui.zoomId = z; draw(); if (!out("ovl").length) fail("zoom overlay empty: " + z); for (const [k, sec] of RANGES) {{ ui.range = sec; draw(); }} ui.range = 0; ui.zoomWin = [0, 4e9]; draw(); ui.zoomWin = null; }}
ui.zoomId = null; draw(); check("zoom overlay");
ui.tab = "leaders"; S.leaders4.jumps = [{{t: "2026-10-05 09:12:00", name: "clever-otter", rank: 3, gain: 2400, trades: 2, likely: [{{market: "RI Senate D", who: "YES seller", px: "0.84-0.87", shares: 112000, value: 14000, caught: 956, why_not: "levels below the sweep"}}]}}]; draw();
if (!out("lead4").includes("clever-otter") || !out("lead4").includes("levels below the sweep")) fail("leaders: jump table");
ui.tab = "admin"; draw();
if (!out("admin4").includes("✓")) fail("admin: reconciliation check not shown as ok");
if (out("admin4").includes("nok")) fail("admin: red check although ok");
if (!out("admin4").includes("Coverage")) fail("admin: coverage missing");
if (!out("ctlbar").length) fail("admin: controls missing");
S.sigday.ok = false; S.sigday.check = 312; S.admin4.recon = {{ok: false, check: 312, today: 500, sum: 100, drift: 88}}; draw();
if (!out("admin4").includes("nok") || !out("admin4").includes("off by")) fail("admin: gap over 25 not red");
if (document.body.innerHTML && document.body.innerHTML.includes("alert")) fail("warning banner");
"""
        render_views(self, dashboard.render(p), extra)

    def test_v3_only_payload_renders_with_not_available_yet(self):
        p = fixture()
        extra = f"""
ui.tab = "live"; draw();
if (!out("daily4").includes("not available yet")) fail("daily4 should say not available yet");
if (!out("liveTiles").includes({json.dumps(f"{p['money']['account_sig']:,.0f}")})) fail("account value falls back to money.account_sig");
if (!out("liveTiles").includes("no sample at 17:00")) fail("today tile should say no sample yet");
ui.tab = "strategies"; draw(); if (!out("strat4").includes("not available yet")) fail("strategies should say not available yet");
ui.tab = "holdings"; draw(); if (!out("hold4").includes("<table")) fail("holdings fall back to the v3 rows");
ui.tab = "leaders"; draw(); if (!out("lead4").length) fail("leaders fall back to the v3 view");
"""
        render_views(self, dashboard.render(p), extra)

    def test_status_bar_paused_and_degraded(self):
        p = fixture4()
        extra = """
S.status = {kind: "PAUSED", text: "SIG writes failing · since 12:00 · resumes by itself when the condition clears"}; draw();
const bar = () => (els["live"] || {}).innerHTML || "";
if (!bar().includes("PAUSED") || !bar().includes("resumes by itself")) fail("paused status bar: " + bar());
S.status = {kind: "DEGRADED", text: "SIG slow: 6 × 429 in 10 min"}; draw();
if (!bar().includes("DEGRADED") || !bar().includes("429")) fail("degraded status bar: " + bar());
S.status = {kind: "TRADING", text: "last loop 3 s ago · 412 orders resting · C53"}; draw();
if (!bar().includes("TRADING") || !bar().includes("C53")) fail("trading status bar: " + bar());
"""
        render_views(self, dashboard.render(p), extra)

    def test_fixture_snapshot_is_small(self):
        self.assertLessEqual(len(dashboard.dumps(fixture())), 300_000)
        self.assertLessEqual(len(dashboard.dumps(fixture4())), 300_000)


class V3DiffTests(unittest.TestCase):
    def test_holdings_rows_by_ex_with_removals(self):
        a = fixture()
        b = json.loads(json.dumps(a))
        b["holdings"][3] = dict(b["holdings"][3], shares=b["holdings"][3]["shares"] + 100)
        gone = b["holdings"].pop(5)["ex"]
        b["holdings"].append(dict(b["holdings"][0], ex="new-1"))
        d = dashboard.diff(a, b)
        self.assertEqual(sorted(r["ex"] for r in d["holdings"]["rows"]), sorted([b["holdings"][3]["ex"], "new-1"]))
        self.assertEqual(d["holdings"]["gone"], [gone])
        self.assertNotIn("markets", d)
        self.assertEqual(dashboard.diff(a, json.loads(json.dumps(a))), {}, "nothing changed: nothing sent")

    def test_activity_sends_only_new_ids(self):
        a = fixture()
        b = json.loads(json.dumps(a))
        top = max(x["id"] for x in a["activity"])
        new = [dict(id=top + 2, t="2026-10-02 13:10:00", layer="mm", text="b"), dict(id=top + 1, t="2026-10-02 13:09:00", layer="sniper", text="a")]
        b["activity"] = (new + b["activity"])[:300]
        d = dashboard.diff(a, b)
        self.assertEqual([x["id"] for x in d["activity"]], [top + 2, top + 1])
        empty = dict(a, activity=[])
        self.assertEqual(len(dashboard.diff(empty, dict(a, activity=[dict(id=0, t="x", layer="mm", text="first")]))["activity"]), 1, "id 0 counts")

    def test_sections_appearing_changing_or_disappearing(self):
        a, b = v2_only(fixture()), fixture()
        d = dashboard.diff(a, b)
        for k in V3_KEYS:
            self.assertIn(k, d)
        self.assertIsInstance(d["holdings"], list, "no rows before: the whole list")
        c = dict(b, money=dict(b["money"], account_sig=1.0))
        self.assertEqual(set(dashboard.diff(b, c)), {"money"})
        self.assertEqual(dashboard.diff(b, v2_only(b))["money"], None, "a section the engine stops sending becomes null")


@NEEDS
class V3ServerTests(unittest.TestCase):
    def test_stream_and_load_time_with_the_v3_fixture(self):
        p = fixture()
        q = json.loads(json.dumps(p))
        q["loop"] += 1
        q["holdings"][0] = dict(q["holdings"][0], shares=1.0)
        q["activity"] = [dict(id=10 ** 6, t="2026-10-02 13:10:00", layer="mm", text="new fill")] + q["activity"][:299]
        states = iter([p, p, q] + [q] * 50)
        c, _ = client(lambda: next(states))
        t = time.time()
        r = c.get(f"/?t={TOKEN}")
        self.assertEqual(r.status_code, 200)
        self.assertLess(time.time() - t, 1.0, "page loads in under 1 s")
        dashboard.SSE_PERIOD_S, old = 0.01, dashboard.SSE_PERIOD_S
        try:
            msgs = sse(c, 2)
        finally:
            dashboard.SSE_PERIOD_S = old
        self.assertEqual([m[0] for m in msgs], ["snapshot", "delta"])
        d = msgs[1][1]
        self.assertEqual(set(d), {"loop", "holdings", "activity"})
        self.assertEqual([r["ex"] for r in d["holdings"]["rows"]], [p["holdings"][0]["ex"]])
        self.assertEqual([x["id"] for x in d["activity"]], [10 ** 6])

    def test_real_server_serves_the_v3_page_in_under_a_second(self):
        port = free_port()
        p = fixture()
        h = dashboard.serve_in_thread(dashboard.create_app(lambda: p, FakeControls(), token=TOKEN), port=port)
        try:
            self.assertTrue(h.started)
            t = time.time()
            body = urllib.request.urlopen(f"http://127.0.0.1:{port}/?t={TOKEN}", timeout=5).read()
            self.assertLess(time.time() - t, 1.0)
            self.assertIn(b"Account value", body)
            self.assertLess(len(body), 400_000)
        finally:
            h.stop()


if __name__ == "__main__":
    unittest.main()
