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
            self.assertIn(b"Piero's Intel", body)
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
const side = () => (els["side5"] || {}).innerHTML || "";
drawAll("every tab");
ui.tab = "overview"; for (const b of ["fair", "board"]) { ui.v5.basis = b; draw(); } for (const pv of ["strategy", "market"]) { ui.v5.pv = pv; draw(); }
ui.v5.oexp = true; draw(); ui.v5.oexp = false; ui.v5.hlx = "cash"; draw(); ui.v5.hlx = null; draw(); check("overview toggles");
ui.tab = "live"; for (const c of (S.v5lv && S.v5lv.cards) || []) { ui.v5.card = c.key; draw(); } ui.v5.card = null;
for (const f of ["trades", "alerts", "market", "all"]) { ui.v5.actf = f; draw(); } check("live panel + filters");
ui.tab = "markets"; for (const f of ["hold", "nohold", "g3", "g5", "orders", "g3empty", "all"]) { ui.v5.mkf = f; draw(); }
for (const k of ["race", "gap", "vol24", "orders", "hold", "sig_yes"]) for (const dir of [1, -1]) { ui.v5.mksort = {k, dir}; draw(); }
ui.v5.mkq = "nh-01"; draw(); ui.v5.mkq = ""; ui.v5.mkview = "map"; draw(); ui.v5.mkview = "table"; ui.v5.mkcol = {Senate: true}; draw(); ui.v5.mkcol = {};
const r0 = (S.v5mk_rows || [])[0]; if (r0) { ui.v5.mk = r0.ex; ui.v5.mkd = {book: {src: "feed", bids: [[0.4, 100, 0]], asks: [[0.41, 50, 10]]}, orders: [{status: "open", layer: "tm", side: "yes", price: 0.4, qty: 10}], fills: [{t: "x", layer: "tm", side: "yes", price: 0.4, qty: 1}]}; draw(); ui.v5.mk = null; ui.v5.mkd = null; draw(); }
check("markets filters, sort, map, panel");
ui.tab = "admin"; for (const h of (S.v5ad && S.v5ad.health) || []) { ui.v5.hchk = h.key; draw(); } ui.v5.hchk = null;
ui.v5.confirm = {text: "Touch maker goes Off: its 7 bids are cancelled, its exits keep resting", btn: null}; draw(); if (!(els["modal5"] || {}).innerHTML.includes("Confirm")) fail("confirm box"); ui.v5.confirm = null; draw();
ui.v5.advOpen = true; draw(); ui.v5.advOpen = false; check("admin");
apply({loop: 999, v5mk_rows: {rows: [], gone: []}, events: [{id: 1e9, t: "x", kind: "K", text: "t"}]}, "delta"); drawAll("delta");
const saved = JSON.parse(JSON.stringify(S));
for (const k of ["v5hdr", "v5ov", "v5lv", "v5mk", "v5mk_rows", "v5ad", "leaders4", "leaders", "status", "overview"])
  for (const v of [undefined, null, {}, [], "x", 0]) { if (v === undefined) delete S[k]; else S[k] = v; drawAll(k + " = " + JSON.stringify(v)); S = JSON.parse(JSON.stringify(saved)); }
for (const k of ["v5ov", "v5lv", "v5mk", "v5ad"]) for (const f of Object.keys(saved[k] || {})) { S[k][f] = null; drawAll(k + "." + f + " = null"); S = JSON.parse(JSON.stringify(saved)); }
S = {}; drawAll("empty payload"); for (const t of ["overview", "live", "markets", "admin"]) { ui.tab = t; draw(); }
if (!Object.values(els).some(e => String(e.innerHTML).includes("no sample yet"))) fail("empty payload: no 'no sample yet'");
S = saved; drawAll("restored");
""" + extra + "\nconsole.log('ok');"
    fd, p = tempfile.mkstemp(suffix=".js")
    with os.fdopen(fd, "w") as fh:
        fh.write(f"const DATA = {json.dumps(data)};\n" + stub + js + "\n" + run)
    r = subprocess.run(["node", p], capture_output=True, text=True)
    os.remove(p)
    test.assertEqual(r.stdout.strip().splitlines()[-1:] or [""], ["ok"], r.stderr[-2500:])


def fixture5():
    """A v5 payload (q_dash5.build's sections) with plausible numbers, merged into the v4 fixture (Leaders keeps its renderer)."""
    p = fixture4()
    now = time.time()
    days = []
    for k in range(8):
        d = time.strftime("%Y-%m-%d", time.gmtime(now - (7 - k) * 86400))
        board, real = [-7698.8, 360.2, 2105.0, -1212.5, 3444.0, 870.1, 1310.4, 120.0][k], [-558.7, -1755.2, 820.0, 430.0, 910.0, 40.0, 260.0, 40.0][k]
        days.append(dict(day=d, board=board, realized=real, open=round(board - real, 2), fair=board + 500, fair_open=round(board + 500 - real, 2), partial=k == 7,
                         start=100000 + k * 100, end=100000 + k * 100 + board, start_fair=100000, end_fair=101000))
    rows = []
    for i, (race, party) in enumerate([("NH-01 House race", "D"), ("NH-01 House race", "R"), ("Rhode Island Senate", "D"), ("Rhode Island Senate", "R"), ("Texas Governor", "D"), ("U.S. House", "R")]):
        rows.append(dict(ex=str(900 + i), race=race, party=party, group=("House" if "House race" in race else "National" if race.startswith("U.S.") else "Senate" if "Senate" in race else "Governor"),
                         state="New Hampshire" if race.startswith("NH") else "", title=f"Will the {'Democratic' if party == 'D' else 'Republican'} Party win the {race}?", contract=f"{party} wins",
                         sig_bid=0.45, sig_ask=0.46, sig_yes=0.46, sig_no=0.55, pm_bid=0.49, pm_ask=0.50, pm_yes=0.50, pm_no=0.51, fair=0.495, gap_c=-4.0 + i,
                         hold=dict(side="no", shares=39467, value=33200.5, avg=0.8397, pnl=150.2, subs={"RI Senate R": 7161, "Day-1 and manual holdings": 20144}) if i == 3 else None,
                         orders=8 if i == 3 else 0, vol24=1_200_000 - i * 1000, depth=None, why="" if i == 3 else "no side ≥ 2¢ cheap vs fair"))
    cards = [dict(key=k, name=n, group=g, status=st, reason="", rules="Two sentences.", layers=[k], control=[k] if g == "strategy" else [], cap=c, in_use=u, pnl=pl, pnl_fair=pl + 40,
                  today=12.0, today_fair=20.0, entries=3, exits=2, last_fill_t=now - 120, fills=[dict(t="2026-10-08 17:21:08", name="RI Senate R", side="yes", qty=500, px=0.16, layer=k, realized=5.0)])
             for k, n, g, st, c, u, pl in [("tm", "Touch maker", "strategy", "Pilot", 30000, 8509, 170.0), ("sm", "Spread maker", "strategy", "Gated", 10000, 3625, 42.0),
                                            ("catcher", "Catcher", "strategy", "Live", 24700, 8350, 4660.0), ("sniper", "Sniper, fat-finger", "strategy", "Live", None, 400, -12.0),
                                            ("rotation", "Rotation", "strategy", "Live", None, 0, 55.0), ("pairs", "Pairs", "strategy", "Live", None, 0, 3.0),
                                            ("ri", "RI Senate R", "apart", "Unwinding", 9996, 33140, -900.0), ("walls", "Walls", "apart", "Held", None, 9400, 300.0),
                                            ("held", "Day-1 and manual holdings", "apart", "Held", None, 30000, -2800.0), ("fm", "Fair maker", "retired", "Retired", None, 1600, 0.0),
                                            ("scalp", "Scalper v1", "retired", "Retired", None, 0, 24.0)]]
    for c in cards:
        c["sub"] = {"tm": "Market making", "sm": "Market making", "catcher": "Sweep catching", "sniper": "Sweep catching", "rotation": "Cash management", "pairs": "Cash management"}.get(c["key"])
    tot = round(sum(c["pnl"] for c in cards), 2)
    p.update(
        v5hdr=dict(t=now, status="trading", reason="", loop_age=2),
        v5ov=dict(day_no=8, days_total=35, settle_s=26 * 86400 + 14 * 3600, board=dict(pnl=1375.0, account=101375.0, pct=0.01375), fair=dict(pnl=11157.0, account=111157.0, pct=0.11157),
                  today=dict(board=120.0, realized=40.0, open=80.0, fair=210.0), days=days, account_series=[[now - 3600 * (40 - k), 100000 + 30 * k, 108000 + 70 * k] for k in range(41)],
                  rank=dict(as_of="2026-10-08 19:23:00", total=1341, rank=277, pct=277 / 1341, day=dict(rank=74, total=429, pct=74 / 429),
                            lines=[dict(n=100, pnl=9456.0, pct=100 / 1341, gap_marks=8081.0, gap_fair=-1701.0), dict(n=50, pnl=24992.0, pct=50 / 1341, gap_marks=23617.0, gap_fair=13835.0),
                                   dict(n=25, pnl=53292.0, pct=25 / 1341, gap_marks=51917.0, gap_fair=42135.0)],
                            series=[[now - 3600 * (60 - k), max(277, 1100 - 14 * k)] for k in range(61)]),
                  port=dict(invested=80375.0, available=21000.0, above_floor=16000.0, account=101375.0,
                            slices=[dict(ex=str(900 + i), name=f"Market {i}", side="no", shares=1000 * (10 - i), value=8000.0 - 600 * i, pct=0.07, avg=0.8, mark=0.82, fair=0.95,
                                         pnl_marks=120.0, pnl_fair=900.0, subs={"Catcher": 500, "Day-1 and manual holdings": 500}) for i in range(10)],
                            other=[dict(ex="990", name="Small market", side="yes", shares=100, value=6375.0, pct=0.06, avg=0.4, mark=0.41, fair=0.45, pnl_marks=1.0, pnl_fair=5.0, subs={})],
                            other_value=6375.0, by_strategy=[dict(key="ri", name="RI Senate R", value=40000.0), dict(key="held", name="Day-1 and manual holdings", value=40375.0)]),
                  archive=dict(fills=15000, first="2026-10-01T15:00:00Z", last="2026-10-09T03:00:00Z", cash_replayed=-80000.0, cash_sig=21000.0)),
        v5lv=dict(server=dict(state="trading", reason="", host="engine-host", region="sfo2", provider="DigitalOcean", vcpu=2, mem_gb=3.8, cpu=15.0, mem=22.0, disk=31.0,
                              release="C86c-1741", up_s=8040, mirror_age_s=40, loop_age=1, sig_ms=35.0, pm_age=2, feed=dict(connected=True, mode="log", bpm=34.0),
                              reads=75, read_limit=95, writes=5, write_limit=30, rl_1h=0),
                  cash=dict(pool=21000.0, floor=5000.0, buffer=18021.0, positions=80375.0), cards=cards, check=dict(sum=tot, board=tot, gap=0.0),
                  days={"catcher": [["2026-10-07", 120.0], ["2026-10-08", -30.0]]}, snap_hist=[],
                  activity=[dict(id=f"f{k}", t=now - 60 * k, kind=["trade", "alert", "market"][k % 3], strat="catcher" if k % 3 == 0 else None, text=f"event {k}") for k in range(40)]),
        v5mk=dict(as_of=now, pm_as_of=now - 2, summary=dict(contracts=len(rows), held=1, gap3_empty=2)),
        v5mk_rows=rows,
        v5ad=dict(paused=False, controls=[dict(key="tm", name="Touch maker", layers=["tm"], state="pilot", entries=7, exits=19),
                                          dict(key="sm", name="Spread maker", layers=["sm"], state="pilot", entries=12, exits=9),
                                          dict(key="catcher", name="Catcher", layers=["dump"], state="full", entries=48, exits=21),
                                          dict(key="sniper", name="Sniper, fat-finger", layers=["sniper", "fatfinger"], state="full", entries=3, exits=0),
                                          dict(key="pairs", name="Pairs", layers=["pairs"], state="off", entries=0, exits=0)],
                  health=[dict(key="loop", label="Loop age", value=2, unit="s", threshold="green <= 30 s", dot="green", hist=[[now, 2]]),
                          dict(key="rl", label="Rate-limit errors (1 h)", value=0, unit="", threshold="0", dot="green", hist=[]),
                          dict(key="tb", label="Tracebacks (1 h)", value=1, unit="", threshold="0 green, 1-3 amber", dot="amber", hist=[[now - 60, 0], [now, 1]]),
                          dict(key="io", label="Reads / writes per min", text="75 / 95 · 5 / 30", threshold="under the limits", dot="green", hist=[]),
                          dict(key="audit", label="Order audit (foreign, duplicate, I2)", text="0 · 0 · 0", threshold="all 0", dot="green", hist=[]),
                          dict(key="mirror", label="Mac mirror age", value=40, unit="s", threshold="green <= 180 s", dot="green", hist=[])],
                  alerts=[dict(t="2026-10-08 17:43:20", kind="CONTROL", text="layer swing: full -> off")],
                  advanced=dict(config={"CASH_RESERVE": 5000}, overrides={"paused": False}, releases=["2026-10-08 17:43:20 C86c-1741"], logs=[], rollback=None)))
    return p


class PageTests(unittest.TestCase):
    def test_every_view_renders(self):
        render_views(self, dashboard.render(payload(n=60, loop=5)))

    def test_v5_fixture_renders_every_page_with_its_numbers(self):
        p = fixture5()
        extra = """
ui.tab = "overview"; draw();
if (!out("ovBand").includes("+1,375") || !out("ovBand").includes("+11,157") || !out("ovBand").includes("realized")) fail("overview: the three P&L figures");
if ((out("ovDays").match(/<rect class="b5/g) || []).length < 8) fail("overview: daily bars");
if (!out("ovDays").includes("hatch5")) fail("overview: today hatched");
if (!out("ovRank").includes("#277 / 1,341") || !out("ovRank").includes("#25 line") || !out("ovRank").includes("Gap at fair") || !out("ovRank").includes("19:23")) fail("overview: ranking table");
if (!out("ovRankC").includes("top 25")) fail("overview: rank chart lines");
if (!out("ovPort").includes("101,375") || !out("ovPort").includes("Cash available") || !out("ovPort").includes("Other (1 markets)")) fail("overview: donut + table");
ui.v5.oexp = true; draw(); if (!out("ovPort").includes("Small market")) fail("overview: Other expands"); ui.v5.oexp = false;
ui.tab = "live"; draw();
if (!out("lvStrip").includes("engine-host") || !out("lvStrip").includes("C86c-1741") || !out("lvStrip").includes("75 / 95")) fail("live: server strip");
for (const c of S.v5lv.cards) if (!out("lvPipe").includes('data-card="' + c.key + '"')) fail("live: no card for " + c.key);
if (!out("lvPipe").includes("all strategies + holdings")) fail("live: check line");
S.v5lv.cards[0].last_fill_t += 30; draw(); if (!out("lvPipe").includes("glow5")) fail("live: a fill makes its card glow");
ui.v5.card = "catcher"; draw(); if (!side().includes("Last 20 fills")) fail("live: side panel"); ui.v5.card = null;
if ((out("lvAct").match(/class="r/g) || []).length !== 40) fail("live: 40 activity rows");
ui.tab = "markets"; draw();
if (!out("mkHead").includes("6 contracts") || !out("mkHead").includes("we hold 1")) fail("markets: summary");
ui.v5.mkq = "NH-01"; draw(); if ((out("mkTable").match(/data-mk="9/g) || []).length !== 2) fail("markets: search NH-01 finds both contracts"); ui.v5.mkq = "";
draw(); if (!out("mkTable").includes("NO 39,467")) fail("markets: we hold pill");
ui.tab = "admin"; draw(); if (!out("adHealth").includes("Loop age") || !out("adCtl").includes("Touch maker")) fail("admin");
S.v5hdr = {status: "degraded", reason: "1 rate-limit error in 10 min", loop_age: 3}; draw(); if (!(els["live"] || {}).innerHTML.includes("Degraded")) fail("header amber on rate-limit errors");
ui.tab = "leaders"; draw(); if (!out("lead4").length) fail("leaders");
"""
        render_views(self, dashboard.render(p), extra)


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
            self.assertIn(b"Piero's Intel", body)
            self.assertLess(len(body), 600_000)
        finally:
            h.stop()


if __name__ == "__main__":
    unittest.main()
