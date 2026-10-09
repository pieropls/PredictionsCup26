"""R. DASHBOARD v5 UI (docs/dashboard_v5.md §1-§4 + the Admin follow-up): the CSS and the page code of Overview · Live · Markets ·
Admin, injected into g_dashboard.PAGE. Leaders keeps its v4 renderer. Data comes only from the payload sections v5hdr, v5ov,
v5lv, v5mk, v5mk_rows and v5ad (q_dash5.build)."""

CSS5 = r"""
:root{--ink:#0E171E;--page:#F4F7F9;--panel:#FFFFFF;--rule:#DCE4EA;--sigb:#0862A6;--steel:#6A8FA3;--gain:#1F8A5B;--loss:#C0392B;--amber:#B7791F;
--bg:var(--page);--card:var(--panel);--line:var(--rule);--acc:var(--sigb);--good:var(--gain);--bad:var(--loss);--ink2:#3B4C59;--mute:#62757F;--chip:#EAF0F4;--axis:#B9C7D1}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--ink:#E6EEF3;--page:#0E171E;--panel:#142230;--rule:#24384A;--sigb:#3E8FD0;--steel:#8FA9B8;--gain:#3CB37A;--loss:#E06A5F;--amber:#E0A84A;
--bg:var(--page);--card:var(--panel);--line:var(--rule);--acc:var(--sigb);--good:var(--gain);--bad:var(--loss);--ink2:#C2D1DB;--mute:#8FA3B1;--chip:#1B2D3D;--axis:#33495C}}
:root[data-theme=dark]{--ink:#E6EEF3;--page:#0E171E;--panel:#142230;--rule:#24384A;--sigb:#3E8FD0;--steel:#8FA9B8;--gain:#3CB37A;--loss:#E06A5F;--amber:#E0A84A;
--bg:var(--page);--card:var(--panel);--line:var(--rule);--acc:var(--sigb);--good:var(--gain);--bad:var(--loss);--ink2:#C2D1DB;--mute:#8FA3B1;--chip:#1B2D3D;--axis:#33495C}
body{font:14px/1.45 Archivo,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--page);color:var(--ink)}
.n,td.n,th.n,.big5,.fig .v,.num{font-variant-numeric:tabular-nums}
header{background:var(--panel);border-bottom:1px solid var(--rule);padding:8px 24px}
header h1{font-size:15px;font-weight:600;margin:0 14px 0 8px}
.logo{background:#fff;border-radius:4px;padding:4px 7px;display:inline-flex;align-items:center;border:1px solid var(--rule)}.logo img{height:24px;width:auto;display:block}
nav button{border-radius:6px;font-weight:500}nav button.on{background:var(--sigb);border-color:var(--sigb)}
main{max-width:1240px;margin:0 auto;padding:24px}
.card{border-radius:6px;box-shadow:none}
.p5{background:var(--panel);border:1px solid var(--rule);border-radius:6px;padding:16px;min-width:0}
.p5 h3{font-size:15px;font-weight:600;color:var(--ink);margin:0 0 10px}
.lab5{font-size:13px;font-weight:500;color:var(--mute)}
.big5{font-size:44px;font-weight:700;line-height:1.05;letter-spacing:-.01em;margin:4px 0}
.sub5{font-size:13px;color:var(--mute)}.sub5 b{color:var(--ink);font-weight:600}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:16px}.g2>*{min-width:0}.g2.port5{grid-template-columns:auto 1fr;align-items:start}.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:0}
.sec5{margin:0 0 16px}
.hero5{position:relative;height:240px;border-radius:6px 6px 0 0;overflow:hidden;background:#0E171E}
.hero5 img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;object-position:right center}
.hero5:after{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(8,14,20,.78) 0%,rgba(8,14,20,.45) 45%,rgba(8,14,20,0) 70%);pointer-events:none}
.hero5 .txt{z-index:1;position:absolute;left:28px;top:0;bottom:0;display:flex;flex-direction:column;justify-content:center;max-width:62%}
.hero5 h2{color:#fff;font-size:48px;font-weight:700;margin:0;letter-spacing:-.01em;text-shadow:0 1px 8px rgba(0,0,0,.55)}
.hero5 .s{color:#C9D6DF;font-size:16px;margin-top:6px;text-shadow:0 1px 6px rgba(0,0,0,.6)}
.band5{border:1px solid var(--rule);border-top:0;border-radius:0 0 6px 6px;background:var(--panel)}
.band5 .fig{padding:16px 20px;border-right:1px solid var(--rule)}.band5 .fig:last-child{border-right:0}
.seg5{display:inline-flex;border:1px solid var(--rule);border-radius:6px;overflow:hidden;margin-left:auto}.seg5 button{border:0;background:var(--panel);color:var(--mute);padding:4px 10px;font:inherit;font-size:13px;cursor:pointer}
.seg5 button.on{background:var(--sigb);color:#fff}
.h5{display:flex;align-items:center;gap:8px;margin:0 0 10px}.h5 h3{margin:0}
table.t5{width:100%;border-collapse:collapse;background:transparent}table.t5 th{font-size:13px;font-weight:500;color:var(--mute);text-align:left;padding:6px 8px;border-bottom:1px solid var(--rule);white-space:nowrap}
table.t5 td{padding:7px 8px;border-bottom:1px solid var(--rule);white-space:nowrap}table.t5 tr:last-child td{border-bottom:0}table.t5 .n{text-align:right}
table.t5 tr.hl td{background:var(--chip)}table.t5 tr.cl{cursor:pointer}table.t5 tr.cl:hover td{background:var(--chip)}
.wrap5{overflow-x:auto;max-width:100%}
.dot5{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;vertical-align:0}.dot5.green{background:var(--gain)}.dot5.amber{background:var(--amber)}.dot5.red{background:var(--loss)}
.strip5{background:var(--panel);border:1px solid var(--rule);border-radius:6px;padding:10px 16px;font-size:13px;color:var(--mute);line-height:1.7}.strip5 b{color:var(--ink);font-weight:600}
.pipe5{display:grid;grid-template-columns:150px 140px 1fr 150px;gap:14px;align-items:center}
.src5,.eng5,.pos5{background:var(--panel);border:1px solid var(--rule);border-radius:6px;padding:10px 12px;font-size:13px}
.cards5{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:10px}
.sc5{background:var(--panel);border:1px solid var(--rule);border-radius:6px;padding:12px 14px;cursor:pointer;position:relative}
.sc5 .nm{font-weight:600;font-size:15px}.sc5 .pl{font-size:26px;font-weight:700;margin:6px 0 0}.sc5 .meta{font-size:12.5px;color:var(--mute);margin-top:6px}
.sc5 .flow{position:absolute;left:-14px;top:50%;height:0;border-top:solid var(--steel)}
.bar5{height:5px;background:var(--chip);border-radius:3px;overflow:hidden;margin-top:8px}.bar5 i{display:block;height:100%;background:var(--sigb)}
.glow5{animation:glow5 1s ease-out 1}@keyframes glow5{0%{box-shadow:0 0 0 3px var(--sigb)}100%{box-shadow:0 0 0 0 transparent}}
.chip5{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;font-weight:500;margin-left:6px;background:var(--chip);color:var(--ink2)}
.chip5.Live{background:var(--gain);color:#fff}.chip5.Gated{background:var(--steel);color:#fff}.cgl5{font-size:12px;font-weight:600;color:var(--mute);text-transform:uppercase;letter-spacing:.04em;margin:2px 0 6px}.cg5+.cg5{margin-top:10px}.chip5.Pilot{background:var(--amber);color:#1d1400}.chip5.Off{background:var(--rule);color:var(--mute)}.chip5.Unwinding{background:var(--sigb);color:#fff}
.chip5.trade{background:var(--sigb);color:#fff}.chip5.alert{background:var(--loss);color:#fff}.chip5.market{background:var(--steel);color:#fff}
.act5 .r{display:flex;gap:10px;padding:6px 0;border-bottom:1px solid var(--rule);font-size:13.5px}.act5 .r .tm{color:var(--mute);min-width:44px}
.act5 .new{animation:slide5 .4s ease-out}@keyframes slide5{from{transform:translateY(-6px);opacity:0}to{transform:none;opacity:1}}
.side5{position:fixed;top:0;right:0;bottom:0;width:min(520px,100%);background:var(--panel);border-left:1px solid var(--rule);z-index:26;overflow:auto;padding:18px 20px}
.side5 .x{float:right}
.fil5{display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin:0 0 10px}.fil5 button{border:1px solid var(--rule);background:var(--panel);color:var(--ink2);border-radius:14px;padding:3px 10px;font:inherit;font-size:13px;cursor:pointer}
.fil5 button.on{background:var(--sigb);color:#fff;border-color:var(--sigb)}
.wrap5.mk5{max-height:72vh;overflow:auto}.mk5 th{position:sticky;top:0;background:var(--panel);z-index:1}.mk5 td.race{white-space:normal;min-width:150px;font-weight:500}.mk5 .grp td{background:var(--chip);font-weight:600;cursor:pointer}
.pill5{display:inline-block;padding:1px 8px;border-radius:10px;background:var(--sigb);color:#fff;font-size:12.5px}
.why5{white-space:normal;min-width:180px;max-width:300px;color:var(--mute);font-size:12.5px}
.tiles5{display:grid;grid-template-columns:repeat(auto-fill,minmax(64px,1fr));gap:6px}.tile5{height:44px;border-radius:5px;border:3px solid var(--rule);font-size:10.5px;padding:2px 4px;overflow:hidden;cursor:pointer;line-height:1.2}
.modal5{position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:40;display:flex;align-items:center;justify-content:center}.modal5 .b{background:var(--panel);border:1px solid var(--rule);border-radius:6px;padding:20px;max-width:440px;margin:16px}
.btn5{border:1px solid var(--rule);background:var(--panel);color:var(--ink);border-radius:6px;padding:5px 12px;font:inherit;cursor:pointer}.btn5.pri{background:var(--sigb);color:#fff;border-color:var(--sigb)}.btn5.dng{background:var(--loss);color:#fff;border-color:var(--loss)}
.na5{color:var(--mute);font-style:italic;padding:10px 0}
svg .b5r{fill:var(--sigb)}svg .b5o{fill:var(--sigb);opacity:.42}svg .b5rn{fill:var(--loss)}svg .b5on{fill:var(--loss);opacity:.42}
@media (max-width:900px){.g2,.g2.port5{grid-template-columns:1fr}.pipe5{grid-template-columns:1fr}.sc5 .flow{display:none}}
@media (max-width:640px){main{padding:16px}header{padding:6px 16px;flex-wrap:wrap;gap:6px}header .logo img{height:22px}header h1{display:none}
header nav{order:3;flex:1 0 100%;display:flex;flex-wrap:nowrap;overflow-x:auto;gap:4px;-webkit-overflow-scrolling:touch}header nav button{flex:0 0 auto;padding:4px 10px}
header #live{font-size:12px}
.mk5 tr.cl{display:flex;flex-wrap:wrap}.mk5 tr.cl td{flex:0 0 25%;box-sizing:border-box}.mk5 tr.cl td.race,.mk5 tr.cl td.ct,.mk5 tr.cl td.why5{flex-basis:100%;max-width:none}
.mk5 tr.cl td.hold5{flex-basis:50%}.mk5 tr.cl td.race{font-size:15px;padding-top:4px}.mk5 tr.cl td.ct{font-weight:600}
.mk5 tr.cl td[data-l]:before{display:block;font-size:11px}.mk5 tr.cl td[data-l=""]:before{display:none}.wrap5.mk5{max-height:none;overflow:visible}.hero5{height:160px}.hero5 h2{font-size:30px}.hero5 .s{font-size:13px}.hero5 .txt{left:16px;max-width:80%}
.g3{grid-template-columns:1fr}.band5 .fig{border-right:0;border-bottom:1px solid var(--rule)}.big5{font-size:34px}
.mk5 table,.mk5 thead,.mk5 tbody,.mk5 tr,.mk5 td{display:block}.mk5 thead{display:none}.mk5 tr{border-bottom:1px solid var(--rule);padding:6px 0}.mk5 td{border:0;padding:2px 4px;white-space:normal}
.mk5 td.n{text-align:left}.mk5 td[data-l]:before{content:attr(data-l) " ";color:var(--mute);font-size:12px}}
"""

JS5 = r"""
// ============================================================================ v5 pages (docs/dashboard_v5.md)
const V = k => obj(S[k]);
const k1 = x => (x = val(x)) == null ? "–" : Math.abs(x) >= 1000 ? num(x / 1000, 1) + "k" : num(x);
const sg1 = x => (x = val(x)) == null ? "–" : (x > 0 ? "+" : "") + k1(x);
const cents = x => (x = val(x)) == null ? "–" : (x > 0 ? "+" : "") + num(x, 1) + "¢";
const na5 = t => `<div class="na5">${esc(t || "no sample yet")}</div>`;
const tsAgo = t => val(t) == null ? "–" : ago(t);                 // t = epoch seconds
const cardTip = h => tipAttr(h);
ui.v5 = Object.assign({basis: "board", pv: "market", oexp: false, hlx: null, mkq: "", mkf: "all", mkg: "", mkview: "table", mksort: {k: "gap", dir: -1}, mkcol: {},
  card: null, mk: null, mkd: null, actf: "all", acts: "", seen: {}, glowT: {}, hchk: null, confirm: null, advOpen: false}, ui.v5 || {});

function hdr5() {
  const h = V("v5hdr"), st = h.status || "trading", cls = st === "trading" ? "green" : st === "paused" ? "red" : "amber";
  const lab = st === "trading" ? "Trading" : st === "paused" ? "Paused" : "Degraded";
  return `<span class="dot5 ${cls}"></span><b>${lab}</b>${h.reason ? " · " + esc(h.reason) : ""}, last loop ${h.loop_age == null ? "–" : h.loop_age + " s"}`;
}

// ------------------------------------------------------------------------------------------- Overview
function ov_hero() {
  const o = V("v5ov"), d = Math.floor((o.settle_s || 0) / 86400), h = Math.floor(((o.settle_s || 0) % 86400) / 3600);
  return `<div class="hero5"><img alt="" src="${CUP_SRC}"><div class="txt"><h2>Piero's Intel</h2><div class="s">Susquehanna Predictions Cup, midterm elections 2026</div>
    <div class="s">Day ${o.day_no || "–"} of ${o.days_total || 35}. Settles Nov 4, 09:00 PT: ${d} days ${h} h left</div></div></div>`;
}
function ov_band() {
  const o = V("v5ov"), b = obj(o.board), f = obj(o.fair), t = obj(o.today);
  if (!o.board) return `<div class="band5">${na5()}</div>`;
  const fig = (lab, big, l1, l2) => `<div class="fig"><div class="lab5">${lab}</div><div class="big5 ${cls(big)}">${sgn(big)}</div><div class="sub5">${l1}</div>${l2 ? `<div class="sub5">${l2}</div>` : ""}</div>`;
  return `<div class="band5 g3">${fig("Leaderboard P&L", b.pnl, `account <b>${num(b.account)}</b> (${sgn(b.pct * 100, 1)}% since Oct 1)`)}
    ${fig("P&L at Polymarket fair", f.pnl, `account <b>${num(f.account)}</b> (${sgn(f.pct * 100, 1)}% since Oct 1)`)}
    ${fig("Today (SIG day)", t.board, `realized <b class="${cls(t.realized)}">${sgn(t.realized)}</b>, open positions <b class="${cls(t.open)}">${sgn(t.open)}</b>`, `at fair: <b class="${cls(t.fair)}">${sgn(t.fair)}</b>`)}</div>`;
}
function ov_days() {
  const o = V("v5ov"), days = arr(o.days).filter(d => d && d.board != null);
  if (!days.length) return na5(o.days_error ? String(o.days_error.error || o.days_error) : "no sample yet");
  const fair = ui.v5.basis === "fair", W = width("ovDays"), H = 230, L = 46, R = 8, T = 18, B = 26;
  const tot = d => fair ? d.fair : d.board, rl = d => d.realized, op = d => fair ? d.fair_open : d.open;
  let lo = 0, hi = 0;
  for (const d of days) { const parts = [rl(d), op(d)]; let p = 0, n = 0; for (const v of parts) { if (v > 0) p += v; else n += v; } hi = Math.max(hi, p, tot(d) || 0); lo = Math.min(lo, n, tot(d) || 0); }
  [lo, hi] = pad(lo, hi, 0.12);
  const Y = v => T + (hi - v) / (hi - lo) * (H - T - B), bw = Math.max(8, Math.min(46, (W - L - R) / days.length * 0.62)), step = (W - L - R) / days.length;
  let s = `<svg width="${W}" height="${H}"><defs><pattern id="hatch5" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="2" height="5" fill="var(--panel)" opacity=".7"/></pattern></defs>`;
  for (const v of ticks(lo, hi, 4).ticks.filter(v => v >= lo && v <= hi)) s += `<line class="gl" x1="${L}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}"/><text x="${L - 6}" y="${f1(Y(v) + 4)}" text-anchor="end">${kf(v, Math.abs(hi - lo) / 4)}</text>`;
  s += `<line class="zl" x1="${L}" x2="${W - R}" y1="${f1(Y(0))}" y2="${f1(Y(0))}"/>`;
  days.forEach((d, i) => {
    const x = L + i * step + (step - bw) / 2; let up = 0, dn = 0;
    for (const [v, c] of [[rl(d), "r"], [op(d), "o"]]) {
      if (v == null || v === 0) continue;
      const y0 = v > 0 ? Y(up) : Y(dn), y1 = v > 0 ? Y(up + v) : Y(dn + v);
      s += `<rect class="b5${c}${v < 0 ? "n" : ""}" x="${f1(x)}" y="${f1(Math.min(y0, y1))}" width="${f1(bw)}" height="${f1(Math.max(1, Math.abs(y1 - y0)))}"/>`;
      if (d.partial) s += `<rect x="${f1(x)}" y="${f1(Math.min(y0, y1))}" width="${f1(bw)}" height="${f1(Math.max(1, Math.abs(y1 - y0)))}" fill="url(#hatch5)"/>`;
      if (v > 0) up += v; else dn += v;
    }
    const t = tot(d), ty = t >= 0 ? Y(Math.max(up, t)) - 5 : Y(Math.min(dn, t)) + 13;
    s += `<text class="vl" x="${f1(x + bw / 2)}" y="${f1(ty)}" text-anchor="middle">${sg1(t)}</text>`;
    s += `<text x="${f1(x + bw / 2)}" y="${H - 8}" text-anchor="middle">${esc(dday(d.day))}</text>`;
    s += `<rect x="${f1(L + i * step)}" y="${T}" width="${f1(step)}" height="${H - T - B}" fill="transparent"${tipAttr(`<b>${esc(dday(d.day))}${d.partial ? " (today, open)" : ""}</b><br>${fair ? "at fair" : "on the board"} <b>${sgn(t)}</b><br>realized ${sgn(d.realized)} · open positions ${sgn(op(d))}<br><span class="mute">start ${num(fair ? d.start_fair : d.start)} → end ${num(fair ? d.end_fair : d.end)}</span>`)}/>`;
  });
  return s + `</svg>` + legend([[sw("var(--sigb)"), "realized"], [`<i class="sw" style="background:var(--sigb);opacity:.42"></i>`, "open positions"], ["", "today hatched until the day closes · SIG days 00:00 UTC"]]);
}
function ov_acct() {
  const o = V("v5ov"), pts = arr(o.account_series).filter(p => p && p[1] != null);
  if (pts.length < 2) return na5();
  const W = width("ovAcct"), H = 230, L = 54, R = 10, T = 12, B = 26, t0 = pts[0][0], t1 = pts[pts.length - 1][0];
  let lo = Infinity, hi = -Infinity; for (const p of pts) for (const v of [p[1], p[2]]) if (v != null) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  [lo, hi] = [lo - (hi - lo) * 0.06, hi + (hi - lo) * 0.06];
  const X = t => L + (t - t0) / (t1 - t0 || 1) * (W - L - R), Y = v => T + (hi - v) / (hi - lo || 1) * (H - T - B);
  const id = "ovacct", series = [{name: "SIG marks", color: "var(--sigb)", t: pts.map(p => p[0]), v: pts.map(p => p[1]), Y, gap: 3600, fmt: v => num(v)},
    {name: "at fair", color: "var(--steel)", t: pts.map(p => p[0]), v: pts.map(p => p[2]), Y, gap: 3600, fmt: v => num(v)}];
  CH[id] = {id, t0, t1, L, R, W, X, series};
  let s = `<svg width="${W}" height="${H}">`;
  for (const v of ticks(lo, hi, 4).ticks.filter(v => v >= lo && v <= hi)) s += `<line class="gl" x1="${L}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}"/><text x="${L - 6}" y="${f1(Y(v) + 4)}" text-anchor="end">${kf(v, (hi - lo) / 4)}</text>`;
  { const tt = timeTicks(t0, t1, W); for (const t of tt.ticks) s += `<text x="${f1(X(t))}" y="${H - 8}" text-anchor="middle">${esc(tt.label(t))}</text>`; }
  series.forEach((sr, i) => { s += `<path class="ln" stroke="${sr.color}" d="${linePath(pts.map(p => [p[0], p[i + 1]]), X, Y)}"/><circle id="xh-${id}-${i}" r="3.5" fill="${sr.color}" display="none"/>`; });
  s += `<line id="xh-${id}" class="xh" y1="${T}" y2="${H - B}" display="none"/><rect data-chart="${id}" x="${L}" y="${T}" width="${W - L - R}" height="${H - T - B}" fill="transparent"/></svg>`;
  return s + legend([[sw("var(--sigb)"), "SIG marks"], [sw("var(--steel)"), "at Polymarket fair"]]);
}
function ov_rank() {
  const r = obj(V("v5ov").rank); if (!r.total) return na5(r.error);
  const ov = obj(V("v5ov")), b = obj(ov.board), t = obj(ov.today), d = obj(r.day);
  const top = p => p == null ? "–" : "top " + num(p * 100, 1) + "%";
  let s = `<div class="wrap5"><table class="t5"><thead><tr><th></th><th class="n">Rank</th><th class="n">P&L (board)</th><th class="n">Percentile</th><th class="n">Gap at marks</th><th class="n">Gap at fair</th></tr></thead><tbody>`;
  s += `<tr><td><b>Us, overall</b></td><td class="n">#${num(r.rank)} of ${num(r.total)}</td><td class="n ${cls(b.pnl)}">${sgn(b.pnl)}</td><td class="n">${top(r.pct)}</td><td class="n">—</td><td class="n">—</td></tr>`;
  s += `<tr><td><b>Us, today</b></td><td class="n">${d.rank ? `#${num(d.rank)} of ${num(d.total)} on today's board` : "–"}</td><td class="n ${cls(t.board)}">${sgn(t.board)}</td><td class="n">${d.pct ? top(d.pct) + " of today's board" : "–"}</td><td class="n">—</td><td class="n">—</td></tr>`;
  for (const l of arr(r.lines)) s += `<tr><td>Top ${l.n} line</td><td class="n">#${l.n}</td><td class="n">${sgn(l.pnl)}</td><td class="n">${top(l.pct)}</td><td class="n">${sgn(l.gap_marks)}</td><td class="n ${val(l.gap_fair) < 0 ? "good" : ""}">${sgn(l.gap_fair)}</td></tr>`;
  return s + `</tbody></table></div><div class="sub5" style="margin-top:8px">Thresholds as of ${esc(String(r.as_of || "–").slice(11, 16))} · a negative gap at fair means we'd be above that line if every market settled at today's Polymarket fair</div>`;
}
function ov_rankchart() {
  const r = obj(V("v5ov").rank), pts = arr(r.series); if (pts.length < 2) return na5();
  const W = width("ovRankC"), H = 230, L = 46, R = 46, T = 12, B = 26, t0 = pts[0][0], t1 = pts[pts.length - 1][0];
  const lmin = Math.log10(10), lmax = Math.log10(Math.max(1500, ...pts.map(p => p[1])));
  const X = t => L + (t - t0) / (t1 - t0 || 1) * (W - L - R), Y = v => T + (Math.log10(Math.max(1, v)) - lmin) / (lmax - lmin) * (H - T - B);
  let s = `<svg width="${W}" height="${H}">`;
  for (const v of [10, 25, 50, 100, 250, 500, 1000]) if (Math.log10(v) <= lmax) s += `<line class="gl" x1="${L}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}"/><text x="${L - 6}" y="${f1(Y(v) + 4)}" text-anchor="end">#${v}</text>`;
  for (const v of [100, 50, 25]) s += `<line x1="${L}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}" stroke="var(--steel)" stroke-dasharray="4 4"/><text x="${W - R + 4}" y="${f1(Y(v) + 4)}" class="lbl">top ${v}</text>`;
  { const tt = timeTicks(t0, t1, W); for (const t of tt.ticks) s += `<text x="${f1(X(t))}" y="${H - 8}" text-anchor="middle">${esc(tt.label(t))}</text>`; }
  const id = "ovrank", sr = {name: "our rank", color: "var(--sigb)", t: pts.map(p => p[0]), v: pts.map(p => p[1]), Y, gap: 7200, fmt: v => "#" + num(v)};
  CH[id] = {id, t0, t1, L, R, W, X, series: [sr]};
  s += `<path class="ln" stroke="var(--sigb)" d="${linePath(pts, X, Y)}"/><circle id="xh-${id}-0" r="3.5" fill="var(--sigb)" display="none"/>`;
  s += `<line id="xh-${id}" class="xh" y1="${T}" y2="${H - B}" display="none"/><rect data-chart="${id}" x="${L}" y="${T}" width="${W - L - R}" height="${H - T - B}" fill="transparent"/></svg>`;
  return s + `<div class="sub5">Overall rank, log scale, better is higher</div>`;
}
const RAMP = ["#0862A6", "#1A6CA8", "#2C77AB", "#3E81AD", "#4F8BAF", "#5F95B1", "#6A8FA3", "#7A9DAE", "#8AABB9", "#9AB8C4", "#A9C3CC"];
function ov_port() {
  const p = obj(V("v5ov").port); if (p.account == null) return na5();
  const byS = ui.v5.pv === "strategy";
  let sl = byS ? arr(p.by_strategy).map((r, i) => ({key: r.key, name: r.name, value: r.value})) : arr(p.slices).map(r => Object.assign({key: r.ex}, r));
  if (!byS && (arr(p.other).length || Math.abs(val(p.val_gap) || 0) > 1)) sl.push({key: "other", name: `Other (${arr(p.other).length} markets)`, value: p.other_value});
  sl.push({key: "cash", name: "Cash available", value: p.available, cash: true});
  const tot = sl.reduce((a, r) => a + Math.max(0, r.value || 0), 0) || 1;
  const R0 = 92, R1 = 58, C = 110; let a = -Math.PI / 2, svg = `<svg width="${2 * C}" height="${2 * C}" viewBox="0 0 ${2 * C} ${2 * C}">`;
  sl.forEach((r, i) => {
    const v = Math.max(0, r.value || 0), da = v / tot * 2 * Math.PI; if (da <= 0) return;
    const a1 = a + da, big = da > Math.PI ? 1 : 0, col = r.cash ? "var(--rule)" : RAMP[Math.min(i, RAMP.length - 1)];
    const P = (rr, aa) => `${f1(C + rr * Math.cos(aa))} ${f1(C + rr * Math.sin(aa))}`;
    const hl = ui.v5.hlx === r.key;
    const tip = r.cash ? `<b>Cash available</b><br>${num(r.value)} (${num(p.above_floor)} above the 5k floor)` : byS ? `<b>${esc(r.name)}</b><br>value ${num(r.value)} at SIG marks` :
      r.key === "other" ? `<b>${esc(r.name)}</b><br>value ${num(r.value)}` :
      `<b>${esc(r.name)}</b> · ${esc(String(r.side || "").toUpperCase())}<br>${num(r.shares)} shares · avg ${px(r.avg)} · mark ${px(r.mark)} · fair ${px(r.fair)}<br>value <b>${num(r.value)}</b> · P&L at marks ${sgn(r.pnl_marks)} · at fair ${sgn(r.pnl_fair)}<br><span class="mute">${Object.entries(obj(r.subs)).map(([k, v]) => esc(k) + " " + num(v)).join(" · ")}</span>`;
    svg += `<path d="M ${P(R0 + (hl ? 5 : 0), a)} A ${R0 + (hl ? 5 : 0)} ${R0 + (hl ? 5 : 0)} 0 ${big} 1 ${P(R0 + (hl ? 5 : 0), a1)} L ${P(R1, a1)} A ${R1} ${R1} 0 ${big} 0 ${P(R1, a)} Z" fill="${col}" stroke="var(--panel)" stroke-width="1.5" data-slice="${esc(r.key)}"${tipAttr(tip)}/>`;
    a = a1;
  });
  svg += `<text x="${C}" y="${C - 4}" text-anchor="middle" class="lbl" style="font-size:12px">account</text><text x="${C}" y="${C + 16}" text-anchor="middle" class="vl" style="font-size:17px">${num(p.account)}</text></svg>`;
  let rows = byS ? sl : sl.filter(r => r.key !== "other" || !ui.v5.oexp);
  if (!byS && ui.v5.oexp) rows = rows.slice(0, -1).concat(arr(p.other).map(r => Object.assign({key: r.ex, sub: true}, r)), [sl[sl.length - 1]]);
  let t = `<div class="wrap5"><table class="t5"><thead><tr><th>${byS ? "Strategy" : "Market"}</th>${byS ? "" : "<th>Side</th><th class='n'>Shares</th>"}<th class="n">Value</th><th class="n">% of account</th>${byS ? "" : "<th class='n'>Avg</th><th class='n'>Mark</th><th class='n'>Fair</th><th class='n'>P&L marks</th><th class='n'>P&L fair</th>"}</tr></thead><tbody>`;
  for (const r of rows) {
    const cl = r.key === "other" ? "cl" : "";
    t += `<tr class="${cl} ${ui.v5.hlx === r.key ? "hl" : ""}" data-slicerow="${esc(r.key)}"${r.key === "other" ? ' data-oexp="1"' : ""}><td>${r.sub ? "&nbsp;&nbsp;" : ""}${esc(r.name)}${r.key === "other" ? (ui.v5.oexp ? " ▾" : " ▸") : ""}</td>`
      + (byS ? "" : `<td>${esc(String(r.side || "").toUpperCase())}</td><td class="n">${r.shares == null ? "" : num(r.shares)}</td>`)
      + `<td class="n">${num(r.value)}</td><td class="n">${pct((r.value || 0) / (p.account || 1))}</td>`
      + (byS ? "" : `<td class="n">${r.avg == null ? "" : px(r.avg)}</td><td class="n">${r.mark == null ? "" : px(r.mark)}</td><td class="n">${r.fair == null ? "" : px(r.fair)}</td><td class="n ${cls(r.pnl_marks)}">${r.pnl_marks == null ? "" : sgn(r.pnl_marks)}</td><td class="n ${cls(r.pnl_fair)}">${r.pnl_fair == null ? "" : sgn(r.pnl_fair)}</td>`) + `</tr>`;
  }
  t += `</tbody></table></div>`;
  return `<div class="h5"><h3>Portfolio: where the money is</h3><div class="seg5"><button data-pv="market" class="${!byS ? "on" : ""}">by market</button><button data-pv="strategy" class="${byS ? "on" : ""}">by strategy</button></div></div>
    <div class="g2 port5"><div><div style="display:flex;gap:24px;margin-bottom:10px"><div><div class="lab5">Invested</div><div class="big5" style="font-size:30px">${k1(p.invested)}</div><div class="sub5">${pct(p.invested / (p.account || 1), 0)} of the account</div></div>
    <div><div class="lab5">Available</div><div class="big5" style="font-size:30px">${k1(p.available)}</div><div class="sub5">${k1(p.above_floor)} above the 5k floor</div></div></div>${svg}</div><div>${t}</div></div>`;
}

// ------------------------------------------------------------------------------------------- Live
function lv_strip() {
  const s = obj(V("v5lv").server); if (!s.state) return na5();
  const c = s.state === "trading" ? "green" : s.state === "paused" ? "red" : "amber", f = obj(s.feed);
  return `<div class="strip5"><span class="dot5 ${c}"></span><b>${s.state === "trading" ? "Live" : s.state === "paused" ? "Paused" : "Degraded"}</b>${s.reason ? " (" + esc(s.reason) + ")" : ""}
    &nbsp; <b>${esc(s.host || "")}</b>${s.provider ? " · " + esc(s.provider) + " " + esc(String(s.region || "").toUpperCase()) : ""}${s.vcpu ? ` · ${s.vcpu} vCPU / ${s.mem_gb} GB` : ""} &nbsp; release <b>${esc(s.release || "–")}</b> · up ${tsAgo(Date.now() / 1000 - (s.up_s || 0))}<br>
    last loop <b>${s.loop_age == null ? "–" : s.loop_age + " s"}</b> · SIG <b>${s.sig_ms == null ? "–" : num(s.sig_ms) + " ms"}</b> · Polymarket feed <b>${s.pm_age == null ? "–" : s.pm_age + " s"}</b> · SIG live feed <span class="dot5 ${f.connected ? "green" : "amber"}"></span><b>${f.bpm == null ? "–" : num(f.bpm, 0)} batches/min</b> (${esc(f.mode || "–")})<br>
    reads <b>${num(s.reads)} / ${num(s.read_limit)}</b> per min · writes <b>${num(s.writes)} / ${num(s.write_limit)}</b> · rate-limit errors <b class="${s.rl_1h ? "bad" : ""}">${num(s.rl_1h)}</b> (1 h) · CPU <b>${s.cpu == null ? "–" : num(s.cpu) + "%"}</b> · memory <b>${s.mem == null ? "–" : num(s.mem) + "%"}</b> · disk <b>${s.disk == null ? "–" : num(s.disk) + "%"}</b> · Mac mirror <b>${s.mirror_age_s == null ? "–" : tsAgo(Date.now() / 1000 - s.mirror_age_s) + " ago"}</b></div>`;
}
function card5(c) {
  const capS = c.cap ? `${k1(c.in_use)} of ${k1(c.cap)}` : `${k1(c.in_use)} in use`, w = c.cap ? Math.min(100, 100 * (c.in_use || 0) / c.cap) : 0;
  const glow = ui.v5.glowT[c.key] && Date.now() - ui.v5.glowT[c.key] < 1200 ? " glow5" : "";
  const thick = Math.max(1, Math.min(10, Math.sqrt(Math.max(0, c.in_use || 0)) / 12));
  return `<div class="sc5${glow}" data-card="${esc(c.key)}"${tipAttr(`at fair since inception <b>${sgn(c.pnl_fair)}</b> · today ${sgn(c.today_fair)}`)}><i class="flow" style="border-top-width:${f1(thick)}px;width:14px"></i>
    <div><span class="nm">${esc(c.name)}</span><span class="chip5 ${esc(c.status)}" title="${esc(c.reason || "")}">${esc(c.status)}</span></div>
    <div class="pl ${cls(c.pnl)}">${sgn(c.pnl)}</div><div class="sub5">today <b class="${cls(c.today)}">${sgn(c.today)}</b> · since Oct 1 on the board</div>
    <div class="bar5"><i style="width:${f1(w)}%"></i></div><div class="meta">${capS}${c.cap ? " allotted" : ""} · ${num(c.entries)} entries, ${num(c.exits)} exits resting · last fill ${c.last_fill_t ? tsAgo(c.last_fill_t) + " ago" : "–"}</div></div>`;
}
function lv_pipe() {
  const L = V("v5lv"), cards = arr(L.cards); if (!cards.length) return na5();
  const strat = cards.filter(c => c.group === "strategy"), apart = cards.filter(c => c.group === "apart"), ret = cards.filter(c => c.group === "retired");
  for (const c of cards) { const prev = ui.v5.seen[c.key]; if (prev && c.last_fill_t && c.last_fill_t > prev) ui.v5.glowT[c.key] = Date.now(); if (c.last_fill_t) ui.v5.seen[c.key] = c.last_fill_t; }
  const cash = obj(L.cash), s = obj(L.server), chk = obj(L.check);
  return `<div class="pipe5"><div style="display:grid;gap:8px"><div class="src5"><span class="dot5 ${obj(s.feed).connected ? "green" : "amber"}"></span>SIG live feed</div><div class="src5"><span class="dot5 green"></span>SIG REST</div><div class="src5"><span class="dot5 ${val(s.pm_age) != null && s.pm_age < 60 ? "green" : "amber"}"></span>Polymarket</div></div>
    <div class="eng5"><b>Engine</b><div class="sub5">plan · risk · execution</div></div>
    <div>${[...new Set(strat.map(c => c.sub || ""))].map(g => `<div class="cg5">${g ? `<div class="cgl5">${esc(g)}</div>` : ""}<div class="cards5">${strat.filter(c => (c.sub || "") === g).map(card5).join("")}</div></div>`).join("")}</div>
    <div style="display:grid;gap:8px"><div class="pos5"><b>Positions</b><div class="big5" style="font-size:24px">${k1(cash.positions)}</div><div class="sub5">at SIG marks</div></div><div class="pos5"><b>Exits</b><div class="sub5">sell-backs turn positions back into cash</div></div></div></div>
    <div class="strip5" style="margin-top:12px">Cash pool <b>${num(cash.pool)}</b> (floor ${k1(cash.floor)} · buffer ${k1(cash.buffer)}) ◄ exits · <span class="${Math.abs(val(chk.gap) || 0) > 25 ? "bad" : ""}">all strategies + holdings = <b>${sgn(chk.sum)}</b> vs leaderboard P&L ${sgn(chk.board)} (gap ${sgn(chk.gap)})</span></div>
    <h3 style="margin:16px 0 8px;font-size:15px">Not strategies, shown apart</h3><div class="cards5">${apart.map(card5).join("")}</div>
    <details style="margin-top:12px"><summary>Retired (${ret.length})</summary><div class="cards5" style="margin-top:8px">${ret.map(card5).join("")}</div></details>`;
}
function lv_side() {
  const k = ui.v5.card; if (!k) return "";
  const L = V("v5lv"), c = arr(L.cards).find(x => x.key === k); if (!c) return "";
  const days = arr(obj(L.days)[k]);
  let bars = "";
  if (days.length) {
    const W = 460, H = 120, mx = Math.max(1, ...days.map(d => Math.abs(d[1]))), bw = Math.max(6, (W - 20) / days.length - 4);
    bars = `<svg width="${W}" height="${H}">` + days.map((d, i) => { const h = Math.abs(d[1]) / mx * (H / 2 - 14), x = 10 + i * (bw + 4); return `<rect x="${x}" y="${d[1] >= 0 ? H / 2 - h : H / 2}" width="${bw}" height="${Math.max(1, h)}" fill="${d[1] >= 0 ? "var(--gain)" : "var(--loss)"}"${tipAttr(`${esc(dday(d[0]))}: ${sgn(d[1])}`)}/>`; }).join("") + `<line class="zl" x1="0" x2="${W}" y1="${H / 2}" y2="${H / 2}"/></svg><div class="sub5">realized per SIG day from the ledger's fills</div>`;
  }
  const fills = arr(c.fills).map(f => `<tr><td>${esc(String(f.t).slice(5, 16))}</td><td>${esc(f.name)}</td><td>${esc(String(f.side).toUpperCase())}</td><td class="n">${num(f.qty)}</td><td class="n">${px(f.px)}</td><td class="n ${cls(f.realized)}">${f.realized ? sgn(f.realized) : ""}</td></tr>`).join("");
  return `<div class="side5"><button class="btn5 x" data-closecard="1">✕</button><h3 style="font-size:18px;margin:0 0 6px">${esc(c.name)} <span class="chip5 ${esc(c.status)}">${esc(c.status)}</span></h3>
    <p>${esc(c.rules)}</p><div class="sub5">layers: ${arr(c.layers).map(esc).join(", ") || "–"} · switch: ${arr(c.control).map(esc).join(", ") || "none"}${c.cap ? ` · allotted ${num(c.cap)}, in use ${num(c.in_use)}` : ""}</div>
    <h3 style="margin-top:16px">P&L per day since inception</h3>${bars || na5("no fills yet")}
    <h3 style="margin-top:16px">Last 20 fills</h3>${fills ? `<div class="wrap5"><table class="t5"><thead><tr><th>Time</th><th>Market</th><th>Side</th><th class="n">Shares</th><th class="n">Price</th><th class="n">Realized</th></tr></thead><tbody>${fills}</tbody></table></div>` : na5("no fills yet")}</div>`;
}
function lv_act() {
  const L = V("v5lv"); let a = arr(L.activity);
  if (ui.v5.actf !== "all") a = a.filter(r => r.kind === ui.v5.actf.replace(/s$/, ""));
  if (ui.v5.acts) a = a.filter(r => r.strat === ui.v5.acts);
  const seen = ui.v5.actSeen || new Set(); const fresh = new Set();
  const rows = a.map(r => { const n = !seen.has(r.id); if (n) fresh.add(r.id); return `<div class="r ${n && seen.size ? "new" : ""}"><span class="tm">${r.t ? esc(hm(r.t)) : ""}</span><span class="chip5 ${esc(r.kind)}">${esc(r.kind)}</span><span>${esc(r.text)}</span></div>`; }).join("");
  ui.v5.actSeen = new Set([...seen, ...fresh]);
  const strats = arr(L.cards).filter(c => c.group !== "retired");
  return `<div class="fil5">${["all", "trades", "alerts", "market"].map(f => `<button data-actf="${f}" class="${ui.v5.actf === f ? "on" : ""}">${f}</button>`).join("")}
    <select id="acts"><option value="">every strategy</option>${strats.map(c => `<option value="${esc(c.key)}" ${ui.v5.acts === c.key ? "selected" : ""}>${esc(c.name)}</option>`).join("")}</select></div><div class="act5">${rows || na5("nothing yet")}</div>`;
}

// ------------------------------------------------------------------------------------------- Markets
function mkRows() {
  let rows = arr(S.v5mk_rows);
  const q = (ui.v5.mkq || "").trim().toLowerCase();
  if (q) rows = rows.filter(r => [r.race, r.state, r.title, r.contract, r.party, r.group].some(x => String(x || "").toLowerCase().includes(q)));
  const f = ui.v5.mkf;
  if (f === "hold") rows = rows.filter(r => r.hold);
  if (f === "nohold") rows = rows.filter(r => !r.hold);
  if (f === "g3") rows = rows.filter(r => Math.abs(val(r.gap_c) || 0) >= 3);
  if (f === "g5") rows = rows.filter(r => Math.abs(val(r.gap_c) || 0) >= 5);
  if (f === "orders") rows = rows.filter(r => r.orders);
  if (f === "g3empty") rows = rows.filter(r => !r.hold && !r.orders && Math.abs(val(r.gap_c) || 0) >= 3);
  if (ui.v5.mkg) rows = rows.filter(r => r.group === ui.v5.mkg);
  return rows;
}
function mk_head() {
  const m = V("v5mk"), sm = obj(m.summary);
  return `<div class="sub5" style="margin-bottom:8px">Prices as of ${m.as_of ? esc(hm(m.as_of)) + ":" + String(new Date(m.as_of * 1000).getSeconds()).padStart(2, "0") : "–"} (SIG) · Polymarket ${m.pm_as_of ? tsAgo(m.pm_as_of) + " ago" : "–"}</div>
    <div style="margin-bottom:10px">${num(sm.contracts)} contracts · we hold ${num(sm.held)} · <a href="#" data-mkf="g3empty">${num(sm.gap3_empty)} have a gap ≥ 3¢ and nothing from us</a></div>
    <div class="fil5"><input id="mkq" placeholder="search state, race, district (press /)" value="${esc(ui.v5.mkq)}" style="min-width:240px">
    ${[["all", "All"], ["hold", "We hold"], ["nohold", "We don't hold"], ["g3", "Gap ≥ 3¢"], ["g5", "Gap ≥ 5¢"], ["orders", "Has our orders"]].map(([k, t]) => `<button data-mkf="${k}" class="${ui.v5.mkf === k ? "on" : ""}">${t}</button>`).join("")}
    <select id="mkg"><option value="">every race type</option>${["Senate", "Governor", "House", "National"].map(g => `<option ${ui.v5.mkg === g ? "selected" : ""}>${g}</option>`).join("")}</select>
    <div class="seg5"><button data-mkview="table" class="${ui.v5.mkview === "table" ? "on" : ""}">Table</button><button data-mkview="map" class="${ui.v5.mkview === "map" ? "on" : ""}">Map</button></div></div>`;
}
const MKC = [["race", "Race", 0], ["contract", "Contract", 0], ["sig_yes", "SIG Buy YES", 1], ["sig_no", "SIG Buy NO", 1], ["pm_yes", "PM Buy YES", 1], ["pm_no", "PM Buy NO", 1], ["gap", "Gap SIG−PM", 1], ["hold", "We hold", 0], ["orders", "Orders", 1], ["vol24", "24 h SIG volume", 1], ["why", "Why we're not trading it", 0]];
function mk_table() {
  const rows = mkRows(); if (!arr(S.v5mk_rows).length) return na5();
  if (ui.v5.mkview === "map") return mk_map(rows);
  const races = new Map(); for (const r of rows) { const k = r.group + "|" + r.race; if (!races.has(k)) races.set(k, []); races.get(k).push(r); }
  const srt = ui.v5.mksort, keyv = rs => { const r = rs.reduce((a, b) => Math.abs(val(b.gap_c) || 0) > Math.abs(val(a.gap_c) || 0) ? b : a, rs[0]);
    return srt.k === "gap" ? Math.abs(val(r.gap_c) || 0) : srt.k === "race" ? r.race : srt.k === "vol24" ? rs.reduce((a, b) => a + (b.vol24 || 0), 0) : srt.k === "orders" ? rs.reduce((a, b) => a + (b.orders || 0), 0) : srt.k === "hold" ? rs.reduce((a, b) => a + (b.hold ? b.hold.value : 0), 0) : val(r[srt.k]) || 0; };
  const groups = {};
  for (const [k, rs] of races) { const g = k.split("|")[0]; (groups[g] = groups[g] || []).push(rs); }
  let s = `<div class="wrap5 mk5"><table class="t5"><thead><tr>${MKC.map(([k, t, n]) => `<th class="${n ? "n" : ""}" data-mksort="${k}" style="cursor:pointer">${t}${srt.k === k ? (srt.dir < 0 ? " ▾" : " ▴") : ""}</th>`).join("")}</tr></thead><tbody>`;
  for (const g of ["Senate", "Governor", "House", "National"]) {
    const list = groups[g]; if (!list) continue;
    list.sort((a, b) => { const x = keyv(a), y = keyv(b); return (x > y ? 1 : x < y ? -1 : 0) * srt.dir; });
    const shut = ui.v5.mkcol[g];
    s += `<tr class="grp" data-mkcol="${g}"><td colspan="${MKC.length}">${shut ? "▸" : "▾"} ${g} (${list.length} races)</td></tr>`;
    if (shut) continue;
    for (const rs of list) {
      rs.sort((a, b) => String(a.contract).localeCompare(String(b.contract)));
      rs.forEach((r, i) => {
        const gap = val(r.gap_c), gi = gap == null ? 0 : Math.min(1, Math.abs(gap) / 15);
        s += `<tr class="cl" data-mk="${esc(r.ex)}">${i === 0 ? `<td class="race" rowspan="${rs.length}" data-l="">${esc(r.race)}</td>` : ""}<td class="ct" data-l="">${esc(r.contract)}</td>
          <td class="n" data-l="SIG YES"${tipAttr(`SIG bid ${px(r.sig_bid)} / ask ${px(r.sig_ask)}${r.depth ? "" : ""}`)}>${px(r.sig_yes)}</td><td class="n" data-l="SIG NO">${px(r.sig_no)}</td>
          <td class="n" data-l="PM YES"${tipAttr(`Polymarket bid ${px(r.pm_bid)} / ask ${px(r.pm_ask)} · fair ${px(r.fair)}`)}>${px(r.pm_yes)}</td><td class="n" data-l="PM NO">${px(r.pm_no)}</td>
          <td class="n" data-l="gap" style="background:${gap == null ? "transparent" : (gap > 0 ? "rgba(192,57,43," : "rgba(8,98,166,") + f1(0.08 + 0.4 * gi) + ")"}">${cents(gap)}</td>
          <td class="hold5" data-l="hold">${r.hold ? `<span class="pill5">${esc(r.hold.side.toUpperCase())} ${num(r.hold.shares)}</span> <span class="sub5">${k1(r.hold.value)}</span>` : "—"}</td>
          <td class="n" data-l="orders">${num(r.orders)}</td><td class="n" data-l="24 h">${k1(r.vol24)}</td><td class="why5" data-l="">${esc(r.why || "")}</td></tr>`;
      });
    }
  }
  return s + `</tbody></table></div>`;
}
function mk_map(rows) {
  const by = {}; for (const r of rows) { const k = r.group + "|" + r.race; (by[k] = by[k] || []).push(r); }
  let s = "";
  for (const g of ["Senate", "Governor", "House", "National"]) {
    const races = Object.entries(by).filter(([k]) => k.startsWith(g + "|")).sort((a, b) => String(a[1][0].state + a[0]).localeCompare(String(b[1][0].state + b[0])));
    if (!races.length) continue;
    s += `<h3 style="margin:12px 0 6px;font-size:15px">${g}</h3><div class="tiles5">`;
    for (const [k, rs] of races) {
      const hv = rs.reduce((a, r) => a + (r.hold ? r.hold.value : 0), 0), gap = Math.max(...rs.map(r => Math.abs(val(r.gap_c) || 0)));
      const fill = hv >= 5000 ? "var(--sigb)" : hv > 0 ? "color-mix(in srgb, var(--sigb) 35%, var(--panel))" : "var(--panel)";
      const ring = gap >= 5 ? "var(--loss)" : gap >= 3 ? "var(--amber)" : "var(--rule)";
      s += `<div class="tile5" data-mk="${esc(rs[0].ex)}" style="background:${fill};border-color:${ring};color:${hv >= 5000 ? "#fff" : "var(--ink)"}"${tipAttr(`<b>${esc(rs[0].race)}</b><br>held ${num(hv)} · biggest gap ${num(gap, 1)}¢`)}>${esc(String(rs[0].race).replace(" House race", "").replace(" Senate", " Sen").replace(" Governor", " Gov"))}</div>`;
    }
    s += `</div>`;
  }
  return s + legend([["", "fill = how much we hold · ring = the SIG−Polymarket gap (amber ≥ 3¢, red ≥ 5¢)"]]);
}
function mk_panel() {
  const ex = ui.v5.mk; if (!ex) return "";
  const r = arr(S.v5mk_rows).find(x => x.ex === ex); if (!r) return "";
  const d = obj(ui.v5.mkd), bk = obj(d.book);
  const lv = (xs, side) => arr(xs).map(([p, q, mine]) => `<tr><td class="n">${px(p)}</td><td class="n">${num(q)}</td><td class="n">${mine ? "<b>" + num(mine) + " ours</b>" : ""}</td></tr>`).join("");
  const book = bk.src ? `<div class="g2"><div><div class="lab5">Bids (YES)</div><table class="t5">${lv(bk.bids)}</table></div><div><div class="lab5">Asks (YES)</div><table class="t5">${lv(bk.asks)}</table></div></div><div class="sub5">from the ${esc(bk.src)}</div>` : na5(LIVE ? "loading…" : "the book shows on the live page");
  const h = r.hold, subs = h ? Object.entries(obj(h.subs)).map(([k, v]) => esc(k) + " " + num(v)).join(" · ") : "";
  const fills = arr(d.fills).slice(0, 10).map(f => `<tr><td>${esc(String(f.t).slice(5, 16))}</td><td>${esc(f.layer)}</td><td>${esc(String(f.side).toUpperCase())}</td><td class="n">${num(f.qty)}</td><td class="n">${px(f.price)}</td></tr>`).join("");
  const ords = arr(d.orders).filter(o => o.status === "open").map(o => `<tr><td>${esc(o.layer)}</td><td>${esc(String(o.side).toUpperCase())}</td><td class="n">${px(o.price)}</td><td class="n">${num(o.qty)}</td></tr>`).join("");
  return `<div class="side5"><button class="btn5 x" data-closemk="1">✕</button><h3 style="font-size:17px;margin:0 0 4px">${esc(r.title)}</h3><div class="sub5">${esc(r.race)} · ${esc(r.contract)}</div>
    <h3 style="margin-top:14px">SIG book</h3>${book}
    <h3 style="margin-top:14px">Polymarket</h3><div>bid <b>${px(r.pm_bid)}</b> · ask <b>${px(r.pm_ask)}</b> · fair <b>${px(r.fair)}</b> · updated ${obj(V("v5mk")).pm_as_of ? tsAgo(V("v5mk").pm_as_of) + " ago" : "–"}</div>
    <h3 style="margin-top:14px">Our position</h3>${h ? `<div><b>${esc(h.side.toUpperCase())} ${num(h.shares)}</b> · avg ${px(h.avg)} · value ${num(h.value)} · P&L at marks <b class="${cls(h.pnl)}">${sgn(h.pnl)}</b></div><div class="sub5">${subs}</div>` : na5("none")}
    <h3 style="margin-top:14px">Our resting orders</h3>${ords ? `<table class="t5">${ords}</table>` : na5(r.orders ? "loading…" : "none")}
    <h3 style="margin-top:14px">Last 10 fills</h3>${fills ? `<table class="t5">${fills}</table>` : na5(LIVE ? "none" : "on the live page")}
    <h3 style="margin-top:14px">Planner</h3><div>${esc(r.why || "active: our orders are resting")}</div></div>`;
}
async function openMk(ex) {
  ui.v5.mk = ui.v5.mk === ex ? null : ex; ui.v5.mkd = null; draw();
  if (!ui.v5.mk || !LIVE) return;
  try { const r = await fetch(`/api/market/${encodeURIComponent(ex)}?t=${encodeURIComponent(TOKEN)}`); ui.v5.mkd = await r.json(); draw(); } catch (e) { say("market: " + e, true); }
}

// ------------------------------------------------------------------------------------------- Admin
function ad_controls() {
  const a = V("v5ad"); if (!a.controls) return na5();
  const p = a.paused;
  let s = `<div class="h5"><h3>Controls</h3></div><div style="margin-bottom:12px"><button class="btn5 ${p ? "pri" : "dng"} ctl" data-c5="${p ? "resume" : "pause"}">${p ? "Resume trading" : "Pause new risk"}</button>
    <span class="sub5" style="margin-left:8px">${p ? "Paused: no new entries; exits keep resting." : "Trading. Pause stops every new entry; exits keep resting."}</span></div>`;
  s += `<div class="wrap5"><table class="t5"><thead><tr><th>Strategy</th><th>Layers</th><th>State</th><th class="n">Entries</th><th class="n">Exits</th></tr></thead><tbody>`;
  for (const c of arr(a.controls)) {
    s += `<tr><td><b>${esc(c.name)}</b></td><td class="sub5">${arr(c.layers).map(esc).join(", ")}</td><td>${LIVE ? "" : `<span class="chip5 ${c.state === "full" ? "Live" : c.state === "pilot" ? "Pilot" : "Off"}" style="margin-left:0">${c.state === "full" ? "Live" : c.state === "pilot" ? "Pilot" : "Off"}</span>`}<div class="seg5 ctl">${[["off", "Off"], ["pilot", "Pilot"], ["full", "Live"]].map(([k, t]) => `<button data-c5="state" data-key="${esc(c.key)}" data-st="${k}" class="${c.state === k ? "on" : ""}">${t}</button>`).join("")}</div></td>
      <td class="n">${num(c.entries)}</td><td class="n">${num(c.exits)}</td></tr>`;
  }
  return s + `</tbody></table></div><div class="sub5" style="margin-top:6px">${LIVE ? "Every change asks for a confirmation and is logged with time and source." : "Controls work on the live page only."}</div>`;
}
function ad_health() {
  const a = V("v5ad"), hs = arr(a.health); if (!hs.length) return na5();
  let s = `<div class="h5"><h3>Health</h3></div><div class="wrap5"><table class="t5"><thead><tr><th>Check</th><th class="n">Value</th><th>Threshold</th></tr></thead><tbody>`;
  for (const h of hs) {
    const v = h.text ? h.text : h.value == null ? "–" : num(h.value) + (h.unit || "");
    s += `<tr class="cl" data-hchk="${esc(h.key)}"><td><span class="dot5 ${esc(h.dot)}"></span>${esc(h.label)}</td><td class="n">${esc(v)}</td><td class="sub5">${esc(h.threshold)}</td></tr>`;
    if (ui.v5.hchk === h.key) s += `<tr><td colspan="3" class="sub5">last values: ${arr(h.hist).map(([t, x]) => `${esc(hm(t))} ${esc(x == null ? "–" : x)}`).join(" · ") || "none yet (one per minute)"}</td></tr>`;
  }
  return s + `</tbody></table></div>`;
}
function ad_alerts() {
  const a = V("v5ad"), al = arr(a.alerts);
  return `<div class="h5"><h3>Recent alerts and control changes</h3></div><div class="act5">${al.map(r => `<div class="r"><span class="tm">${esc(String(r.t || "").slice(11, 16))}</span><span>${esc(r.text)}</span></div>`).join("") || na5("none")}</div>`;
}
function ad_adv() {
  const a = obj(V("v5ad").advanced);
  const rel = arr(a.releases).map(l => { const m = /(C[0-9a-z]+-\d{4})/.exec(l); return `<tr><td>${esc(l)}</td><td>${m ? `<button class="btn5 ctl" data-c5="rollback" data-rel="${esc(m[1])}">Roll back to ${esc(m[1])}</button>` : ""}</td></tr>`; }).join("");
  const rb = a.rollback ? `<div class="sub5">Rollback request: ${esc(JSON.stringify(a.rollback))}</div>` : "";
  return `<details ${ui.v5.advOpen ? "open" : ""} id="adv5"><summary>Advanced</summary>
    <h3 style="margin-top:12px">Releases</h3>${rel ? `<table class="t5">${rel}</table>` : na5("no release log yet")}${rb}
    <h3 style="margin-top:12px">Config</h3><pre style="white-space:pre-wrap;font-size:12px">${esc(JSON.stringify(a.config, null, 1))}</pre>
    <h3>Overrides</h3><pre style="white-space:pre-wrap;font-size:12px;max-height:300px;overflow:auto">${esc(JSON.stringify(a.overrides, null, 1))}</pre>
    <h3>Engine log</h3><div class="act5">${arr(a.logs).map(r => `<div class="r"><span class="tm">${esc(String(r.t || "").slice(11, 19))}</span><span>${esc(r.kind)} · ${esc(r.text)}</span></div>`).join("")}</div></details>`;
}
function confirm5() {
  const c = ui.v5.confirm; if (!c) return "";
  return `<div class="modal5"><div class="b"><div style="font-size:15px;margin-bottom:14px">${esc(c.text)}</div><button class="btn5 pri" data-c5ok="1">Confirm</button> <button class="btn5" data-c5no="1">Cancel</button></div></div>`;
}
function c5text(btn) {
  const a = V("v5ad"), act = btn.dataset.c5;
  if (act === "pause") return "Pause new risk: every strategy stops placing entries and their resting entry bids are cancelled; exits keep resting.";
  if (act === "resume") return "Resume trading: the strategies place entries again under their caps.";
  if (act === "rollback") return `Roll back to ${btn.dataset.rel}: the Mac pushes that release to the server on its next mirror pull and restarts the engine (about 30 s without trading).`;
  const c = arr(a.controls).find(x => x.key === btn.dataset.key) || {}, st = btn.dataset.st, nm = {off: "Off", pilot: "Pilot", full: "Live"}[st];
  if (st === "off") return `${c.name} goes Off: its ${c.entries || 0} entry orders are cancelled, its ${c.exits || 0} exits keep resting.`;
  if (st === "pilot") return `${c.name} goes Pilot: it trades at its pilot caps (${c.entries || 0} entries and ${c.exits || 0} exits resting now).`;
  return `${c.name} goes Live: it trades at full size within its caps.`;
}
function c5send(btn) {
  const a = V("v5ad"), act = btn.dataset.c5;
  if (act === "pause" || act === "resume") return send(act, {});
  if (act === "rollback") return send("rollback_request", {release: btn.dataset.rel}, true);
  const c = arr(a.controls).find(x => x.key === btn.dataset.key) || {};
  for (const l of arr(c.layers)) send("layer_state", {layer: l, state: btn.dataset.st}, true);
}

// ------------------------------------------------------------------------------------------- wiring
function v5click(e) {
  const t = e.target; if (!t || !t.closest) return false;
  const q = s => t.closest(s);
  let el;
  if ((el = q("[data-basis5]"))) { ui.v5.basis = el.dataset.basis5; draw(); return true; }
  if ((el = q("[data-pv]"))) { ui.v5.pv = el.dataset.pv; ui.v5.oexp = false; draw(); return true; }
  if ((el = q("[data-oexp]"))) { ui.v5.oexp = !ui.v5.oexp; draw(); return true; }
  if ((el = q("[data-card]"))) { ui.v5.card = el.dataset.card; draw(); return true; }
  if ((el = q("[data-closecard]"))) { ui.v5.card = null; draw(); return true; }
  if ((el = q("[data-actf]"))) { ui.v5.actf = el.dataset.actf; draw(); return true; }
  if ((el = q("[data-mkf]"))) { e.preventDefault(); ui.v5.mkf = el.dataset.mkf; draw(); return true; }
  if ((el = q("[data-mkview]"))) { ui.v5.mkview = el.dataset.mkview; draw(); return true; }
  if ((el = q("[data-mksort]"))) { const k = el.dataset.mksort; ui.v5.mksort = {k, dir: ui.v5.mksort.k === k ? -ui.v5.mksort.dir : -1}; draw(); return true; }
  if ((el = q("[data-mkcol]"))) { const g = el.dataset.mkcol; ui.v5.mkcol[g] = !ui.v5.mkcol[g]; draw(); return true; }
  if ((el = q("[data-closemk]"))) { ui.v5.mk = null; draw(); return true; }
  if ((el = q("[data-mk]"))) { openMk(el.dataset.mk); return true; }
  if ((el = q("[data-hchk]"))) { ui.v5.hchk = ui.v5.hchk === el.dataset.hchk ? null : el.dataset.hchk; draw(); return true; }
  if ((el = q("[data-c5ok]"))) { const b = ui.v5.confirm && ui.v5.confirm.btn; ui.v5.confirm = null; if (b) c5send(b); draw(); return true; }
  if ((el = q("[data-c5no]"))) { ui.v5.confirm = null; draw(); return true; }
  if ((el = q("[data-c5]"))) { if (!LIVE) { say("controls work on the live page only", true); return true; } ui.v5.confirm = {text: c5text(el), btn: el}; draw(); return true; }
  if ((el = q("summary")) && el.parentElement && el.parentElement.id === "adv5") { ui.v5.advOpen = !el.parentElement.open; return false; }
  return false;
}
document.addEventListener("mouseover", e => { const s = e.target && e.target.closest && e.target.closest("[data-slicerow],[data-slice]"); const k = s ? (s.dataset.slicerow || s.dataset.slice) : null; if (k !== ui.v5.hlx && ui.tab === "overview") { ui.v5.hlx = k; draw(); } });
document.addEventListener("keydown", e => { if (e.key === "/" && ui.tab === "markets" && document.activeElement && document.activeElement.id !== "mkq") { const i = document.getElementById("mkq"); if (i) { e.preventDefault(); i.focus(); } } });
document.addEventListener("input", e => { if (e.target.id === "mkq") { ui.v5.mkq = e.target.value; draw(); } });
document.addEventListener("change", e => { if (e.target.id === "mkg") { ui.v5.mkg = e.target.value; draw(); } if (e.target.id === "acts") { ui.v5.acts = e.target.value; draw(); } });
"""
