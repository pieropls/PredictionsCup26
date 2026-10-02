"""H. STATUS: the engine at a glance in the terminal, from data/state.json (no API calls).  python3 h_status.py"""
import json
import os
import subprocess
import time

from a_config import STATE_FILE, PID_FILE


def f(x, fmt="{:.3f}"):
    return "–" if x is None else fmt.format(x)


pid = open(PID_FILE).read().strip() if os.path.exists(PID_FILE) else None
up = pid and subprocess.run(["ps", "-o", "etime=", "-p", pid], capture_output=True, text=True).stdout.strip()
print(f"engine: {'RUNNING pid ' + pid + ', up ' + up if up else 'NOT RUNNING'}")
if not os.path.exists(STATE_FILE):
    raise SystemExit("no data/state.json yet")
s = json.load(open(STATE_FILE))
print(f"{'STOPPED' if s['stopped'] else 'LIVE' if s['live'] else 'DRY RUN'} | updated {time.time() - os.path.getmtime(STATE_FILE):.0f}s ago | "
      f"Polymarket {'fresh' if s['pm_fresh'] else 'STALE'} | open orders {len(s['open_orders'])} | writes left {s['writes_left']}")
p = s.get("pnl") or {}
print(f"\nP&L  leaderboard (SIG valuation) {f(p.get('total'), '{:+,.0f}')} | at SIG book {f(p.get('book'), '{:+,.0f}')} | at Polymarket {f(p.get('fair'), '{:+,.0f}')}")
for k, g in sorted((p.get("groups") or {}).items(), key=lambda kv: kv[0].startswith("closed")):
    print(f"  {k:26} valuation {g['val']:+9,.0f}   book {g['book']:+9,.0f}   PM {g['fair']:+9,.0f}")
r = s.get("risk") or {}
print(f"risk: filled {f(r.get('filled'), '{:,.0f}')} + resting {f(r.get('resting'), '{:,.0f}')} | cash {f(r.get('cash'), '{:,.0f}')} | budget left {f(r.get('budget'), '{:,.0f}')}")
print(s.get("summary", ""))
print("\nPOSITION BOTS")
for b in s["bots"]:
    if b["kind"] == "position":
        print(f"  {b['name']:10} pos {b['pos']:+,.0f} avg {f(b['avg'])} | {b['status']}")
mk = sorted((b for b in s["bots"] if b["kind"] == "market"), key=lambda b: -abs(b["pnl_book"] or 0))
print(f"\nMARKET BOTS: {len(mk)} | holding {sum(1 for b in mk if b['pos'])} | with orders {sum(1 for b in mk if b['orders'])} | "
      f"focus {sum(1 for b in mk if b['focus'])}")
for b in mk[:10]:
    print(f"  {b['name'][4:36]:32} pos {b['pos']:+8,.0f} | book P&L {f(b['pnl_book'], '{:+,.0f}'):>7} | {b['status']}")
op = [x for x in s.get("pairs", []) if x["status"] == "ARB OPEN"]
print(f"\nPAIR BOTS: {len(s.get('pairs', []))} races | arbs open now {len(op)} | sets sent {sum(x['sets'] for x in s.get('pairs', []))} | "
      f"locked (verified) {sum(x['locked'] for x in s.get('pairs', [])):+,.0f}")
for x in sorted(op, key=lambda x: -max(x['no_edge'] or -9, x['yes_edge'] or -9))[:6]:
    print(f"  {x['race']:28} bid sum {f(x['bid_sum'])} (NO edge {f(x['no_edge'])}) | ask sum {f(x['ask_sum'])} (YES edge {f(x['yes_edge'])})")
print("\nLATEST FILLS")
for x in [x for x in s["fills"] if not x["before_engine"]][:8]:
    print(f"  {x['t'][11:]} {x['bot'][:28]:28} {x['layer'] or '-':7} {(x['action'] or ''):4} {x['side'].upper():3} {x['qty']:>7,.0f} @ {x['price']:.3f}"
          f"  vs SIG {f(x['sig_edge'])} vs PM {f(x['edge'])}")
print("\nLATEST EVENTS")
for e in s["events"][:10]:
    print(f"  {e['t']} {e['kind']:7}{e['text'][:150]}")
