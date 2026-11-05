#!/usr/bin/env python3
"""Server-side ops for the SIG Cup engine (Piero 00:20 Oct 26 item 1): every night-critical job runs on engine-host from systemd
timers as user sig, never from a Mac session. Stdlib only. Units: deploy/server/ops/systemd/ (install: deploy/server/ops/install.sh).

  watch           every minute: phone alerts (ntfy) for PAUSE, an ERR burst (>= 3 in 10 min), the engine down > 60 s, RULE0 BROKEN,
                  FMM STOPPED, election stops, PERMS FAIL, and election calls on Nov 3 (12:00 -> Nov 4 09:00 PT)
  health          every 30 min: one OPS HEALTH line in RUN_LOG (alerts on what looks wrong)
  cash            12:00 and 15:00: the ELECTION CASH check against the day's ramp; alert when usable is short of it
  summary         08:00 and 20:00: one OPS SUMMARY line in RUN_LOG, and to the phone at low priority (a heartbeat: silence is not "all fine")
  checklist       Nov 3 14:46: the engine's 14:45 ELECTION CHECKLIST line to the phone (alert if it is missing)
  rehearsal       control election_rehearsal with the plan and START below (Sat Oct 31 15:00 PT)
  probe           control election_probe ex 947 YES
  rehearsal-eval  research/night/rehearsal_eval.py on the server's own files -> data/ops/night_rehearsal_<day>.md + a RUN_LOG line
  test            one test alert to the phone
  golive          Nov 3 14:40 PT (Piero 16:20 Nov 2, docs/catch_v3_allin.md C1): control election_mode live and (C133, Piero 18:05)
                  election_recycle live (both refused before Nov 3 14:35);
                  the engine's 14:45 ELECTION CHECKLIST line then reads "mode live"
  golive-test     the same unit and control path on a harmless value: election_mode / election_recycle shadow (what they are), Nov 2 evening
  flip-stop       FINAL (Piero 00:50 Nov 3, docs/night_final_nov3.md): Tue 08:00 control flip -> off (no new FLIP entries; its flips sell on)
  final-watch     FINAL: every 30 min 01:00-08:30 one FINAL WATCH line (ERR / PAUSE / RULE0, restarts, loop p95, FMM / FLIP / CATCH, usable)
  final-1320      FINAL CHECK 13:20: no pending cancels; CATCH / FLIP / FMM entry bids 0; exits resting
  final-1420      FINAL CHECK 14:20: night_lite, loop p95 (10 min) < 3 s, pending cancels 0, usable, the 14:40 timer, WAVE_MULT, END_SWEEP, the lane
  final-1446      FINAL CHECK 14:46: the checklist line / overrides show live for election_mode and election_recycle, else FINAL ALERT + the fallback

The ntfy topic comes from the unit's EnvironmentFile (/etc/sig-engine/env): any variable named NTFY*, read here and never printed or
logged (only the variable's name is)."""
import collections
import contextlib
import fcntl
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.request

ENG = "/opt/sig-engine"
DATA = f"{ENG}/data"
RUN_LOG = f"{ENG}/docs/RUN_LOG.md"
STATE = f"{DATA}/ops/{{}}.json"                        # one state file per job (watch runs every minute beside the others)
PLAN = "/opt/research/night/rehearsal_plan.json"
START_PT = "2026-10-31 15:00:00"                           # rehearsal 3 (Piero 22:40 Oct 27 item 4, with the probe; 1 was Oct 26, 2 Oct 27)
PROBE = dict(ex="947", side="yes")
GOLIVE_NOT_BEFORE = "2026-11-03 14:35"                     # C1: election_mode is never set live before Tue (Piero 16:20 Nov 2)
CALLS_FROM, CALLS_UNTIL = "2026-11-03 12:00", "2026-11-04 09:00"
RL = re.compile(r"^- \[?(\d\d):(\d\d)\]? (.*)$")
DEDUP_S, MAX_PER_H = 600, 30

os.environ.setdefault("TZ", "America/Los_Angeles")
time.tzset()


def now_s():
    return time.strftime("%Y-%m-%d %H:%M")


def load(job):
    try:
        return json.load(open(STATE.format(job)))
    except (OSError, ValueError):
        return {}


def save(job, st):
    path = STATE.format(job)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(st, fh)
    os.replace(tmp, path)


def runlog(text):
    """One line in RUN_LOG, the log.sh format (- [HH:MM] ...)."""
    with open(RUN_LOG, "a") as fh:
        fh.write(f"- [{time.strftime('%H:%M')}] {text}\n")


# ------------------------------------------------------------------------------------------------------------- alerts
ALERT_NAMES = re.compile(r"NTFY|NOTIF|TOPIC|ALERT|PUSH|PHONE", re.I)


def ntfy_target():
    """(url, token, the variable names seen): the topic variable is the first name with TOPIC in it, else NTFY / NTFY_URL; its value
    is a full URL or a bare ntfy.sh topic. Values are never printed."""
    env = {k: v for k, v in os.environ.items() if ALERT_NAMES.search(k) and not k.upper().startswith("SIG_") and v}
    tok_k = next((k for k in env if "TOKEN" in k.upper() or "AUTH" in k.upper()), None)
    srv_k = next((k for k in env if "SERVER" in k.upper() or "HOST" in k.upper()), None)
    topic_k = (next((k for k in env if "TOPIC" in k.upper()), None) or next((k for k in ("NTFY", "NTFY_URL") if k in env), None)
               or next((k for k in env if k not in (tok_k, srv_k) and (env[k].startswith("http") or re.fullmatch(r"[A-Za-z0-9_\-]{6,64}", env[k]))), None))
    if not topic_k:
        return None, None, sorted(env)
    v = env[topic_k].strip()
    url = v if v.startswith("http") else (env.get(srv_k) if srv_k else "https://ntfy.sh").rstrip("/") + "/" + v
    return url, (env.get(tok_k) if tok_k else None), sorted(env)


def send(title, msg, prio="default", tags=()):
    url, tok, names = ntfy_target()
    if not url:
        return False, f"no NTFY topic variable in the unit's environment (names seen: {names or 'none'})"
    req = urllib.request.Request(url, data=msg.encode()[:3500], method="POST",
                                 headers={"Title": title.encode("ascii", "replace").decode()[:200], "Priority": prio, "Tags": ",".join(tags)})
    if tok:
        req.add_header("Authorization", "Bearer " + tok)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return 200 <= r.status < 300, f"HTTP {r.status} via the variable(s) {'+'.join(names)}"
    except Exception as e:                                  # noqa: BLE001 - an alert failure must never stop the watcher
        return False, f"send failed: {type(e).__name__}"


def alert(st, key, title, msg, prio="high", tags=("warning",)):
    """Dedup per key (10 min) and at most 30 a rolling hour; an alert that can't be sent leaves a RUN_LOG line."""
    t = time.time()
    sent = st.setdefault("sent", {})
    if t - float(sent.get(key, 0)) < DEDUP_S:
        return
    hour = [x for x in st.get("hour", []) if t - x < 3600]
    if len(hour) >= MAX_PER_H:
        st["hour"] = hour
        if t - float(sent.get("_flood", 0)) > 3600:
            sent["_flood"] = t
            runlog(f"OPS ALERT FLOOD: {MAX_PER_H} alerts in an hour; further alerts this hour go to RUN_LOG only")
        runlog(f"OPS ALERT (not sent, flood): {title}: {msg[:200]}")
        return
    if ntfy_target()[0] is None:                           # Piero 00:50 Oct 26: silent until NTFY_TOPIC exists (no errors, no per-alert lines)
        if not st.get("idle_noted"):
            st["idle_noted"] = True
            runlog("OPS alerts idle: no NTFY_TOPIC in /etc/sig-engine/env yet; they start by themselves once it is set")
        sent[key] = t
        return
    if st.pop("idle_noted", None):
        runlog("OPS alerts active: NTFY_TOPIC found")
    ok, how = send(title, msg, prio, tags)
    sent[key] = t
    st["hour"] = hour + [t]
    if not ok:
        runlog(f"OPS ALERT NOT SENT ({how}): {title}: {msg[:200]}")


# ------------------------------------------------------------------------------------------------------------- reading
def read_new(path, st, key, cap=2_000_000):
    """The bytes appended to `path` since the last call (the first call starts at the end)."""
    try:
        size = os.path.getsize(path)
    except OSError:
        return ""
    off = st.get(key)
    if off is None or off > size:
        st[key] = size
        return ""
    if size - off > cap:
        off = size - cap
    with open(path, "rb") as fh:
        fh.seek(off)
        data = fh.read(size - off)
    st[key] = size
    return data.decode(errors="replace")


def tail(path, nbytes):
    try:
        with open(path, "rb") as fh:
            fh.seek(max(0, os.path.getsize(path) - nbytes))
            return fh.read().decode(errors="replace")
    except OSError:
        return ""


def last_line(pattern, nbytes=400_000):
    hits = [ln for ln in tail(RUN_LOG, nbytes).splitlines() if re.search(pattern, ln)]
    return hits[-1] if hits else None


def engine_active():
    try:
        return subprocess.run(["systemctl", "is-active", "sig-engine"], capture_output=True, text=True, timeout=10).stdout.strip() == "active"
    except Exception:                                       # noqa: BLE001
        return False


def state_age():
    """Seconds since the engine last wrote engine.log (every loop, ~5-10 s; state.json is only written once a minute)."""
    try:
        return time.time() - os.path.getmtime(f"{DATA}/engine.log")
    except OSError:
        return 1e9


def control(action, args):
    tok = re.search(r"[?&]t=([A-Za-z0-9_\-]+)", open(f"{DATA}/dashboard_url.txt").read()).group(1)
    body = json.dumps(dict(action=action, args=args, confirm=True)).encode()
    req = urllib.request.Request(f"http://127.0.0.1:8765/api/control?t={tok}", data=body, method="POST",
                                 headers={"Content-Type": "application/json", "Origin": "http://127.0.0.1:8765", "Referer": "http://127.0.0.1:8765/"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def num(s):
    return float(s.replace(",", ""))


# ------------------------------------------------------------------------------------------------------------- jobs
def cpu_sample(st, t):
    """Piero 10:15 Oct 26 item 3: the engine's CPU (utime + stime of its main process), memory and the box's load, once a minute ->
    data/ops/cpu.jsonl (the rehearsal's 17:00-wave CPU)."""
    try:
        pid = subprocess.run(["systemctl", "show", "sig-engine", "-p", "MainPID", "--value"], capture_output=True, text=True, timeout=10).stdout.strip()
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        cpu = (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")
        rss = int(open(f"/proc/{pid}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20
        row = dict(t=round(t, 1), pid=int(pid), cpu_s=round(cpu, 2), rss_mb=round(rss, 1), load1=os.getloadavg()[0])
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return
    path = f"{DATA}/ops/cpu.jsonl"
    with open(path, "a") as fh:
        fh.write(json.dumps(row) + "\n")
    if os.path.getsize(path) > 4_000_000:                 # keep the last ~2 MB
        keep = open(path).read()[-2_000_000:].split("\n", 1)[-1]
        with open(path, "w") as fh:
            fh.write(keep)


def topic_check(st):
    """Piero 10:15 Oct 26 item 4: the first time NTFY_TOPIC is there, send the test alert by itself."""
    url = ntfy_target()[0]
    if url and not st.get("topic_ok"):
        st["topic_ok"] = True
        ok, how = send("SIG ops test", f"First alert from engine-host ops at {now_s()} PT: NTFY_TOPIC found, alerts are live.", "high", ("bell",))
        runlog(f"OPS ALERT TEST (automatic, NTFY_TOPIC found): {'sent' if ok else 'NOT sent'} ({how})")
        st.pop("idle_noted", None)
    elif not url and st.get("topic_ok"):
        st.pop("topic_ok", None)


def watch(st):
    t = time.time()
    cpu_sample(st, t)
    topic_check(st)
    # 1. the engine down > 60 s (a push's restart gets 4 min before it counts)
    down = not engine_active() or state_age() > 60
    if down:
        st.setdefault("down_since", t)
        try:
            push_age = t - os.path.getmtime(f"{DATA}/releases.log")
        except OSError:
            push_age = 1e9
        grace = 240 if push_age < 300 else 60
        if t - st["down_since"] >= grace and not st.get("down_alerted"):
            st["down_alerted"] = True
            alert(st, "down", "SIG ENGINE DOWN", f"sig-engine {'inactive' if not engine_active() else 'not looping'} for {t - st['down_since']:.0f} s "
                  f"(engine.log {state_age():.0f} s old) at {now_s()} PT", "urgent", ("rotating_light",))
            runlog(f"OPS ENGINE DOWN for {t - st['down_since']:.0f} s (alert sent)")
    elif st.get("down_since"):
        if st.get("down_alerted"):
            alert(st, "back", "SIG engine back", f"sig-engine running again after {t - st['down_since']:.0f} s", "default", ("white_check_mark",))
            runlog(f"OPS ENGINE BACK after {t - st['down_since']:.0f} s")
        st.pop("down_since", None); st.pop("down_alerted", None)
    # 2. ERR burst: >= 3 engine ERR lines in 10 min (engine.log; the engine itself pauses at 5)
    n_err = len(re.findall(r"(?m)^\s+ERR\s", read_new(f"{DATA}/engine.log", st, "elog_off")))
    hist = [x for x in st.get("err", []) if t - x[0] < 600] + ([[t, n_err]] if n_err else [])
    st["err"] = hist
    st["err_total"] = int(st.get("err_total", 0)) + n_err
    if sum(n for _, n in hist) >= 3:
        alert(st, "errburst", "SIG ERR burst", f"{sum(n for _, n in hist)} engine ERR lines in 10 min ({now_s()} PT); the engine pauses new risk at 5",
              "high", ("warning",))
    # 3. RUN_LOG lines
    calls_on = CALLS_FROM <= now_s() < CALLS_UNTIL
    for ln in read_new(RUN_LOG, st, "rl_off").splitlines():
        m = RL.match(ln)
        if not m:
            continue
        txt = m.group(3)
        if txt.startswith(("OPS ", "OPS:")):
            continue
        if txt.startswith("PAUSE") or "paused 3x within 2 h" in txt:
            st["pause_total"] = int(st.get("pause_total", 0)) + 1
            alert(st, "pause:" + txt[:40], "SIG PAUSE", txt[:600], "high", ("pause_button",))
        elif txt.startswith("RULE0 BROKEN"):
            st["rule0_total"] = int(st.get("rule0_total", 0)) + 1
            alert(st, "rule0:" + txt[:60], "SIG RULE0 BROKEN", txt[:600], "urgent", ("no_entry",))
        elif txt.startswith("FMM STOPPED"):
            alert(st, "fmmstop", "SIG FMM STOPPED", txt[:600], "high", ("stop_sign",))
        elif txt.startswith("PERMS FAIL") or " PERMS FAIL" in txt[:30]:
            alert(st, "perms", "SIG PERMS FAIL", txt[:600], "high", ("lock",))
        elif re.match(r"ELECTION \((live|shadow)\) STOP", txt) or "the sniper stops on this race" in txt:
            alert(st, "elstop:" + txt[:50], "SIG ELECTION STOP", txt[:600], "high", ("stop_sign",))
        elif re.match(r"ELECTION \(live\) REVERSAL", txt) or (calls_on and re.match(r"ELECTION \(\w+\) REVERSAL", txt)):
            alert(st, "elrev:" + txt[:60], "SIG REVERSAL", txt[:600], "urgent", ("rewind",))
        elif calls_on and re.match(r"ELECTION \(\w+\) (CALL|STRICT) ", txt):
            alert(st, "call:" + txt[:70], "SIG CALL", txt[:600], "high", ("ballot_box_with_check",))
        elif calls_on and txt.startswith("ELECTION CHECKLIST"):
            alert(st, "checklist", "SIG 14:45 checklist", txt[:1500], "high", ("clipboard",))


def health(st):
    act, age = engine_active(), state_age()
    loops = [int(x) for x in re.findall(r"took (\d+)s", tail(f"{DATA}/engine.log", 400_000))[-200:]]
    mean = sum(loops) / len(loops) if loops else float("nan")
    since = subprocess.run(["systemctl", "show", "sig-engine", "-p", "ActiveEnterTimestamp", "--value"], capture_output=True, text=True).stdout.strip()
    fmm = last_line(r"FMM \((live|stopped|paper)\) quotes") or ""
    fm = re.search(r"FMM \((\w+)\).*?inventory at cost ([\d,]+ of [\d,]+)", fmm)
    el = last_line(r"ELECTION \((\w+)\) last hour") or ""
    em = re.search(r"ELECTION \((\w+)\)", el)
    w = load("watch")                                      # the watcher's running totals (read only)
    h0 = st.get("health0", {})
    d = {k: int(w.get(k, 0)) - int(h0.get(k, 0)) for k in ("err_total", "pause_total", "rule0_total")}
    st["health0"] = {k: int(w.get(k, 0)) for k in ("err_total", "pause_total", "rule0_total")}
    bad = (not act) or age > 60 or d["err_total"] >= 3 or d["pause_total"] or d["rule0_total"]
    line = (f"OPS HEALTH: sig-engine {'active' if act else 'NOT ACTIVE'} since {since}, last loop {age:.0f} s ago, loop mean {mean:.1f} s; last 30 min "
            f"ERR {d['err_total']} / PAUSE {d['pause_total']} / RULE0 {d['rule0_total']}; FMM {fm.group(1) if fm else '?'} "
            f"({fm.group(2) if fm else '?'}); election {em.group(1) if em else '?'}" + ("; LOOK" if bad else ""))
    runlog(line)
    if bad:
        alert(st, "health", "SIG health check", line, "high", ("warning",))


def ramp_target(t=None):
    """The day's reserve from a_config (firm ramp + the elastic 15k), the engine's own numbers."""
    sys.path.insert(0, ENG)
    import a_config as c                                    # noqa: E402 - config only
    er = c.ELECTION_RESERVE
    s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t or time.time()))
    if not er.get("on") or s >= er["until"]:
        return 0.0
    firm = 0.0
    for at, v in er["steps"]:
        if s >= at:
            firm = float(v)
    return firm + (float(er.get("elastic") or 0) if er.get("elastic_from", "9") <= s else 0.0)


def cash(st):
    ln = last_line(r"ELECTION CASH:")
    m = re.search(r"usable (-?[\d,]*\d)", ln or "")
    a = re.search(r"sale asks resting would bring ([\d,]+)", ln or "")
    usable = num(m.group(1)) if m else None
    target = ramp_target()
    if usable is None:
        runlog("OPS CASH: no ELECTION CASH line found in RUN_LOG")
        alert(st, "cashmissing", "SIG cash check", "no ELECTION CASH line in RUN_LOG", "high")
        return
    short = target - usable
    txt = (f"OPS CASH {time.strftime('%H:%M')}: usable {usable:,.0f} vs today's ramp target {target:,.0f}: "
           + (f"SHORT {short:,.0f} (sale asks resting would bring {a.group(1) if a else '?'})" if short > 0 else "OK"))
    runlog(txt)
    if short > 0 and target > 0:
        alert(st, "cashshort", "SIG cash short of the ramp", txt, "high", ("moneybag",))


def rank_line():
    rows = tail(f"{DATA}/leaders.csv", 600_000).splitlines()
    try:
        hdr = open(f"{DATA}/leaders.csv").readline().strip().split(",")
    except OSError:
        return "rank n/a"
    recs = [dict(zip(hdr, r.split(","))) for r in rows[1:] if r.count(",") == len(hdr) - 1]
    if not recs:
        return "rank n/a"
    last = max(r["time"] for r in recs)
    cur = [r for r in recs if r["time"] == last]
    by = {r.get("rank"): r for r in cur}
    us = next((r for r in cur if r.get("ours") == "1"), None)
    f = lambda r: f"{float(r['value']):,.0f}" if r and r.get("value") else "?"
    return f"rank {us.get('rank') if us else '?'} ({f(us)}) vs #50 {f(by.get('50'))} / #100 {f(by.get('100'))} at {last[11:16]}"


def summary(st):
    w = load("watch")
    s0 = st.get("sum0", {})
    d = {k: int(w.get(k, 0)) - int(s0.get(k, 0)) for k in ("err_total", "pause_total", "rule0_total")}
    st["sum0"] = {k: int(w.get(k, 0)) for k in ("err_total", "pause_total", "rule0_total")}
    try:
        s = json.load(open(f"{DATA}/state.json"))
        a = s.get("account") or {}
        acct = (f"account {a.get('totalAccountValue', 0):,.0f}, cash {a.get('totalAccountValue', 0) - a.get('totalHoldingsValue', 0):,.0f}, "
                f"P&L {(s.get('pnl') or {}).get('total', 0):+,.0f} (at fair {(s.get('pnl') or {}).get('fair', 0):+,.0f})")
    except (OSError, ValueError):
        acct = "account n/a"
    fmm = (last_line(r"FMM \((live|stopped|paper)\) quotes") or "")
    fm = re.search(r"FMM \((\w+)\).*?realized ([+\-]?[\d,]*\d).*?inventory at cost ([\d,]+ of [\d,]*\d).*?coverage (\d+ of \d+)", fmm)
    el = (last_line(r"ELECTION \((\w+)\) last hour") or "")
    ec = (last_line(r"ELECTION CASH:") or "")
    ecm = re.search(r"usable (-?[\d,]*\d).*?election reserve now ([\d,]*\d)", ec)
    since = subprocess.run(["systemctl", "show", "sig-engine", "-p", "ActiveEnterTimestamp", "--value"], capture_output=True, text=True).stdout.strip()
    line = (f"OPS SUMMARY {time.strftime('%H:%M')}: since the last summary ERR {d['err_total']} / PAUSE {d['pause_total']} / RULE0 {d['rule0_total']}; "
           f"sig-engine {'active' if engine_active() else 'NOT ACTIVE'} since {since}; {acct}; "
           + (f"FMM {fm.group(1)} realized {fm.group(2)}, inventory {fm.group(3)}, {fm.group(4)} books; " if fm else "FMM n/a; ")
           + (f"election {re.search(r'ELECTION \((\w+)\)', el).group(1)}; " if el else "")
           + (f"ELECTION CASH usable {ecm.group(1)} vs reserve now {ecm.group(2)}; " if ecm else "")
           + rank_line())
    runlog(line)
    if ntfy_target()[0]:
        send(f"SIG summary {time.strftime('%H:%M')}", line[len("OPS SUMMARY 00:00: "):][:1500], "low", ("memo",))   # the daily heartbeat


def checklist(st):
    today = time.strftime("%H:%M")
    ln = last_line(r"ELECTION CHECKLIST", 200_000)
    if ln and RL.match(ln) and RL.match(ln).group(1) + ":" + RL.match(ln).group(2) >= "14:45":
        alert(st, "checklist", "SIG 14:45 checklist", RL.match(ln).group(3)[:1500], "high", ("clipboard",))
        runlog("OPS CHECKLIST: the engine's 14:45 line sent to the phone")
    else:
        msg = (f"no ELECTION CHECKLIST line since 14:45 (now {today}); engine {'active' if engine_active() else 'NOT ACTIVE'}, "
               f"last loop {state_age():.0f} s ago; usable target {ramp_target():,.0f}")
        alert(st, "checklist-missing", "SIG checklist MISSING", msg, "urgent", ("warning",))
        runlog("OPS CHECKLIST MISSING: " + msg)


def rehearsal(st):
    start = time.mktime(time.strptime(START_PT, "%Y-%m-%d %H:%M:%S"))
    try:
        p = json.load(open(PLAN))
        if abs(float(p.get("start_epoch") or 0) - start) > 1:
            raise ValueError(f"the plan is built for {p.get('start_pt')}, not {START_PT}")
        r = control("election_rehearsal", dict(file=PLAN, start=start))
        txt = f"OPS REHEARSAL started by the server timer: {r.get('message') or r}"
        runlog(txt)
        alert(st, "rehearsal", "SIG rehearsal started", txt, "default", ("clapper",))
    except Exception as e:                                  # noqa: BLE001
        runlog(f"OPS REHEARSAL FAILED to start: {e!r}")
        alert(st, "rehearsal-fail", "SIG rehearsal FAILED to start", repr(e)[:500], "urgent", ("warning",))


def probe(st):
    try:
        r = control("election_probe", dict(PROBE))
        txt = f"OPS PROBE sent by the server timer ({PROBE['ex']} {PROBE['side'].upper()}): {r.get('message') or r}"
        runlog(txt)
        alert(st, "probe", "SIG probe sent", txt, "default", ("satellite",))
    except Exception as e:                                  # noqa: BLE001
        runlog(f"OPS PROBE FAILED: {e!r}")
        alert(st, "probe-fail", "SIG probe FAILED", repr(e)[:500], "urgent", ("warning",))


def rehearsal_eval(st):
    out = f"{DATA}/ops/night_rehearsal_{START_PT[:10]}.md"
    here = os.path.dirname(os.path.abspath(__file__))
    env = dict(os.environ, PYTHONPATH=ENG)
    py = f"{ENG}/.venv/bin/python3" if os.path.exists(f"{ENG}/.venv/bin/python3") else sys.executable
    r = subprocess.run([py, f"{here}/night/rehearsal_eval.py", "--from-dir", ENG, "--start", START_PT, "--plan-json", PLAN, "--out", out],
                       capture_output=True, text=True, timeout=900, env=env, cwd=ENG)
    body = open(out).read() if os.path.exists(out) else ""
    n_pass, n_fail = len(re.findall(r"\bPASS\b", body)), len(re.findall(r"\bFAIL\b", body))
    head = re.search(r"(?im)^.*verdict.*$", body)
    txt = (f"OPS REHEARSAL RESULT: {n_pass} PASS / {n_fail} FAIL mentions; {head.group(0).strip()[:300] if head else ''} "
           f"(data/ops/{os.path.basename(out)}; evaluator exit {r.returncode}{'; ' + r.stderr.strip()[-200:] if r.returncode else ''})")
    runlog(txt)
    c = subprocess.run([py, f"{here}/night/rehearsal_criteria.py", "--base", ENG, "--start", START_PT], capture_output=True, text=True, timeout=600, cwd=ENG)
    lines = [ln for ln in c.stdout.splitlines() if ln.startswith("REHEARSAL ")] or [f"REHEARSAL criteria: script exit {c.returncode} {c.stderr.strip()[-200:]}"]
    for ln in lines:                                       # Piero 10:15 Oct 26 item 3: one pass / fail line each
        runlog("OPS " + ln)
    bad = [ln for ln in lines if ": FAIL" in ln or "NO DATA" in ln]
    alert(st, "rehearsal-result", "SIG rehearsal result", txt + "\n" + "\n".join(lines), "high" if (bad or n_fail or r.returncode) else "default", ("clipboard",))


def test(st):
    print("environment variable NAMES (values never shown):", " ".join(sorted(k for k in os.environ if k not in ("PATH", "LANG", "HOME", "LOGNAME", "USER", "SHELL", "INVOCATION_ID", "JOURNAL_STREAM", "SYSTEMD_EXEC_PID", "TZ", "PWD", "MEMORY_PRESSURE_WATCH", "MEMORY_PRESSURE_WRITE"))))
    ok, how = send("SIG ops test", f"Test alert from engine-host ops.py at {now_s()} PT (Piero 00:20 Oct 26 item 1).", "high", ("bell",))
    print(f"test alert: {'sent' if ok else 'NOT sent'} ({how})")
    runlog(f"OPS ALERT TEST: {'sent' if ok else 'NOT sent'} ({how})")


# ---- FINAL (Piero 00:50 Nov 3, docs/night_final_nov3.md): the run-in to the night. Read-only on the engine except flip-stop's control call.
FLIP_STOP_AT, WATCH_UNTIL = "2026-11-03 07:59", "2026-11-03 08:31"
ENTRY_LAYERS, EXIT_LAYERS = ("catch", "flip", "fmm", "fmmtake"), ("catchexit", "flipexit", "fmmexit")
FALLBACK = "ssh $SIG_SERVER systemctl start sig-ops@golive.service"   # the golive job: election_mode + election_recycle live (refused before 14:35)


def loop_p95(minutes):
    """p95 of the engine's 'took Ns' over the last `minutes` (engine.log lines start HH:MM:SS)."""
    now = time.time(); lt = time.localtime(now); xs = []
    for ln in tail(f"{DATA}/engine.log", 6_000_000).splitlines():
        m = re.match(r"(\d\d):(\d\d):(\d\d) loop \d+ \|.*took (\d+)s", ln)
        if m:
            t = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, int(m.group(1)), int(m.group(2)), int(m.group(3)), 0, 0, -1))
            if 0 <= now - t <= minutes * 60:
                xs.append(int(m.group(4)))
    xs.sort()
    return (xs[min(len(xs) - 1, int(0.95 * len(xs)))] if xs else None), len(xs)


def mm_facts():
    L = json.load(open(f"{DATA}/ledger.json")); S = json.load(open(f"{DATA}/state.json"))
    a = S.get("account") or {}
    n, notl = collections.Counter(), collections.Counter()
    for o in L["orders"].values():
        if o.get("status") == "open" and o.get("layer") in ENTRY_LAYERS + EXIT_LAYERS:
            n[o["layer"]] += 1; notl[o["layer"]] += float(o.get("qty") or 0) * float(o.get("price") or 0)
    sh = {pre: round(sum(abs(v) for b in L["subbooks"].values() for k, v in b.items() if str(k).startswith(pre))) for pre in ("fmm:", "flip:", "catch2:", "catch:")}
    q = (S.get("executor") or {}).get("queue") or {}
    u = re.search(r"usable ([\d,]+)", last_line(r"ELECTION CASH:") or "")
    return dict(cash=round((a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)), n=n, notl=notl, sh=sh,
                cancels=int(q.get("0", 0)), state_t=S.get("time"), usable_line=u.group(1) if u else "?")


def final_watch(st):
    if now_s() >= WATCH_UNTIL:
        return
    try:
        w = load("watch"); f0 = st.get("final0", {})
        d = {k: int(w.get(k, 0)) - int(f0.get(k, 0)) for k in ("err_total", "pause_total", "rule0_total")} if f0 else {k: 0 for k in ("err_total", "pause_total", "rule0_total")}
        st["final0"] = {k: int(w.get(k, 0)) for k in ("err_total", "pause_total", "rule0_total")}
        nr = subprocess.run(["systemctl", "show", "sig-engine", "-p", "NRestarts", "--value"], capture_output=True, text=True).stdout.strip()
        since = subprocess.run(["systemctl", "show", "sig-engine", "-p", "ActiveEnterTimestamp", "--value"], capture_output=True, text=True).stdout.strip()
        p95, nl = loop_p95(30); m = mm_facts(); n = m["n"]
        fl = re.search(r"FLIP hourly.*?realized ([+\-\d,]+); inventory ([\d,]+) at cost", last_line(r"FLIP hourly") or "")
        cl = re.search(r"exits filled (\d+) \(([\d,]+) shares\), realized ([+\-\d,]+)", last_line(r"CATCH hourly") or "")
        fm = re.search(r"inventory at cost ([\d,]+ of [\d,]+)", last_line(r"FMM \((live|stopped|paper)\) quotes") or "")
        bad = (not engine_active()) or d["err_total"] >= 3 or d["pause_total"] or d["rule0_total"]
        runlog(f"FINAL WATCH: sig-engine {'active' if engine_active() else 'NOT ACTIVE'} since {since}, NRestarts {nr}; last 30 min ERR {d['err_total']} / PAUSE "
               f"{d['pause_total']} / RULE0 {d['rule0_total']}; loop p95 {p95} s ({nl} loops); FMM {m['sh']['fmm:']:,} sh held"
               f"{' (' + fm.group(1) + ' at cost)' if fm else ''}, exits resting {n['fmmexit']}, entry bids {n['fmm'] + n['fmmtake']}; FLIP {m['sh']['flip:']:,} sh"
               f"{' (' + fl.group(2) + ' at cost, realized ' + fl.group(1) + ')' if fl else ''}, flips resting {n['flipexit']}, entry bids {n['flip']}; CATCH bids {n['catch']} "
               f"({m['notl']['catch']:,.0f}), exits resting {n['catchexit']}{', ' + cl.group(1) + ' exit fills, realized ' + cl.group(3) if cl else ''}; v1 {m['sh']['catch:']:,} sh; "
               f"cash {m['cash']:,}, usable {m['usable_line']} (last ELECTION CASH); pending cancels {m['cancels']}" + ("; LOOK" if bad else ""))
    except Exception as e:                                  # noqa: BLE001
        runlog(f"FINAL WATCH FAILED: {e!r}")


def flip_stop(st):
    try:
        if now_s() < FLIP_STOP_AT:
            raise ValueError(f"refused before {FLIP_STOP_AT} PT (now {now_s()})")
        r = control("layer_state", dict(layer="flip", state="off"))
        runlog(f"FINAL FLIP 08:00 by the server timer: control flip -> off ({r.get('message') or r}): no new FLIP entries; its flips keep selling")
    except Exception as e:                                  # noqa: BLE001
        runlog(f"FINAL ALERT: the 08:00 FLIP stop FAILED: {e!r} (by hand: dashboard layer_state flip off)")


def final_1320(st):
    try:
        m = mm_facts(); n = m["n"]; ent = n["catch"] + n["flip"] + n["fmm"] + n["fmmtake"]
        ok = m["cancels"] == 0 and ent == 0
        runlog(f"FINAL CHECK 13:20: {'PASS' if ok else 'FAIL'}: pending cancels {m['cancels']} (want 0); entry bids CATCH {n['catch']} / FLIP {n['flip']} / FMM "
               f"{n['fmm'] + n['fmmtake']} (want 0); exits resting CATCH {n['catchexit']} / FLIP {n['flipexit']} / FMM {n['fmmexit']} (held: CATCH v2 {m['sh']['catch2:']:,} sh, "
               f"v1 {m['sh']['catch:']:,}, FLIP {m['sh']['flip:']:,}, FMM {m['sh']['fmm:']:,}); cash {m['cash']:,}")
    except Exception as e:                                  # noqa: BLE001
        runlog(f"FINAL CHECK 13:20 FAILED to run: {e!r}")


def final_1420(st):
    try:
        sys.path.insert(0, ENG)
        import a_config as A                                # noqa: E402 - the running release's files (unchanged since its start: checked below)
        import n_election as el                             # noqa: E402
        since = subprocess.run(["systemctl", "show", "sig-engine", "-p", "ActiveEnterTimestamp", "--value"], capture_output=True, text=True).stdout.strip()
        t0 = time.mktime(time.strptime(since.rsplit(" ", 1)[0], "%a %Y-%m-%d %H:%M:%S"))
        newer = [f for f in ("a_config.py", "n_election.py", "f_engine.py", "j_executor.py") if os.path.getmtime(f"{ENG}/{f}") > t0]
        nl = A.ELECTION.get("night_lite") or ("", ""); t = time.strftime("%Y-%m-%d %H:%M:%S")
        p95, nloops = loop_p95(10); m = mm_facts()
        tm = subprocess.run(["systemctl", "list-timers", "sig-ops-golive.timer", "--all", "--no-pager"], capture_output=True, text=True).stdout.splitlines()
        armed = any("sig-ops-golive.timer" in ln and "14:40:00" in ln for ln in tm)
        import inspect                                      # noqa: E402
        both = "election_recycle" in inspect.getsource(election_mode_job)
        lr = re.search(r"self\.ex\.lane_reserve = (\d+) if mode == \"live\"", open(f"{ENG}/f_engine.py").read())
        es = A.END_SWEEP
        ok = (nl[0] <= t < nl[1]) and p95 is not None and p95 < 3 and m["cancels"] == 0 and armed and both and el.WAVE_MULT == 2.5 and not newer
        runlog(f"FINAL CHECK 14:20: {'PASS' if ok else 'FAIL'}: night_lite {'active' if nl[0] <= t < nl[1] else 'NOT active'} ({nl[0][11:16]}-{nl[1][5:16]}); loop p95 last 10 min "
               f"{p95} s ({nloops} loops; want < 3); pending cancels {m['cancels']}; usable {m['usable_line']} (cash {m['cash']:,}); 14:40 timer "
               f"{'armed' if armed else 'NOT ARMED'} for election_mode{' + election_recycle' if both else ' (recycle MISSING)'}; WAVE_MULT {el.WAVE_MULT}; END_SWEEP "
               f"{'on' if es.get('on') else 'OFF'} {es['start'][11:16]}-{es['stop'][11:16]}; call-lane reserve {lr.group(1) if lr else '?'}/min while live; "
               f"code changed since the start: {newer or 'none'}")
    except Exception as e:                                  # noqa: BLE001
        runlog(f"FINAL CHECK 14:20 FAILED to run: {e!r}")


def final_1446(st):
    try:
        ck = last_line(r"ELECTION CHECKLIST") or ""
        el_ = json.load(open(f"{DATA}/overrides.json")).get("election") or {}
        ok = bool(re.search(r"ELECTION CHECKLIST.*mode live", ck)) and el_.get("mode") == "live" and el_.get("recycle_mode") == "live"
        runlog(f"FINAL CHECK 14:46: {'PASS' if ok else 'FAIL'}: election_mode {el_.get('mode')!r}, election_recycle {el_.get('recycle_mode')!r}; checklist line: {ck[:200] or '(none)'}")
        if not ok:
            runlog(f"FINAL ALERT: election_mode / election_recycle are not both live at 14:46. By hand (one line): {FALLBACK}")
            alert(st, "final-1446", "SIG FINAL ALERT", f"not live at 14:46; run: {FALLBACK}", "urgent", ("warning",))
    except Exception as e:                                  # noqa: BLE001
        runlog(f"FINAL CHECK 14:46 FAILED to run: {e!r}. By hand: {FALLBACK}")


def election_mode_job(st, mode):
    """C1 (Piero 16:20 Nov 2): election_mode -> mode through the dashboard's control (the engine logs "ELECTION mode -> ..."), then the
    value on disk read back into the OPS line. Live is refused before GOLIVE_NOT_BEFORE."""
    tag = "OPS GOLIVE" if mode == "live" else "OPS GOLIVE TEST"
    try:
        if mode == "live" and now_s() < GOLIVE_NOT_BEFORE:
            raise ValueError(f"election_mode live is refused before {GOLIVE_NOT_BEFORE} PT (now {now_s()})")
        r = control("election_mode", dict(mode=mode))
        r2 = control("election_recycle", dict(mode=mode))   # C133 (Piero 18:05 Nov 2, docs/flip_nov2.md): wave recycling with the mode
        time.sleep(3)
        el_ = json.load(open(f"{DATA}/overrides.json")).get("election") or {}
        txt = (f"{tag} by the server timer: election_mode -> {mode}: {r.get('message') or r}; election_recycle -> {mode}: {r2.get('message') or r2}; "
               f"overrides.json election.mode now {el_.get('mode')!r}, recycle_mode {el_.get('recycle_mode')!r}")
        runlog(txt)
        alert(st, f"golive-{mode}", f"SIG election_mode {mode}", txt, "high" if mode == "live" else "default", ("rocket",))
    except Exception as e:                                  # noqa: BLE001
        runlog(f"{tag} FAILED: {e!r}")
        alert(st, f"golive-{mode}-fail", f"SIG election_mode {mode} FAILED", repr(e)[:500], "urgent", ("warning",))


def golive(st):
    election_mode_job(st, "live")


def golive_test(st):
    election_mode_job(st, "shadow")


def night_hourly(st):
    """Piero 21:05 Nov 3 item 4: every hour through the night one NIGHT HOURLY line: board (SIG account value, P&L), fair (the engine's
    P&L at PM fair, plus every held side called for us that has no PM fair valued at 1.00 instead of the engine's SIG-book-mid fallback,
    item 1), rank vs #50 / #70 / #100 (leaders.csv), cash, and the last hour's RECYCLE FAST fills."""
    try:
        s = json.load(open(f"{DATA}/state.json"))
    except (OSError, ValueError) as e:
        runlog(f"NIGHT HOURLY {time.strftime('%H:%M')}: state.json unreadable ({e!r})")
        return
    a, pnl = s.get("account") or {}, s.get("pnl") or {}
    tav = float(a.get("totalAccountValue") or 0)
    cash_ = tav - float(a.get("totalHoldingsValue") or 0)
    calls = {}
    try:
        for ln in open(f"{DATA}/election/calls.jsonl"):
            if '"CALL"' not in ln and '"REVERSAL"' not in ln:
                continue
            r = json.loads(ln)
            if not str(r.get("t", "")).startswith(time.strftime("%Y-%m-%d", time.localtime(time.time() - 9 * 3600))):
                continue
            if r.get("kind") == "CALL":
                calls[str(r["ex"])] = dict(side=r.get("side"), rev=False)
            elif str(r["ex"]) in calls:
                calls[str(r["ex"])]["rev"] = True
    except (OSError, ValueError):
        pass
    adj, n_adj = 0.0, 0
    for b in s.get("bots") or []:
        q = float(b.get("pos") or 0)
        if abs(q) < 1 or b.get("fair") is not None:
            continue
        held = "yes" if q > 0 else "no"
        c = calls.get(str(b.get("ex")))
        if not c or c["side"] != held or c["rev"]:
            continue
        bid, ask, val = b.get("bid"), b.get("ask"), b.get("valuation")
        if bid is not None and ask is not None:
            m = (bid + ask) / 2 if held == "yes" else 1 - (bid + ask) / 2
        elif val is not None:
            m = val if held == "yes" else 1 - val
        else:
            continue
        adj += abs(q) * (1.0 - m)
        n_adj += 1
    rows = tail(f"{DATA}/leaders.csv", 600_000).splitlines()
    try:
        hdr = open(f"{DATA}/leaders.csv").readline().strip().split(",")
    except OSError:
        hdr = []
    recs = [dict(zip(hdr, r.split(","))) for r in rows[1:] if hdr and r.count(",") == len(hdr) - 1]
    rk = "rank n/a"
    if recs:
        last = max(r["time"] for r in recs)
        cur = [r for r in recs if r["time"] == last]
        by = {r.get("rank"): r for r in cur}
        us = next((r for r in cur if r.get("ours") == "1"), None)
        f = lambda r: f"{float(r['value']):,.0f}" if r and r.get("value") else "?"
        gap = (float(by["70"]["value"]) - float(us["value"])) if us and by.get("70") and us.get("value") and by["70"].get("value") else None
        rk = (f"rank #{us.get('rank') if us else '?'} ({f(us)}) vs #50 {f(by.get('50'))} / #70 {f(by.get('70'))} / #100 {f(by.get('100'))} "
              f"(leaderboard {last[11:16]}); gap to #70 " + ("?" if gap is None else f"{gap:+,.0f} on the board, {gap - adj:+,.0f} after the +{adj:,.0f} fair adjustment"))   # Piero 21:40 item 3
    t0 = time.time() - 3600
    fills = [ln for ln in tail(RUN_LOG, 3_000_000).splitlines() if "RECYCLE FAST FILL" in ln]
    rq = rc = 0.0
    nf = 0
    for ln in fills:
        m = re.search(r"FILL .*? ([\d,]+) at ([\d.]+) at (\d\d:\d\d:\d\d) .*?cash freed ([\d,]+)", ln)
        if not m:
            continue
        ft = time.mktime(time.strptime(time.strftime("%Y-%m-%d ") + m.group(3), "%Y-%m-%d %H:%M:%S"))
        if ft > time.time() + 60:
            ft -= 86400
        if ft >= t0:
            nf += 1; rq += num(m.group(1)); rc += num(m.group(4))
    pf = float(pnl.get("fair") or 0)
    runlog(f"NIGHT HOURLY {time.strftime('%H:%M')}: board {tav:,.0f} (P&L {float(pnl.get('total') or 0):+,.0f}); at fair {pf:+,.0f} engine, "
           f"{pf + adj:+,.0f} with {n_adj} held side(s) called for us and no PM fair at 1.00 (+{adj:,.0f}); {rk}; cash {cash_:,.0f}; "
           f"RECYCLE FAST last hour: {nf} fill(s), {rq:,.0f} shares, cash freed {rc:,.0f}")


# ------------------------------------------------------------------------------------------------ CLEANUP (Piero 01:00 Nov 4)
CLEANUP_FROM = "2026-11-04 01:00:00"
SNAP_DOC = f"{ENG}/docs/snapshot_final_nov4.md"
SNAP_COPY = f"{DATA}/ops/snapshot_final_nov4.md"           # the same file under data/, so the Mac's mirror carries it too
START_VALUE = 100_000.0
GAMMA = "https://gamma-api.polymarket.com"
SIG_TITLE = re.compile(r"Will the (\w+) Party win the (.+?)\??$")
BUY_BAN = ("Georgia", "Alaska", "Maine", "U.S. House", "U.S. Senate")   # item 2: never the control markets, Georgia, Alaska, Maine
LEFT_BY_HAND = (("1067", "Alaska Senate R NO at 0.81"), ("1033", "VA-05 House race D NO at 0.86, 0.84 from 02:30"))   # sell_now can't rest an ask (cancelled a loop later)


def race_party(title):
    m = SIG_TITLE.match((title or "").strip())
    return (m.group(2), m.group(1)[0]) if m else None


def gamma(path):
    req = urllib.request.Request(GAMMA + path, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


def pm_legs(bots):
    """{ex: [Polymarket market ids]}: data/pm_map.json, plus a_config.PM_EXTRA's candidate legs (summed) for the markets it lacks."""
    sys.path.insert(0, ENG)
    import a_config as A                                    # noqa: E402 - config only
    mp = {str(m["ex"]): list(m.get("pm_ids") or ([str(m["pm_id"])] if m.get("pm_id") else [])) for m in json.load(open(f"{DATA}/pm_map.json"))}
    for b in bots:
        rp = race_party(b.get("market"))
        if str(b["ex"]) not in mp and rp in A.PM_EXTRA:
            mp[str(b["ex"])] = list(A.PM_EXTRA[rp])
    return mp


def pm_book(bots):
    """{ex: (status, yes_bid, yes_ask)} from Polymarket's public Gamma API for every mapped SIG market (data/pm_map.json, plus
    a_config.PM_EXTRA's candidate sums): status resolved / proposed (UMA) / open; a resolved or proposed market is at its outcome price."""
    mp = pm_legs(bots)
    ids, raw = sorted({i for v in mp.values() for i in v}), {}
    for closed in ("", "&closed=true"):                     # closed markets come back only when asked for
        todo = [i for i in ids if i not in raw]
        for k in range(0, len(todo), 50):
            for x in gamma(f"/markets?limit=100{closed}&" + "&".join(f"id={j}" for j in todo[k:k + 50])):
                raw[str(x["id"])] = x
    out = {}
    for ex, legs in mp.items():
        legs = [raw.get(i) for i in legs]
        if not legs or any(l is None for l in legs):
            continue
        sts = ["resolved" if l.get("closed") or l.get("umaResolutionStatus") == "resolved" else
               "proposed" if l.get("umaResolutionStatus") == "proposed" else "open" for l in legs]
        status = "resolved" if all(s == "resolved" for s in sts) else "proposed" if "open" not in sts else "open"
        if status == "open":
            b_ = sum(float(l.get("bestBid") or 0) for l in legs)
            a_ = sum(float(l["bestAsk"] if l.get("bestAsk") is not None else (l.get("bestBid") or 0)) for l in legs)
        else:
            b_ = a_ = sum(float(json.loads(l.get("outcomePrices") or "[0]")[0]) for l in legs)
        out[ex] = (status, b_, a_)
    return out


def our_fills():
    """Every fill of the account, oldest first (SIG fill ids grow): data/fills_archive.jsonl (SIG's fills since Oct 1), each with the
    ledger's action where the ledger has its key (the archive leaves `action` out of older fills and of some hand fills, e.g. Piero's
    23:27 Nov 3 Alaska Senate R NO sale); ledger-only fills added."""
    L = json.load(open(f"{DATA}/ledger.json"))
    led = {f["key"]: f for f in L.get("fills") or [] if f.get("key")}
    sub_of = lambda f: ((L.get("orders") or {}).get(str(f.get("order"))) or {}).get("sub")
    out, seen = [], set()
    for ln in open(f"{DATA}/fills_archive.jsonl"):
        if not ln.strip():
            continue
        r = json.loads(ln)
        lf = led.get(r.get("key")) or {}
        seen.add(r.get("key"))
        out.append(dict(id=int(r["id"]), ex=str(r["exchangeId"]), side=r["side"], action=lf.get("action") or r.get("action") or "buy",
                        qty=abs(float(r["quantity"])), price=float(r["price"]), t=lf.get("t"), layer=lf.get("layer"), sub=sub_of(lf)))
    for k, f in led.items():
        if k not in seen and str(k).split(":")[0].isdigit():
            out.append(dict(id=int(str(k).split(":")[0]), ex=str(f["ex"]), side=f["side"], action=f["action"], qty=abs(float(f["qty"])),
                            price=float(f["price"]), t=f["t"], layer=f.get("layer"), sub=sub_of(f)))
    out.sort(key=lambda f: f["id"])
    return out


def fifo_cost(fills, n, held):
    """FIFO sells the oldest lots first, so the n shares still held are the newest buys of the held side: (average, shares covered)."""
    need, cost = n, 0.0
    for f in reversed(fills):
        if need <= 1e-9:
            break
        if f["side"] == held and f["action"] == "buy":
            take = min(need, f["qty"])
            cost += take * f["price"]
            need -= take
    got = n - need
    return (cost / got if got > 0 else None), got


def leaders_now():
    rows = tail(f"{DATA}/leaders.csv", 600_000).splitlines()
    try:
        hdr = open(f"{DATA}/leaders.csv").readline().strip().split(",")
    except OSError:
        return None, {}, None
    recs = [dict(zip(hdr, r.split(","))) for r in rows[1:] if r.count(",") == len(hdr) - 1]
    if not recs:
        return None, {}, None
    last = max(r["time"] for r in recs)
    cur = [r for r in recs if r["time"] == last]
    return next((r for r in cur if r.get("ours") == "1"), None), {r.get("rank"): r for r in cur}, last


def snapshot_data():
    s = json.load(open(f"{DATA}/state.json"))
    bots = s.get("bots") or []
    pm = pm_book(bots)
    pm_t = time.strftime("%H:%M:%S")
    fills = our_fills()
    by_ex = collections.defaultdict(list)
    for f in fills:
        by_ex[f["ex"]].append(f)
    a = s.get("account") or {}
    cash_ = float(a.get("totalAccountValue") or 0) - float(a.get("totalHoldingsValue") or 0)
    rows = []
    for b in bots:
        q = float(b.get("pos") or 0)
        if abs(q) < 1:
            continue
        ex, held, n = str(b["ex"]), ("yes" if q > 0 else "no"), abs(q)
        rp = race_party(b.get("market"))
        yb, ya = b.get("bid"), b.get("ask")
        sb, sa = (yb, ya) if held == "yes" else ((None if ya is None else 1 - ya), (None if yb is None else 1 - yb))
        avg, got = fifo_cost(by_ex[ex], n, held)
        net = sum((f["qty"] if f["action"] == "buy" else -f["qty"]) * (1 if f["side"] == "yes" else -1) for f in by_ex[ex])
        p = pm.get(ex)
        if p is None:
            status, pm_side, settle = "no PM", None, n * (sb or 0)
        else:
            status, pb, pa = p
            if held == "no":
                pb, pa = 1 - pa, 1 - pb
            pm_side = (pb + pa) / 2
            if status == "open":
                settle = n * pm_side
            else:
                settle = n * (1.0 if pm_side >= 0.5 else 0.0)
        grp = "open" if status in ("open", "no PM") else "won" if settle > 0 else "lost"
        cost = n * (avg if avg is not None else float(b.get("avg") or 0))
        rows.append(dict(ex=ex, mkt=f"{rp[0]} {rp[1]}" if rp else b["name"], side=held.upper(), n=n, avg=avg, sig_avg=b.get("avg"), cost=cost,
                         partial=got < n - 0.5, gap=abs(net - q) > 0.5, sb=sb, sa=sa, v_sig=n * (sb or 0), status=status, pm=pm_side,
                         settle=settle, pnl=settle - cost, grp=grp))
    order = {"open": 0, "won": 1, "lost": 2}
    rows.sort(key=lambda r: (order[r["grp"]], -r["cost"]))
    sold, hand, bought = (collections.defaultdict(lambda: [0.0, 0.0]) for _ in range(3))
    for f in fills:
        if (f.get("t") or "") < CLEANUP_FROM:
            continue
        if f.get("layer") == "swingsell" and str(f.get("sub") or "").startswith("cleanup:"):   # item 2's buys (the zero-sum vehicle)
            k = (f["ex"], f["side"])
            bought[k][0] += f["qty"]; bought[k][1] += f["qty"] * f["price"]
        elif f.get("layer") == "swingsell":                 # a sell_now: a buy of the complement at p sells the held side at 1 - p
            k = (f["ex"], "no" if f["side"] == "yes" else "yes")
            sold[k][0] += f["qty"]; sold[k][1] += f["qty"] * (1 - f["price"])
        elif not f.get("layer"):                            # no engine layer: Piero's hand fills
            k = (f["ex"], f["side"], f["action"])
            hand[k][0] += f["qty"]; hand[k][1] += f["qty"] * f["price"]
    names = {str(b["ex"]): (lambda rp: f"{rp[0]} {rp[1]}" if rp else b["name"])(race_party(b.get("market"))) for b in bots}
    cands = []
    for b in bots:
        rp = race_party(b.get("market"))
        p = pm.get(str(b["ex"]))
        if not rp or p is None or any(rp[0].startswith(x) for x in BUY_BAN):
            continue
        for side in ("yes", "no"):
            ask = b.get("ask") if side == "yes" else (None if b.get("bid") is None else round(1 - b["bid"], 3))
            if p[0] == "open":
                ok, cap = (p[1] if side == "yes" else 1 - p[2]) >= 0.995, 0.955
            else:
                ok, cap = (p[1] if side == "yes" else 1 - p[1]) >= 0.999, 0.96
            if ok and ask is not None and ask <= cap + 1e-9:
                cands.append(dict(mkt=f"{rp[0]} {rp[1]}", side=side.upper(), ask=ask, pm=p[0]))
    cands.sort(key=lambda c: c["ask"])
    us, by, lt = leaders_now()
    return dict(s=s, pm_t=pm_t, rows=rows, cash=cash_, sold=sold, hand=hand, bought=bought, names=names, cands=cands, us=us, by=by, lt=lt)


def snapshot_totals(d):
    rows = d["rows"]
    op = [r for r in rows if r["grp"] == "open"]
    return dict(at_bids=d["cash"] + sum(r["v_sig"] for r in rows), at_settle=d["cash"] + sum(r["settle"] for r in rows),
                open_n=len(op), open_cost=sum(r["cost"] for r in op), open_settle=sum(r["settle"] for r in op),
                board=float((d["s"].get("account") or {}).get("totalAccountValue") or 0))


def snapshot_write(d):
    """Piero 01:00 Nov 4 item 4: docs/snapshot_final_nov4.md, every position we hold, and the totals."""
    t = snapshot_totals(d)
    f0 = lambda x: "–" if x is None else f"{x:,.0f}"
    f3 = lambda x: "–" if x is None else f"{x:.3f}"
    lb = lambda r: f"{float(r['value']):,.0f}" if r and r.get("value") else "?"
    us, by = d["us"], d["by"]
    L = [f"# Final snapshot, Wed Nov 4 (refreshed {time.strftime('%H:%M')} PT)", "",
         f"SIG: the engine's state.json at {d['s'].get('time', '?')[11:19]}. Polymarket: the Gamma API at {d['pm_t']}. Prices are for the side we hold. "
         "Average = FIFO on our own fills: the shares still held are the newest buys of that side, not SIG's blended average.", "",
         "## Totals", "", "| | |", "|---|---:|",
         f"| Cash (SIG) | {d['cash']:,.0f} |",
         f"| Account at SIG bids | {t['at_bids']:,.0f} |",
         f"| Account at settlement (cash + settlement values) | {t['at_settle']:,.0f} |",
         f"| P&L at settlement vs the 100,000 start | {t['at_settle'] - START_VALUE:+,.0f} |",
         f"| Open risk at cost | {t['open_cost']:,.0f} in {t['open_n']} race(s); worth {t['open_settle']:,.0f} at PM |",
         f"| Worst case (every open race lost) | {-t['open_cost']:+,.0f} vs cost; account at settlement {t['at_settle'] - t['open_settle']:,.0f} |",
         f"| Board (SIG account value) | {t['board']:,.0f} |",
         f"| Rank (leaderboard {(d['lt'] or '?')[11:16]}) | #{us.get('rank') if us else '?'} ({lb(us)}) vs #50 {lb(by.get('50'))} / #60 {lb(by.get('60'))} / "
         f"#70 {lb(by.get('70'))} / #100 {lb(by.get('100'))} |", "",
         "## CLEANUP (Piero 01:00 Nov 4)", "", "Sold through sell_now:", "", "| Market | Side | Shares | Avg price | Proceeds |", "|---|---|---:|---:|---:|"]
    for (ex, side), (q, v) in sorted(d["sold"].items(), key=lambda kv: -kv[1][1]):
        L.append(f"| {d['names'].get(ex, ex)} | {side.upper()} | {q:,.0f} | {v / q:.3f} | {v:,.0f} |")
    held = {r["ex"]: r["n"] for r in d["rows"]}
    L += ["", "Still held of the sales (a sell_now ask can't rest, so the 10-min cleanup-sweep sells it into bids at the price): "
          + (", ".join(f"{what}: {held[ex]:,.0f} held" for ex, what in LEFT_BY_HAND if held.get(ex, 0) >= 1) or "nothing left"), "",
          "Bought (item 2; walls can't buy since Nov 2 17:00, so a zero-sum subbook_fix + a sell_now limit buy at the ask, sub-book cleanup:EX:side):", "",
          "| Market | Side | Shares | Avg price | Cost |", "|---|---|---:|---:|---:|"]
    for (ex, side), (q, v) in sorted(d["bought"].items(), key=lambda kv: -kv[1][1]):
        L.append(f"| {d['names'].get(ex, ex)} | {side.upper()} | {q:,.0f} | {v / q:.4f} | {v:,.0f} |")
    L += ["", "Piero's hand fills since 01:00:", ""]
    if d["hand"]:
        L += ["| Market | Side | Action | Shares | Avg price | Notional |", "|---|---|---|---:|---:|---:|"]
        for (ex, side, act), (q, v) in sorted(d["hand"].items(), key=lambda kv: -kv[1][1]):
            L.append(f"| {d['names'].get(ex, ex)} | {side.upper()} | {act} | {q:,.0f} | {v / q:.3f} | {v:,.0f} |")
    else:
        L.append("none")
    L += ["", f"Buy candidates now (item 2's rules; usable = cash − 1,000 = {max(0.0, d['cash'] - 1000):,.0f}; max 5,000 a market): "
          + (", ".join(f"{c['mkt']} {c['side']} ask {c['ask']:.3f} ({c['pm']})" for c in d["cands"]) or "none"), "",
          f"## Positions ({len(d['rows'])}): open races first, then won, then lost", "",
          "| Market | Side | Shares | Avg (FIFO) | Cost | SIG bid / ask | Value at SIG bid | PM | Settlement value | P&L at settlement |",
          "|---|---|---:|---:|---:|---|---:|---|---:|---:|"]
    ff_st = load("fatfinger")
    ix = next(i for i, x in enumerate(L) if x.startswith("## Positions"))
    sc = ff_st.get("scan") or {}
    ffl = ["## FATFINGER watch (Piero 02:15 Nov 4: resolved winners at <= 0.85, every 20 s to 08:55)", "",
           f"Last scan {sc.get('t', 'not yet')}: {sc.get('books', '?')} resolved books read, {sc.get('cands', 0)} with an ask <= 0.85.", ""]
    ev = ff_st.get("events") or []
    if ev:
        ffl += ["| Time | Kind | Market | Side | Shares | Price | Cost / proceeds | Cash after |", "|---|---|---|---|---:|---:|---:|---:|"]
        ffl += [f"| {e['t']} | {e['kind']} | {e['mkt']} | {e['side']} | {e['shares']:,} | {e['px']:.4f} | {e['amount']:,.0f} | {e['cash']:,} |" for e in ev]
        b_ = [e for e in ev if e["kind"] == "BUY"]
        ffl += ["", f"Bought {sum(e['shares'] for e in b_):,} shares for {sum(e['amount'] for e in b_):,.0f} (worth {sum(e['shares'] for e in b_):,} at settlement)."]
    else:
        ffl.append("No fat-finger ask so far.")
    L[ix:ix] = ffl + [""]
    grp = None
    for r in d["rows"]:
        if r["grp"] != grp:
            grp = r["grp"]
            L.append(f"| **{grp}** | | | | | | | | | |")
        mark = ("*" if r["partial"] else "") + ("†" if r["gap"] else "")
        pmt = r["status"] if r["pm"] is None else (f"{r['status']} {r['pm']:.3f}")
        L.append(f"| {r['mkt']} | {r['side']} | {r['n']:,.0f} | {f3(r['avg'])}{mark} | {r['cost']:,.0f} | {f3(r['sb'])} / {f3(r['sa'])} | {f0(r['v_sig'])} | {pmt} | "
                 f"{r['settle']:,.0f} | {r['pnl']:+,.0f} |")
    L += ["", "\\* our fills cover only part of the shares (the rest are costed at SIG's average). † SIG's fill history doesn't rebuild this position "
          "exactly (fills without an action before the ledger's Nov 2 19:02 start); the FIFO still uses the newest buys.", ""]
    txt = "\n".join(L)
    for p in (SNAP_DOC, SNAP_COPY):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p + ".tmp", "w") as fh:
            fh.write(txt)
        os.replace(p + ".tmp", p)
    return t


def snapshot(st):
    d = snapshot_data()
    t = snapshot_write(d)
    us = d["us"]
    runlog(f"FINAL SNAPSHOT {time.strftime('%H:%M')}: docs/snapshot_final_nov4.md ({len(d['rows'])} positions; Mac copy data/ops/snapshot_final_nov4.md): "
           f"cash {d['cash']:,.0f}; account at SIG bids {t['at_bids']:,.0f}, at settlement {t['at_settle']:,.0f} (P&L {t['at_settle'] - START_VALUE:+,.0f} vs 100k); "
           f"open risk {t['open_cost']:,.0f} at cost in {t['open_n']} race(s); rank #{us.get('rank') if us else '?'}")
    return d, t


def cleanup_report(st):
    """Piero 01:00 Nov 4 item 3: the 03:00 and 08:30 CLEANUP report (and the snapshot refresh, item 4)."""
    d, t = snapshot(st)
    sold = "; ".join(f"{d['names'].get(ex, ex)} {side.upper()} {q:,.0f} at {v / q:.3f}" for (ex, side), (q, v) in d["sold"].items()) or "nothing"
    hand = "; ".join(f"{d['names'].get(ex, ex)} {act} {side.upper()} {q:,.0f} at {v / q:.3f}" for (ex, side, act), (q, v) in d["hand"].items()) or "nothing"
    bought = "; ".join(f"{d['names'].get(ex, ex)} {side.upper()} {q:,.0f} at {v / q:.4f}" for (ex, side), (q, v) in d["bought"].items()) or "nothing"
    held = {r["ex"]: r["n"] for r in d["rows"]}
    rest = ", ".join(f"{d['names'].get(ex, ex)} {held[ex]:,.0f}" for ex, _ in LEFT_BY_HAND if held.get(ex, 0) >= 1) or "none"
    runlog(f"CLEANUP REPORT {time.strftime('%H:%M')}: sold {sold} (proceeds {sum(v for _, v in d['sold'].values()):,.0f}); still held of the sales: {rest}; "
           f"bought {bought} (cost {sum(v for _, v in d['bought'].values()):,.0f}); Piero's hand fills since 01:00: {hand}; cash {d['cash']:,.0f}; value at PM (account at settlement) {t['at_settle']:,.0f} "
           f"({t['at_settle'] - START_VALUE:+,.0f} vs 100k); open risk left {t['open_cost']:,.0f} at cost in {t['open_n']} race(s), worst case {-t['open_cost']:+,.0f}")


SWEEP_UNTIL, KEEP_CASH, MAX_PER_MKT = "2026-11-04 08:30", 1_000.0, 5_000.0   # item 2: keep >= 1,000 SIG cash, max 5,000 a market
VA05 = dict(ex="1033", sub="callsnipe:1033:no", px=0.86, px_after=0.84, after="2026-11-04 02:30", floor=0.82)   # item 1c


def ledger_now():
    return json.load(open(f"{DATA}/ledger.json"))


@contextlib.contextmanager
def sn_lock():
    """One user of the engine's single sell_now slot at a time across the ops jobs (cleanup-sweep, fatfinger)."""
    os.makedirs(f"{DATA}/ops", exist_ok=True)
    with open(f"{DATA}/ops/sell_now.lock", "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def sell_now_write(spec, by="Piero 01:00 Nov 4 CLEANUP (sig-ops cleanup-sweep)"):
    """Queue one sell_now in data/overrides.json the way the dashboard's control does (atomic; refused while one is pending or another
    owner's is sent; retried if the engine saved meanwhile). True when queued."""
    p = f"{DATA}/overrides.json"
    for _ in range(5):
        m0 = os.stat(p).st_mtime
        d = json.load(open(p))
        sn = d.get("sell_now") or {}
        if sn.get("state") == "pending" or (sn.get("state") == "sent" and not re.search(r"CLEANUP|FATFINGER", str(sn.get("by")))):
            return False
        d["sell_now"] = dict(spec, clip=5000, state="pending", t=time.strftime("%Y-%m-%d %H:%M:%S"), by=by)
        with open(p + ".ops.tmp", "w") as fh:
            fh.write(json.dumps(d, indent=1))
        if os.stat(p).st_mtime != m0:
            os.remove(p + ".ops.tmp")
            time.sleep(0.3)
            continue
        os.replace(p + ".ops.tmp", p)
        return True
    return False


def sell_now_wait(s=150):
    t0 = time.time()
    while time.time() - t0 < s:
        sn = json.load(open(f"{DATA}/overrides.json")).get("sell_now") or {}
        if sn.get("state") == "done":
            time.sleep(2)
            return True
        time.sleep(1)
    return False


def swing_fills(ex, side, since):
    return [f for f in ledger_now().get("fills") or [] if f["ex"] == ex and f.get("layer") == "swingsell" and f["t"] >= since and f["side"] == side]


def vehicle_close(st):
    """Close the unfilled part of the zero-sum vehicle (the 'sold' side's sub-book back to 0), if one is open."""
    v = st.get("vehicle")
    if not v:
        return
    cur = float((ledger_now().get("subbooks") or {}).get(v["ex"], {}).get(v["s_sold"], 0.0))
    if abs(cur) >= 0.5:
        control("subbook_fix", dict(ex=v["ex"], moves={v["s_sold"]: -cur, v["s_buy"]: cur}, reason="Piero 01:00 Nov 4 CLEANUP buy: closing the unfilled part"))
        for _ in range(30):
            time.sleep(2)
            if abs(float((ledger_now().get("subbooks") or {}).get(v["ex"], {}).get(v["s_sold"], 0.0))) < 0.5:
                break
        else:
            runlog(f"FINAL ALERT CLEANUP: the buy vehicle {v['s_sold']} did not close ({cur:+,.0f}); the job retries next run")
            return
    st.pop("vehicle", None)


def vehicle_buy(st, ex, buy, lim, n, tag="cleanup", ttl=15.0, by="Piero 01:00 Nov 4 CLEANUP (sig-ops cleanup-sweep)"):
    """Item 2 through existing controls (walls can't buy since Nov 2 17:00): subbook_fix opens TAG:EX:<other side> / TAG:EX:<buy side>
    (zero-sum: the position is unchanged), a sell_now of the first is a limit buy of `buy` at `lim` (fill-or-cancel), the rest is closed.
    -> (shares, cost)."""
    sold = "no" if buy == "yes" else "yes"
    s_sold, s_buy, sg = f"{tag}:{ex}:{sold}", f"{tag}:{ex}:{buy}", (-1 if sold == "no" else 1)
    with sn_lock():
        control("subbook_fix", dict(ex=ex, moves={s_sold: sg * n, s_buy: -sg * n}, reason=f"{by}: zero-sum vehicle for a sell_now limit buy"))
        st["vehicle"] = dict(ex=ex, s_sold=s_sold, s_buy=s_buy)
        for _ in range(120):
            time.sleep(0.5)
            if abs(float((ledger_now().get("subbooks") or {}).get(ex, {}).get(s_sold, 0.0)) - sg * n) < 0.5:
                break
        else:
            vehicle_close(st)
            return 0.0, 0.0
        t0 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 2))
        if sell_now_write(dict(ex=ex, side=sold, px=round(1 - lim, 3), qty=n, ttl_s=ttl, subs=[s_sold]), by=by):
            sell_now_wait()
        fl = swing_fills(ex, buy, t0)
        vehicle_close(st)
    return sum(f["qty"] for f in fl), sum(f["qty"] * f["price"] for f in fl)


def cleanup_sweep(st):
    """Piero 01:00 Nov 4 items 1c and 2 (01:25: 'you execute the buys'), every 10 min until 08:30: VA-05 House race D NO into bids at 0.86
    (0.84 from 02:30; floor 0.82), then buys with the cash above 1,000: PM side resolved at 1.00 (SIG ask <= 0.96) or PM side bid >= 0.995
    (ask <= 0.955), cheapest ask first, limit ask + 1c within the cap, max 5,000 a market, never control markets / Georgia / Alaska / Maine."""
    if now_s() > SWEEP_UNTIL:
        return
    vehicle_close(st)
    s = json.load(open(f"{DATA}/state.json"))
    name = lambda ex: next(((lambda rp: f"{rp[0]} {rp[1]}" if rp else b["name"])(race_party(b.get("market"))) for b in s["bots"] if str(b["ex"]) == ex), ex)
    acct = lambda s_: float((s_.get("account") or {}).get("totalAccountValue") or 0) - float((s_.get("account") or {}).get("totalHoldingsValue") or 0)
    held = next((-float(b.get("pos") or 0) for b in s["bots"] if str(b["ex"]) == VA05["ex"]), 0.0)
    q = int(min(held, abs(float((ledger_now().get("subbooks") or {}).get(VA05["ex"], {}).get(VA05["sub"], 0.0)))))
    if q >= 10:
        px = VA05["px_after"] if now_s() >= VA05["after"] else VA05["px"]
        assert px >= VA05["floor"]
        t0 = time.strftime("%Y-%m-%d %H:%M:%S")
        with sn_lock():
            done = sell_now_write(dict(ex=VA05["ex"], side="no", px=px, qty=q, ttl_s=15.0, subs=[VA05["sub"]])) and sell_now_wait()
        if done:
            fl = swing_fills(VA05["ex"], "yes", t0)
            f_q = sum(f["qty"] for f in fl)
            if f_q:
                v = sum(f["qty"] * (1 - f["price"]) for f in fl)
                runlog(f"CLEANUP 1c SELL VA-05 House race D NO {f_q:,.0f} at {v / f_q:.3f} (sell_now into bids >= {px:.2f}): cash freed {v:,.0f}; {q - f_q:,.0f} left")
                st["sale_t"] = time.time()
    led = ledger_now()
    spent, last = collections.Counter(), float(st.get("buy_t") or 0)
    for f in led.get("fills") or []:
        sub = ((led.get("orders") or {}).get(str(f.get("order"))) or {}).get("sub") or ""
        if f["t"] >= CLEANUP_FROM and f.get("layer") == "swingsell" and sub.startswith("cleanup:"):
            spent[str(f["ex"])] += f["qty"] * f["price"]
            last = max(last, time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S")))
    if time.mktime(time.strptime(s["time"], "%Y-%m-%d %H:%M:%S")) < last + 60:
        return                                              # state.json (written every 5 min) doesn't show the last buys' cash yet
    usable = acct(s) - KEEP_CASH - 50.0
    if usable < 200:
        return
    pm = pm_book(s["bots"])
    miss = {ex: t for ex, t in (st.get("miss") or {}).items() if time.time() - t < 1800}   # no seller at the limit: not retried for 30 min
    cands = []
    for b in s["bots"]:
        rp, p, ex = race_party(b.get("market")), pm.get(str(b["ex"])), str(b["ex"])
        if not rp or p is None or any(rp[0].startswith(x) for x in BUY_BAN) or spent[ex] >= MAX_PER_MKT - 50 or ex in miss:
            continue
        for side in ("yes", "no"):
            ask = b.get("ask") if side == "yes" else (None if b.get("bid") is None else round(1 - b["bid"], 3))
            ok, cap = (((p[1] if side == "yes" else 1 - p[2]) >= 0.995, 0.955) if p[0] == "open" else ((p[1] if side == "yes" else 1 - p[1]) >= 0.999, 0.96))
            if ok and ask is not None and ask <= cap + 1e-9:
                cands.append((ask, ex, side, min(cap, round(ask + 0.01, 3))))
    for ask, ex, side, lim in sorted(cands)[:3]:
        n = int(min(usable, MAX_PER_MKT - spent[ex]) / lim)
        if n < 10:
            break
        f_q, cost = vehicle_buy(st, ex, side, lim, n)
        st["buy_t"] = time.time()
        if not f_q:
            miss[ex] = time.time()
        st["miss"] = miss
        if f_q:
            usable -= cost
            runlog(f"CLEANUP BUY {name(ex)} {side.upper()} {f_q:,.0f} at {cost / f_q:.4f} avg (limit {lim:.3f}, SIG ask {ask:.3f}, PM {pm[ex][0]}): "
                   f"cost {cost:,.0f}; cash ~{KEEP_CASH + 50.0 + usable:,.0f}")
        if usable < 200:
            break


# ------------------------------------------------------------------------------------------------ FATFINGER (Piero 02:15 Nov 4)
FF = dict(start="2026-11-04 02:15", until="2026-11-04 08:55", max_px=0.85, sell_min=0.965, fund_pm=0.99, every_s=20.0, fund_max=25_000.0)
FF_BY = "Piero 02:15 Nov 4 FATFINGER (sig-ops fatfinger)"


def dash_get(path):
    tok = re.search(r"[?&]t=([A-Za-z0-9_\-]+)", open(f"{DATA}/dashboard_url.txt").read()).group(1)
    req = urllib.request.Request(f"http://127.0.0.1:8765{path}{'&' if '?' in path else '?'}t={tok}",
                                 headers={"Origin": "http://127.0.0.1:8765", "Referer": "http://127.0.0.1:8765/"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read().decode()


def dash_snapshot():
    txt = dash_get("/events?limit=1")
    return json.loads(txt[txt.index("event: snapshot"):].split("data: ", 1)[1].split("\n\n", 1)[0])


def live_book(ex):
    """The engine's live SIG book for one market (its realtime feed, <= 600 s old; never a SIG read): {bids, asks: [[px, qty, ours]]}."""
    return json.loads(dash_get(f"/api/market/{ex}")).get("book") or {}


def fill_delta(f):
    q = f["qty"] if f["side"] == "yes" else -f["qty"]
    return q if f["action"] == "buy" else -q


def ff_cash(snap):
    """SIG cash now: the dashboard's cash (the engine's account read when the payload was built, every 5 min at night) plus every ledger
    fill since: a buy that opens pays its price, a buy against what we hold redeems the pair (+ 1 - price), a sale gets its price."""
    led = ledger_now()
    later = [f for f in led.get("fills") or [] if f["t"] > (snap.get("t") or "")]
    net = collections.defaultdict(float)
    for ex, books in (led.get("subbooks") or {}).items():
        net[str(ex)] = sum(books.values())
    for f in later:
        net[str(f["ex"])] -= fill_delta(f)
    cash_ = float((snap.get("money") or {}).get("cash") or 0)
    for f in sorted(later, key=lambda f: (f["t"], str(f.get("key")))):
        ex, d = str(f["ex"]), fill_delta(f)
        if f["action"] == "sell":
            cash_ += f["qty"] * f["price"]
        else:
            closed = min(abs(d), abs(net[ex])) if net[ex] * d < 0 else 0.0
            cash_ += closed * (1 - f["price"]) - (f["qty"] - closed) * f["price"]
        net[ex] += d
    return cash_


def ff_levels(book, side, max_px):
    """Levels (ours left out: no self-trade) where `side` buys at <= max_px: YES = the asks, NO = 1 - the YES bids."""
    if side == "yes":
        return [(float(p), float(q)) for p, q, mine in book.get("asks") or [] if p <= max_px + 1e-9 and not mine]
    return [(round(1 - float(p), 3), float(q)) for p, q, mine in book.get("bids") or [] if 1 - p <= max_px + 1e-9 and not mine]


def ff_event(st, kind, name, side, q, px, amount, cash_):
    st.setdefault("events", []).append(dict(t=time.strftime("%H:%M:%S"), kind=kind, mkt=name, side=side.upper(), shares=round(q), px=round(px, 4),
                                            amount=round(amount, 2), cash=round(cash_)))
    what = {"BUY": "cost", "FUND": "proceeds", "MISS": "nothing filled; the level"}[kind]
    runlog(f"FATFINGER {kind} {name} {side.upper()} {q:,.0f} at {px:.4f} ({what} {amount:,.0f}); cash ~{cash_:,.0f}")


def ff_fund(st, res, need, skip_ex, names, snap):
    """Item 3: sell near-certain winners (PM side resolved / proposed for us, or open with PM side >= 0.99) into SIG bids >= 0.965,
    engine sub-books only (never Piero's manual book), highest bid first, until `need` is raised. -> cash raised."""
    cands = []
    for ex, books in (ledger_now().get("subbooks") or {}).items():
        ex, net, p = str(ex), sum(books.values()), res.get(str(ex))
        if ex == skip_ex or abs(net) < 10 or not p:
            continue
        held = "yes" if net > 0 else "no"
        pm_side = (p[1] if held == "yes" else 1 - p[2]) if p[0] == "open" else (p[1] if held == "yes" else 1 - p[1])
        subs = sorted(((k, v) for k, v in books.items() if k != "manual" and (v > 0) == (held == "yes") and abs(v) >= 10), key=lambda kv: -abs(kv[1]))
        eng_q = min(abs(net), sum(abs(v) for _, v in subs))
        if pm_side < FF["fund_pm"] or eng_q < 10:
            continue
        book = live_book(ex)
        if held == "yes":
            bids = [(float(b), float(q)) for b, q, mine in book.get("bids") or [] if b >= FF["sell_min"] - 1e-9 and not mine]
        else:
            bids = [(round(1 - float(a), 3), float(q)) for a, q, mine in book.get("asks") or [] if 1 - a >= FF["sell_min"] - 1e-9 and not mine]
        depth = sum(q for _, q in bids)
        if depth >= 10:
            cands.append((-max(b for b, _ in bids), ex, held, int(min(eng_q, depth)), [k for k, _ in subs]))
    raised = 0.0
    for _, ex, held, q, subs in sorted(cands):
        n = int(min(q, math.ceil((need - raised) / FF["sell_min"]) + 1))
        if raised >= need or n < 10:
            break
        close = "no" if held == "yes" else "yes"
        t0 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 2))
        with sn_lock():
            ok = sell_now_write(dict(ex=ex, side=held, px=FF["sell_min"], qty=n, ttl_s=5.0, subs=subs), by=FF_BY) and sell_now_wait()
        fl = swing_fills(ex, close, t0) if ok else []
        f_q, v = sum(f["qty"] for f in fl), sum(f["qty"] * (1 - f["price"]) for f in fl)
        if f_q:
            raised += v
            ff_event(st, "FUND", names.get(ex, ex), held, f_q, v / f_q, v, ff_cash(snap))
    return raised


def ff_scan(st):
    s = json.load(open(f"{DATA}/state.json"))
    bots = {str(b["ex"]): b for b in s.get("bots") or []}
    names = {ex: (lambda rp, b=b: f"{rp[0]} {rp[1]}" if rp else b["name"])(race_party(b.get("market"))) for ex, b in bots.items()}
    if time.time() - float(st.get("res_t") or 0) > 300 or not st.get("res"):
        st["res"], st["res_t"] = {ex: list(p) for ex, p in pm_book(list(bots.values())).items()}, time.time()
    res = st["res"]
    miss = {k: t for k, t in (st.get("miss") or {}).items() if time.time() - t < 600}   # a level that didn't fill: not retried for 10 min
    cands, n_books = [], 0
    for ex, p in res.items():
        b = bots.get(ex)
        if p[0] != "resolved" or not b or not str(b.get("name", "")).startswith("MKT"):   # resolved only; never Piero's position markets
            continue
        rp = race_party(b.get("market"))
        side = "yes" if p[1] >= 0.999 else "no" if p[1] <= 0.001 else None
        if not rp or side is None or any(rp[0].startswith(x) for x in BUY_BAN):
            continue
        try:
            lv = sorted(ff_levels(live_book(ex), side, FF["max_px"]))
            n_books += 1
        except Exception:                                   # noqa: BLE001 - one market's book; the next scan retries
            continue
        if lv and f"{ex}:{side}:{lv[0][0]}" not in miss:
            cands.append((lv[0][0], ex, side, lv))
    st["scan"] = dict(t=time.strftime("%H:%M:%S"), books=n_books, cands=len(cands))
    snap = dash_snapshot() if cands else None
    for _, ex, side, lv in sorted(cands):
        usable = ff_cash(snap) - KEEP_CASH - 20.0
        total = sum(p_ * q_ for p_, q_ in lv)
        if total > usable + 1:
            ff_fund(st, res, min(total - usable, FF["fund_max"]), ex, names, snap)
            usable = ff_cash(snap) - KEEP_CASH - 20.0
        n, cost, lim = 0, 0.0, None
        for p_, q_ in lv:
            take = int(min(q_, max(0.0, usable - cost) / p_))
            if take < 1:
                break
            n, cost, lim = n + take, cost + take * p_, p_
            if take < q_:
                break
        if n < 10:
            continue
        f_q, f_cost = vehicle_buy(st, ex, side, lim, n, tag="ffw", ttl=5.0, by=FF_BY)
        if f_q:
            ff_event(st, "BUY", names.get(ex, ex), side, f_q, f_cost / f_q, f_cost, ff_cash(snap))
        else:
            miss[f"{ex}:{side}:{lv[0][0]}"] = time.time()
            ff_event(st, "MISS", names.get(ex, ex), side, n, lim, n * lim, ff_cash(snap))
    st["miss"] = miss


def fatfinger(st):
    """Piero 02:15 Nov 4: FAT-FINGER WATCH to 08:55. Every 20 s (3 scans a run, a run a minute) every book whose side is resolved at 1.00 on
    Polymarket (the Gamma API, every 5 min) is read from the engine's live feed; any ask <= 0.85 (all depth there, ours left out) is bought
    through the zero-sum vehicle (ffw:EX:side) up to cash - 1,000; an ask bigger than the cash is funded first by selling near-certain
    winners into bids >= 0.965 (ff_fund). Never unresolved markets, the control markets, Alaska, Maine, Georgia. FATFINGER lines in RUN_LOG."""
    if not FF["start"] <= now_s() <= FF["until"]:
        return
    vehicle_close(st)
    t_start = time.time()
    for i in range(3):
        t0 = time.time()
        try:
            ff_scan(st)
        except Exception as e:                              # noqa: BLE001
            st["err"] = f"{time.strftime('%H:%M:%S')} {e!r}"
            if time.time() - float(st.get("err_log_t") or 0) > 1800:
                runlog(f"FATFINGER scan error (logged at most every 30 min): {e!r}")
                st["err_log_t"] = time.time()
        save("fatfinger", st)
        if i == 2 or time.time() - t_start > 40:
            break
        time.sleep(max(0.0, FF["every_s"] - (time.time() - t0)))


# ------------------------------------------------------------------------------------- SETTLEMENT WATCH (Piero 10:25 Nov 4, read-only)
SETTLE_READ = [f"{ENG}/.venv/bin/python3", f"{ENG}/deploy/server/ops/settle_read.py"]   # b_sig with the unit's environment: 5 reads a run
FINAL_DOC, FINAL_COPY = f"{ENG}/docs/final_result_nov4.md", f"{DATA}/ops/final_result_nov4.md"
EMAIL_DOC, EMAIL_COPY = f"{ENG}/docs/settle_alert_email_nov4.md", f"{DATA}/ops/settle_alert_email_nov4.md"
SETTLE_TO = os.environ.get("SETTLE_TO", "")                # the organisers' address, from the env file
CONTROL_T = re.compile(r"win the U\.S\. (House|Senate)\?$")
WATCH_LABEL = {"Nebraska Senate I": "SIG's contract winner label is 'Nonpartisan'"}   # item 2: Nebraska Senate (all 3 legs) and the 4 control markets


def settle_read(final=False):
    r = subprocess.run(SETTLE_READ + (["final"] if final else []), capture_output=True, text=True, timeout=600)
    if r.returncode:
        raise RuntimeError(f"settle_read rc {r.returncode}: {r.stderr.strip()[-300:]}")
    return json.loads(r.stdout)


def snapshot_rows():
    """The 08:30 snapshot's positions table: {(market, side): dict(n, cost, grp, pm)}; grp open / won / lost (1.00 or 0 a share)."""
    out, grp, on = {}, None, False
    for ln in open(SNAP_DOC):
        on = on or ln.startswith("## Positions")
        if not on or not ln.startswith("| ") or ln.startswith("| Market"):
            continue
        c = [x.strip() for x in ln.strip().strip("|").split("|")]
        if c[0].startswith("**"):
            grp = c[0].strip("*")
        elif len(c) >= 10 and grp:
            out[(c[0], c[1].lower())] = dict(n=num(c[2]), cost=num(c[4]), grp=grp, pm=c[7])
    return out


def sig_net(p):
    return sum(float(l["quantity"]) * (1 if l["side"].lower() == "yes" else -1) for l in p.get("lots") or [])


def settle_yes(m, p, ex):
    """(YES's payout a share, how it was read): settledWith first (an option, this exchange's id, or a close-out price), else a settled
    position row's currentPrice (YES terms, like every row); (None, raw) while SIG shows no settlement."""
    sw = (m or {}).get("settledWith")
    if sw is not None:
        v = (sw.get("option") or sw.get("outcome") or sw.get("exchangeId") or sw.get("id") or sw.get("price")) if isinstance(sw, dict) else sw
        s_ = str(v).strip()
        ids = {str(x.get("id")) for x in (m.get("exchanges") or [])}
        if s_.upper() in ("YES", "TRUE"):
            return 1.0, f"settledWith {sw!r}"
        if s_.upper() in ("NO", "FALSE"):
            return 0.0, f"settledWith {sw!r}"
        if s_ in ids:
            return (1.0 if s_ == ex else 0.0), f"settledWith {sw!r} (exchange id)"
        with contextlib.suppress(ValueError):
            if 0 <= float(s_) <= 1:
                return float(s_), f"settledWith {sw!r} (price)"
    if p and p.get("settled") and p.get("currentPrice") is not None:
        return float(p["currentPrice"]), f"settled position row, currentPrice {p['currentPrice']}, moneyEarned {p.get('moneyEarned')} (settledWith {sw!r})"
    return None, f"settledWith {sw!r}"


def pm_official(ex, bots):
    """The official result as Polymarket states it: each mapped question (pm_legs), its outcome, UMA status and close time."""
    legs = pm_legs(bots).get(ex) or []
    if not legs:
        return "no Polymarket question mapped"
    out = []
    for i in legs:
        x = (gamma(f"/markets?closed=true&id={i}") or gamma(f"/markets?id={i}") or [None])[0] or {}
        px = json.loads(x.get("outcomePrices") or "[]")
        res = ("Yes" if float(px[0]) >= 0.5 else "No") if px else "?"
        out.append(f"'{x.get('question') or i}' -> {res} (outcome prices {px}, UMA {x.get('umaResolutionStatus') or '?'}, closed {x.get('closed')}"
                   f"{', ' + str(x.get('closedTime')) if x.get('closedTime') else ''})")
    return "Polymarket " + ("(the sum of its legs) " if len(out) > 1 else "") + "; ".join(out)


def settle_base(st, sig, bots):
    """The first run: every SIG position row we hold, with the 08:30 snapshot's expectation (1.00 won, 0 lost, None open)."""
    names = {str(b["ex"]): (lambda rp: f"{rp[0]} {rp[1]}" if rp else b["name"])(race_party(b.get("market"))) for b in bots}
    doc, base = snapshot_rows(), {}
    for p in sig["positions"]:
        q = sig_net(p)
        if abs(q) < 0.5:
            continue
        ex, side = str(p["exchangeId"]), ("yes" if q > 0 else "no")
        nm = names.get(ex) or (lambda rp: f"{rp[0]} {rp[1]}" if rp else p.get("marketTitle"))(race_party(p.get("marketTitle")))
        r = doc.get((nm, side)) or {}
        base[ex] = dict(name=nm, side=side, n=abs(q), mid=str(p.get("marketId")), title=p.get("marketTitle"), grp=r.get("grp", "not in the 08:30 snapshot"),
                        exp={"won": 1.0, "lost": 0.0}.get(r.get("grp")), cost=r.get("cost", float(p.get("costBasis") or 0)), doc_n=r.get("n"), pm_doc=r.get("pm"))
    watch_ = {str(x["id"]): x for m in sig["markets"] for x in m.get("exchanges") or []
              if CONTROL_T.search(m.get("title") or "") or "Nebraska Senate" in (m.get("title") or "")}
    off = [f"{b['name']} {b['side'].upper()} SIG {b['n']:,.0f} vs doc {b['doc_n']:,.0f}" for b in base.values() if b["doc_n"] is not None and abs(b["doc_n"] - b["n"]) >= 0.5]
    miss = [b["name"] for b in base.values() if b["doc_n"] is None]
    st.update(base=base, watch=sorted(watch_), cash0=float(sig["tour"]["myBalance"]), cash_last=float(sig["tour"]["myBalance"]),
              t0=time.strftime("%Y-%m-%d %H:%M"), done={}, alerts=[], seen_mk={}, wonly={},
              snap_at_settle=next((m.group(1) for m in map(re.compile(r"^\| Account at settlement[^|]*\| ([\d,]+) \|").match, open(SNAP_DOC)) if m), None))
    runlog(f"SETTLE WATCH armed (Piero 10:25 Nov 4, read-only, no orders): server timer sig-ops-settle every 15 min until every market we hold is settled; "
           f"5 SIG reads a run (tournament, market list, positions). Baseline {time.strftime('%H:%M')}: {len(base)} positions, cash {st['cash0']:,.2f}; "
           f"tournament {sig['tour'].get('status')}, markets settled {sum(1 for m in sig['markets'] if m.get('settledWith') is not None)} of {len(sig['markets'])}. "
           f"Expected values from the 08:30 docs/snapshot_final_nov4.md: {sum(1 for b in base.values() if b['exp'] == 1.0)} won (1.00), "
           f"{sum(1 for b in base.values() if b['exp'] == 0.0)} lost (0), {sum(1 for b in base.values() if b['exp'] is None)} open (PM's result when it comes); "
           f"shares vs the doc: {'; '.join(off) or 'all equal'}{'; not in the doc: ' + ', '.join(miss) if miss else ''}. Watched: Nebraska Senate (I leg's label 'Nonpartisan') "
           f"and the 4 control markets ({len(watch_)} exchanges)")


def settle_expect(b, ex, pm):
    """(expected payout a share for our side, its text, PM's side price while PM is open): the 08:30 snapshot's 1.00 / 0 for the races
    PM had resolved by then, else PM's result since (resolved or proposed), else None with PM's price."""
    if b.get("exp") is not None:
        return b["exp"], f"{b['exp']:.2f} (08:30 snapshot: PM resolved, {b['grp']})", None
    if not pm or ex not in pm:
        return None, "none: no PM price", None
    stt, pb, pa = pm[ex]
    side_p = (pb + pa) / 2 if b["side"] == "yes" else 1 - (pb + pa) / 2
    if stt != "open":
        e = 1.0 if side_p >= 0.5 else 0.0
        return e, f"{e:.2f} (PM {stt} since 08:30)", None
    return None, f"none yet: PM open, our side {side_p:.3f}", side_p


def settle_alert(st, kind, ex, b, d, exp, exp_txt, bots):
    try:
        official = pm_official(ex, bots)
    except Exception as e:                                  # noqa: BLE001
        official = f"Polymarket read failed: {e!r}"[:200]
    held = f"{b['side'].upper()} {b['n']:,.0f} shares" if b["n"] else "not held"
    st["alerts"].append(dict(kind=kind, ex=ex, mid=b["mid"], title=b.get("title"), name=b["name"], side=b["side"], n=b["n"], yes=d["yes"], pay=d["pay"], exp=exp,
                             exp_txt=exp_txt, how=d["how"], on=d["on"], official=official))
    runlog(f"SETTLE ALERT {b['name']} ({b.get('title')}, market {b['mid']}, exchange {ex}): SIG {d['how']} on {d['on']} -> {b['side'].upper()} paid {d['pay']:.2f} a share "
           f"({held}), expected {exp_txt}. Official result: {official}"
           + ("; email to the organisers drafted, not sent: docs/settle_alert_email_nov4.md" if kind == "result" else "; check it by hand (PM not resolved)"))
    if kind == "result":
        settle_email(st)


def settle_email(st):
    """Item 4: the draft to the organisers for every alert against a resolved result (never sent)."""
    real = [a for a in st["alerts"] if a["kind"] == "result"]
    if not real:
        return
    us = (leaders_now()[0] or {}).get("name") or "?"
    L = [f"DRAFT, not sent (Piero 10:25 Nov 4 item 4). Written {time.strftime('%Y-%m-%d %H:%M')} PT by the settlement watch; check each line before sending.", "",
         f"To: {SETTLE_TO}", f"Subject: Settlement review request, Midterm Elections tournament ({len(real)} market{'s' * (len(real) > 1)})", "",
         "Hello,", "", f"I believe the following market{'s were' if len(real) > 1 else ' was'} settled against the official result. My account: {us}.", ""]
    for a in real:
        L += [f"- Market: \"{a['title']}\" (market {a['mid']}, exchange {a['ex']})",
              f"  - Official result: {a['official']}",
              f"  - SIG's settlement: {a['how']} on {a.get('on') or '?'}, i.e. YES paid {a['yes']:.2f} a share",
              (f"  - My position: {a['n']:,.0f} {a['side'].upper()}; at the official result it pays {a['n'] * a['exp']:,.0f}, it was settled at {a['n'] * a['pay']:,.0f}"
               if a["n"] else "  - My position: none (reported as a market I follow)"), ""]
    L += ["Could you review and re-settle these markets on the official result?", "", "Thank you,", "pieropls", ""]
    for p in (EMAIL_DOC, EMAIL_COPY):
        with open(p + ".tmp", "w") as fh:
            fh.write("\n".join(L))
        os.replace(p + ".tmp", p)


def settle_final(st, sig):
    """Item 3: docs/final_result_nov4.md once every market we hold is settled."""
    lb = sig.get("leaders") or {}
    rk = {int(r["rank"]): r for r in lb.get("rows") or [] if r.get("rank")}
    me = lb.get("me") or {}
    if not rk:                                              # the engine's own leaderboard file, if the final read came back empty
        us, by, _ = leaders_now()
        rk, me = {int(k): dict(pnl=v.get("value")) for k, v in by.items() if k}, dict(rank=(us or {}).get("rank"), pnl=(us or {}).get("value"))
    pl = lambda r: "?" if not r or r.get("pnl") is None else f"{float(r['pnl']):+,.0f}"
    cash_ = float(sig["tour"]["myBalance"])
    base, done = st["base"], st["done"]
    paid, cost = sum(done[ex]["value"] for ex in base), sum(b["cost"] for b in base.values())
    exp_v = sum(base[ex]["n"] * base[ex]["exp"] for ex in base if base[ex]["exp"] is not None)
    L = [f"# Final result, Midterm Elections (written {time.strftime('%a %b %d %H:%M')} PT)", "",
         f"SIG's own reads (settle_read.py): the tournament ({sig['tour'].get('status')}), the market list, our position rows and the leaderboard. "
         f"Every one of the {len(base)} positions held at the {st['t0'][11:]} baseline is settled.", "",
         "## Totals", "", "| | |", "|---|---:|",
         f"| Final cash (SIG) | {cash_:,.2f} |",
         f"| Final P&L vs the 100,000 start | {cash_ - START_VALUE:+,.2f} |",
         f"| Cash at the baseline ({st['t0'][11:]}) | {st['cash0']:,.2f} |",
         f"| Settlement payouts (our sides, at SIG's settlement) | {paid:,.0f} |",
         f"| Cash credited since the baseline | {cash_ - st['cash0']:+,.0f} |",
         f"| The 08:30 snapshot's account at settlement | {st.get('snap_at_settle') or '?'} |",
         f"| Positions at cost / P&L on them at settlement | {cost:,.0f} / {paid - cost:+,.0f} |",
         f"| SETTLE ALERTs | {len(st['alerts'])} ({sum(1 for a in st['alerts'] if a['kind'] == 'result')} against a resolved result) |",
         f"| Rank (SIG leaderboard, by P&L) | #{me.get('rank', '?')} ({pl(me)}) of {lb.get('total') or '?'} |",
         f"| vs #50 / #60 / #70 | {pl(rk.get(50))} / {pl(rk.get(60))} / {pl(rk.get(70))} |", "",
         "## Positions", "", "| Market | Side | Shares | Cost | SIG settled | Paid a share | Value | Expected | P&L | Check |", "|---|---|---:|---:|---|---:|---:|---|---:|---|"]
    for ex, d in sorted(((ex, done[ex]) for ex in base), key=lambda kv: -kv[1]["value"]):
        b = base[ex]
        L.append(f"| {b['name']} | {b['side'].upper()} | {b['n']:,.0f} | {b['cost']:,.0f} | {d['how']} | {d['pay']:.2f} | {d['value']:,.0f} | {d['exp_txt']} | "
                 f"{d['value'] - b['cost']:+,.0f} | {'**SETTLE ALERT**' if d['alert'] else 'ok'} |")
    wo = [(ex, d) for ex, d in done.items() if ex not in base]
    if wo:
        L += ["", "Watched markets we don't hold (Nebraska Senate, the control markets): " + "; ".join(
            f"exchange {ex}: {d['how']} -> YES {d['yes']:.2f}, expected {d['exp_txt']}{' **SETTLE ALERT**' if d['alert'] else ''}" for ex, d in wo)]
    if st["alerts"]:
        L += ["", "## SETTLE ALERTs", ""] + [f"- {a['name']} {a['side'].upper()} {a['n']:,.0f}: {a['how']} -> paid {a['pay']:.2f}, expected {a['exp_txt']}; official: {a['official']}"
                                           for a in st["alerts"]]
    L += ["", f"Expected at the 08:30 snapshot (won 1.00, lost 0) for the decided races: {exp_v:,.0f}.", ""]
    for p in (FINAL_DOC, FINAL_COPY):
        with open(p + ".tmp", "w") as fh:
            fh.write("\n".join(L))
        os.replace(p + ".tmp", p)
    runlog(f"SETTLE FINAL {time.strftime('%H:%M')}: every market we hold is settled ({len(base)}); final cash {cash_:,.2f}, P&L {cash_ - START_VALUE:+,.2f} vs 100k; "
           f"rank #{me.get('rank', '?')} ({pl(me)}) vs #50 {pl(rk.get(50))} / #60 {pl(rk.get(60))} / #70 {pl(rk.get(70))}; SETTLE ALERTs {len(st['alerts'])}; docs/final_result_nov4.md")


def settle(st):
    """Piero 10:25 Nov 4: SETTLEMENT WATCH, read-only (no orders, no controls). Every 15 min: SIG's market list, our position rows and cash
    (settle_read.py); each newly settled market we hold -> a SETTLE line (side, shares, value, cash), compared with the 08:30 snapshot's
    expectation (or Polymarket's result since, for races open then): a difference -> SETTLE ALERT with the official result and SIG's
    settledWith; a real one -> the organisers' email drafted (docs/settle_alert_email_nov4.md, never sent). All settled -> SETTLE FINAL."""
    if st.get("final_t"):                                   # all settled: only the races settled while PM was open, hourly, PM reads only
        if not any(d.get("pending_pm") for d in st["done"].values()) or time.strftime("%M") >= "15" or now_s() > "2026-11-30":
            return
        sig = dict(tour=st["final_tour"], markets=[], positions=[])
    s = json.load(open(f"{DATA}/state.json"))
    bots = s.get("bots") or []
    sig = sig if st.get("final_t") else settle_read()
    if not st.get("base"):
        settle_base(st, sig, bots)
        save("settle", st)
    mk = {str(x["id"]): m for m in sig["markets"] for x in m.get("exchanges") or []}
    pos = {str(p["exchangeId"]): p for p in sig["positions"]}
    cash_ = float(sig["tour"]["myBalance"])
    try:
        pm = pm_book(bots)
    except Exception as e:                                  # noqa: BLE001 - Gamma down: the open races' expectation waits
        pm, st["pm_err"] = None, repr(e)[:200]
    new, base, done = [], st["base"], st["done"]
    for ex, d in done.items():                               # settled while PM was still open: compare once PM has its result
        if d.get("pending_pm"):
            b = base.get(ex) or st["wonly"][ex]
            exp, exp_txt, pm_p = settle_expect(b, ex, pm)
            if exp is not None:
                kind = "result" if abs(d["pay"] - exp) > 0.005 else None
                d.update(pending_pm=False, exp_txt=exp_txt, alert=d["alert"] or bool(kind))
                runlog(f"SETTLE CHECK {b['name']}: settled {d['t']} at {d['pay']:.2f} a share for our {b['side'].upper()} while PM was open; PM's result now: expected {exp_txt}"
                       + ("" if kind else "; ok"))
                if kind:
                    settle_alert(st, kind, ex, b, d, exp, exp_txt, bots)
    for ex in sorted(set(base) | set(st["watch"])):
        if ex in done:
            continue
        b = base.get(ex) or st["wonly"].setdefault(ex, dict(name=(lambda rp: f"{rp[0]} {rp[1]}" if rp else ex)(race_party((mk.get(ex) or {}).get("title"))),
                                                          side="yes", n=0.0, cost=0.0, exp=None, mid=str((mk.get(ex) or {}).get("id")), title=(mk.get(ex) or {}).get("title")))
        yes, how = settle_yes(mk.get(ex), pos.get(ex), ex)
        if yes is None:
            if ex in pos or ex not in base:
                continue
            how = f"position row gone, market {(mk.get(ex) or {}).get('status')} ({how})"
            if st["seen_mk"].get(ex) != how:
                st["seen_mk"][ex] = how
                runlog(f"SETTLE NOTE {b['name']} {b['side'].upper()} {b['n']:,.0f}: {how}; waiting for SIG's settlement")
            continue
        pay = yes if b["side"] == "yes" else 1 - yes
        exp, exp_txt, pm_p = settle_expect(b, ex, pm)
        kind = ("result" if exp is not None and abs(pay - exp) > 0.005 else
                "vs PM price" if pm_p is not None and abs(pay - pm_p) >= 0.9 else None)
        d = done[ex] = dict(t=time.strftime("%H:%M"), yes=yes, pay=pay, value=b["n"] * pay, how=how, on=(mk.get(ex) or {}).get("settledOn"), exp_txt=exp_txt,
                            alert=bool(kind), pending_pm=exp is None and pm_p is not None)
        tag = f" [watched: {WATCH_LABEL.get(b['name'], 'control market, settled manually by SIG')}]" if ex in st["watch"] else ""
        held = f"{b['side'].upper()} {b['n']:,.0f} shares" if ex in base else "not held"
        runlog(f"SETTLE {b['name']}: {held}; SIG {how}, settledOn {d['on']} -> {pay:.2f} a share = {d['value']:,.0f} (cash change +{d['value']:,.0f}; cost {b['cost']:,.0f}, "
               f"P&L {d['value'] - b['cost']:+,.0f}); expected {exp_txt}{tag}{'' if kind or d['pending_pm'] else '; ok'}{'; compared again once PM resolves' if d['pending_pm'] else ''}")
        if kind:
            settle_alert(st, kind, ex, b, d, exp if exp is not None else pm_p, exp_txt, bots)
        new.append(ex)
    if st.get("final_t"):
        if len(st["alerts"]) != st.get("final_doc_alerts"):
            settle_final(st, dict(sig, leaders=st.get("final_lb")))
            st["final_doc_alerts"] = len(st["alerts"])
        return
    cnt = [ex for ex in base if ex in done]
    paid = sum(done[ex]["value"] for ex in cnt)
    dc, dc0 = cash_ - float(st["cash_last"]), cash_ - float(st["cash0"])
    gap = dc0 - paid
    pend = [base[ex]["name"] for ex in cnt if done[ex].get("pending_pm")]
    runlog(f"SETTLE WATCH {time.strftime('%H:%M')}: settled {len(cnt)} of {len(base)} we hold ({len([e for e in new if e in base])} new); payouts so far {paid:,.0f}; "
           f"cash {cash_:,.2f} ({dc:+,.0f} since the last check, {dc0:+,.0f} since the {st['t0'][11:]} baseline"
           + (f"; credited vs payouts {gap:+,.0f}" if abs(gap) >= 1 else "") + f"); SIG markets settled {sum(1 for m in sig['markets'] if m.get('settledWith') is not None)} "
           f"of {len(sig['markets'])}; SETTLE ALERTs {len(st['alerts'])}" + (f"; {len(pend)} settled while PM open, compared once PM resolves"
                                                                      + (f" ({', '.join(pend)})" if len(pend) <= 6 else "") if pend else ""))
    st["cash_last"] = cash_
    if len(cnt) == len(base) and not st.get("final_t"):
        if abs(gap) >= 1 and not st.get("gap_wait"):        # payouts not all credited yet: one more run before the final numbers
            st["gap_wait"] = True
            return
        fin = settle_read(final=True)
        st.update(final_t=time.strftime("%Y-%m-%d %H:%M"), final_lb=fin.get("leaders"), final_tour=fin["tour"], final_doc_alerts=len(st["alerts"]))
        save("settle", st)
        settle_final(st, fin)


JOBS = {"watch": watch, "health": health, "cash": cash, "summary": summary, "checklist": checklist, "rehearsal": rehearsal,
        "probe": probe, "rehearsal-eval": rehearsal_eval, "test": test, "golive": golive, "golive-test": golive_test,
        "flip-stop": flip_stop, "final-watch": final_watch, "final-1320": final_1320, "final-1420": final_1420, "final-1446": final_1446, "night-hourly": night_hourly,
        "snapshot": snapshot, "cleanup-report": cleanup_report, "cleanup-sweep": cleanup_sweep, "fatfinger": fatfinger, "settle": settle}

if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else ""
    if job not in JOBS:
        raise SystemExit(f"usage: ops.py {{{'|'.join(JOBS)}}}")
    st = load(job)
    try:
        JOBS[job](st)
    finally:
        save(job, st)
