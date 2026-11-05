#!/usr/bin/env python3
"""Election-night dress rehearsal, the evaluator (docs/night_reconcile.md C19 against election_night_v2.md §4.6's pass criteria).
After the Sun Oct 25 15:00-21:00 PT shadow run: reads the server's files over ssh (read-only `cat` / `tail`; never the env file,
never the control API), compares the injected rows with research/night/rehearsal_plan.py's expected list and writes
docs/night_rehearsal.md: a pass/fail table first, then the details.

  python3 research/night/rehearsal_eval.py --start "2026-10-25 15:00:00"           # the rehearsal (6 h window)
  python3 research/night/rehearsal_eval.py --start "2026-10-22 06:30" --hours 0.5 --out /tmp/x.md   # any other window
  python3 research/night/rehearsal_eval.py --template                               # docs/night_rehearsal.md before the run

Sources (server, read-only): data/election/calls.jsonl + shadow.jsonl, docs/RUN_LOG.md (ELECTION / INJECT / PROBE / hourly lane
lines, minute resolution), data/engine.log (event lines timed by the next loop line, the loop's 'took N s'), data/ledger.json
(orders placed in the window, the cancels' owners), data/state.json (positions, manual books), /opt/research/night/rehearsal_plan
_expected.json (else the Mac copy). Rows are synthetic when their book is in the plan and inside that injection's active window
(start .. last quote + 120 s): the engine's own `injected` flag is true for every race of the loaded plan, started or not.
Without a plan for the window (a smoke test), the injected books and windows come from the RUN_LOG INJECT lines."""
import argparse
import collections
import datetime
import json
import os
import re
import shlex
import sys
import time

sys.dont_write_bytecode = True
os.environ["TZ"] = "America/Los_Angeles"
time.tzset()
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
import n_election as el  # noqa: E402  (constants only)
import rehearsal_plan as rp  # noqa: E402  (ssh, paths, waves)

DOC = os.path.join(ROOT, "docs", "night_rehearsal.md")
ENG = rp.ENGINE
SIG_TITLE = rp.SIG_TITLE
PULL_LAYERS = set(el.PULL_LAYERS)
EL_LAYERS = ("callsnipe", "drift", "driftexit", "callexit", "settle")
LOG_TS = re.compile(r"^(\d\d):(\d\d):(\d\d) loop (\d+) \|.*?took (\d+)s")
RL_LINE = re.compile(r"^- \[?(\d\d):(\d\d)\]? (.*)$")
RL_HEAD = re.compile(r"^### (\d\d):(\d\d) PT")


# --------------------------------------------------------------------------------------------- reading
def parse_t(s):
    s = s.strip()
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return time.mktime(time.strptime(s, f))
        except ValueError:
            pass
    raise SystemExit(f"--start: {s!r} is not 'YYYY-MM-DD HH:MM[:SS]' (PT)")


class Server:
    """Read-only access: `cat` / `tail -c` / `stat` / `date` over ssh, or a local copy laid out like /opt/sig-engine (--local: the
    Mac mirror; --from-dir: any directory, e.g. a snapshot taken after the run)."""

    def __init__(self, local, base=None):
        self.local = bool(local or base)
        self.base = base or ROOT

    def lpath(self, p):
        return p.replace(ENG + "/", self.base.rstrip("/") + "/")

    def text(self, path, tail_bytes=None):
        if self.local:
            lp = self.lpath(path)
            if not os.path.exists(lp):
                return ""
            with open(lp, "rb") as fh:
                if tail_bytes:
                    fh.seek(max(0, os.path.getsize(lp) - tail_bytes))
                return fh.read().decode(errors="replace")
        cmd = (f"tail -c {int(tail_bytes)} {shlex.quote(path)}" if tail_bytes else f"cat {shlex.quote(path)}") + " 2>/dev/null || true"
        return rp.ssh(cmd, timeout=300).decode(errors="replace")

    def now_and_sizes(self, paths):
        if self.local:
            lps = [self.lpath(p) for p in paths]
            now = max([os.path.getmtime(x) for x in lps if os.path.exists(x)] + [0.0]) if self.base != ROOT else time.time()
            return now or time.time(), {p: os.path.getsize(x) if os.path.exists(x) else 0 for p, x in zip(paths, lps)}
        out = rp.ssh("date +%s; " + "; ".join(f"stat -c %s {shlex.quote(p)} 2>/dev/null || echo 0" for p in paths)).decode().split()
        return float(out[0]), {p: int(v) for p, v in zip(paths, out[1:])}


def jsonl(text, t0, t1):
    out = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except ValueError:
            continue
        ts = r.get("ts")
        if ts is None and r.get("t"):
            try:
                ts = time.mktime(time.strptime(str(r["t"])[:19], "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                continue
        if ts is not None and t0 <= float(ts) <= t1:
            r["_ts"] = float(ts)
            out.append(r)
    return out


def dated(lines, now, pat_fn):
    """Walk a log backwards from `now` and give each timed line its date: a time later than the previous (forward) one by > 6 h
    means the walk crossed a midnight. pat_fn(line) -> seconds of day or None. -> [(epoch or None, line)] in file order."""
    lt = time.localtime(now)
    day = datetime.date(lt.tm_year, lt.tm_mon, lt.tm_mday)
    now_sod = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
    out, prev = [], None
    for line in reversed(lines):
        sod = pat_fn(line)
        if sod is None:
            out.append((None, line)); continue
        if (prev is not None and sod > prev + 6 * 3600) or (prev is None and sod > now_sod + 600):
            day -= datetime.timedelta(days=1)               # crossed a midnight going back (or the last line is from yesterday)
        prev = sod
        out.append((time.mktime((day.year, day.month, day.day, sod // 3600, sod % 3600 // 60, sod % 60, 0, 0, -1)), line))
    return list(reversed(out))


def runlog_lines(text, now):
    """RUN_LOG '- HH:MM text' lines -> [(epoch of the minute, text)]."""
    def sod(line):
        m = RL_LINE.match(line) or RL_HEAD.match(line)
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 if m else None
    out = []
    for t, line in dated(text.splitlines(), now, sod):
        m = RL_LINE.match(line)
        if t is not None and m:
            out.append((t, m.group(3)))
    return out


def englog(text, now):
    """engine.log -> (loops [(epoch, n, took_s)], events [(epoch, kind, text)]); an event line is timed by the next loop line (it is
    printed inside that loop, before its summary)."""
    lines = text.splitlines()[1:]                            # the first line of a tail is partial
    def sod(line):
        m = LOG_TS.match(line)
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)) if m else None
    loops, events, pend = [], [], []
    for t, line in dated(lines, now, sod):
        m = LOG_TS.match(line)
        if t is not None and m:
            loops.append((t, int(m.group(4)), int(m.group(5))))
            events += [(t, k, x) for k, x in pend]
            pend = []
        elif line.startswith("  ") and len(line) > 9 and line[2:9].strip().isupper():
            pend.append((line[2:9].strip(), line[9:].strip()))
    events += [(now, k, x) for k, x in pend]
    return loops, events


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def f_s(x, nd=2):
    return "n/a" if x is None else f"{(0.0 if abs(x) < 0.5 * 10 ** -nd else x):.{nd}f} s"


# --------------------------------------------------------------------------------------------- the plan / the injections
def load_expected(srv, start, path):
    if path == "none":
        return None, "no plan (--plan none)"
    cands = [path] if path not in (None, "auto") else [rp.REMOTE_EXPECTED, rp.LOCAL_EXPECTED]
    for p in cands:
        try:
            txt = srv.text(p) if p.startswith("/opt/") else (open(p).read() if os.path.exists(p) else "")
        except (OSError, RuntimeError):
            continue
        if not txt.strip():
            continue
        try:
            d = json.loads(txt)
        except ValueError:
            continue
        s0 = d.get("summary", {}).get("start_epoch")
        if s0 is not None and abs(float(s0) - start) > 60:
            return None, f"{p}: built for {d['summary'].get('start_pt')} PT, not this window: evaluated without a plan"
        return d, p
    return None, "no expected file found: evaluated without a plan"


def windows_from_plan(exp, start):
    out = []
    for j in exp["injections"]:
        t0 = start + j["at"]
        out.append(dict(ex=str(j["ex"]), race=j["race"], party=j["party"], wave=j["wave"], kind=j["kind"], tag=j.get("tag"),
                        t0=t0, close=t0 + j["close_at"], last=t0 + j["path_s"], end=t0 + j["path_s"] + el.TTL_S + 5))
    return out


def windows_from_log(rl, books, t0, t1):
    """No plan: the RUN_LOG INJECT lines ('... into <race> <party> over N s [from HH:MM:SS] ...' and '<race>: synthetic path done')."""
    by_name = {f"{b['race']} {b['party']}": ex for ex, b in books.items()}
    out = []
    for t, x in rl:
        if not (t0 - 3600 <= t <= t1) or " INJECT " not in f" {x}":
            continue
        m = re.search(r"INJECT (.+?): (\d+) synthetic PM quotes into (.+?) over (\d+) s(?: from (\d\d:\d\d:\d\d))?", x)
        if m:
            ex = by_name.get(m.group(3))
            st_ = t
            if m.group(5):
                st_ = time.mktime(time.strptime(time.strftime("%Y-%m-%d", time.localtime(t)) + " " + m.group(5), "%Y-%m-%d %H:%M:%S"))
            out.append(dict(ex=ex, race=m.group(1), party=m.group(3).rsplit(" ", 1)[-1], wave=None, kind="manual injection", tag=None,
                            t0=st_, close=st_, last=st_ + float(m.group(4)), end=st_ + float(m.group(4)) + el.TTL_S + 60))
    for t, x in rl:                                          # the 'path done' line closes the window (minute resolution: + 59 s)
        m = re.search(r"INJECT (.+?): synthetic path done", x)
        if m:
            for w in out:
                if w["race"] == m.group(1) and w["t0"] - 60 <= t and w.get("done") is None:
                    w["end"] = max(w["end"], t + 59); w["done"] = t
    return [w for w in out if w["ex"]]


# --------------------------------------------------------------------------------------------- evaluation
class Ev:
    def __init__(self, a):
        self.a = a
        self.start = parse_t(a.start)
        self.t1 = self.start + a.hours * 3600
        self.srv = Server(a.local, a.from_dir)
        self.rows = []                                       # the table: (criterion, target, measured, status)
        self.det = []                                        # markdown detail sections

    def crit(self, name, target, measured, status):
        self.rows.append((name, target, measured, status))

    def section(self, title, lines):
        self.det.append(f"\n## {title}\n\n" + "\n".join(lines) + "\n")

    # ------------------------------------------------------------------ load
    def load(self):
        a, S = self.a, self.start
        files = [f"{ENG}/data/engine.log", f"{ENG}/docs/RUN_LOG.md"]
        self.now, sizes = self.srv.now_and_sizes(files)
        span_h = max(1.0, (self.now - S) / 3600 + 2)
        tail = int(min(sizes[files[0]], max(4e6, span_h * 1.2e6)))
        hi = self.t1 + 3600                                  # the hourly line after the window
        self.calls = jsonl(self.srv.text(f"{ENG}/data/election/calls.jsonl"), S - 600, hi)
        self.shadow = jsonl(self.srv.text(f"{ENG}/data/election/shadow.jsonl"), S - 600, hi)
        self.rl = runlog_lines(self.srv.text(files[1]), self.now)
        self.loops, self.events = englog(self.srv.text(files[0], tail_bytes=tail), self.now)
        self.log_from = self.loops[0][0] if self.loops else None
        st = json.loads(self.srv.text(f"{ENG}/data/state.json") or "{}")
        self.books = {}
        for b in st.get("bots") or []:
            m = SIG_TITLE.match(str(b.get("market") or "").strip())
            ex = str(b["ex"])
            race = "U.S. Senate control" if ex in rp.SENATE_EXS else (m.group(2) if m else str(b.get("market")))
            self.books[ex] = dict(race=race, party=m.group(1)[0] if m else "?", pos=float(b.get("pos") or 0), manual=b.get("kind") == "position",
                                  name=f"{race} {m.group(1)[0] if m else '?'}")
        self.race_of = {ex: b["race"] for ex, b in self.books.items()}
        try:
            pc = json.loads(self.srv.text(f"{ENG}/data/election/poll_close.json") or "{}").get("markets") or []
        except ValueError:
            pc = []
        self.state_of = {}                                   # n_election's state: poll_close.json's, the race for the control markets
        for r in pc:
            race = self.race_of.get(str(r["ex"]), "")
            self.state_of[str(r["ex"])] = race if "control" in race.lower() else r.get("state")
        try:
            self.ledger = json.loads(self.srv.text(f"{ENG}/data/ledger.json") or "{}")
        except ValueError:
            self.ledger = {}
        try:
            self.owner = json.loads(self.srv.text(f"{ENG}/data/order_owner.json") or "{}")   # the engine's id -> owner map (a second source)
        except ValueError:
            self.owner = {}
        self.releases = []                                   # data/releases.log: 'YYYY-MM-DD HH:MM:SS <release>' per push
        for line in self.srv.text(f"{ENG}/data/releases.log").splitlines():
            m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) (\S+)", line)
            if m:
                self.releases.append((parse_t(m.group(1)), m.group(2)))
        self.exp, self.exp_src = load_expected(self.srv, S, a.plan)
        self.paths = {}                                      # ex -> [(abs t, bid, ask)] of the plan (the price check's fallback)
        if self.exp:
            self.win = windows_from_plan(self.exp, S)
            try:
                pj = json.loads((open(a.plan_json).read() if a.plan_json else self.srv.text(rp.REMOTE_PLAN)) or "{}")
            except (OSError, ValueError, RuntimeError):
                pj = {}
            for i in pj.get("injections") or []:
                self.paths[str(i["ex"])] = [(S + i["at"] + p_, b_, a_) for p_, b_, a_ in i["path"]]
        else:
            self.win = windows_from_log(self.rl, self.books, S, self.t1)
        self.win_of = collections.defaultdict(list)
        for w in self.win:
            self.win_of[w["ex"]].append(w)
        self.inj_races = {w["race"] for w in self.win}

    def synth(self, ex, ts):
        """The injection window this row falls in (book in the plan and ts inside start .. last quote + 120 s), else None."""
        for w in self.win_of.get(str(ex), ()):
            if w["t0"] - 1 <= ts <= w["end"]:
                return w
        return None

    def in_win(self, ts):
        return self.start <= ts <= self.t1

    # ------------------------------------------------------------------ the criteria
    def run(self):
        S = self.start
        cw = [r for r in self.calls if self.in_win(r["_ts"])]
        sw = [r for r in self.shadow if self.in_win(r["_ts"])]
        for r in cw:
            r["_w"] = self.synth(r.get("ex"), r["_ts"])
        syn = [r for r in cw if r["_w"]]
        real = [r for r in cw if not r["_w"]]
        self.syn, self.real = syn, real
        n_inj_flag = sum(1 for r in cw if r.get("injected"))
        head = (f"Window {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(S))} - {time.strftime('%H:%M:%S', time.localtime(self.t1))} PT; "
                f"calls.jsonl rows in the window {len(cw)} ({n_inj_flag} flagged injected by the engine), synthetic (plan book inside its "
                f"injection window) {len(syn)}; shadow.jsonl rows {len(sw)}; plan: {self.exp_src}; injection windows {len(self.win)}"
                + (f" ({', '.join(sorted(self.inj_races))})" if self.win and len(self.inj_races) <= 6 else ""))
        self.head = head
        if not syn and not self.win:
            self.head += "\n\n**No injected rows in this window** (no injection ran): only the real-feed criteria apply."

        # 1. the real feed
        bad_real = [r for r in real if r.get("kind") in ("CALL", "STRICT", "REVERSAL")]
        mislab = [r for r in bad_real if r.get("injected")]
        kinds = collections.Counter(r.get("kind") for r in real)
        self.crit("Real feed: CALL / STRICT / REVERSAL rows outside the injections", "0 (PRECALL / NOCALL allowed)",
                  f"{len(bad_real)} ({len(mislab)} of them flagged injected=true: a real quote on a book not injected, or outside its window); "
                  f"PRECALL {kinds.get('PRECALL', 0)}, NOCALL {kinds.get('NOCALL', 0)}", "PASS" if not bad_real else "FAIL")
        lines = [f"- {r['t']} {r['kind']} {r.get('race')} ex {r.get('ex')} {str(r.get('side')).upper()} rule {r.get('rule')} PM bid {r.get('pm_bid')} "
                 f"(injected flag {r.get('injected')}){' <- a book of an injected race that was NOT injected: its real quotes met the synthetic close' if r.get('race') in self.inj_races else ''}"
                 for r in bad_real[:40]]
        self.section("1. Real feed (rows outside the injections)", [f"Rows by kind: {dict(kinds) or 'none'}."] + (lines or ["- no CALL / STRICT / REVERSAL row: PASS"]))

        # 2-5. detection against the plan
        self.detection(syn)
        self.flags(cw, sw)
        # 6-7. call -> would-be order, the hourly lane line
        self.lane([r for r in cw if r.get("kind") == "CALL"], sw)
        # 8. reversals
        self.reversals(syn, sw)
        # 9. pulls
        self.pulls(syn, sw)
        # 10. sizes and prices
        self.sizes(sw)
        # 11-12. the probe and the orders actually sent
        self.probe_and_sent(sw)
        # 13. manual orders
        self.manual(sw)
        # 14-16. engine health, loops, the injections themselves
        self.health()
        self.injections_loaded()

    def detection(self, syn):
        S = self.start
        calls = [r for r in syn if r.get("kind") == "CALL"]
        if not self.exp:
            lines = [f"- {r['t']} CALL {r.get('race')} ex {r['ex']} {str(r.get('side')).upper()} rule {r.get('rule')} PM bid {r.get('pm_bid')}"
                     + (f"; its 0.95 run began {r['_ts'] - r['signal_t']:.1f} s before the call (at "
                        f"{time.strftime('%H:%M:%S', time.localtime(r['signal_t']))}{', before the injection started: real quotes began it' if r['signal_t'] < r['_w']['t0'] else ''})"
                        if r.get("signal_t") else "") for r in calls]
            self.crit("Detection: expected calls found", "every expected CALL", f"no plan for this window; {len(calls)} synthetic CALL rows", "NO PLAN" if calls else "NO DATA")
            self.crit("Detection latency, rule B", "<= 2.5 s from the first qualifying synthetic quote", "no plan (quote times unknown)", "NO PLAN" if calls else "NO DATA")
            self.crit("Detection latency, rule A", "<= 20 s + 2.5 s", "no plan (quote times unknown)", "NO PLAN" if calls else "NO DATA")
            self.section("2. Detection (no plan: the synthetic CALL rows as found)", lines or ["- none"])
            return
        exp_calls = [dict(e, race=r["race"], wave=r["wave"], kind_race=r["kind"]) for r in self.exp["races"] for e in r["events"] if e["kind"] == "CALL"]
        used, found, miss, lat_a, lat_b, lag, rows, env = set(), [], [], [], [], [], [], []
        for e in sorted(exp_calls, key=lambda x: x["t"]):
            lo, hi = S + e.get("t_lo", e["t"]) - 5, S + e.get("t_hi", e["t"]) + 60
            cand = [r for r in calls if id(r) not in used and str(r["ex"]) == e["ex"] and r.get("side") == e["side"] and lo <= r["_ts"] <= hi]
            if not cand:
                miss.append(e); rows.append(f"| {e['wave']} | {e['race']} | {e['ex']} {e['side'].upper()} | {e['rule']} | {e['t_pt']} | MISSING | | | |")
                continue
            r = min(cand, key=lambda x: x["_ts"]); used.add(id(r))
            rule_ok = r.get("rule") in (e.get("rules_seen") or [e["rule"]])
            l_due = r["_ts"] - (S + e["t"])
            l_first = r["_ts"] - (S + e.get("t_first", e["t"]))
            (lat_b if r.get("rule") == "B" else lat_a).append(l_first)
            lag.append(l_due)
            found.append((e, r, rule_ok))
            env.append((r.get("rule"), r["_ts"] <= S + e.get("t_hi", e["t"]) + 1, l_due))
            rows.append(f"| {e['wave']} | {e['race']} | {e['ex']} {e['side'].upper()} | {e['rule']} | {e['t_pt']} | {time.strftime('%H:%M:%S', time.localtime(r['_ts']))} "
                        f"{r.get('rule')}{'' if rule_ok else ' (rule differs)'} | {l_first:+.1f} s | {l_due:+.1f} s | {r.get('pm_bid')} vs {e.get('pm_bid')} |")
        extra = [r for r in calls if id(r) not in used]
        early = [r for r in calls if r["_ts"] < r["_w"]["close"] - 0.5]
        n_ok = sum(1 for _, _, ok in found if ok)
        self.crit("Detection: expected calls found (book, side, rule)", f"{len(exp_calls)} of {len(exp_calls)}",
                  f"{len(found)} found ({n_ok} with the expected rule), {len(miss)} missing",
                  "PASS" if not miss and n_ok == len(found) and exp_calls else ("NO DATA" if not calls and exp_calls and self.now < S + min(e["t"] for e in exp_calls) + 120 else "FAIL"))
        loop_p = [b - a for (a, _, _), (b, _, _) in zip(self.loops, self.loops[1:]) if self.in_win(b)]
        lp95 = pct(loop_p, 0.95)

        def lat_status(xs, tgt):
            return "NO DATA" if not xs else ("PASS" if max(xs) <= tgt else "FAIL")
        n_env = sum(1 for _, inside, _ in env if inside)
        self.crit("Detection latency, rule B (CALL ts - the first qualifying synthetic quote)", "<= 2.5 s, every call",
                  f"n {len(lat_b)}, p50 {f_s(pct(lat_b, .5), 2)}, p95 {f_s(pct(lat_b, .95), 2)}, max {f_s(max(lat_b) if lat_b else None, 2)}", lat_status(lat_b, 2.5))
        self.crit("Detection latency, rule A (CALL ts - (the quote completing the 20-s hold - 20 s))", "<= 22.5 s, every call",
                  f"n {len(lat_a)}, p50 {f_s(pct(lat_a, .5), 2)}, p95 {f_s(pct(lat_a, .95), 2)}, max {f_s(max(lat_a) if lat_a else None, 2)}; {n_env} of "
                  f"{len(env)} calls inside the plan's timing envelope", lat_status(lat_a, 22.5))
        nocall_races = [r for r in self.exp["races"] if r["expect"] != "CALL" and r["expect"] != "REVERSAL"]
        self.crit("False calls on injected books (none expected: spread, other book > 0.04, slow / uncalled races, extra calls)", "0",
                  f"{len(extra)} unexpected CALL rows" + (f": {', '.join(sorted({r.get('race') for r in extra}))}" if extra else ""), "PASS" if not extra else "FAIL")
        self.crit("No CALL before the race's synthetic close", "0", f"{len(early)}", "PASS" if not early else "FAIL")
        carried = [r for r in calls if r.get("signal_t") and float(r["signal_t"]) < r["_w"]["t0"] - 0.5]
        self.crit("0.95 runs restart at the injection's start (no run begun on real quotes carries in)", "0 CALL rows whose run began before t0",
                  f"{len(carried)} of {len(calls)}" + (f": {', '.join(sorted({str(r.get('race')) for r in carried}))}" if carried else ""),
                  "PASS" if calls and not carried else ("NO DATA" if not calls else "FAIL"))
        gr, g_ok = [], True
        for r in self.exp["races"]:
            if not r.get("mode"):
                continue
            want = "spread" if r["mode"] == "spread" else "another book"
            mine = [x for x in self.syn if x.get("race") == r["race"]]
            n_g = sum(1 for x in mine if x.get("kind") == "NOCALL" and any(want in g for g in (x.get("guards") or [])))
            n_c = sum(1 for x in mine if x.get("kind") == "CALL")
            g_ok &= n_g > 0 and n_c == 0
            gr.append(f"{r['race']} ({r['mode']}): NOCALL with the {want} guard {n_g}, CALL {n_c}")
        if gr:
            self.crit("Guards on the must-not-call cases (PM spread > 3c; the other book > 0.04)", "NOCALL rows with that guard, no CALL",
                      "; ".join(gr), "PASS" if g_ok else ("NO DATA" if not self.syn else "FAIL"))
        nc_lines = []
        for r in nocall_races:
            mine = [x for x in self.syn if x.get("race") == r["race"]]
            k = collections.Counter(x.get("kind") for x in mine)
            want_nc = sorted({g.split(" ")[0] + " " + g.split(" ")[1] for e in r["events"] if e["kind"] == "NOCALL" for g in (e.get("guards") or [])})
            got_nc = sorted({g.split(" ")[0] + " " + g.split(" ")[1] for x in mine if x.get("kind") == "NOCALL" for g in (x.get("guards") or [])})
            nc_lines.append(f"- {r['wave']} {r['race']} ({r['kind']}{', ' + r['mode'] if r.get('mode') else ''}): expected {r['expect']}"
                            + (f" with guards {want_nc}" if want_nc else "") + f"; rows {dict(k) or 'none'}" + (f", NOCALL guards seen {got_nc}" if got_nc else "")
                            + ("  **CALLED**" if k.get("CALL") else ""))
        lagd = (f"C112 feeds the synthetic quotes from their own thread (f_engine.el_inject_loop -> el_inject_feed), each at its own time, so "
                f"CALL ts - the ideal-clock call time (each quote at its scheduled second) is the detector's whole latency: p50 {f_s(pct(lag, .5), 2)}, "
                f"p95 {f_s(pct(lag, .95), 2)}, max {f_s(max(lag) if lag else None, 2)}. Rule B is timed from the first qualifying quote (>= 0.98 after "
                f"the close, the guards passing); rule A from 20 s before the quote that completes the 20-s hold (a run begun before the close, or "
                f"before a guard cleared, completes at the first quote after it). 'Envelope' = the plan's replays with 0.05-2 s of injector-thread "
                f"delay and 5-16 s loops (main-loop period in the window: p50 {f_s(pct(loop_p, .5), 1)}, p95 {f_s(lp95, 1)}).")
        self.section("2. Detection (expected vs found)", [lagd, "", "| wave | race | book | rule | expected (ideal clock) | found | latency | vs ideal | PM bid found vs expected |",
                                                           "|---|---|---|---|---|---|---|---|---|"] + rows
                     + (["", "Unexpected CALL rows on injected books:"] + [f"- {r['t']} {r.get('race')} ex {r['ex']} {str(r.get('side')).upper()} rule {r.get('rule')} PM bid {r.get('pm_bid')}" for r in extra] if extra else [])
                     + (["", "CALL rows before the synthetic close:"] + [f"- {r['t']} {r.get('race')} ex {r['ex']}" for r in early] if early else [])
                     + ["", "Must-not-call / uncalled races:"] + (nc_lines or ["- none in the plan"]))

    def flags(self, cw, sw):
        """C112: the engine's `injected` flag (calls.jsonl, shadow.jsonl orders) is true exactly while one of the race's injections has
        started and not yet been removed. Rows within 20 s of a window's edge are not judged (the loop flushes rows a loop late)."""
        def active(race, ts, pad):
            return any(w["race"] == race and w["t0"] + pad <= ts <= w["end"] - pad for w in self.win)
        rows = [(r, r.get("race")) for r in cw if "injected" in r]
        rows += [(r, self.race_of.get(str(r["items"][0][0]))) for r in sw if r.get("kind") == "order" and "injected" in r and r.get("items")]
        bad, judged = [], 0
        for r, race in rows:
            want_in, want_out = active(race, r["_ts"], 20), not active(race, r["_ts"], -20)
            if not (want_in or want_out):
                continue                                     # at an edge
            judged += 1
            if bool(r.get("injected")) != want_in:
                bad.append((r, race))
        self.crit("Engine `injected` flag = the injections that have started", "0 rows off",
                  f"{len(bad)} of {judged} rows off ({len(rows) - judged} at a window edge, not judged)", "PASS" if not bad else "FAIL")
        self.section("2b. The engine's injected flag", [f"- {r.get('t')} {r.get('kind')} {race} ({r.get('why') or r.get('rule') or ''}) flagged "
                                                        f"{r.get('injected')}, the plan says {'started' if not r.get('injected') else 'not running'}" for r, race in bad[:30]]
                     or [f"- every judged row ({judged}) agrees with the plan's windows"])

    def lane(self, calls, sw):
        """Every CALL row of the window (the lane does not care which feed made the call; real ones are a FAIL of item 1 anyway)."""
        orders = [r for r in sw if r.get("kind") == "order" and r.get("why") == "call" and r.get("ts")]
        lat, none, lines, pairs = [], [], [], []
        for c in calls:
            nxt = [o for o in orders if o["items"] and str(o["items"][0][0]) == str(c["ex"]) and o["items"][0][1] == c.get("side")
                   and c["_ts"] - 0.05 <= float(o["ts"]) <= c["_ts"] + 30]
            if nxt:
                o = min(nxt, key=lambda x: float(x["ts"]))
                lat.append(float(o["ts"]) - c["_ts"])
                pairs.append((c, float(o["ts"]) - c["_ts"]))
                lines.append(f"- {c['t']} {c.get('race')} {c['ex']} {str(c['side']).upper()} ({'synthetic' if c.get('_w') else 'REAL feed'}): would-be order "
                             f"{float(o['ts']) - c['_ts']:.3f} s later: {o['items'][0]}")
            else:
                b = self.books.get(str(c["ex"])) or {}
                held = (b.get("pos", 0) < 0) if c.get("side") == "yes" else (b.get("pos", 0) > 0)
                none.append(c)
                later = [o for o in orders if o["items"] and str(o["items"][0][0]) == str(c["ex"]) and o["items"][0][1] == c.get("side")
                         and c["_ts"] + 30 < float(o["ts"]) <= (c["_w"]["end"] if c.get("_w") else self.t1)]
                lines.append(f"- {c['t']} {c.get('race')} {c['ex']} {str(c['side']).upper()} ({'synthetic' if c.get('_w') else 'REAL feed'}): no would-be order within 30 s"
                             + (" (we hold the other side of this book now: C107)" if held else " (no SIG ask under min(0.97, PM bid - 3c), cash, or the race / state cap)")
                             + (f"; the first one came {float(later[0]['ts']) - c['_ts']:.0f} s later, when the PM bid / the SIG book moved (not the lane): "
                                f"{later[0]['items'][0]}" if later else ""))
        p50, p95 = pct(lat, .5), pct(lat, .95)
        st_ = "NO DATA" if not lat else ("PASS" if p50 <= 1.0 and p95 <= 2.0 else "FAIL")
        self.crit("Call -> would-be order (shadow row ts - CALL ts)", "p50 <= 1 s, p95 <= 2 s",
                  f"n {len(lat)} (of {len(calls)} calls; {len(none)} with no order), p50 {f_s(p50, 3)}, p95 {f_s(p95, 3)}", st_)
        slow = sorted([(c, l) for c, l in pairs if l > 0.5], key=lambda x: -x[1])   # Piero 10:55 Oct 23 item 4: every call-lane step over 500 ms, listed
        self.crit("Call-lane steps over 500 ms (call -> would-be order)", "none; each one listed", f"{len(slow)} of {len(pairs)}"
                  + (": " + "; ".join(f"{c['t'][11:19]} {c.get('race')} {str(c['side']).upper()} {l * 1000:.0f} ms" for c, l in slow[:12]) if slow else ""),
                  "NO DATA" if not pairs else ("PASS" if not slow else "FAIL"))
        hl = []
        for t, x in self.rl:
            if self.start <= t <= self.t1 + 3600 and "last hour:" in x and x.startswith("ELECTION ("):
                m = re.search(r"call lane \(quote -> order handed over\) n (\d+) p50 (\d+) ms p95 (\d+) ms", x)
                hl.append((t, x, m))
        lm = [m for _, _, m in hl if m and int(m.group(1)) > 0]
        st2 = "NO DATA" if not lm else ("PASS" if all(int(m.group(2)) <= 1000 and int(m.group(3)) <= 2000 for m in lm) else "FAIL")
        self.crit("Hourly lane line (quote -> order handed over)", "p50 <= 1,000 ms, p95 <= 2,000 ms",
                  "; ".join(f"{time.strftime('%H:%M', time.localtime(t))} n {m.group(1)} p50 {m.group(2)} p95 {m.group(3)} ms" for t, _, m in hl if m) or f"{len(hl)} hourly lines, no lane numbers", st2)
        self.section("3. Call -> would-be order (the call lane, shadow)", [f"Pairs: n {len(lat)}, p50 {f_s(p50, 3)}, p95 {f_s(p95, 3)}, max {f_s(max(lat) if lat else None, 3)}.", ""] + lines
                     + ["", "Hourly lines (E.lane_lat is the last 500 samples since the engine started, not the hour only):"]
                     + [f"- {time.strftime('%H:%M', time.localtime(t))} {x[:400]}" for t, x, _ in hl])

    def reversals(self, syn, sw):
        revs = [r for r in syn if r.get("kind") == "REVERSAL"]
        exp_rev = [dict(e, race=r["race"]) for r in (self.exp or {}).get("races", []) for e in r["events"] if e["kind"] == "REVERSAL"]
        S = self.start
        found = []
        for e in exp_rev:
            ok = [r for r in revs if str(r["ex"]) == e["ex"] and S + e.get("t_lo", e["t"]) - 5 <= r["_ts"] <= S + e.get("t_hi", e["t"]) + 60]
            found.append((e, ok[0] if ok else None))
        after, lines = [], []
        for r in revs:
            w = r["_w"]
            race_exs = {x["ex"] for x in self.win if x["race"] == r.get("race")}
            o_after = [o for o in sw if o.get("kind") == "order" and o.get("why") == "call" and o.get("items") and str(o["items"][0][0]) in race_exs
                       and r["_ts"] + 0.5 < o["_ts"] <= w["end"]]
            after += o_after
            lines.append(f"- {r['t']} REVERSAL {r.get('race')} ex {r['ex']} {str(r.get('side')).upper()}: would-be orders on the race after it: {len(o_after)}")
        miss = [e for e, r in found if r is None]
        if exp_rev:
            st_ = "PASS" if not miss and not after else "FAIL"
            meas = f"{len(found) - len(miss)} of {len(exp_rev)} expected REVERSAL rows; would-be orders after a reversal {len(after)}"
        else:
            st_ = "NO PLAN" if revs else "NO DATA"
            meas = f"{len(revs)} REVERSAL rows; would-be orders after one {len(after)}"
        self.crit("Reversals: the race stops (REVERSAL row), no would-be order after it", "every expected REVERSAL; 0 orders after", meas, st_)
        self.section("4. Reversals", [f"- expected {e['t_pt']} {e['race']} ex {e['ex']} {e['side'].upper()}: " + ("found " + r["t"] if r else "MISSING") for e, r in found] + lines or ["- none"])

    def pulls(self, syn, sw):
        """C14 under C112: each planned race pulls ONCE, at its injection's start (el_inject_begin sets its first close to the synthetic close,
        the injection starts 7 min before it, so Election.on_tick pulls at the next loop). POLLS lines (engine.log, timed by the loop line
        that printed them; the RUN_LOG minute lines when the tail is short) and the shadow cancel rows ('POLLS <race>') vs each wave's
        first CALL."""
        polls = collections.defaultdict(list)
        for t, k, x in self.events:
            m = re.match(r"POLLS (.+?): first poll close", x) if k == "NIGHT" else None
            if m and self.start - 900 <= t <= self.t1:
                polls[m.group(1)].append(t)
        if self.log_from is None or self.log_from > self.start - 900:
            for t, x in self.rl:                             # engine.log tail too short: the RUN_LOG lines (minute resolution, + 59 s)
                m = re.search(r"POLLS (.+?): first poll close", x)
                if m and self.start - 900 <= t <= self.t1 and not polls.get(m.group(1)):
                    polls[m.group(1)].append(t + 59)
        cancels = collections.defaultdict(list)
        for r in self.shadow:
            if r.get("kind") == "cancel" and str(r.get("reason", "")).startswith("POLLS ") and self.start - 900 <= r["_ts"] <= self.t1:
                cancels[str(r["reason"])[6:]].append(r["_ts"])
        exp_pull = {r["race"]: r.get("pull") for r in (self.exp or {}).get("races", [])}
        waves = collections.defaultdict(list)
        for w in self.win:
            waves[w["wave"] or "manual"].append(w)
        planned = {w["race"] for w in self.win}
        stray = sorted(r for r in polls if r not in planned)
        dup = sorted(r for r in planned if len(polls.get(r, [])) > 1)
        missing = sorted(r for r in planned if not polls.get(r))
        out, lines = [], []
        for wv, ws in sorted(waves.items(), key=lambda kv: min(w["close"] for w in kv[1])):
            races = sorted({w["race"] for w in ws})
            first_call = min((r["_ts"] for r in syn if r.get("kind") == "CALL" and r.get("race") in races), default=None)
            pt = [max(polls[r][:1] + cancels.get(r, [])) for r in races if polls.get(r)]
            last_pull = max(pt) if pt else None
            lead = None if (first_call is None or last_pull is None) else first_call - last_pull
            if first_call is not None:
                out.append((wv, lead, len(pt), len(races)))
            ep = [exp_pull[r]["pt"] for r in races if exp_pull.get(r)]
            lines.append(f"- wave {wv}: POLLS lines {len(pt)} of {len(races)} races, last pull (POLLS line or its shadow cancels) "
                         f"{time.strftime('%H:%M:%S', time.localtime(last_pull)) if last_pull else 'n/a'}"
                         + (f" (expected {min(ep)[:8]}-{max(ep)[-8:]})" if ep else "")
                         + f"; first CALL {time.strftime('%H:%M:%S', time.localtime(first_call)) if first_call else 'none'} -> lead {f_s(lead, 0)}; "
                         f"shadow pull cancels {sum(len(cancels.get(r, [])) for r in races)}")
        worst = min((l for _, l, _, _ in out if l is not None), default=None)
        if not out:
            st_, meas = "NO DATA", "no wave with a call"
        else:
            ok = all(l is not None and l >= 300 for _, l, _, _ in out) and not missing and not dup and not stray
            st_ = "PASS" if ok else "FAIL"
            meas = (f"worst lead {f_s(worst, 0)} over {len(out)} waves with a call; races pulled {len(planned) - len(missing)} of {len(planned)}, "
                    f"twice {len(dup)}, unplanned POLLS {len(stray)}")
        self.crit("Poll-close pulls before the first call of each wave (once per race)", ">= 5 min before; one POLLS per planned race, none other", meas, st_)
        self.section("5. Poll-close pulls", ["C112 sets an injected race's FIRST close to the synthetic close at the injection's start; the plan starts every "
                                              "injection 7 min before it, so the C14 pull (first close - 10 min) fires at the first main loop after the start, "
                                              "once per race, and leads the first CALL by ~7 min less one loop.", ""] + lines
                     + ([f"- races with no POLLS line: {', '.join(missing)}"] if missing else []) + ([f"- pulled twice: {', '.join(dup)}"] if dup else [])
                     + ([f"- POLLS for races not in the plan (a real race pulled before Nov 3): {', '.join(stray)}"] if stray else []))

    def sizes(self, sw):
        """C112: a shadow would-be order is confirmed at once and keeps its budget (as an accepted live order), and the depth it took
        leaves the book for 120 s: the race / state caps are cumulative, as on the night."""
        orders = [r for r in sw if r.get("kind") == "order" and r.get("why") == "call" and r.get("items")]
        snipe = []                                           # RUN_LOG 'SNIPE <race>: buy NO 312 at <= 0.940 (PM bid 0.960; ...'
        for t, x in self.rl:
            m = re.search(r"SNIPE (.+?): buy (YES|NO) ([\d,]+) at <= ([\d.]+) \(PM bid ([\d.]+)", x)
            if m and self.start - 60 <= t <= self.t1 + 60:
                snipe.append((t, m.group(1), m.group(2).lower(), int(m.group(3).replace(",", "")), float(m.group(4)), float(m.group(5))))
        cash_l = []
        for t, x in self.rl:
            m = re.search(r"ELECTION CASH: cash ([\d,]+)", x)
            if m:
                cash_l.append((t, float(m.group(1).replace(",", ""))))
        armed_l = []                                         # the rules in force: 'ELECTION armed (...): N books / M races ... PM bid - Xc)'
        for t, x in self.rl:
            m = re.search(r"ELECTION armed \(\w+\): (\d+) books / (\d+) races.*?PM bid - (\d+)c", x)
            if m:
                armed_l.append((t, int(m.group(2)), int(m.group(3)) / 100))

        def rules_at(ts):
            prev = [a_ for a_ in armed_l if a_[0] <= ts + 60]
            n, under = (prev[-1][1], prev[-1][2]) if prev else (116, el.PRICE_UNDER_PM)
            return n, under, (2.0 if abs(under - 0.02) < 1e-9 else el.WAVE_MULT)   # C110 (11:59 Oct 22) moved 2c / 2x to 3c / 1x together
        bad_px, rows = [], []
        race_q, race_c, race_share, state_c, per_book = (collections.defaultdict(float), collections.defaultdict(float), collections.defaultdict(float),
                                                         collections.defaultdict(float), collections.Counter())
        for o in orders:
            ex, side, _, px, q = o["items"][0]
            ex = str(ex); race = self.race_of.get(ex); w = self.synth(ex, o["_ts"])
            sn = [s_ for s_ in snipe if s_[1] == race and s_[2] == side and s_[3] == int(q) and abs(s_[4] - px) < 1e-6 and abs(s_[0] - o["_ts"]) <= 120]
            pm_bid = sn[0][5] if sn else None
            src = "SNIPE line"
            if pm_bid is None and self.paths.get(ex):          # the plan's own quotes of the 30 s before (the most lenient side bid)
                sbs = [(b_ if side == "yes" else 1 - a_) for t_, b_, a_ in self.paths[ex] if o["_ts"] - 30 <= t_ <= o["_ts"]]
                pm_bid, src = (round(max(sbs), 3), "plan path") if sbs else (None, "")
            n_races, under, mult = rules_at(o["_ts"])
            cap = None if pm_bid is None else round(min(el.PRICE_CAP, pm_bid - under), 3)
            if cap is not None and px > cap + 1e-9:
                bad_px.append((o, cap))
            prev = [c for t, c in cash_l if t <= o["_ts"]] or [c for t, c in cash_l]
            cash = prev[-1] if prev else None
            k_closed = len({x["race"] for x in self.win if x["close"] <= o["_ts"] <= x["end"]})
            share = None if cash is None else max(0.0, cash - el.CASH_FLOOR) * mult / max(1, n_races - k_closed + 1)
            key = (race, w["t0"] if w else None)             # one injection of the race (its calls are cleared when it ends)
            race_q[key] += q; race_c[key] += q * px
            if share is not None:
                race_share[key] = max(race_share[key], share)
            state_c[(self.state_of.get(ex) or race, w["t0"] if w else None)] += q * px
            per_book[(ex, side)] += 1
            rows.append(f"| {time.strftime('%H:%M:%S', time.localtime(o['_ts']))}{'' if w else ' (REAL)'} | {race} | {ex} {side.upper()} | {q:,} | {px:.3f} | "
                        f"{'n/a' if pm_bid is None else f'{pm_bid} ({src})'} | "
                        f"{cap if cap is not None else 'n/a'} | {q * px:,.0f} | {'n/a' if share is None else f'{share:,.0f}'} |")
        over_share = [(k, c, race_share[k]) for k, c in race_c.items() if k in race_share and c > race_share[k] * 1.02 + 1]
        max_race = max(race_q.values(), default=0)
        max_state = max(state_c.values(), default=0)
        max_one = max((o["items"][0][4] for o in orders), default=0)
        resnipe = max(per_book.values(), default=0)
        if not orders:
            st_ = "NO DATA"
        else:
            st_ = "PASS" if (max_race <= el.RACE_SHARES and max_state <= el.STATE_CAP and not bad_px and not over_share) else "FAIL"
        unpriced = sum(1 for r in rows if "| n/a |" in r)
        self.crit("Would-be order sizes and prices (cumulative per race / state, as on the night)",
                  f"<= {el.RACE_SHARES:,} sh a race, <= {el.STATE_CAP:,.0f} at cost a state, <= the wave share; price <= min(0.97, PM bid - 3c)",
                  f"{len(orders)} orders on {len(per_book)} book sides (most on one: {resnipe}); max one {max_one:,} sh; a race {max_race:,.0f} sh; a state "
                  f"{max_state:,.0f}; over the wave share {len(over_share)}; over the price cap {len(bad_px)} ({unpriced} unpriced: no SNIPE line, no plan quote)", st_)
        self.section("6. Would-be call orders (shadow)", [
            f"Caps cumulative per race (per injection) and per state: C112 confirms each shadow order at once (it keeps its budget, as an accepted "
            f"live order does) and holds the depth it took off the book for 120 s, so a called book is no longer re-sniped every minute (most "
            f"orders on one book side here: {resnipe}). The wave share bound is (cash - 7,000) x {el.WAVE_MULT} (2 before C110) / (the armed races - "
            f"the injected races closed at that moment + 1), cash from the latest ELECTION CASH line (an upper bound: the sniper also nets its "
            f"orders of the last 120 s); the price cap is min(0.97, PM side bid - the 'PM bid - Nc' of the ELECTION armed line in force: "
            f"{sorted({a_[2] for a_ in armed_l}) or [el.PRICE_UNDER_PM]}).", "",
            "| time | race | book | qty | price | PM side bid | cap | cost | wave share bound |", "|---|---|---|---|---|---|---|---|---|"] + rows[:300]
            + ([f"", f"Price over the cap: " + "; ".join(f"{o['t']} {o['items'][0]} cap {c}" for o, c in bad_px[:20])] if bad_px else [])
            + ([f"", f"Over the wave share: " + "; ".join(f"{k[0]} cost {c:,.0f} > share {s_:,.0f}" for k, c, s_ in over_share[:20])] if over_share else []))

    def probe_and_sent(self, sw):
        pl = [(t, x) for t, x in self.rl if self.start <= t <= self.t1 and re.match(r"ELECTION \(\w+\) PROBE ", x)]
        if not pl:
            pl = [(t, "ELECTION PROBE " + x[6:]) for t, k, x in self.events if k == "NIGHT" and x.startswith("PROBE ") and self.start <= t <= self.t1]
        ms = [(t, x, re.search(r"SIG answered in (\d+) ms \((accepted|not accepted)", x)) for t, x in pl]
        acc = [m for _, _, m in ms if m and m.group(2) == "accepted"]
        if not pl:
            st_, meas = "NO DATA", "no PROBE line in the window"
        else:
            st_ = "PASS" if len(acc) == 1 and len(pl) == 1 else ("FAIL" if not acc else "INFO")
            meas = "; ".join(f"{time.strftime('%H:%M', time.localtime(t))} " + (f"SIG answered in {m.group(1)} ms ({m.group(2)})" if m else x[:120]) for t, x, m in ms)
        self.crit("The probe: one genuine minimum-size order through the call lane", "one PROBE line, accepted; SIG round trip in ms", meas, st_)
        sent = [r for r in sw if r.get("kind") == "sent"]
        orders = (self.ledger.get("orders") or {}).values()
        placed = [o for o in orders if o.get("layer") in EL_LAYERS and o.get("ts") and self.in_win(float(o["ts"]))]
        probe = [o for o in placed if str(o.get("sub") or "").startswith("probe:")]
        other = [o for o in placed if o not in probe]
        st2 = "PASS" if not sent and not other and len(probe) <= 1 else "FAIL"
        self.crit("Orders actually sent by the election layers", "only the probe (shadow 'sent' rows 0)",
                  f"shadow 'sent' rows {len(sent)}; ledger election-layer orders {len(placed)} (probe {len(probe)}, other {len(other)})", st2)
        self.section("7. The probe and the orders sent", [f"- {time.strftime('%H:%M', time.localtime(t))} {x[:300]}" for t, x in pl]
                     + [f"- ledger order {o['id']} {o.get('layer')} {o.get('sub')} ex {o.get('ex')} {o.get('side')} {o.get('action')} {o.get('qty')} at {o.get('price')} "
                        f"placed {o.get('placed')} status {o.get('status')} filled {o.get('filled')}" for o in placed[:20]]
                     + [f"- shadow 'sent' row {r.get('t')} {r.get('items')}" for r in sent[:20]] or ["- no PROBE line, no order sent"])

    def manual(self, sw):
        orders = self.ledger.get("orders") or {}
        manual_ex = {ex for ex, b in self.books.items() if b["manual"]}
        canc = [r for r in sw if r.get("kind") == "cancel"]
        bad = []
        for r in canc:
            oid = str(r.get("id"))
            o = orders.get(oid) or {}
            engine_entry = o.get("layer") in PULL_LAYERS | set(el.SNIPE_LAYERS) or (not o and oid in self.owner)
            if str(r.get("ex")) in manual_ex or not engine_entry:
                bad.append((r, o.get("layer") or f"not in the ledger; order_owner {self.owner.get(oid)!r}"))
        stop = [x for t, k, x in self.events if k == "NIGHT" and x.startswith("STOP ") and self.in_win(t)]
        self.crit("No manual order touched (shadow cancels only of engine entry orders)", "0",
                  f"{len(canc)} shadow cancels, {len(bad)} not an engine entry order / on a manual book; kill switch {len(stop)}", "PASS" if not bad else "FAIL")
        self.section("8. Shadow cancels", [f"- {r.get('t')} cancel {r.get('id')} ex {r.get('ex')} ({r.get('reason')}): {l!r} (the C14 pulls take only orders "
                                            f"whose layer is a pull layer: a foreign / manual order here is a FAIL)" for r, l in bad[:40]]
                     or [f"- {len(canc)} shadow cancels, every one an engine entry order (ledger layer in the pull layers), none on a manual book "
                         f"({', '.join(sorted(self.books[e]['name'] for e in manual_ex)) or 'no manual book'})"])

    def health(self):
        ev = [(t, k, x) for t, k, x in self.events if self.in_win(t)]
        err = [(t, x) for t, k, x in ev if k == "ERR"]
        el_err = [(t, x) for t, x in err if re.search(r"election|call lane|drift plan|LANE|inject|rehearsal|probe", x, re.I)]
        el_err += [(t, x) for t, k, x in ev if k == "NIGHT" and re.match(r"(LANE|INJECT) .*(error|not read)", x)]
        pause = [(t, x) for t, k, x in ev if k == "PAUSE"]
        gaps = [(t, x) for t, k, x in ev if k == "NIGHT" and x.startswith("FEEDGAP")]
        kinds = collections.Counter(re.sub(r"[\d.,]+", "#", x)[:70] for _, x in err)
        cover = "" if (self.log_from is not None and self.log_from <= self.start) else f" (engine.log tail starts {time.strftime('%m-%d %H:%M', time.localtime(self.log_from)) if self.log_from else 'n/a'}: part of the window not covered)"
        self.crit("Engine ERR / PAUSE lines in the window", "0 election ERR, 0 PAUSE (a pause stops the call lane)",
                  f"ERR {len(err)} (election path {len(el_err)}), PAUSE {len(pause)}, PM FEEDGAP (30-s silence: 0.95 runs restart) {len(gaps)}{cover}",
                  "PASS" if not el_err and not pause else "FAIL")
        live = [r for t, r in self.releases if t <= self.start]
        pushed = [(t, r) for t, r in self.releases if self.start < t <= self.t1]
        lw = [(t, n) for t, n, _ in self.loops if self.start - 60 <= t <= self.t1]
        restarts = [t for (t0_, n0), (t1_, n1) in zip(lw, lw[1:]) if n1 < n0 for t in (t1_,)]
        rel = live[-1] if live else "unknown"
        mnum = re.match(r"C(\d+)", rel)
        old = bool(mnum) and int(mnum.group(1)) < 112
        self.crit("Engine release and restarts in the window", "C112 or later live; 0 restarts / pushes (the injections live in memory)",
                  f"live at the start: {rel}; pushes in the window {len(pushed)}" + (f" ({', '.join(f'{r} at {time.strftime('%H:%M', time.localtime(t))}' for t, r in pushed)})" if pushed else "")
                  + f"; loop-counter restarts {len(restarts)}" + (f" ({', '.join(time.strftime('%H:%M:%S', time.localtime(t)) for t in restarts)})" if restarts else ""),
                  "FAIL" if (old or pushed or restarts) else ("PASS" if live else "NO DATA"))
        took = [s for t, n, s in self.loops if self.in_win(t)]
        per = [b[0] - a[0] for a, b in zip(self.loops, self.loops[1:]) if self.in_win(b[0])]
        self.crit("Loop time (engine.log 'took N s') and the loop period", "INFO (Oct 22 baseline: took 1-5 s, period 5-16 s)",
                  f"took p50 {f_s(pct(took, .5), 0)}, p95 {f_s(pct(took, .95), 0)}, max {f_s(max(took) if took else None, 0)}; period p50 {f_s(pct(per, .5), 0)}, "
                  f"p95 {f_s(pct(per, .95), 0)}, max {f_s(max(per) if per else None, 0)} ({len(took)} loops)", "INFO" if took else "NO DATA")
        self.section("9. Engine health", [f"ERR lines by text: {dict(kinds.most_common(12)) or 'none'}"]
                     + [f"- {time.strftime('%H:%M:%S', time.localtime(t))} ERR {x[:200]}" for t, x in el_err[:20]]
                     + [f"- {time.strftime('%H:%M:%S', time.localtime(t))} PAUSE {x[:200]}" for t, x in pause[:20]])

    def injections_loaded(self):
        reh = [(t, x) for t, x in self.rl if self.start - 3600 <= t <= self.t1 and "REHEARSAL plan" in x]
        inj = [(t, x) for t, x in self.rl if self.start - 3600 <= t <= self.t1 + 600 and re.search(r"INJECT .+: \d+ synthetic PM quotes", x)]
        done = [(t, x) for t, x in self.rl if self.start - 3600 <= t <= self.t1 + 600 and "synthetic path done" in x]
        n_plan = len((self.exp or {}).get("injections") or [])
        n_races = len((self.exp or {}).get("races") or [])
        m = re.search(r"(\d+) injections scheduled", reh[-1][1]) if reh else None
        if self.exp:
            ok = bool(m) and int(m.group(1)) == n_plan and len(done) >= n_races
            meas = (f"REHEARSAL line: {m.group(1) if m else 'missing'} scheduled (plan {n_plan}); INJECT lines {len(inj)}; 'path done' {len(done)} of {n_races} races")
            st_ = "PASS" if ok else "FAIL"
        else:
            meas, st_ = f"no plan; INJECT lines {len(inj)}, 'path done' {len(done)}", "INFO" if inj else "NO DATA"
        self.crit("Injections loaded and finished", "every planned injection scheduled, every race restored", meas, st_)
        self.section("10. Injection log", [f"- {time.strftime('%H:%M', time.localtime(t))} {x[:260]}" for t, x in (reh + inj[:12] + done[:40])] or ["- none"])

    # ------------------------------------------------------------------ output
    def markdown(self):
        S = self.start
        n_pass = sum(1 for r in self.rows if r[3] == "PASS")
        n_fail = sum(1 for r in self.rows if r[3] == "FAIL")
        n_blk = sum(1 for r in self.rows if r[3] == "BLOCKED")
        out = [f"# Election-night dress rehearsal: results\n",
               f"Evaluated {time.strftime('%Y-%m-%d %H:%M PT', time.localtime(self.now))} by `research/night/rehearsal_eval.py --start \"{self.a.start}\""
               f"{' --hours ' + str(self.a.hours) if self.a.hours != rp.WINDOW_H else ''}` (server files read-only). Criteria: docs/night_reconcile.md C19 / "
               f"election_night_v2.md §4.6. **{n_pass} PASS, {n_fail} FAIL**" + (f", {n_blk} BLOCKED" if n_blk else "") + ".\n",
               self.head + "\n",
               "| # | criterion | target | measured | status |", "|---|---|---|---|---|"]
        for i, (c, t, m, s_) in enumerate(self.rows, 1):
            out.append(f"| {i} | {c} | {t} | {m.replace('|', '/')} | **{s_}** |")
        out += self.det
        out.append("\n## Known limits\n\n" + LIMITS)
        if self.exp:
            out += plan_section(self.exp)
        out.append(f"\n---\nPlan: `research/night/rehearsal_plan.py` (expected list `research/night/rehearsal_plan_expected.json`, server copy "
                   f"`{rp.REMOTE_EXPECTED}`). Engine: C111 `election_rehearsal` (f_engine.el_rehearsal_load / el_inject_start / el_inject_step).\n")
        return "\n".join(out)


LIMITS = ("What the rehearsal still cannot show (engine C112, f_engine.el_inject_loop / el_inject_begin / el_inject_feed / el_place / el_levels): "
          "(1) whether a would-be order goes depends on the real SIG book and our positions (C107: no buy into a book where we hold the other "
          "side): at Oct 22's books about half the calls have none, so the call -> order lane is timed on the rest; (2) an injected PRECALL can be "
          "hidden by the real feed's 30-min PRECALL throttle on that book (PRECALLs are not a criterion); (3) the pull lead is 7 min less one "
          "main loop (the pull fires at the first loop after the injection's start); (4) the injections live in memory only: an engine restart "
          "or a release push during the window loses them (item 'release and restarts'; re-send the rest with rehearsal_plan.py --after HH:MM); "
          "(5) the hourly lane line's p50 / p95 are over the last 500 samples since the engine started, not the hour; (6) rows within 20 s of an "
          "injection's start or end are not judged for the `injected` flag (the loop flushes rows up to a loop late).")


def exp_waves(exp):
    """[{close_pt}] per wave from the expected list (the races' synthetic closes)."""
    seen = collections.OrderedDict()
    for r in (exp or {}).get("races") or []:
        seen.setdefault(r["wave"], dict(close_pt=r["close_pt"]))
    return list(seen.values())


def plan_section(exp):
    s = exp.get("summary") or {}
    out = [f"\n## The plan\n\n{s.get('races')} races, {s.get('injections')} injections (every book of each race), {s.get('expected_calls')} expected CALL rows, "
           f"{s.get('expected_reversals')} REVERSAL rows; must-not-call / uncalled in the window: {', '.join(s.get('no_call_races') or [])}; peak "
           f"{s.get('peak_books')} books at once ({s.get('peak_at_pt')}); the last race restored {s.get('last_restore_pt')} PT. Start {s.get('start_pt')} PT = "
           f"epoch {float(s.get('start_epoch') or 0):.0f}. Built {s.get('built')} for {s.get('engine_release', 'C112')} from {s.get('inputs')} (cash then "
           f"{s.get('cash_at_build', 0):,}). Times on the ideal clock (each quote at its own second); every race checked against "
           f"{len(rp.VARIANTS)} replays with 0.05-2 s of injector-thread delay and 5-16 s main loops. Pull lead per wave (expected, worst variant): "
           + ", ".join(f"{w['wave']} {w['min_lead_s']:.0f} s" for w in s.get("waves") or [] if w.get("min_lead_s") is not None) + ".\n",
           "| wave (Nov 3) | synthetic close | race | kind | 2024 path (x compression) | expected |", "|---|---|---|---|---|---|"]
    for r in exp.get("races") or []:
        ev = "; ".join(f"{e['kind']} {e['ex']} {e['side'].upper()}{(' ' + e['rule']) if e.get('rule') else ''} {e['t_pt']}" for e in r["events"] if e["kind"] in ("CALL", "REVERSAL"))
        nc = sorted({g.split(' ')[0] + ' ' + g.split(' ')[1] for e in r["events"] if e["kind"] == "NOCALL" for g in (e.get("guards") or [])})
        what = ev or (f"NOCALL ({', '.join(nc)} guard), no CALL" if nc else r["expect"])
        out.append(f"| {r['wave']} | {r['close_pt']} | {r['race']} | {r['kind']}{' (' + r['mode'] + ')' if r.get('mode') and r['mode'] != r['kind'] else ''} | "
                   f"{r['template_2024']} (x{r['k']}) | {what} |")
    return out


def template(a):
    """docs/night_rehearsal.md before the run: the table with every measured value pending, the run sheet, the plan."""
    try:
        exp = json.load(open(rp.LOCAL_EXPECTED))
    except (OSError, ValueError):
        exp = None
    s = (exp or {}).get("summary") or {}
    pr = s.get("probe_suggestion") or {}
    crit = [("Real feed: CALL / STRICT / REVERSAL rows outside the injections", "0 (PRECALL / NOCALL allowed)"),
            ("Detection: expected calls found (book, side, rule)", f"{s.get('expected_calls', '?')} of {s.get('expected_calls', '?')}"),
            ("Detection latency, rule B (CALL ts - the first qualifying synthetic quote)", "<= 2.5 s, every call"),
            ("Detection latency, rule A (CALL ts - (the quote completing the 20-s hold - 20 s))", "<= 22.5 s, every call"),
            ("False calls on injected books (spread, other book > 0.04, slow / uncalled races, extra calls)", "0"),
            ("No CALL before the race's synthetic close", "0"),
            ("0.95 runs restart at the injection's start (no run begun on real quotes carries in)", "0 CALL rows whose run began before t0"),
            ("Guards on the must-not-call cases (PM spread > 3c; the other book > 0.04)", "NOCALL rows with that guard, no CALL"),
            ("Engine `injected` flag = the injections that have started", "0 rows off"),
            ("Call -> would-be order (shadow row ts - CALL ts)", "p50 <= 1 s, p95 <= 2 s"),
            ("Hourly lane line (quote -> order handed over)", "p50 <= 1,000 ms, p95 <= 2,000 ms"),
            ("Reversals: the race stops (REVERSAL row), no would-be order after it", f"{s.get('expected_reversals', '?')} REVERSAL rows; 0 orders after"),
            ("Poll-close pulls before the first call of each wave (once per race)", ">= 5 min before; one POLLS per planned race, none other"),
            ("Would-be order sizes and prices (cumulative per race / state, as on the night)", "<= 15,000 sh a race, <= 20,000 at cost a state, <= the wave share; price <= min(0.97, PM bid - 3c)"),
            ("The probe: one genuine minimum-size order through the call lane", "one PROBE line, accepted; SIG round trip in ms"),
            ("Orders actually sent by the election layers", "only the probe"),
            ("No manual order touched (shadow cancels only of engine entry orders)", "0"),
            ("Engine ERR / PAUSE lines in the window", "0 election ERR, 0 PAUSE"),
            ("Engine release and restarts in the window", "C112 or later live; 0 restarts / pushes"),
            ("Loop time and loop period", "INFO"),
            ("Injections loaded and finished", "every planned injection scheduled, every race restored")]
    out = ["# Election-night dress rehearsal: results\n",
           f"**Template, written {time.strftime('%Y-%m-%d %H:%M PT')}: every measured value is pending.** After the run, "
           f"`python3 research/night/rehearsal_eval.py --start \"{rp.START_PT}\"` (from the Mac; it reads the server's files over ssh, read-only) "
           "rewrites this page with the results. Sun Oct 25 15:00-21:00 PT on the live engine in shadow (zero orders except the one probe), "
           "2024 Polymarket paths injected into every wave (`research/night/rehearsal_plan.py`). Criteria: docs/night_reconcile.md C19 / "
           "election_night_v2.md §4.6.\n",
           "| # | criterion | target | measured | status |", "|---|---|---|---|---|"]
    out += [f"| {i} | {c} | {t} | pending | pending |" for i, (c, t) in enumerate(crit, 1)]
    out += ["\n## Run sheet (Sun Oct 25)\n",
            "1. Before 14:50: the server runs C112 or later (`/opt/sig-engine/RELEASE`; C111 steps the synthetic quotes from the main loop and "
            "pulls at the close), election mode shadow, new risk not paused (a pause stops the call lane: no would-be orders), no release push "
            "planned before 21:00 (a restart loses the injections).",
            f"2. 14:50-15:05, Piero: control `election_rehearsal` with `file=/opt/research/night/rehearsal_plan.json`, `start={float(s.get('start_epoch') or 0):.0f}` "
            f"({rp.START_PT} PT). RUN_LOG shows `INJECT REHEARSAL plan rehearsal_plan.json: {s.get('injections', '?')} injections scheduled from 15:00` "
            "and one INJECT line per book. The first injections start 15:08 (sent later, the first wave's due quotes arrive in one burst).",
            f"3. 15:02-15:07 (no injection running), Piero: control `election_probe` with `ex={pr.get('ex', '?')}`, `side={pr.get('side', 'yes')}` "
            f"({pr.get('name', '?')} YES: a {pr.get('qty', 10)}-share bid at the touch, 120 s; SIG bid then {pr.get('sig_bid_now')}, PM fair {pr.get('pm_fair_now')}). "
            "RUN_LOG: `PROBE ... SIG answered in N ms (accepted ...)`.",
            "4. Every injection starts 7 min before its close and its race pulls then (C14, once per race). Waves close (synthetic) at "
            + ", ".join(w["close_pt"][:5] for w in exp_waves(exp)) + f"; the last race is restored {s.get('last_restore_pt', '20:47')}.",
            "5. If the engine restarts mid-run, its injections are gone: `python3 research/night/rehearsal_plan.py --after HH:MM` writes the races "
            "not yet started to `/opt/research/night/rehearsal_plan_after_HHMM.json`; send that file with the same `start`.",
            f"6. After 21:00: `python3 research/night/rehearsal_eval.py --start \"{rp.START_PT}\"` -> this page.\n", LIMITS]
    if exp:
        out += plan_section(exp)
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=rp.START_PT, help="window start, PT 'YYYY-MM-DD HH:MM[:SS]' (default the rehearsal, %(default)s)")
    ap.add_argument("--hours", type=float, default=rp.WINDOW_H)
    ap.add_argument("--plan", default="auto", help="auto (server, then the Mac copy, used only if built for --start) | none | a path")
    ap.add_argument("--out", default=DOC, help="markdown output (default docs/night_rehearsal.md)")
    ap.add_argument("--local", action="store_true", help="read the Mac mirror instead of the server")
    ap.add_argument("--from-dir", help="read a local directory laid out like /opt/sig-engine (data/, docs/) instead of the server")
    ap.add_argument("--plan-json", help="the plan file itself (default the server's /opt/research/night/rehearsal_plan.json): the price check's fallback")
    ap.add_argument("--template", action="store_true", help="write the pre-run template to --out and stop")
    a = ap.parse_args()
    if a.template:
        txt = template(a)
    else:
        ev = Ev(a)
        ev.load()
        if ev.now < ev.start:
            sys.exit(f"the window starts {time.strftime('%Y-%m-%d %H:%M', time.localtime(ev.start))} PT, after the server's clock "
                     f"({time.strftime('%Y-%m-%d %H:%M', time.localtime(ev.now))}): nothing to evaluate yet (docs/night_rehearsal.md untouched; --template writes the pre-run page)")
        ev.run()
        if ev.now < ev.t1:
            ev.head = (f"**PARTIAL: the window runs until {time.strftime('%H:%M', time.localtime(ev.t1))} PT; evaluated at "
                       f"{time.strftime('%H:%M', time.localtime(ev.now))}. Re-run after it ends.**\n\n") + ev.head
        txt = ev.markdown()
    with open(a.out, "w") as fh:
        fh.write(txt + "\n")
    print(txt.split("\n## ")[0] if not a.template else txt)
    print(f"\nwritten {a.out}")


if __name__ == "__main__":
    main()
