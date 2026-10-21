"""Election-night simulator.

Default (docs/night_sim.md, night_reconcile §4 item 10): the rehearsal: the 116 races of data/election/poll_close.json (pulls at the
first close - 10 min, calls only after the last close), Polymarket quote streams replayed from the 2024 1-min tapes
(tests/fixtures/night24_paths.json.gz, mapped by pre-close level), a SIG book that lags PM, takers, and n_election.Election wired as
f_engine wires it, every order through the real j_executor.Executor on FakeSIG (batches <= 50, our 25 writes/min limiter, SIG's 30-s
minimum expiry). Scenarios: normal, lane, slow count, reversal night, thin PM books, 40 calls in 20 min + 234 pulls, kill switch,
SIG 5xx / 429 bursts, PM feed down 3 min, SIG-gap sensitivity, 100k start. Code: tests/night_world.py, night_rig.py, night_report.py.
    python3 -m tests.sim_election [--md out.md] [--only name,name] [--seed N] [-v]

--legacy: the day-5 simulator below (Oct 4: its own CLOSE_ET table, synthetic cliff calls, Election driven without the executor):
1 normal night · 2 forty calls in 20 min · 3 SIG 5xx bursts and 429s · 4 Polymarket feed down 3 min · 5 a wrong early call that
reverses.    python3 -m tests.sim_election --legacy [report.md]"""
import json
import os
import random
import re
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))
import n_election as el                                   # noqa: E402
from fake_sig import FakeSIG, VClock                      # noqa: E402

TITLE = re.compile(r"Will the (\w+) Party win the (.+?)\??$")
ABBR = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado", "CT": "Connecticut",
        "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
        "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan",
        "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
        "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
        "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"}
# first poll close, ET, Nov 3 2026 (UNVERIFIED: verify by hand before Oct 25)
CLOSE_ET = {18: ["Indiana", "Kentucky"], 19: ["Florida", "Georgia", "New Hampshire", "South Carolina", "Vermont", "Virginia"],
            19.5: ["North Carolina", "Ohio", "West Virginia"],
            20: ["Alabama", "Connecticut", "Delaware", "Illinois", "Kansas", "Maine", "Maryland", "Massachusetts", "Michigan", "Mississippi",
                 "Missouri", "New Jersey", "Oklahoma", "Pennsylvania", "Rhode Island", "Tennessee", "Texas"],
            20.5: ["Arkansas"],
            21: ["Arizona", "Colorado", "Louisiana", "Minnesota", "Nebraska", "New Mexico", "New York", "North Dakota", "South Dakota",
                 "Wisconsin", "Wyoming"],
            22: ["Iowa", "Idaho", "Montana", "Nevada", "Utah"], 23: ["California", "Oregon", "Washington"], 24: ["Hawaii", "Alaska"]}
T0 = 0.0                                                  # sim clock 0 = 17:00 ET Nov 3 (14:00 PT)
HOUR = 3600.0


def et(t):
    """sim seconds -> 'HH:MM ET'."""
    h = 17 + t / HOUR
    return f"{int(h) % 24:02d}:{int((h % 1) * 60):02d} ET"


def state_of(race):
    m = re.match(r"([A-Z]{2})-\d\d House race", race)
    if m:
        return ABBR.get(m.group(1))
    for suffix in (" Governor", " Senate"):
        if race.endswith(suffix):
            return race[: -len(suffix)]
    return None


def load_universe():
    with open(os.path.join(ROOT, "tests", "fixtures", "universe.json")) as fh:
        u = json.load(fh)
    meta, close = {}, {}
    for m in u["markets"]:
        mm = TITLE.match(m["title"])
        if not mm or not m.get("exchanges"):
            continue
        ex = str(m["exchanges"][0]["id"])
        race = mm.group(2)
        meta[ex] = dict(race=race, party=mm.group(1)[0], title=m["title"])
        st = state_of(race)
        for h, states in CLOSE_ET.items():
            if st in states:
                close[race] = (h - 17) * HOUR
    return meta, close


class Night:
    """One scripted night: truth (the winner per race), Polymarket paths, SIG books that lag the calls, taker flow."""

    def __init__(self, meta, close, seed, scenario):
        self.rng = random.Random(seed)
        self.meta, self.close, self.scenario = meta, close, scenario
        self.races = sorted({m["race"] for m in meta.values()})
        self.by_race = {r: [ex for ex, m in meta.items() if m["race"] == r] for r in self.races}
        self.winner, self.f0, self.call_t, self.pm, self.sig, self.depth = {}, {}, {}, {}, {}, {}
        for r in self.races:
            exs = self.by_race[r]
            w = self.rng.choice(exs)                      # the winning market of the race
            fav = self.rng.uniform(0.55, 0.95)
            self.winner[r] = w
            for ex in exs:
                self.f0[ex] = fav if ex == w else (1 - fav) / max(1, len(exs) - 1)
                self.pm[ex] = self.f0[ex]
                self.sig[ex] = self.f0[ex]
                self.depth[ex] = self.rng.randint(1_500, 8_000)
            c = close.get(r, 3 * HOUR)
            self.call_t[r] = c + self.rng.uniform(0, 6 * HOUR)
        if scenario in ("stress", "feed_drop"):           # 40 calls in 20 min (the feed drop sits inside the burst)
            burst = sorted(self.races, key=lambda r: self.call_t[r])[:40]
            t0 = 3 * HOUR
            for i, r in enumerate(burst):
                self.call_t[r] = t0 + i * 30.0
        self.called = {}                                  # race -> t (when PM called it)
        self.fake_calls = {}                              # scenario 5: race -> (t_wrong, t_reverse)
        if scenario == "reversal":
            for r in sorted(self.races, key=lambda r: self.call_t[r])[:5]:
                self.fake_calls[r] = (self.call_t[r] - 20 * 60, self.call_t[r] - 16 * 60)
        self.messages = []                                # (t, ex, pm) pushed to the sniper

    def pm_now(self, ex, t):
        r = self.meta[ex]["race"]
        w = ex == self.winner[r]
        if r in self.fake_calls:
            tw, tr = self.fake_calls[r]
            if tw <= t < tr:
                return 0.98 if w else 0.01                 # the wrong early call: looks like the real one
            if tr <= t < self.call_t[r]:
                return 0.70 if w else 0.15
        if t >= self.call_t[r]:
            return 0.99 if w else 0.005
        return self.f0[ex]

    def sig_book(self, ex, t):
        """SIG lags: after the call the winner's ask walks from f0 to 0.97 over 10-30 min; losers' bids fall likewise."""
        r = self.meta[ex]["race"]
        w = ex == self.winner[r]
        c = self.call_t[r]
        if t < c:
            m = self.f0[ex]
        else:
            lag = 600 + (hash(ex) % 1200)
            k = min(1.0, (t - c) / lag)
            m = self.f0[ex] + k * ((0.97 if w else 0.01) - self.f0[ex])
        spread = 0.01 if t < c else 0.02
        bid, ask = round(max(0.005, m - spread / 2), 3), round(min(0.995, m + spread / 2), 3)
        return bid, ask, self.depth[ex]


def run(scenario="normal", seed=1, kill_at=None, verbose=False, race_share=None):
    meta, close = load_universe()
    el.RACE_SHARE = race_share if race_share is not None else 0.25
    night = Night(meta, close, seed, scenario)
    clock = VClock(0.0)
    fake = FakeSIG(list(meta), {}, {}, seed=seed, clock=clock, writes_per_min=30)
    fake.processing = (0.05, 0.3)
    cash = [100_000.0]
    pos = fake.pos
    writes = []                                           # sim times of our writes
    fails = [0]
    layers = {}                                           # order id -> layer

    def book(ex):
        return night.sig_book(ex, clock.time())

    def place(items, layer, ttl_s):
        try:
            res = fake.place_batch(items, ttl_s=ttl_s)
        except (fake.RateLimited, fake.Unknown, RuntimeError):
            fails[0] += 1
            return None
        writes.append(clock.time())
        for _, ok, r in res:
            if ok:
                layers[str(r["orderId"])] = layer
        return "k%d" % len(writes)

    def cancel(o, reason=""):
        try:
            fake.cancel(o["id"])
        except Exception:
            fails[0] += 1
            return False
        writes.append(clock.time())
        return True

    log = (lambda k, t: print(f"{et(clock.time())} {k:9} {t}")) if verbose else (lambda k, t: None)
    E = el.Election(meta, close, clock.time, place, cancel, book, log)

    def open_orders():
        return [dict(id=str(i), **o) for i, o in fake.book.items()]

    def layer_of(o):
        return layers.get(o["id"])

    def free_cash():
        return cash[0]

    def settle_fills(t):
        """Our resting buys fill against SIG's book: a buy at >= the ask fills for the visible depth; post-call bids get
        hit by takers (30% a minute, 100-1,000 shares) after the call."""
        for oid, o in list(fake.book.items()):
            bid, ask, depth = night.sig_book(o["ex"], t)
            side_ask = ask if o["side"] == "yes" else round(1 - bid, 3)
            r = meta[o["ex"]]["race"]
            q = 0
            if o["price"] >= side_ask - 1e-9:
                q = min(o["qty"], depth)
            elif t >= night.call_t[r] and layers.get(str(oid)) == "postcall" and night.rng.random() < 0.3 / 6:
                q = min(o["qty"], night.rng.randint(100, 1000))
            if q >= 1:
                px = min(o["price"], side_ask) if o["price"] >= side_ask - 1e-9 else o["price"]   # a crossing buy fills at the ask
                before = pos.get(o["ex"], 0)
                fake.fill(oid, q, price=px)
                cash[0] -= q * px
                after = pos.get(o["ex"], 0)
                if abs(after) < abs(before):               # SIG nets YES against NO: every netted pair pays 1 now
                    cash[0] += min(q, abs(before) - max(0, abs(after)) if before * after >= 0 else abs(before)) * 1.0
            elif o.get("expires") and t > o["expires"]:
                del fake.book[oid]

    kill_t = None
    end = max(night.call_t.values()) + 40 * 60
    t = 0.0
    feed_down = (3 * HOUR + 5 * 60, 3 * HOUR + 8 * 60) if scenario == "feed_drop" else None
    next_tick, step_i = 0.0, 0
    while t < end:
        t = clock.time()
        dt = 1.0 if any(abs(t - c) < 45 * 60 for c in night.call_t.values()) else 10.0
        # Polymarket messages: every market whose PM changed, two messages 1.5 s apart around a call
        if not (feed_down and feed_down[0] <= t < feed_down[1]):
            for ex in meta:
                p = night.pm_now(ex, t)
                if abs(p - night.pm.get(ex, -1)) > 1e-9 or (step_i % 5 == 0 and E.calls.get(ex) is None and
                                                             (el.side_price(p, "yes") >= el.CALL_FOR or el.side_price(p, "no") >= el.CALL_FOR)):
                    night.pm[ex] = p
                    E.on_pm(ex, p, t, free_cash(), pos)
        if scenario == "faults" and step_i % 40 == 0:
            fake.faults += ["503", ("429", 5), "503"]
        if t >= next_tick:
            E.on_tick(t, free_cash(), pos, open_orders(), layer_of)
            next_tick = t + 10.0
        settle_fills(t)
        if kill_at is not None and t >= kill_at and kill_t is None:
            w0 = len(writes)
            n = E.kill(t, open_orders(), pos, layer_of)
            kill_t = (clock.time() - t, n, len(writes) - w0)
        clock.advance(dt)
        step_i += 1
    # settlement: the winner pays 1
    value = 0.0
    for ex, q in pos.items():
        r = meta[ex]["race"]
        pay = 1.0 if ex == night.winner[r] else 0.0
        value += q * pay if q > 0 else -q * (1 - pay)
    # the perfect-information bound: at every call, buy the winner at SIG's ask of that instant for the visible depth
    # with the same per-race (25%) and floor limits, cash compounding in call order
    bound, c_ = 0.0, 100_000.0
    for r in sorted(night.races, key=lambda r: night.call_t[r]):
        w = night.winner[r]
        bid, ask, depth = night.sig_book(w, night.call_t[r])
        budget = min(0.25 * c_, c_ - 10_000)
        q = int(min(depth, budget / ask)) if ask <= el.MAX_BUY else 0
        bound += q * (1 - ask)
        c_ -= q * ask
    per_min = {}
    for w in writes:
        per_min[int(w // 60)] = per_min.get(int(w // 60), 0) + 1
    lat = E.latency()
    out = dict(scenario=scenario, seed=seed, races=len(night.races), calls_detected=len(E.calls),
               reversed=sum(1 for c in E.calls.values() if c.get("reversed")),
               sniper_buys=sum(c["bought"] for c in E.calls.values()), sniper_cost=round(sum(c["cost"] for c in E.calls.values())),
               pnl=round(cash[0] + value - 100_000.0), bound=round(bound), latency=lat,
               writes=len(writes), writes_per_min_max=max(per_min.values()) if per_min else 0,
               minutes_over_30=sum(1 for v in per_min.values() if v > 30), write_failures=fails[0],
               cash_min=round(min(c for _, c in E.cash_path)) if E.cash_path else None, cash_end=round(cash[0]),
               pulled=len(E.pulled), kill=kill_t,
               events=[(et(t_), k, x) for t_, k, x in E.events[:8]],
               _pos=dict(pos), _winner=dict(night.winner), _meta=meta, _calls=E.calls, _night=night, _E=E)
    return out


SCENARIOS = [("normal", {}), ("stress", {}), ("faults", {}), ("feed_drop", {}), ("reversal", {}), ("stress", dict(kill_at=3 * HOUR + 40 * 60, race_share=0.05))]


def legacy_main(path=None):
    rows = []
    for name, kw in SCENARIOS:
        r = run(name, seed=7, **kw)
        rows.append(r)
        print(json.dumps({k: v for k, v in r.items() if k != "events" and not k.startswith("_")}))
    if path:
        L = ["# Election-night simulator report (day 5 §5)", "",
             f"Written by Claude Code, {time.strftime('%Y-%m-%d %H:%M')} PT, from `tests/sim_election.py` on the fake exchange: "
             "the 117 races of the tournament universe, first poll-close times by state (ET, **UNVERIFIED** until the Oct 25 rehearsal), "
             "calls 0-6 h after the close as Polymarket jumps (two messages >= 1 s apart past 0.97), SIG lagging the call (the winner's ask walks "
             "to 0.97 over 10-30 min), takers hitting our post-call bids. The module `n_election.py` is driven directly (its fast path "
             "`on_pm` from the feed, `on_tick` every 10 s); the live engine, its executor lanes and the real websocket are not in the loop, "
             "so the latencies here are the module's own (the confirm delay), not the end-to-end number. **Nothing from this is deployed.**", "",
             "| scenario | calls | sniper buys (sh) | cost | P&L at settlement | perfect-information bound | PM→order p50 / p95 s | writes | max writes/min | min over 30 | write failures | min free cash | poll-close pulls | kill switch |",
             "|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|"]
        for r in rows:
            k = r["kill"]
            L.append(f"| {r['scenario']}{' + kill at ' + et(3 * HOUR + 40 * 60) + ' (5% per race)' if k else ''} | {r['calls_detected']} ({r['reversed']} reversed) | {r['sniper_buys']:,.0f} | {r['sniper_cost']:,} | "
                     f"**{r['pnl']:+,}** | {r['bound']:+,} | {r['latency']['p50']} / {r['latency']['p95']} (n {r['latency']['n']}) | {r['writes']} | "
                     f"{r['writes_per_min_max']} | {r['minutes_over_30']} | {r['write_failures']} | {r['cash_min']:,} | {r['pulled']} | "
                     f"{'' if not k else f'{k[0]:.1f} s, {k[1]} orders pulled, {k[2]} writes'} |")
        L += ["", "## What the runs say",
              "- **Normal night:** every call is detected from the second confirming message (1.5 s in the script); the sniper buys the winner at SIG's ask while it is <= 0.95, then rests 0.96-0.99 bids. P&L vs the bound shows what the 25%-per-race, floor + 5k and visible-depth limits leave on the table.",
              "- **Stress (40 calls in 20 min):** the write budget is the binding limit: one 20-s order per call per re-plan; the table's max writes/min and minutes over 30 say whether the 30/min limit was hit (writes refused by the fake count as failures; 3 failures stop a race).",
              "- **5xx / 429 bursts:** refused writes are retried on the next message; a race stops after 3 failures (ELECTION_NIGHT §2 guard). Compare calls detected vs sniper buys with the normal night.",
              "- **Feed down 3 min** (inside the 40-call burst): calls in the gap are detected when the feed returns; those whose SIG ask already passed 0.95 are missed (compare sniper buys with the stress run).",
              "- **Wrong early call that reverses:** the reversal guard sells back at market (max 3c loss) and stops the race; the real call later is still taken by a fresh call record only if the first one was not marked reversed — by design it is not (one shot per race), which the rehearsal must decide on.",
              "- **Kill switch:** `election_stop` pulls every entry / sniper / post-call order with one cancel each; the time is the cancel count / the write limit.",
              "- **Cash:** with 25% of free cash per race, compounding, the sniper spends the account on the first ~10 calls of the night (the min-free-cash column sits at the floor + 5k in every run) and nothing is left for the later calls, the post-call bids or settlement morning. The committed-cash guard (orders out but unfilled count against free cash) keeps the floor; the sizing rule itself is ELECTION_NIGHT §7 question 2 and should become something like min(25%, free cash / races still to call).",
              "", "## Open before Oct 25",
              "1. Verify the poll-close table by hand (it is typed from memory).", "2. End-to-end latency through the engine's executor lane (fast path `front=True`): target <= 1 s.",
              "3. Size: 25% of free cash per race or per state (ELECTION_NIGHT §7).", "4. Whether a reversed race may be called again later."]
        with open(path, "w") as fh:
            fh.write("\n".join(L) + "\n")
        print("wrote", path)
    return rows


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--legacy" in argv:
        argv.remove("--legacy")
        return legacy_main(argv[0] if argv else None)
    import night_report
    return night_report.main(argv)


if __name__ == "__main__":
    main()
