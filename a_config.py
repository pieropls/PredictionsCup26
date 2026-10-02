"""A. CONFIG: every setting of the pipeline.

Files, in the order data flows through them:
  a_config.py      settings (this file)
  b_sig.py         SIG Super Market API: reads, batch + atomic multi-leg orders, write budget
  c_polymarket.py  Polymarket fair prices for every SIG market
  d_ledger.py      record of every order, fill and arb basket (data/ledger.json)
  e_bots.py        the bots: PositionBot (manual positions), MarketBot (one per market), PairBot (one per race)
  f_engine.py      THE LOOP. Run: python3 -u f_engine.py --live      (dry run without --live)
  g_dashboard.py   writes data/dashboard.html every loop
  h_status.py      terminal summary: python3 h_status.py
  data/            everything the engine writes;  tests/  offline tests: python3 -m unittest discover tests
"""
import os

# A1. connection --------------------------------------------------------------------------------------------------
BASE_URL = "https://sig.thesuper.market/api/v1"
def _api_key():
    """SIG_API_KEY from the environment (the VM), else ~/.config/sig/api_key (chmod 600). Never stored in the code."""
    if os.environ.get("SIG_API_KEY"):
        return os.environ["SIG_API_KEY"].strip()
    path = os.path.expanduser("~/.config/sig/api_key")
    return open(path).read().strip() if os.path.exists(path) else ""


API_KEY = _api_key()                                        # My Profile -> API Keys (read + trade)
TOURNAMENT = "midterm-elections"
DRY_RUN = True                  # f_engine.py --live switches this off
ENGINE_HOST = os.environ.get("SIG_ENGINE_HOST", "MacBookPino")   # the ONE machine allowed to run --live, compared
                                # with `scutil --get LocalHostName` (or data/host_id), never the Wi-Fi-derived hostname

# A2. engine loop and SIG limits (100 reads + 30 writes / min per account; SIG processes orders slowly under load) ----
LOOP_S = 15
WRITES_PER_MIN = 25
BATCH_SIZE = 20                 # orders per batch write
MAX_PLACE_PER_LOOP = 60         # new orders per loop
SETTLE_S = 90                   # after a write with an unknown outcome: nothing new on those markets for this long
PM_MAX_AGE_S = 90               # Polymarket data older than this: no new market-bot orders
TICK, MIN_QTY, QTY_TOL = 0.005, 10, 0.25
READ_TIMEOUT_S = 45             # SIG's authenticated reads take 18-34 s under load (Oct 1 night); 15 s was too short
INITIAL_BALANCE = 100_000

# A3. account risk (checked before every order: filled + resting + this loop's new orders) -------------------------
RISK_MAX_EXPOSURE = 90_000      # SIG of exposure across market + pair bots (at Polymarket fair, at cost without one)
CASH_RESERVE = 5_000            # night plan §4.2 hard floor: free cash after every resting engine entry buy
MARKET_MAX_POS = 3_000          # SIG per market, per side

# A4. position bots: manual positions, worked out with resting limit exits -------------------------------------------
# exits: (price in the HELD side's terms, fraction of the position), filled top-down so a sweep takes the cheapest
# first; exits below the Polymarket fair are held back.  adds: (price, SIG notional) far bids that add on spikes.
POSITION_BOTS = {
    "AK-GOV-D": dict(market=368, exits=[(0.30, .15), (0.40, .20), (0.50, .20), (0.60, .20), (0.775, .25)], adds=[(0.10, 300)]),
    "RI-GOV-D": dict(market=384, exits=[(0.97, .50), (0.98, .50)], adds=[]),
}
POSITION_MAX_ADD = 1.5
MANUAL_MARKETS = []             # SIG market ids no bot may touch

# A5. market bots: one per market, every price checked against Polymarket AND the SIG book --------------------------
MB_PM_MAX_SPREAD = 0.04         # no Polymarket reference when its own spread is wider
MB_EXIT_EDGE = 0.01             # inventory offered back at max(PM fair + this, SIG best price - 1 tick); never > held
MB_TOUCH_PM_EDGE = 0.015        # touch: join/improve the SIG best bid, only below SIG mid AND below PM fair - this
MB_TOUCH_NOTIONAL = 100
MB_TAKE_EDGE = 0.04             # take: SIG has gapped this far through PM fair -> buy the gap (marketable limit)
MB_TAKE_NOTIONAL = 150
MB_LADDER = [(0.10, 150), (0.20, 250)]   # sweep catchers: resting at PM fair -/+ distance (SIG notional)
MB_FOCUS = 40                   # markets that get touch + ladder (most room in the SIG book); every market gets exit/take
MB_REFILL_S = 300               # after a fill, that side waits this long before adding again
MB_QUOTE_TTL_S = 900            # market-bot orders expire on SIG after this long (backstop if the engine stalls)
MB_MIN_PX, MB_MAX_PX = 0.01, 0.99

# Polymarket lists these races only by candidate: SIG (race, party) -> PM market ids summed
PM_EXTRA = {
    ("Alaska Governor", "D"): ["635016", "635014"],                              # Kreiss-Tomkins, T. Begich
    ("Alaska Governor", "R"): ["635012", "635004", "635011", "635003", "635006"],  # Wilson, Bronson, Taylor, Bishop, Dahlstrom
    ("Alaska Senate", "D"): ["634976"],                                          # Peltola
    ("Alaska Senate", "R"): ["634974"],                                          # Sullivan
    ("Nebraska Senate", "I"): ["634893"],                                        # Osborn
}

# A5b. strategy layers v2 (night plan §4-5; plan_review A2-A6, A9) -----------------------------------------------------
ROC_MIN, ROC_REF = 0.04, 0.08   # D4: entries need edge/price >= 4%; size scales with roc / 8%, clipped 0.5x..2x
RECYCLE_BID_EDGE = 0.005        # recycler: sell into the SIG bid while it is >= fair_side + this
RECYCLE_REST_EDGE = 0.01        # ... else rest an exit at max(fair_side + this, best ask - 1 tick)
RECYCLE_TAKE_TTL_S, RECYCLE_REST_TTL_S = 300, 1800
MM_EDGE, MM_U_MULT, MM_NOTIONAL, MM_TTL_S = 0.02, 1.5, 100, 1800     # e(p) = max(2c, 1.5u) * s(p); base SIG per quote
LADDER_LEVELS = [(0.10, 150), (0.20, 250)]   # (base offset x s(p), base SIG notional)
LADDER_TTL_S = 4 * 3600
LADDER_CAPACITY_SHARE = 0.4     # ladder placements use at most this share of SIG's measured processing capacity
TAKE_EDGE, TAKE_U_MULT, TAKE_NOTIONAL, TAKE_TTL_S = 0.04, 1.5, 150, 300   # take gaps >= max(4c*s(p), 1.5u)
FAIR_HALF_LIFE_S, FAIR_JUMP, FAIR_CONFIRM_S = 20.0, 0.015, 5.0       # A6: EWMA mid; a >= 1.5c move must persist 5 s
PM_MIN_LIQUIDITY = 2_000        # A6: no fair when Polymarket liquidity is below this
REF_WINDOW_S, REF_MIN_SAMPLES, REF_MAX_SPREAD = 1800, 6, 0.10       # ladder anchor: 30-min median SIG mid
BREAKER_WINDOW_S, BREAKER_MARKETS, BREAKER_SHARE, BREAKER_PAUSE_S = 600, 5, 0.8, 1800   # §4.5 correlated-fill breaker
MM_PILOT_MARKETS, LADDER_PILOT_MARKETS, PILOT_S = 10, 40, 2 * 3600
REQUOTE_CANCELS_PER_MIN = 20    # A9: MM/ladder requote cancels per minute (one per write); picked-off quotes always go

# A6. pair bots: one per race. NO on every party pays >= legs-1; YES on every party pays 1 unless an outsider wins ---
PAIR_ENABLED = True
PAIR_EDGE = 0.02                # locked per set at the margin (A12: >= 2c)
PAIR_THIRD_PARTY = 0.01         # YES sets price the outsider at max(this, 1 - sum of Polymarket fairs)
PAIR_MAX_QTY = 3_000            # shares per leg per shot
PAIR_MAX_SHOTS = 3              # sets sent per loop (each costs ~30 s of SIG time: book reads + an atomic write)
PAIR_MIN_LOCK = 3.0             # skip sets locking less than this many SIG (qty x margin): not worth the loop time
PAIR_TTL_S = 120                # unfilled leg remainders expire; must exceed SIG's processing delay (§9.3/9.5: up to ~40 s)
PAIR_COOLDOWN_S = 30            # per race

# A6b. night plan (docs/night_plan.md): capital buckets per layer, (pilot, full, max) in SIG. The dashboard can set a
# layer's cap anywhere in 0..max; without an override the cap follows the layer's state (pilot / full) ----------------
LAYER_CAPS = {
    "mm": {"inventory": (3_000, 15_000, 25_000)},              # new inventory at cost, beyond what's held now
    "ladder": {"filled": (3_000, 15_000, 25_000), "resting": (10_000, 60_000, 80_000)},
    "pairs": {"capital": (3_000, 10_000, 15_000)},
    "sniper": {"per_market": (1_000, 3_000, 3_000), "per_day": (10_000, 10_000, 10_000)},
}
KAPPA_FROM_OCT30 = 0.03         # D3: capital discount from Oct 30 (election-night cash)
LOSS_STOP = 2_000               # D5: bot-owned P&L at fair, rolling 24 h

# A7. files ---------------------------------------------------------------------------------------------------------
DATA = os.environ.get("SIG_DATA") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")   # releases share it
PM_MAP_FILE = os.path.join(DATA, "pm_map.json")
POSITION_REF_FILE = os.path.join(DATA, "position_ref.json")
LEDGER_FILE = os.path.join(DATA, "ledger.json")
PNL_FILE = os.path.join(DATA, "pnl.csv")
PNL_EVERY_S = 120
STATE_FILE = os.path.join(DATA, "state.json")
DASHBOARD_FILE = os.path.join(DATA, "dashboard.html")
PID_FILE = os.path.join(DATA, "engine.pid")
LOCK_FILE = os.path.join(DATA, "engine.lock")
OVERRIDES_FILE = os.path.join(DATA, "overrides.json")     # layer flags and controls (survive restarts)
DASHBOARD_URL_FILE = os.path.join(DATA, "dashboard_url.txt")   # the live dashboard's URL with its access token
STATE_EVERY_S = 60              # data/state.json and the static data/dashboard.html: rewritten at most this often
HOST_ID_FILE = os.path.join(DATA, "host_id")
NIGHT_LOG_FILE = os.path.join(os.path.dirname(DATA), "docs", "NIGHT_LOG.md")
