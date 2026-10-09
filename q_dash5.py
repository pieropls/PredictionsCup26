"""Q. DASHBOARD v5 PAYLOAD ("Piero's Intel", docs/dashboard_v5.md): Overview · Live · Markets · Admin.

Dashboard code only: reads the engine's own state and data files, never trades and never calls SIG (the fills archive is
filled by tools/fills_backfill.py once and by the engine's own fills reads from then on).

  P&L (§0)      one SIG day = 00:00 UTC to 00:00 UTC (16:00 PT after Nov 1). Day P&L on the board = account value at the end
                minus at the start (pnl.csv samples nearest the boundaries; Oct 1 starts at 100,000); realized = that day's
                position-reducing fills at SIG's average cost per exchange; open positions = day P&L - realized (adds up by
                construction). At fair the same with pnl.csv account_pm.
  Strategies    one registry (REGISTRY) maps engine layers and sub-books to strategies. A bucket's P&L since inception =
  (§3.3)        the cash flows of its fills (from the complete archive, netted per exchange like SIG) + its current shares at
                SIG's marks; the sum over buckets is account - 100,000 by construction. Today = now - the 00:00 UTC snapshot.
"""
import calendar
import collections
import json
import os
import re
import shutil
import socket
import time

DATA = os.environ.get("SIG_DATA", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
ROOT = os.path.dirname(os.path.abspath(__file__))
INITIAL = 100_000.0
SETTLE_UTC = calendar.timegm((2026, 11, 4, 17, 0, 0))          # every market settles Nov 4 09:00 PT
CUP_START_UTC = calendar.timegm((2026, 10, 1, 0, 0, 0))         # SIG day 1 = Oct 1 (UTC)
FLOOR = 5_000.0

# ------------------------------------------------------------------------------------------------ the strategy registry (§3.2)
# One place: which engine layers and sub-books each strategy covers, its switch, its cap. Redefining strategies = editing this.
REGISTRY = [
    # Market making
    dict(key="tm", name="Touch maker", group5="Market making", layers=["tm", "tmexit"], subs=["tm:"], control=["tm"],
         rules="Joins the best bid on the side that is >= 2c cheap vs fair (4c under BUFFER) on every market-side with that edge. "
               "Exits at the touch ask, never below entry + 0.5c; steps aside in spread-maker and reservoir markets."),
    dict(key="sm", name="Spread maker", group5="Market making", gated=True, layers=["sm", "smexit"], subs=["sm:"], control=["sm"],
         rules="Quotes both sides at the touch on the bounciest markets, keeps its queue place, refills on fill; regime-gated (new quotes only in BOUNCE markets). "
               "Polymarket fair sets the skew: the cheap side may run to the cap, the rich side holds one clip."),
    # Sweep catching
    dict(key="catcher", name="Catcher", group5="Sweep catching", layers=["dump", "dumpexit"], subs=["dump:"], control=["dump"],
         rules="Rests bids under the SIG touch on favourites and catches sweeps far below Polymarket fair (L0 at the touch, L1-L3 deeper); aged catch lots stay its own. "
               "Each catch is sold back on a ladder from fill + 1c, never below entry + 0.5c; lots >= 8c under fair are held while cash is idle."),
    dict(key="sniper", name="Sniper, fat-finger", group5="Sweep catching", layers=["sniper", "fatfinger"], subs=["sniper", "fatfinger", "fat:"], control=["sniper", "fatfinger"],
         rules="The new-market sniper takes asks far below fair in the first minutes of a market; fat-finger bids rest at the extremes (0.01-0.15 / 0.85-0.99) for mistakes. Small and occasional."),
    # Cash management
    dict(key="rotation", name="Rotation", group5="Cash management", layers=["release", "recycle"], subs=["recycle:"], control=[],
         rules="While free cash is under BUFFER, holdings with < 8% return left to settlement rest asks at the touch, lowest return first. "
               "Never below entry + 0.5c, never a lot >= 8c under fair, never manual, wall, sleeve or pair-set lots."),
    dict(key="pairs", name="Pairs", group5="Cash management", layers=["pairs", "unwind", "repair"], subs=["pairset:"], control=["pairs"],
         rules="Atomic NO + YES sets bought under 1 and unwound early when the pair can be sold above cost."),
]
APART = [
    dict(key="ri", name="RI Senate R", layers=["swing", "swingsell"], subs=["swing:"], control=["swing"], status="Unwinding",
         rules="Piero's partial unwind: 11,900 NO held, the rest offered at the position average (0.840) as passive asks. "
               "RI is bought only by the catcher on pumps; catcher lots there cycle at entry + 2c."),
    dict(key="walls", name="Walls", layers=["wall", "wallexit"], subs=["wall:"], control=["wall"], status="Held",
         rules="Piero's manual walls (the NM Senate R walls): held to settlement, buy-backs only under his rules."),
    dict(key="held", name="Day-1 and manual holdings", layers=["exit", "ladder", "rebound", "position", "sleeve"], subs=["inventory", "manual", "sleeve:"],
         control=[], status="Held", rules="Inventory, manual positions, settlement holds and fills from before the engine. Held to settlement."),
]
RETIRED = [
    dict(key="fm", name="Fair maker", layers=["fm", "fmexit"], subs=["fm:"]),
    dict(key="scalp", name="Scalper v1", layers=["scalp", "scalpexit"], subs=["scalp:"]),
    dict(key="mm", name="Old maker", layers=["mm"], subs=["mm"]),
    dict(key="take", name="Takes", layers=["take"], subs=[]),
    dict(key="touch", name="Touch (v1)", layers=["touch", "touchexit"], subs=["touch:"]),
    dict(key="set", name="Pair-set entries", layers=["set"], subs=[]),
    dict(key="longshot", name="Longshot", layers=["longshot", "longexit"], subs=["longshot:"]),
]
ALL_BUCKETS = REGISTRY + APART + RETIRED
RI_EX = {"1077"}


def bucket_of_layer(layer, sub="", ex=""):
    """The registry key of an order (by its layer, then its sub-book); RI walls go to RI; unknown -> held."""
    layer, sub, ex = str(layer or ""), str(sub or ""), str(ex or "")
    if layer in ("wall", "wallexit") or sub.startswith("wall:"):
        return "ri" if ex in RI_EX or sub.startswith("wall:1077") else "walls"
    for b in ALL_BUCKETS:
        if layer and layer in b["layers"]:
            return b["key"]
    return bucket_of_sub(sub, ex)


def bucket_of_sub(sub, ex=""):
    sub, ex = str(sub or ""), str(ex or "")
    if sub.startswith("wall:"):
        return "ri" if ex in RI_EX or sub.startswith("wall:1077") else "walls"
    for b in ALL_BUCKETS:
        if any(sub == p or sub.startswith(p) for p in b["subs"] if p):
            return b["key"]
    return "held"


def name_of(key):
    return next((b["name"] for b in ALL_BUCKETS if b["key"] == key), key)


# ------------------------------------------------------------------------------------------------ small helpers
def _read_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return default


def _write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh)
    os.replace(tmp, path)


def utc_day(ts):
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def day_start(day):
    y, m, d = map(int, day.split("-"))
    return calendar.timegm((y, m, d, 0, 0, 0))


def local_ts(s):
    """'2026-10-01 16:23:09' in the machine's local time (PT on the server and the Mac) -> epoch."""
    return time.mktime(time.strptime(s[:19], "%Y-%m-%d %H:%M:%S"))


def iso_ts(s):
    from datetime import datetime
    return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()


_cache = {}


def cached(key, every_s, fn):
    c = _cache.get(key)
    now = time.time()
    if c is None or now - c[0] >= every_s:
        try:
            v = fn()
        except Exception as err:                            # never break the payload: the section says why
            v = dict(error=f"{key}: {err!r}")
        _cache[key] = (now, v)
        return v
    return c[1]


# ------------------------------------------------------------------------------------------------ the fills archive (§0)
ARCHIVE = "fills_archive.jsonl"
_arch = dict(keys=None)


def archive_path():
    return os.path.join(DATA, ARCHIVE)


def archive_append(fills, orders=None):
    """Append the engine's fresh fills reads (raw SIG rows) to the archive, deduplicated by id:orderId, with the order's direction
    (buy / sell) from the engine's records when it knows it (the feed has none). Never raises."""
    try:
        if not fills:
            return 0
        if _arch["keys"] is None:
            ks = set()
            if os.path.exists(archive_path()):
                with open(archive_path()) as fh:
                    for l in fh:
                        try:
                            ks.add(json.loads(l)["key"])
                        except Exception:
                            pass
            _arch["keys"] = ks
        new = []
        for f in fills:
            if not isinstance(f, dict) or "id" not in f or "filledAt" not in f:
                continue
            k = f"{f['id']}:{f.get('orderId')}"
            if k in _arch["keys"]:
                continue
            _arch["keys"].add(k)
            act = ((orders or {}).get(str(f.get("orderId"))) or {}).get("action")
            new.append(dict(f, key=k, **({"action": act} if act in ("buy", "sell") else {})))
        if new:
            new.sort(key=lambda f: f["filledAt"])
            with open(archive_path(), "a") as fh:
                for f in new:
                    fh.write(json.dumps(f) + "\n")
        return len(new)
    except Exception:
        return 0


def load_archive():
    """Every archived fill once (the engine's appends and a backfill run can both write a fill: deduplicated by id:orderId; a
    torn line is skipped)."""
    rows, seen = [], set()
    if os.path.exists(archive_path()):
        with open(archive_path()) as fh:
            for l in fh:
                try:
                    f = json.loads(l)
                    k = f.get("key") or f"{f['id']}:{f.get('orderId')}"
                    if k in seen:
                        continue
                    seen.add(k)
                    f["_ts"] = iso_ts(f["filledAt"])
                    rows.append(f)
                except Exception:
                    pass
    rows.sort(key=lambda f: (f["_ts"], str(f.get("id"))))
    return rows


TX_ARCHIVE = "tx_archive.jsonl"


def load_tx():
    """data/tx_archive.jsonl (GET /portfolio/transactions): one row per trade event of ours, at its average price, with BUY / SELL,
    plus the non-trade cash events. The fills feed has no direction; the transactions do, and they reproduce SIG's cash exactly."""
    rows, path, seen = [], os.path.join(DATA, TX_ARCHIVE), set()
    if os.path.exists(path):
        with open(path) as fh:
            for l in fh:
                try:
                    t = json.loads(l)
                    if t.get("event_id") in seen:
                        continue
                    seen.add(t.get("event_id"))
                    t["_ts"] = iso_ts(t["createdAt"])
                    rows.append(t)
                except Exception:
                    pass
    rows.sort(key=lambda t: (t["_ts"], str(t.get("event_id"))))
    return rows


def events(fills, tx, order_bucket=None, sell_orders=(), order_owner=None, fill_fair=None):
    """The replay's input: [(ts, ex, side, qty, price, bucket, sell)].
    Up to the last transaction, every trade transaction is one event per order: its fills (same exchange, same millisecond) give the
    order ids (-> the strategy) and the prices, the transaction gives BUY / SELL; a transaction with no fills on file books to 'held'
    at its own price. After the last transaction the fills alone, with SELL from the engine's own order records."""
    order_bucket, sell_orders, order_owner, fill_fair = order_bucket or {}, set(sell_orders or ()), order_owner or {}, fill_fair or {}
    out, info = [], collections.Counter()
    trades = [t for t in tx if t.get("event_type") == "trade"]
    tx_end = trades[-1]["_ts"] if trades else None
    idx = collections.defaultdict(list)
    for f in fills:
        idx[(str(f.get("exchangeId")), str(f.get("filledAt") or "")[:23])].append(f)
    for t in trades:
        ex, q = str(t.get("exchangeId")), float(t.get("quantity") or 0)
        side, sell, tq = ("yes" if q > 0 else "no"), str(t.get("orderType") or "").upper() == "SELL", abs(q)
        got = idx.get((ex, str(t.get("createdAt") or "")[:23]), [])
        if got and abs(sum(abs(float(f.get("quantity") or 0)) for f in got) - tq) <= 0.5:
            by = collections.defaultdict(lambda: [0.0, 0.0, None])
            for f in got:
                o = by[str(f.get("orderId"))]
                o[0] += abs(float(f.get("quantity") or 0)); o[1] += abs(float(f.get("quantity") or 0)) * float(f.get("price") or 0)
                o[2] = fill_fair.get(f"{f.get('id')}:{f.get('orderId')}", o[2])
            for oid, (qq, cc, fr) in by.items():
                if qq > 0:
                    out.append((t["_ts"], ex, side, qq, cc / qq, order_bucket.get(oid, "held"), sell, order_owner.get(oid), fr))
            info["matched"] += 1
        else:
            out.append((t["_ts"], ex, side, tq, float(t.get("price") or 0), "held", sell, None, None))
            info["unmatched"] += 1
        info["sells"] += sell
    for f in fills:
        ts = f.get("_ts") or iso_ts(f["filledAt"])
        if tx_end is not None and ts <= tx_end:
            continue
        q = abs(float(f.get("quantity") or 0))
        sell = str(f.get("action") or "") == "sell" or str(f.get("orderId")) in sell_orders
        oid = str(f.get("orderId"))
        out.append((ts, str(f.get("exchangeId")), str(f.get("side", "")).lower(), q, float(f.get("price") or 0), order_bucket.get(oid, "held"), sell,
                    order_owner.get(oid), fill_fair.get(f"{f.get('id')}:{oid}")))
        info["after_tx"] += 1
        info["sells"] += sell
    out.sort(key=lambda e: e[0])
    cash_ev = [(t["_ts"], float(t.get("amount") or 0)) for t in tx if t.get("event_type") != "trade" and t.get("amount") is not None
               and str(t.get("transactionType") or "") != "OPENING_BALANCE"]
    return out, cash_ev, dict(info, tx_end=tx_end)


ROTATION = "rotation"


def replay(evs, cash_ev=()):
    """One pass, netted per exchange like SIG (a YES buy against a NO position pairs off at 1; a SELL of a side at p is the same as
    buying the other side at 1 - p), with the shares held split into lots per strategy:
      - a buy that adds to the position is the buying strategy's lot, and it pays the cash;
      - a buy that reduces it sells lots: the order's owner first (its sub-book), then the acting strategy's, then 'held', then the
        largest; the owners get the proceeds, so every strategy's P&L = its cash + the value of its lots;
      - rotation sells other strategies' lots: the owners get the lots' value at Polymarket fair at fill time and rotation keeps the
        difference (the price it gave up against fair; 0 where the fill's fair is not on record).
    -> dict(realized_day, cash (from 0), cash_b, lots {ex: {bucket: shares}}, pos (signed YES shares), avg)."""
    pos, avg = collections.defaultdict(float), collections.defaultdict(float)   # signed YES shares; average cost of the held side
    lots = collections.defaultdict(lambda: collections.defaultdict(float))
    realized_day = collections.defaultdict(float)
    cash, cash_b = 0.0, collections.defaultdict(float)
    for ev in evs:
        ts, ex, side, q, p, b, sell = ev[:7]
        owner, fair = (ev[7], ev[8]) if len(ev) > 8 else (None, None)
        if q <= 0:
            continue
        if sell:
            side, p = ("no" if side == "yes" else "yes"), 1 - p
            fair = None if fair is None else 1 - fair
        d = q if side == "yes" else -q
        P = pos[ex]
        L = lots[ex]
        if P == 0 or (P > 0) == (d > 0):
            avg[ex] = (abs(P) * avg[ex] + q * p) / (abs(P) + q)
            pos[ex] = P + d
            L[b] += q
            cash += -p * q
            cash_b[b] += -p * q
            continue
        r = min(q, abs(P))
        realized_day[utc_day(ts)] += r * ((1 - p) - avg[ex])
        cash += (1 - p) * r
        need, order = r, [x for x in (owner, b, "held") if x] + sorted(L, key=lambda k: -L[k])
        for k in order:
            if need <= 1e-9:
                break
            take = min(need, L.get(k, 0.0))
            if take <= 0:
                continue
            L[k] -= take
            need -= take
            if b == ROTATION and k != ROTATION and fair is not None:
                cash_b[k] += (1 - fair) * take            # the owner: its lot at fair
                cash_b[b] += (fair - p) * take            # rotation: what it gave up against fair
            else:
                cash_b[k] += (1 - p) * take
        if need > 1e-9:                                    # lots short of the position (should not happen): the acting strategy
            cash_b[b] += (1 - p) * need
        for k in [k for k, v in L.items() if v <= 1e-6]:
            del L[k]
        pos[ex] = P + (r if d > 0 else -r)
        if abs(pos[ex]) < 1e-9:
            pos[ex] = 0.0
        rest = q - r
        if rest > 1e-9:                                    # flipped: the rest is a new lot of the acting strategy
            lots[ex] = collections.defaultdict(float, {b: rest})
            avg[ex] = p
            pos[ex] = rest if d > 0 else -rest
            cash -= p * rest
            cash_b[b] -= p * rest
    for ts, amt in cash_ev:                                  # fees and other account cash: shown with the holdings
        cash += amt
        cash_b["held"] += amt
    return dict(realized_day=dict(realized_day), cash=cash, cash_b=dict(cash_b), pos={k: v for k, v in pos.items() if abs(v) >= 0.5}, avg=dict(avg),
                lots={ex: {k: v for k, v in L.items() if v >= 0.5} for ex, L in lots.items() if abs(pos.get(ex, 0)) >= 0.5})


# ------------------------------------------------------------------------------------------------ the order -> strategy map
ORDER_MAP = "order_buckets.json"


def order_buckets(eng):
    """{orderId: bucket} from every order and fill record the engine knows, persisted (orders are pruned from the ledger)."""
    path = os.path.join(DATA, ORDER_MAP)
    m = _read_json(path, {})
    n0 = len(m)
    for oid, o in list(eng.ledger.orders.items()):
        if oid not in m:
            m[oid] = bucket_of_layer(o.get("layer"), o.get("sub"), o.get("ex"))
    for f in list(eng.ledger.fills):
        oid = str(f.get("order") or "")
        if oid and oid not in m:
            m[oid] = "held" if f.get("before_engine") else bucket_of_layer(f.get("layer"), "", f.get("ex"))
    if len(m) != n0:
        try:
            _write_json(path, m)
        except OSError:
            pass
    return m


ORDER_OWNER = "order_owner.json"


def order_owner(eng):
    """{orderId: the strategy whose lots it sells} for reducing orders, from the order's sub-book, persisted."""
    path = os.path.join(DATA, ORDER_OWNER)
    m = _read_json(path, {})
    n0 = len(m)
    red = set(getattr(eng, "REDUCING", ()) or ())
    for oid, o in list(eng.ledger.orders.items()):
        if oid not in m and o.get("layer") in red and o.get("sub"):
            m[oid] = bucket_of_sub(o.get("sub"), o.get("ex"))
    if len(m) != n0:
        try:
            _write_json(path, m)
        except OSError:
            pass
    return m


def fill_fair(eng):
    """{fill key: Polymarket fair of the fill's side at fill time} from the engine's fill records (rotation's give-up)."""
    return {str(f.get("key")): float(f["fair"]) for f in list(eng.ledger.fills) if f.get("key") and isinstance(f.get("fair"), (int, float))}


ORDER_SELLS = "order_sells.json"


def sell_orders(eng):
    """Order ids the engine sent as SELLs (from its order and fill records), persisted: the fills feed has no direction."""
    path = os.path.join(DATA, ORDER_SELLS)
    m = set(_read_json(path, []))
    n0 = len(m)
    for oid, o in list(eng.ledger.orders.items()):
        if o.get("action") == "sell":
            m.add(str(oid))
    for f in list(eng.ledger.fills):
        if f.get("action") == "sell" and f.get("order"):
            m.add(str(f["order"]))
    if len(m) != n0:
        try:
            _write_json(path, sorted(m))
        except OSError:
            pass
    return m


# ------------------------------------------------------------------------------------------------ §0 the daily table
def pnl_samples():
    out = []
    path = os.path.join(DATA, "pnl.csv")
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        head = fh.readline().strip().split(",")
        for l in fh:
            r = dict(zip(head, l.strip().split(",")))
            try:
                t = local_ts(r["time"])
                a = float(r["account_value"]) if r.get("account_value") else None
                f = float(r["account_pm"]) if r.get("account_pm") else None
                out.append((t, a, f))
            except (ValueError, KeyError):
                pass
    return out


def nearest(samples, t, idx):
    """The sample value (index idx) nearest to t."""
    best, bd = None, None
    lo, hi = 0, len(samples) - 1
    while lo < hi:                                          # binary search, then look around
        mid = (lo + hi) // 2
        if samples[mid][0] < t:
            lo = mid + 1
        else:
            hi = mid
    for j in (lo - 1, lo, lo + 1):
        if 0 <= j < len(samples) and samples[j][idx] is not None:
            dd = abs(samples[j][0] - t)
            if bd is None or dd < bd:
                best, bd = samples[j][idx], dd
    return best


def daily_table(now, account_now, fair_now, realized_day):
    """[{day, board, realized, open, fair, fair_open, partial, start, end, start_fair, end_fair}] Oct 1 -> today."""
    s = pnl_samples()
    out = []
    d = utc_day(CUP_START_UTC)
    today = utc_day(now)
    while d <= today:
        t0 = day_start(d)
        t1 = t0 + 86400
        start = INITIAL if d == utc_day(CUP_START_UTC) else nearest(s, t0, 1)
        start_f = INITIAL if d == utc_day(CUP_START_UTC) else nearest(s, t0, 2)
        partial = d == today
        end = account_now if partial else nearest(s, t1, 1)
        end_f = fair_now if partial else nearest(s, t1, 2)
        board = None if start is None or end is None else end - start
        fair = None if start_f is None or end_f is None else end_f - start_f
        real = realized_day.get(d, 0.0)
        out.append(dict(day=d, board=None if board is None else round(board, 2), realized=round(real, 2),
                        open=None if board is None else round(board - real, 2), fair=None if fair is None else round(fair, 2),
                        fair_open=None if fair is None else round(fair - real, 2), partial=partial,
                        start=start, end=end, start_fair=start_f, end_fair=end_f))
        d = utc_day(t1)
    return out


def check_daily_csv(days):
    """§0 test: Oct 1-7 against daily_pnl.csv's account totals at the boundaries (days that differ by > 50, with the reason)."""
    path = os.path.join(DATA, "daily_pnl.csv")
    if not os.path.exists(path):
        return []
    rows = {}
    with open(path) as fh:
        head = fh.readline().strip().split(",")
        for l in fh:
            r = dict(zip(head, l.strip().split(",")))
            rows[r.get("date")] = r
    out = []
    for dd in days:
        r = rows.get(dd["day"])
        if not r or dd.get("end") is None or dd["partial"]:
            continue
        try:
            a = float(r.get("account_sig") or 0)
        except ValueError:
            continue
        if abs(a - dd["end"]) > 50:
            out.append(dict(day=dd["day"], daily_csv=a, ours=round(dd["end"], 2), diff=round(dd["end"] - a, 2),
                            why="daily_pnl.csv stores the engine's own valuation (money.account_sig: cash + holdings at the engine's SIG marks); pnl.csv and this table use SIG's totalAccountValue"))
    return out


# ------------------------------------------------------------------------------------------------ §2.4-2.5 ranking
def ranking(board_pnl, fair_pnl):
    meta = _read_json(os.path.join(DATA, "leaders_meta.json"), {})
    day = _read_json(os.path.join(DATA, "leaders_day.json"), {})
    path = os.path.join(DATA, "leaders.csv")
    last_t, snap, series = None, {}, []
    if os.path.exists(path):
        with open(path) as fh:
            head = fh.readline().strip().split(",")
            for l in fh:
                r = dict(zip(head, l.strip().split(",")))
                t = r.get("time")
                if t != last_t:
                    last_t, snap = t, {}
                try:
                    snap[int(r["rank"])] = float(r["value"])
                    if r.get("ours") == "1":
                        series.append((local_ts(t), int(r["rank"])))
                except (ValueError, KeyError):
                    pass
    total = meta.get("total") or 0
    rank = meta.get("my_rank")
    lines = []
    for n in (100, 50, 25):
        v = snap.get(n)
        lines.append(dict(n=n, pnl=v, pct=(n / total) if total else None, gap_marks=None if v is None else round(v - board_pnl, 2),
                          gap_fair=None if v is None or fair_pnl is None else round(v - fair_pnl, 2)))
    if len(series) > 600:
        step = len(series) / 600
        series = [series[int(i * step)] for i in range(600)] + [series[-1]]
    return dict(as_of=last_t, total=total, rank=rank, pct=(rank / total) if rank and total else None,
                day=dict(rank=day.get("my_rank"), total=day.get("total"), pct=(day.get("my_rank") / day.get("total")) if day.get("my_rank") and day.get("total") else None,
                         t=day.get("t")),
                lines=lines, series=[[round(t), r] for t, r in series])


# ------------------------------------------------------------------------------------------------ positions, values, buckets
def positions(eng):
    """[{ex, name, side, shares, avg, value, cost, vps, fair_side, subs: {bucket: shares}}] from SIG's own position rows."""
    out = []
    for ex, b in list(eng.book.items()):
        q = b.get("pos") or 0
        row = b.get("row") or {}
        if abs(q) < 1:
            continue
        side = "yes" if q > 0 else "no"
        value = float(row.get("marketValue") or 0)
        cost = float(row.get("costBasis") or 0)
        shares = abs(q)
        f = eng.fair.get(ex)
        fs = None if f is None else (f if side == "yes" else 1 - f)
        subs = collections.defaultdict(float)
        books = eng.ledger.subbooks.get(ex, {})
        assigned = 0.0
        aged = sum(float(m[3]) for m in list(getattr(eng.ledger, "moves", []) or []) if len(m) >= 4 and str(m[0]) == ex and str(m[1]).startswith("dump:") and str(m[2]) == "inventory")
        for sub, v in books.items():
            if abs(v) >= 1 and (v > 0) == (q > 0):
                k = bucket_of_sub(sub, ex)
                if sub == "inventory" and aged >= 1:       # aged catch lots (dump tranches moved to inventory after 6 h) stay the catcher's
                    a = min(abs(v), aged)
                    subs["catcher"] += a
                    subs[k] += abs(v) - a
                else:
                    subs[k] += abs(v)
                assigned += abs(v)
        if shares - assigned >= 1:
            subs["held"] += shares - assigned
        elif assigned - shares >= 1:                        # sub-books ahead of SIG (a fill on its way): scale to the position
            k_ = shares / assigned if assigned else 0
            subs = collections.defaultdict(float, {k: v * k_ for k, v in subs.items()})
        out.append(dict(ex=ex, name=eng.name(ex), side=side, shares=shares, avg=float(row.get("avgCost") or 0), value=value, cost=cost,
                        vps=(value / shares) if shares else 0.0, fair_side=fs, subs=dict(subs)))
    return out


def cash_of(eng):
    a = eng.acct or {}
    return float(a.get("totalAccountValue") or 0) - float(a.get("totalHoldingsValue") or 0), float(a.get("totalAccountValue") or 0)


def bucket_totals(eng, pos_rows):
    """{bucket: {board, fair, cash, value, value_fair, cost_now}} since inception (cash-flow method on the full archive)."""
    def run():
        rows = load_archive()
        tx = load_tx()
        evs, cash_ev, info = events(rows, tx, order_buckets(eng), sell_orders(eng), order_owner(eng), fill_fair(eng))
        rep = replay(evs, cash_ev)
        rep["info"] = info
        rep["n_tx"] = len(tx)
        return rep, len(rows), (rows[0]["filledAt"] if rows else None), (rows[-1]["filledAt"] if rows else None)
    res = cached("replay", 60, run)
    if isinstance(res, dict):                               # the replay failed: say so in the payload, never crash it
        raise RuntimeError(res.get("error"))
    rep, n, first, last = res
    out = {b["key"]: dict(board=0.0, fair=0.0, cash=0.0, value=0.0, value_fair=0.0, shares=0.0, cost=0.0) for b in ALL_BUCKETS}
    blank = lambda: dict(board=0.0, fair=0.0, cash=0.0, value=0.0, value_fair=0.0, shares=0.0, cost=0.0)
    for k, cf in rep["cash_b"].items():
        out.setdefault(k, blank())["cash"] += cf
    by_ex = {p["ex"]: p for p in pos_rows}
    lots = rep.get("lots") or {}
    for ex, p in by_ex.items():
        L = lots.get(ex) or {}
        tot = sum(L.values())
        if tot <= 0:                                        # a position the replay has no lots for (should not happen): held
            L, tot = {"held": p["shares"]}, p["shares"]
        split = {}
        for k, sh in L.items():
            sh = sh * p["shares"] / tot                     # scaled to SIG's shares (equal when the archive is complete)
            split[k] = round(sh, 2)
            o = out.setdefault(k, blank())
            o["shares"] += sh
            o["cost"] += sh * (p["avg"] or p["vps"])
            o["value"] += sh * p["vps"]
            o["value_fair"] += sh * (p["fair_side"] if p["fair_side"] is not None else p["vps"])
        p["subs"] = split                                   # the portfolio's split = the strategies' lots (one source for both pages)
    for k, o in out.items():
        o["board"] = round(o["cash"] + o["value"], 2)
        o["fair"] = round(o["cash"] + o["value_fair"], 2)
    return out, dict(fills=n, first=first, last=last, cash_replayed=round(INITIAL + rep["cash"], 2), realized_day=rep["realized_day"], n_tx=rep.get("n_tx"),
                     info=rep.get("info"), pos=rep.get("pos"))


SNAP = "dash5_snap.json"


def day_snapshot(now, totals):
    """The 00:00 UTC snapshot of every bucket's running total (taken on the first payload after 00:00 UTC). -> (snap, on_time)."""
    path = os.path.join(DATA, SNAP)
    s = _read_json(path, {})
    today = utc_day(now)
    if s.get("day") != today:
        hist = (s.get("hist") or [])[-60:]
        if s.get("day"):
            hist.append(dict(day=s["day"], t=s.get("t"), buckets=s.get("buckets")))
        s = dict(day=today, t=now, late_s=round(now - day_start(today)), buckets={k: dict(board=v["board"], fair=v["fair"]) for k, v in totals.items()}, hist=hist)
        try:
            _write_json(path, s)
        except OSError:
            pass
    return s, (s.get("late_s") or 0) <= 600


# ------------------------------------------------------------------------------------------------ server facts (§3.1)
_server = dict(region=None, region_t=0.0, cpu=None)


def server_facts(eng):
    if _server["region"] is None and time.time() - _server["region_t"] > 3600:
        _server["region_t"] = time.time()
        try:
            import urllib.request
            with urllib.request.urlopen("http://169.254.169.254/metadata/v1/region", timeout=0.5) as r:
                _server["region"] = r.read().decode().strip()
        except Exception:
            _server["region"] = ""
    cpu = None
    try:
        with open("/proc/stat") as fh:
            parts = [float(x) for x in fh.readline().split()[1:]]
        idle, tot = parts[3] + (parts[4] if len(parts) > 4 else 0), sum(parts)
        prev = _server["cpu"]
        _server["cpu"] = (idle, tot)
        if prev and tot > prev[1]:
            cpu = round(100 * (1 - (idle - prev[0]) / (tot - prev[1])), 1)
    except Exception:
        pass
    mem = mem_total = None
    try:
        mi = {}
        with open("/proc/meminfo") as fh:
            for l in fh:
                k, v = l.split(":")
                mi[k] = float(v.split()[0])
        mem_total = mi.get("MemTotal")
        mem = round(100 * (1 - mi.get("MemAvailable", 0) / mem_total), 1) if mem_total else None
    except Exception:
        pass
    try:
        du = shutil.disk_usage("/")
        disk = round(100 * du.used / du.total, 1)
    except Exception:
        disk = None
    rel = ""
    try:
        with open(os.path.join(ROOT, "RELEASE")) as fh:
            rel = fh.read().strip()
    except OSError:
        pass
    mirror_age = None
    try:
        mirror_age = round(time.time() - float(open(os.path.join(DATA, "mac_mirror_t")).read().strip()))
    except Exception:
        pass
    region = _server["region"] or ""
    return dict(host=socket.gethostname(), region=region, provider="DigitalOcean" if region else "", vcpu=os.cpu_count(),
                mem_gb=round(mem_total / 1048576, 1) if mem_total else None, cpu=cpu, mem=mem, disk=disk, release=rel,
                up_s=round(time.time() - getattr(eng, "boot_t", time.time())), mirror_age_s=mirror_age)


# ------------------------------------------------------------------------------------------------ health checks (Admin §b)
_hist = collections.defaultdict(lambda: collections.deque(maxlen=10))
_hist_t = dict(t=0.0)


def _dot(v, amber, red, higher_bad=True):
    if v is None:
        return "amber"
    if higher_bad:
        return "red" if v > red else "amber" if v > amber else "green"
    return "red" if v < red else "amber" if v < amber else "green"


def tracebacks_1h():
    def run():
        path = os.path.join(DATA, "engine.log")
        if not os.path.exists(path):
            return 0
        sz = os.path.getsize(path)
        with open(path, "rb") as fh:
            fh.seek(max(0, sz - 3_000_000))
            text = fh.read().decode("utf-8", "replace")
        cut = time.strftime("%H:%M:%S", time.localtime(time.time() - 3600))
        n, cur = 0, "00:00:00"
        for l in text.splitlines():
            m = re.match(r"^(\d\d:\d\d:\d\d) ", l)
            if m:
                cur = m.group(1)
            if l.startswith("Traceback") and (cur >= cut if cut < time.strftime("%H:%M:%S") else True):
                n += 1
        return n
    return cached("tb", 60, run)


def health(eng, api, server, check_gap, snap_ok, v4admin):
    now = time.time()
    h = api.health(600) if hasattr(api, "health") else {}
    rl = sum(1 for t in list(getattr(api, "_throttled", [])) if t >= now - 3600)
    loop_age = round(now - (eng.loop_end_t or now)) if getattr(eng, "loop_end_t", None) else None
    feed = getattr(eng, "mkt_feed", None)
    feed_ok = bool(feed and feed.connected())
    pm_age = round(now - eng.fair_t) if getattr(eng, "fair_t", None) else None
    uf = getattr(eng, "sig_feed", None)
    uf_age = round(now - uf.last_msg_t) if uf and getattr(uf, "last_msg_t", None) else None
    a = v4admin if isinstance(v4admin, dict) else {}
    foreign, dups, i2 = a.get("foreign"), a.get("duplicates"), a.get("trims")
    if isinstance(i2, dict):
        i2 = sum(v for v in i2.values() if isinstance(v, (int, float)))
    tb = tracebacks_1h()
    checks = [
        dict(key="loop", label="Loop age", value=loop_age, unit="s", threshold="green <= 30 s, red > 120 s", dot=_dot(loop_age, 30, 120)),
        dict(key="tb", label="Tracebacks (1 h)", value=tb if isinstance(tb, int) else None, unit="", threshold="0", dot=_dot(tb if isinstance(tb, int) else None, 0, 3)),
        dict(key="rl", label="Rate-limit errors (1 h)", value=rl, unit="", threshold="0 (amber 1-3, red > 3)", dot=_dot(rl, 0, 3)),
        dict(key="reads", label="Reads per minute", value=h.get("reads_last_min"), unit="/min", threshold=f"<= {h.get('read_limit', 95)}", dot=_dot(h.get("reads_last_min"), 85, 95)),
        dict(key="writes", label="Writes per minute", value=h.get("writes_last_min"), unit="/min", threshold="<= 30", dot=_dot(h.get("writes_last_min"), 25, 30)),
        dict(key="feed", label="SIG live feed", value=round(feed.stats.get("messages", 0)) if feed else None, unit=" batches",
             threshold="connected", dot="green" if feed_ok else ("amber" if feed else "red"), text=("connected" if feed_ok else "not connected") if feed else "off"),
        dict(key="pm", label="Polymarket feed", value=pm_age, unit="s old", threshold="<= 60 s", dot=_dot(pm_age, 60, 180)),
        dict(key="uf", label="User fill feed", value=uf_age, unit="s since a message", threshold="connected (quiet is normal)", dot="green" if uf else "red",
             text="connected" if uf else "not started"),
        dict(key="audit", label="Order audit", value=None, unit="", threshold="foreign 0 · duplicates 0 · I2 0",
             dot="green" if not (foreign or dups or i2) else "amber", text=f"foreign {foreign if foreign is not None else '–'} · duplicates {dups if dups is not None else '–'} · I2 {i2 if i2 is not None else '–'}"),
        dict(key="pnl", label="P&L check", value=check_gap, unit="", threshold="|gap| <= 25", dot=_dot(abs(check_gap) if check_gap is not None else None, 25, 100)),
        dict(key="snap", label="00:00 UTC snapshot", value=None, unit="", threshold="taken within 10 min of 00:00 UTC", dot="green" if snap_ok else "amber",
             text="on time" if snap_ok else "late (taken after the first payload of the day)"),
        dict(key="mirror", label="Mac mirror age", value=server.get("mirror_age_s"), unit="s", threshold="<= 120 s", dot=_dot(server.get("mirror_age_s"), 120, 600)),
        dict(key="disk", label="Server disk", value=server.get("disk"), unit="%", threshold="<= 80%", dot=_dot(server.get("disk"), 80, 92)),
        dict(key="cpu", label="Server CPU", value=server.get("cpu"), unit="%", threshold="<= 80%", dot=_dot(server.get("cpu"), 80, 95)),
        dict(key="mem", label="Server memory", value=server.get("mem"), unit="%", threshold="<= 80%", dot=_dot(server.get("mem"), 80, 92)),
    ]
    if now - _hist_t["t"] >= 60:
        _hist_t["t"] = now
        for c in checks:
            _hist[c["key"]].append([round(now), c["value"] if c["value"] is not None else c.get("text")])
    for c in checks:
        c["hist"] = list(_hist[c["key"]])
    return checks, rl


def status_of(eng, rl_10m, loop_age):
    """(state, reason): paused (red) / degraded (amber) / trading (green)."""
    c = eng.controls
    if c.paused or getattr(eng, "fills_blind", None):
        return "paused", "new risk paused" if c.paused else "fills blind"
    why = []
    if rl_10m:
        why.append(f"{rl_10m} rate-limit error{'s' if rl_10m > 1 else ''} in 10 min")
    if loop_age is not None and loop_age > 60:
        why.append(f"loop {loop_age} s old")
    if getattr(eng, "fair_t", None) and time.time() - eng.fair_t > 120:
        why.append("Polymarket stale")
    return ("degraded", "; ".join(why)) if why else ("trading", "")


# ------------------------------------------------------------------------------------------------ markets (§4)
def group_of(race):
    r = str(race or "")
    if r.startswith("U.S."):
        return "National"
    if "Senate" in r:
        return "Senate"
    if "Governor" in r:
        return "Governor"
    return "House"


STATES = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
          "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky",
          "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
          "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
          "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
          "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
          "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"}


def state_of(race):
    r = str(race or "")
    m = re.match(r"^([A-Z]{2})-\d+", r)
    if m:
        return STATES.get(m.group(1), m.group(1))
    for s in STATES.values():
        if r.startswith(s):
            return s
    return "National" if r.startswith("U.S.") else ""


def why_not(eng, ex, side_fair, held, orders):
    """The planner's own reason this market has no order of ours, from its plan state (never re-decided here)."""
    if orders:
        return ""
    if ex in getattr(eng, "manual_ex", set()):
        return "manual market: Piero's position only"
    if eng.ex.is_frozen(ex):
        return "frozen by the executor"
    try:
        if eng.fm_poll_block(ex, time.time()):
            return "excluded: within 10 min of poll close"
    except Exception:
        pass
    if ex in eng.reservoirs():
        r = eng.reservoirs()[ex]
        return "reservoir held (rebuild mode)" if r.get("mode") == "rebuild" else "reservoir: all offered lots resting or held"
    if eng.entry_budget(min(eng.cash_hist) if eng.cash_hist else 0.0) <= 0:
        return "catcher: no cash (free under the 2k guard)"
    race = (eng.meta.get(ex) or {}).get("race")
    if race and any((eng.meta.get(e) or {}).get("race") == race and abs((eng.book.get(e) or {}).get("pos", 0)) >= 1 for e in eng.meta if e != ex):
        cap = None
        try:
            cap = eng.race_room(ex, "yes", side_fair or 0.5)
        except Exception:
            pass
        if cap is not None and cap <= 0:
            return "race cap: we already hold this race through the other contract"
    cut = (getattr(eng, "dump_cut_t", {}) or {}).get((ex, "yes")) or (getattr(eng, "dump_cut_t", {}) or {}).get((ex, "no"))
    if cut and time.time() - cut[0] < 600:
        return f"catcher: {cut[1]}"
    blk = max((getattr(eng, "dump_block", {}) or {}).get((ex, s), 0) for s in ("yes", "no"))
    if blk > time.time():
        return "news guard: Polymarket moved, levels pulled"
    try:
        elig = eng.dump_eligible(ex)
    except Exception:
        elig = None
    if not elig:
        f = eng.fair.get(ex)
        if f is None:
            return "no Polymarket fair"
        return "no side ≥ 2¢ cheap vs fair" if 0.10 < f < 0.90 else "catcher: no favourite side with a level under the touch"
    return "catcher: levels cut for budget (cash)"


def markets(eng, pos_rows=None):
    now = time.time()
    pos = {p["ex"]: p for p in (pos_rows if pos_rows is not None else positions(eng))}
    orders = collections.Counter(o["ex"] for o in list(eng.open))
    vol = {ex: sum(v for _, v in d) for ex, d in list(getattr(eng, "vol24", {}).items())}
    feed = getattr(eng, "mkt_feed", None)
    rows = []
    for b in eng.bots:
        if b.kind != "market":
            continue
        ex = b.ex
        m = eng.meta.get(ex) or {}
        bid, ask, last = eng.px.get(ex, (None, None, None))
        pm = (eng.pm or {}).get(ex) or ()
        pm_bid, pm_ask = (pm[0], pm[1]) if len(pm) >= 2 else (None, None)
        f = eng.fair.get(ex)
        sig_mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        pm_mid = f if f is not None else ((pm_bid + pm_ask) / 2 if pm_bid is not None and pm_ask is not None else None)
        gap = None if sig_mid is None or pm_mid is None else round((sig_mid - pm_mid) * 100, 1)
        p = pos.get(ex)
        depth = None
        if feed:
            bk = feed.book(ex, 120.0)
            if bk:
                depth = dict(bid=bk[0][0] if bk[0] else None, ask=bk[1][0] if bk[1] else None)
        race = m.get("race") or ""
        rows.append(dict(ex=ex, race=race, party=m.get("party"), group=group_of(race), state=state_of(race),
                         title=getattr(b, "market", None) or eng.name(ex), contract=("D wins" if m.get("party") == "D" else "R wins" if m.get("party") == "R" else (m.get("party") or "")),
                         sig_bid=bid, sig_ask=ask, sig_yes=ask, sig_no=None if bid is None else round(1 - bid, 3),
                         pm_bid=pm_bid, pm_ask=pm_ask, pm_yes=pm_ask, pm_no=None if pm_bid is None else round(1 - pm_bid, 3), fair=f, gap_c=gap,
                         hold=None if not p else dict(side=p["side"], shares=round(p["shares"]), value=round(p["value"], 2), avg=p["avg"],
                                                      pnl=round(p["value"] - p["cost"], 2), subs={name_of(k): round(v) for k, v in sorted(p["subs"].items(), key=lambda kv: -kv[1]) if v >= 1}),
                         orders=orders.get(ex, 0), vol24=round(vol.get(ex, 0.0)), depth=depth,
                         why=why_not(eng, ex, f, p, orders.get(ex, 0))))
    as_of = max([getattr(eng, "px_t", 0) or 0, 0])
    no_hold_gap = sum(1 for r in rows if not r["hold"] and not r["orders"] and r["gap_c"] is not None and abs(r["gap_c"]) >= 3)
    return dict(as_of=as_of, pm_as_of=getattr(eng, "fair_t", None), rows=rows,
                summary=dict(contracts=len(rows), held=sum(1 for r in rows if r["hold"]), gap3_empty=no_hold_gap))


def market_book(eng, ex, depth=10):
    """The SIG book for the market panel: the C86 feed's book or the engine's last depth read; never a new read."""
    feed = getattr(eng, "mkt_feed", None)
    bk = feed.book(ex, 600.0) if feed else None
    src = "feed"
    if not bk:
        c = (getattr(eng, "sm_depth", {}) or {}).get(ex) or (getattr(eng, "tm_depth", {}) or {}).get(ex)
        bk = (c[1], c[2]) if c else None
        src = "last read"
    if not bk:
        return dict(src=None, bids=[], asks=[])
    mine = collections.defaultdict(float)
    for o in list(eng.open):
        if o["ex"] == ex:
            key = ("b", round(o["price"], 3)) if o["side"] == "yes" else ("a", round(1 - o["price"], 3))
            mine[key] += o["qty"]
    return dict(src=src, bids=[[p, q, mine.get(("b", round(p, 3)), 0)] for p, q in bk[0][:depth]],
                asks=[[p, q, mine.get(("a", round(p, 3)), 0)] for p, q in bk[1][:depth]])


# ------------------------------------------------------------------------------------------------ activity (§3.4)
ALERT_KINDS = {"PAUSE", "STOP", "CASHSHORT", "BLIND", "BREAKER", "CONTROL", "ERR", "KILL", "AUDIT", "WAKE"}
MARKET_KINDS = {"NEWS", "EVENT", "NEW", "SWEEP"}


def activity(eng, n=40):
    out = []
    fills = list(eng.ledger.fills[-400:])
    group = None
    def flush():
        if group:
            out.append(group)
    rows = []
    for f in reversed(fills):
        if f.get("before_engine") or f.get("action") not in ("buy", "sell"):
            continue
        b = bucket_of_layer(f.get("layer"), "", f.get("ex"))
        fs = eng.side_fair(f["ex"], f["side"]) if hasattr(eng, "side_fair") else None
        rows.append(dict(t=local_ts(f["t"]), ex=f["ex"], side=f["side"], qty=f["qty"], px=f["price"], layer=f.get("layer"), strat=b, fair=f.get("fair") or fs))
    i = 0
    while i < len(rows) and len(out) < n:
        r = rows[i]
        j = i
        if r["qty"] <= 5:
            while j + 1 < len(rows) and rows[j + 1]["qty"] <= 5 and rows[j + 1]["ex"] == r["ex"] and rows[j + 1]["side"] == r["side"] and abs(rows[j + 1]["t"] - r["t"]) <= 600:
                j += 1
        if j > i:
            g = rows[i:j + 1]
            out.append(dict(id=f"g{r['t']}{r['ex']}", t=r["t"], kind="trade", strat=r["strat"],
                            text=f"{len(g)} fills of {', '.join(sorted({str(int(x['qty'])) for x in g}))} share{'s' if any(x['qty'] > 1 for x in g) else ''}, "
                                 f"{eng.name(r['ex'])} {r['side'].upper()} at {r['px']:.3f}, {time.strftime('%H:%M', time.localtime(g[-1]['t']))}–{time.strftime('%H:%M', time.localtime(g[0]['t']))}"))
        else:
            fs = r["fair"]
            under = "" if fs is None else f", {abs(fs - r['px']) * 100:.1f}¢ {'under' if fs >= r['px'] else 'over'} fair"
            out.append(dict(id=f"f{r['t']}{r['ex']}{r['qty']}", t=r["t"], kind="trade", strat=r["strat"],
                            text=f"{name_of(r['strat'])} bought {int(r['qty']):,} {r['side'].upper()} {eng.name(r['ex'])} at {r['px']:.3f}{under}"))
        i = j + 1
    for e in list(eng.events)[-300:]:
        k = str(e.get("kind") or "")
        kind = "alert" if k in ALERT_KINDS else "market" if k in MARKET_KINDS else None
        if not kind:
            continue
        try:
            t = local_ts(e["t"]) if isinstance(e.get("t"), str) and len(e["t"]) >= 19 else float(e.get("ts") or 0)
        except Exception:
            t = 0
        out.append(dict(id=f"e{e.get('id', t)}", t=t, kind=kind, strat=None, text=str(e.get("text", ""))[:240]))
    out.sort(key=lambda r: -(r["t"] or 0))
    return out[:n]


# ------------------------------------------------------------------------------------------------ strategies (Live §3.2)
def strategy_cards(eng, totals, snap, pos_rows):
    now = time.time()
    resting = collections.defaultdict(lambda: [0, 0])                # [entries, exits] by bucket
    REDUCING = getattr(eng, "REDUCING", set())
    for o in list(eng.open):
        rec = eng.ledger.orders.get(o["id"]) or {}
        b = bucket_of_layer(rec.get("layer"), rec.get("sub"), o["ex"])
        resting[b][1 if rec.get("layer") in REDUCING else 0] += 1
    last_fill = {}
    for f in reversed(eng.ledger.fills[-2000:]):
        b = bucket_of_layer(f.get("layer"), "", f.get("ex"))
        if b not in last_fill:
            last_fill[b] = local_ts(f["t"])
    cost = {k: v.get("cost", 0.0) for k, v in totals.items()}
    c = eng.controls
    cards = []
    def caps(key):
        try:
            if key == "catcher":
                return round(float((getattr(eng, "dump_cov", None) or {}).get("budget") or 0))
            if key == "tm":
                return round(eng.tm_cap(now, log=False))
            if key == "sm":
                return round(float(eng.sm_sizes()["cap"]))
            if key == "fm":
                return 5_000
            if key == "ri":
                return round(11_900 * 0.84)
        except Exception:
            return None
        return None
    for group, items in (("strategy", REGISTRY), ("apart", APART), ("retired", RETIRED)):
        for b in items:
            k = b["key"]
            st = "Retired" if group == "retired" else b.get("status")
            reason = ""
            if group == "strategy":
                states = [c.state(l) for l in b.get("control") or []]
                if k == "rotation":
                    st, reason = ("Live", "free cash under BUFFER: asks at the touch") if getattr(eng, "rotation_on", False) else ("Live", "idle: free cash above BUFFER + 5k")
                elif states:
                    s0 = "full" if "full" in states else "pilot" if "pilot" in states else "off"
                    st = {"full": "Live", "pilot": "Pilot", "off": "Off"}[s0]
                    reason = ", ".join(f"{l} {c.state(l)}" for l in b["control"])
                else:
                    st = "Live"
            tot = totals.get(k, {})
            sb = ((snap or {}).get("buckets") or {}).get(k, {})
            fills = [dict(t=f["t"], ex=f["ex"], name=eng.name(f["ex"]), side=f["side"], qty=f["qty"], px=f["price"], layer=f.get("layer"), realized=f.get("realized"))
                     for f in reversed(eng.ledger.fills[-3000:]) if bucket_of_layer(f.get("layer"), "", f.get("ex")) == k][:20]
            if b.get("gated") and st in ("Live", "Pilot"):
                g = (getattr(eng, "sm_cov", None) or {}).get("gate") or {}
                n_all = sum(g.values()) if g else 0
                st, reason = "Gated", (reason + "; " if reason else "") + (f"new quotes only in BOUNCE markets: {g.get('entries', 0)} of {n_all} now"
                                                                           if n_all else "new quotes only in BOUNCE markets")
                if (getattr(eng, "sm_cov", None) or {}).get("exits_only"):
                    reason += "; exits only (rolling 24 h loss stop)"
            cards.append(dict(key=k, name=b["name"], group=group, sub=b.get("group5"), status=st, reason=reason, rules=b.get("rules", ""), layers=b.get("layers"),
                              control=b.get("control") or [], cap=caps(k), in_use=round(cost.get(k, 0.0), 2),
                              pnl=tot.get("board"), pnl_fair=tot.get("fair"),
                              today=None if tot.get("board") is None or sb.get("board") is None else round(tot["board"] - sb["board"], 2),
                              today_fair=None if tot.get("fair") is None or sb.get("fair") is None else round(tot["fair"] - sb["fair"], 2),
                              entries=resting[k][0], exits=resting[k][1], last_fill_t=last_fill.get(k), fills=fills))
    return cards


def strategy_days(eng):
    """Per strategy, per SIG day: realized from the ledger's fills (the board split by strategy before the snapshots exist)."""
    out = collections.defaultdict(lambda: collections.defaultdict(float))
    for f in list(eng.ledger.fills):
        if f.get("before_engine"):
            continue
        try:
            d = utc_day(local_ts(f["t"]))
        except Exception:
            continue
        out[bucket_of_layer(f.get("layer"), "", f.get("ex"))][d] += f.get("realized") or 0.0
    snap = _read_json(os.path.join(DATA, SNAP), {})
    return {k: [[d, round(v, 2)] for d, v in sorted(days.items())] for k, days in out.items()}, snap.get("hist") or []


# ------------------------------------------------------------------------------------------------ admin (§b-d)
CONTROL_KINDS = {"CONTROL", "PAUSE", "STOP", "BREAKER", "KILL", "CASHSHORT", "BLIND", "ALERT", "RATCHET", "TM", "SWING"}


def admin(eng, checks):
    c = eng.controls
    ctl = []
    for b in REGISTRY + APART:
        if not b.get("control"):
            continue
        layers = b["control"]
        states = [c.state(l) for l in layers]
        s0 = "full" if "full" in states else "pilot" if "pilot" in states else "off"
        ent = ext = 0
        for o in list(eng.open):
            rec = eng.ledger.orders.get(o["id"]) or {}
            if bucket_of_layer(rec.get("layer"), rec.get("sub"), o["ex"]) == b["key"]:
                if rec.get("layer") in getattr(eng, "REDUCING", set()):
                    ext += 1
                else:
                    ent += 1
        ctl.append(dict(key=b["key"], name=b["name"], layers=layers, state=s0, entries=ent, exits=ext))
    alerts = []
    for e in list(eng.events)[-600:]:
        k = str(e.get("kind") or "")
        if k in ("CONTROL", "PAUSE", "STOP", "BREAKER", "KILL", "CASHSHORT", "BLIND", "ERR") or "AUTO-OFF" in str(e.get("text", "")) or "->" in str(e.get("text", ""))[:60] and k in ("TM", "SWING", "CASH"):
            alerts.append(dict(t=e.get("t"), kind=k, text=str(e.get("text", ""))[:260]))
    alerts = alerts[-30:][::-1]
    cfg = {}
    try:
        import a_config as A
        for k in ("CASH_RESERVE", "READ_BUDGET", "SPARE_KEEP", "FEED_MODE_DEFAULT", "DUMP_L0_BUDGET", "TM_CAP_COST_PILOT", "TM_CAP_COST", "SM_CAP_COST_PILOT",
                  "RULES_V2", "HOLD_IDLE_GAP", "L3_BUDGET_SHARE", "FAST_PATH_ON", "ENGINE_HOST"):
            if hasattr(A, k):
                cfg[k] = getattr(A, k)
    except Exception:
        pass
    ov = {k: v for k, v in c.d.items() if k not in ("layers", "walls", "decisions", "markets", "fm_stats", "tm_stats", "sm_stats", "swing", "reservoirs", "budget_k", "cash_target")}
    rel_log = []
    try:
        with open(os.path.join(DATA, "releases.log")) as fh:
            rel_log = [l.strip() for l in fh.readlines()[-20:]][::-1]
    except OSError:
        pass
    logs = [dict(t=e.get("t"), kind=e.get("kind"), text=str(e.get("text", ""))[:300]) for e in list(eng.events)[-60:]][::-1]
    return dict(paused=bool(c.paused), controls=ctl, health=checks, alerts=alerts,
                advanced=dict(config=cfg, overrides=json.loads(json.dumps(ov, default=str)), releases=rel_log, logs=logs,
                              rollback=c.d.get("rollback_request")))


# ------------------------------------------------------------------------------------------------ the payload
def build(eng, api, v4=None):
    """{v5hdr, v5ov, v5lv, v5mk, v5ad}: the five top-level sections of the v5 pages (each diffed on its own by the stream)."""
    now = time.time()
    out = {}
    try:
        cash, account = cash_of(eng)
        pos_rows = positions(eng)
        holdings = sum(p["value"] for p in pos_rows)
        fair_hold = sum(p["shares"] * (p["fair_side"] if p["fair_side"] is not None else p["vps"]) for p in pos_rows)
        account_fair = cash + fair_hold
        totals, rep = bucket_totals(eng, pos_rows)
        snap, snap_ok = day_snapshot(now, totals)
        board_pnl = account - INITIAL
        fair_pnl = account_fair - INITIAL
        sum_b = round(sum(v["board"] for v in totals.values()), 2)
        gap = round(sum_b - board_pnl, 2)
        days = cached("days", 60, lambda: daily_table(now, account, account_fair, rep["realized_day"]))
        if isinstance(days, list) and days:
            t = days[-1]
            t["end"], t["end_fair"] = account, account_fair
            if t.get("start") is not None:
                t["board"] = round(account - t["start"], 2); t["open"] = round(t["board"] - t["realized"], 2)
            if t.get("start_fair") is not None:
                t["fair"] = round(account_fair - t["start_fair"], 2); t["fair_open"] = round(t["fair"] - t["realized"], 2)
        today = days[-1] if isinstance(days, list) and days else {}
        series = cached("acct_series", 120, lambda: [[round(t), a, f] for t, a, f in pnl_samples()[::3]])
        server = server_facts(eng)
        checks, rl = health(eng, api, server, gap, snap_ok, (v4 or {}).get("admin"))
        rl10 = sum(1 for t in list(getattr(api, "_throttled", [])) if t >= now - 600)
        loop_age = round(now - eng.loop_end_t) if getattr(eng, "loop_end_t", None) else None
        st, why = status_of(eng, rl10, loop_age)
        h = api.health(600) if hasattr(api, "health") else {}
        feed = getattr(eng, "mkt_feed", None)
        out["v5hdr"] = dict(t=now, status=st, reason=why, loop_age=loop_age)
        # Overview
        slices = sorted(pos_rows, key=lambda p: -p["value"])
        top, rest = slices[:10], slices[10:]
        invested = account - cash                           # SIG's own holdings value: donut + cash = account by construction
        val_gap = round(invested - holdings, 2)             # position rows that SIG values differently (should be ~0 live)
        def row(p):
            return dict(ex=p["ex"], name=p["name"], side=p["side"], shares=round(p["shares"]), value=round(p["value"], 2), pct=p["value"] / account if account else None,
                        avg=round(p["avg"], 4), mark=round(p["vps"], 4), fair=None if p["fair_side"] is None else round(p["fair_side"], 4),
                        pnl_marks=round(p["value"] - p["cost"], 2), pnl_fair=None if p["fair_side"] is None else round(p["shares"] * p["fair_side"] - p["cost"], 2),
                        subs={name_of(k): round(v) for k, v in sorted(p["subs"].items(), key=lambda kv: -kv[1]) if v >= 1})
        by_strat = collections.defaultdict(float)
        for p in pos_rows:
            for k, sh in p["subs"].items():
                by_strat[k] += sh * p["vps"]
        day_no = int((now - CUP_START_UTC) // 86400) + 1
        out["v5ov"] = dict(
            day_no=day_no, days_total=int((SETTLE_UTC - CUP_START_UTC) // 86400) + 1, settle_s=round(SETTLE_UTC - now),
            board=dict(pnl=round(board_pnl, 2), account=round(account, 2), pct=board_pnl / INITIAL),
            fair=dict(pnl=round(fair_pnl, 2), account=round(account_fair, 2), pct=fair_pnl / INITIAL),
            today=dict(board=today.get("board"), realized=today.get("realized"), open=today.get("open"), fair=today.get("fair")),
            days=days if isinstance(days, list) else [], days_error=None if isinstance(days, list) else days,
            account_series=series if isinstance(series, list) else [],
            rank=cached("rank", 60, lambda: ranking(board_pnl, fair_pnl)),
            port=dict(invested=round(invested, 2), available=round(cash, 2), above_floor=round(cash - FLOOR, 2), account=round(account, 2),
                      slices=[row(p) for p in top], other=[row(p) for p in rest], other_value=round(sum(p["value"] for p in rest) + (val_gap if abs(val_gap) > 1 else 0), 2),
                      val_gap=val_gap,
                      by_strategy=[dict(key=k, name=name_of(k), value=round(v, 2)) for k, v in sorted(by_strat.items(), key=lambda kv: -kv[1]) if v >= 1]),
            archive=dict(fills=rep.get("fills"), first=rep.get("first"), last=rep.get("last"), cash_replayed=rep.get("cash_replayed"), cash_sig=round(cash, 2),
                         transactions=rep.get("n_tx"), info=rep.get("info"),
                         pos_mismatch=sum(1 for p in pos_rows if abs((rep.get("pos") or {}).get(p["ex"], 0.0) - (p["shares"] if p["side"] == "yes" else -p["shares"])) > 1)
                         + sum(1 for ex in (rep.get("pos") or {}) if ex not in {p["ex"] for p in pos_rows})))
        # Live
        cards = strategy_cards(eng, totals, snap, pos_rows)
        sdays, shist = cached("sdays", 120, lambda: strategy_days(eng)) if True else ({}, [])
        out["v5lv"] = dict(
            server=dict(state=st, reason=why, **server, loop_age=loop_age, sig_ms=round(1000 * (h.get("read") or {}).get("p50"), 0) if (h.get("read") or {}).get("p50") else None,
                        pm_age=round(now - eng.fair_t) if getattr(eng, "fair_t", None) else None,
                        feed=dict(connected=bool(feed and feed.connected()), mode=eng.feed_mode() if hasattr(eng, "feed_mode") else None,
                                  bpm=None if not feed else _feed_bpm(feed)),
                        reads=h.get("reads_last_min"), read_limit=h.get("read_limit"), writes=h.get("writes_last_min"), write_limit=h.get("write_limit"), rl_1h=rl),
            cash=dict(pool=round(cash, 2), floor=FLOOR, buffer=eng.buffer() if hasattr(eng, "buffer") else None, positions=round(holdings, 2)),
            cards=cards, check=dict(sum=sum_b, board=round(board_pnl, 2), gap=gap), days=sdays, snap_hist=shist, activity=activity(eng))
        # Markets
        mk = markets(eng, pos_rows)
        out["v5mk"] = dict(as_of=mk["as_of"], pm_as_of=mk["pm_as_of"], summary=mk["summary"])
        out["v5mk_rows"] = mk["rows"]
        # Admin
        out["v5ad"] = admin(eng, checks)
    except Exception as err:
        out["v5err"] = f"v5 payload: {err!r}"
    return out


_bpm = dict(t=0.0, n=0, v=None)


def _feed_bpm(feed):
    now = time.time()
    n = feed.stats.get("messages", 0)
    if _bpm["t"] and now - _bpm["t"] >= 30:
        _bpm["v"] = round((n - _bpm["n"]) / ((now - _bpm["t"]) / 60), 1)
        _bpm.update(t=now, n=n)
    elif not _bpm["t"]:
        _bpm.update(t=now, n=n)
    return _bpm["v"]
