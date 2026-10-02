"""C. POLYMARKET: fair values for SIG markets (public Gamma API, no key, doesn't count against the SIG rate limit).
market_map() matches each SIG market to the Polymarket market(s) for the same race and party; fairs() reads live quotes."""
import collections
import json
import os
import re
import urllib.request

from a_config import PM_MAP_FILE, PM_EXTRA, FAIR_HALF_LIFE_S, FAIR_JUMP, FAIR_CONFIRM_S, PM_MIN_LIQUIDITY, MB_PM_MAX_SPREAD

GAMMA = "https://gamma-api.polymarket.com"
SIG_TITLE = re.compile(r"Will the (\w+) Party win the (.+?)\??$")


def _get(path):
    return json.load(urllib.request.urlopen(urllib.request.Request(GAMMA + path, headers={"User-Agent": "Mozilla/5.0"}), timeout=20))


def race_party(title):
    """'Will the Democratic Party win the Alaska Governor?' -> ('Alaska Governor', 'D'); None if the title doesn't fit."""
    m = SIG_TITLE.match(title.strip())
    return (m.group(2), m.group(1)[0]) if m else None


def _pm_event_title(race):
    if race == "U.S. House":
        return "Which party will win the House in 2026?"
    if race == "U.S. Senate":
        return "Which party will win the Senate in 2026?"
    m = re.match(r"(\w\w-\d\d) House race", race)
    return f"{m.group(1)} House Election Winner" if m else f"{race} Election Winner"


def _pm_party(m):
    g = re.search(r"\(([DRI])\)\s*$", m.get("groupItemTitle") or "")
    if g:
        return g.group(1)
    if re.search(r"\b(Democratic Party|Democrats)\b", m["question"]):
        return "D"
    if re.search(r"\b(Republican Party|Republicans)\b", m["question"]):
        return "R"
    return None


def _events():
    events, off = [], 0
    while True:
        page = _get(f"/events?tag_slug=midterms&closed=false&limit=100&offset={off}")
        events += page
        off += 100
        if len(page) < 100:
            return events


def _match(sm, by_title):
    """One SIG market -> its map entry, or None unless exactly one open Polymarket event and party market match."""
    rp = race_party(sm["title"])
    if not rp or not sm.get("exchanges"):
        return None
    es = by_title.get(_pm_event_title(rp[0]), [])
    cands = [m for e in es for m in e["markets"] if not m.get("closed") and _pm_party(m) == rp[1]]
    if len(es) == 1 and len(cands) == 1:
        return dict(race=rp[0], party=rp[1], mid=str(sm["id"]), ex=str(sm["exchanges"][0]["id"]),
                    pm_ids=[str(cands[0]["id"])], pm_q=cands[0]["question"])
    return None


def build_map(sig_markets):
    """Strict match on Polymarket event title + party label; anything ambiguous is left out. Saved to PM_MAP_FILE."""
    by_title = collections.defaultdict(list)
    for e in _events():
        by_title[e["title"].strip()].append(e)
    out = [m for m in (_match(sm, by_title) for sm in sig_markets) if m]
    with open(PM_MAP_FILE, "w") as fh:
        json.dump(out, fh, indent=1)
    return out


def map_one(sm, events=None):
    """Sniper: map one new SIG market (strict, as build_map); None without a confident match. Not saved."""
    by_title = collections.defaultdict(list)
    for e in (events if events is not None else _events()):
        by_title[e["title"].strip()].append(e)
    return _match(sm, by_title)


def market_map(sig_markets):
    """Saved map (built if missing) plus PM_EXTRA candidate sums for races Polymarket lists only by candidate."""
    if os.path.exists(PM_MAP_FILE):
        with open(PM_MAP_FILE) as fh:
            mp = json.load(fh)
    else:
        mp = build_map(sig_markets)
    for m in mp:                                        # older map files stored a single pm_id
        m.setdefault("pm_ids", [str(m.pop("pm_id"))] if "pm_id" in m else [])
    have = {(m["race"], m["party"]) for m in mp}
    for sm in sig_markets:
        rp = race_party(sm["title"])
        if rp in PM_EXTRA and rp not in have and sm.get("exchanges"):
            mp.append(dict(race=rp[0], party=rp[1], mid=str(sm["id"]), ex=str(sm["exchanges"][0]["id"]),
                           pm_ids=list(PM_EXTRA[rp]), pm_q=f"sum of {len(PM_EXTRA[rp])} candidate markets"))
    return mp


def fairs(mp):
    """{ex: (pm_bid, pm_ask, liquidity)} for every mapped exchange. A candidate sum treats a missing bid as 0 and a
    missing ask as the bid (liquidity: the smallest leg); a single market needs both sides. Closed or missing legs drop
    the exchange."""
    ids = sorted({i for m in mp for i in m["pm_ids"]})
    quotes = {}
    for i in range(0, len(ids), 100):
        for x in _get("/markets?limit=100&" + "&".join(f"id={j}" for j in ids[i:i + 100])):
            if not x.get("closed"):
                quotes[str(x["id"])] = (x.get("bestBid"), x.get("bestAsk"), float(x.get("liquidityNum") or x.get("liquidity") or 0))
    out = {}
    for m in mp:
        legs = [quotes.get(i) for i in m["pm_ids"]]
        if not legs or any(l is None for l in legs):
            continue
        if len(legs) == 1 and None in legs[0][:2]:
            continue
        out[m["ex"]] = (sum(float(l[0] or 0) for l in legs), sum(float(l[1] if l[1] is not None else (l[0] or 0)) for l in legs),
                        min(l[2] for l in legs))
    return out


class FairTracker:
    """plan_review A6: fair = Polymarket mid smoothed by an EWMA (half-life 20 s). A move of >= 1.5c acts only once it has
    persisted 5 s (seen again on a later poll), then fair jumps to it. u = Polymarket half-spread. No fair when the
    spread is wider than MB_PM_MAX_SPREAD, liquidity < PM_MIN_LIQUIDITY, or the quote is older than 60 s."""

    def __init__(self, half_life=FAIR_HALF_LIFE_S, jump=FAIR_JUMP, confirm_s=FAIR_CONFIRM_S, max_age=60.0):
        self.half_life, self.jump, self.confirm_s, self.max_age = half_life, jump, confirm_s, max_age
        self.f, self.u, self.t, self.pending = {}, {}, {}, {}

    def update(self, quotes, now):
        for ex, q in quotes.items():
            b, a = float(q[0]), float(q[1])
            liq = q[2] if len(q) > 2 else PM_MIN_LIQUIDITY
            if not 0 < a - b <= MB_PM_MAX_SPREAD or liq < PM_MIN_LIQUIDITY:
                self.f.pop(ex, None)
                continue
            mid, self.u[ex] = (a + b) / 2, (a - b) / 2
            old, last = self.f.get(ex), self.t.get(ex, now)
            self.t[ex] = now
            if old is None:
                self.f[ex] = mid
                continue
            if abs(mid - old) >= self.jump - 1e-12:
                p = self.pending.get(ex)
                if p and (mid - old) * (p[1] - old) > 0 and now - p[0] >= self.confirm_s:
                    self.f[ex] = mid                        # confirmed: act on it
                    self.pending.pop(ex, None)
                elif not p or (mid - old) * (p[1] - old) <= 0:
                    self.pending[ex] = (now, mid)
                continue
            self.pending.pop(ex, None)
            w = 1 - 0.5 ** (max(now - last, 0.0) / self.half_life)
            self.f[ex] = old + w * (mid - old)

    def fair(self, ex, now):
        return self.f.get(ex) if now - self.t.get(ex, -1e18) <= self.max_age else None

    def fairs(self, now):
        return {ex: v for ex, v in self.f.items() if now - self.t.get(ex, -1e18) <= self.max_age}
