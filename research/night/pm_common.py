"""Shared poll-close table and helpers for the night-tapes scripts."""
import datetime as dt

# Poll closing times, ET (EST on all three election days: DST had ended), (first, last) in hours after midnight of election day.
# Source: 270toWin poll-closing table (statutory hours, unchanged 2022-2026) cross-checked with data/election/poll_close.json (statutes).
PC = {"Alabama": (20, 20), "Alaska": (24, 25), "Arizona": (21, 21), "Arkansas": (20.5, 20.5), "California": (23, 23), "Colorado": (21, 21),
      "Connecticut": (20, 20), "Delaware": (20, 20), "Florida": (19, 20), "Georgia": (19, 19), "Hawaii": (24, 24), "Idaho": (22, 23),
      "Illinois": (20, 20), "Indiana": (18, 19), "Iowa": (21, 21), "Kansas": (20, 21), "Kentucky": (18, 19), "Louisiana": (21, 21),
      "Maine": (20, 20), "Maryland": (20, 20), "Massachusetts": (20, 20), "Michigan": (20, 21), "Minnesota": (21, 21), "Mississippi": (20, 20),
      "Missouri": (20, 20), "Montana": (22, 22), "Nebraska": (21, 21), "Nevada": (22, 22), "New Hampshire": (19, 20), "New Jersey": (20, 20),
      "New Mexico": (21, 21), "New York": (21, 21), "North Carolina": (19.5, 19.5), "North Dakota": (20, 21), "Ohio": (19.5, 19.5),
      "Oklahoma": (20, 20), "Oregon": (22, 23), "Pennsylvania": (20, 20), "Rhode Island": (20, 20), "South Carolina": (19, 19),
      "South Dakota": (20, 21), "Tennessee": (20, 20), "Texas": (20, 21), "Utah": (22, 22), "Vermont": (19, 19), "Virginia": (19, 19),
      "Washington": (23, 23), "West Virginia": (19.5, 19.5), "Wisconsin": (21, 21), "Wyoming": (21, 21)}
DISTRICT_PC = {"FL-13": (19, 19), "OR-05": (23, 23)}   # Pinellas is ET; OR-05 has no Mountain-time county
ED24 = dt.datetime(2024, 11, 5, tzinfo=dt.timezone.utc).timestamp() + 5 * 3600   # midnight ET (EST = UTC-5)
ED22 = dt.datetime(2022, 11, 8, tzinfo=dt.timezone.utc).timestamp() + 5 * 3600


def close_ts(ed, state, district=None, control=None):
    if control:
        return ed + 19 * 3600, ed + 25 * 3600   # first wave 19:00 ET .. last state (Alaska Aleutians) 01:00 ET
    f, l = DISTRICT_PC.get(district, PC[state])
    return ed + f * 3600, ed + l * 3600


def et(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone(dt.timedelta(hours=-5))).strftime("%m-%d %H:%M ET")


def parse_closed(s):
    if not s:
        return None
    s = s.replace("+00", "+00:00") if s.endswith("+00") else s.replace("Z", "+00:00")
    return dt.datetime.fromisoformat(s).timestamp()


def q(vals, ps=(0.1, 0.25, 0.5, 0.75, 0.9)):
    v = sorted(vals)
    if not v:
        return {}
    out = {}
    for p in ps:
        i = min(len(v) - 1, max(0, int(round(p * (len(v) - 1)))))
        out[f"p{int(p*100)}"] = v[i]
    out["n"], out["min"], out["max"], out["mean"] = len(v), v[0], v[-1], sum(v) / len(v)
    return out


