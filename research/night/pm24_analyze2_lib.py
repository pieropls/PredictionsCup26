"""Loader + 0.50-artifact cleaner shared with pm24_analyze2.py (same rule)."""
import json, os
ART_LO, ART_HI = 0.475, 0.525


def load_clean(raw, tok):
    h = json.load(open(os.path.join(raw, "hist24", f"{tok[:20]}_f1.json"))).get("history", [])
    s = [(x["t"], x["p"]) for x in h]
    n, bad, i = len(s), [False] * len(s), 0
    while i < n:
        if ART_LO <= s[i][1] <= ART_HI:
            j = i
            while j + 1 < n and ART_LO <= s[j + 1][1] <= ART_HI:
                j += 1
            prev = s[i - 1][1] if i > 0 else None
            nxt = s[j + 1][1] if j + 1 < n else None
            far = lambda v: v is None or v < 0.30 or v > 0.70
            if far(prev) and far(nxt) and not (prev is None and nxt is None):
                for k in range(i, j + 1):
                    bad[k] = True
            i = j + 1
        else:
            i += 1
    return [x for x, b in zip(s, bad) if not b]
