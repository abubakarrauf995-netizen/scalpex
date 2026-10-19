from typing import List, Dict, Tuple


def red(c):
    return c["close"] < c["open"]


def green(c):
    return c["close"] > c["open"]



def rsi14(candles):
    """Return RSI(14) from CLOSED 1H candles only. Informational only; never used for setup validity."""
    cs=[c for c in candles if c.get("closed", True)]
    if len(cs) < 15:
        return None
    closes=[float(c["close"]) for c in cs]
    gains=[]; losses=[]
    for i in range(1, len(closes)):
        d=closes[i]-closes[i-1]
        gains.append(max(d,0.0)); losses.append(max(-d,0.0))
    avg_gain=sum(gains[:14])/14
    avg_loss=sum(losses[:14])/14
    for i in range(14, len(gains)):
        avg_gain=((avg_gain*13)+gains[i])/14
        avg_loss=((avg_loss*13)+losses[i])/14
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs=avg_gain/avg_loss
    return 100.0-(100.0/(1.0+rs))


def pump(section):
    o = section[0]["open"]
    h = max(x["high"] for x in section)
    return (h - o) / o * 100.0


def line_y(x1, y1, x2, y2, x):
    if x2 == x1:
        return y1
    return y1 + (y2 - y1) * (x - x1) / (x2 - x1)


def trendline_clear(candles, a, b):
    """First-red LOW -> last-red LOW.
    Wick touch/cross is allowed. Candle BODY may not touch the line at all.
    The check is geometric over each full candle time interval, not a visual sample.
    """
    if b <= a:
        return True, None
    x1, y1 = candles[a]["open_time"], candles[a]["low"]
    x2, y2 = candles[b]["open_time"], candles[b]["low"]
    for j in range(a, b + 1):
        start = candles[j]["open_time"]
        end = candles[j + 1]["open_time"] if j < b else x2
        if end < start:
            end = start
        ys = line_y(x1, y1, x2, y2, start)
        ye = line_y(x1, y1, x2, y2, end)
        line_lo, line_hi = min(ys, ye), max(ys, ye)
        body_lo = min(candles[j]["open"], candles[j]["close"])
        body_hi = max(candles[j]["open"], candles[j]["close"])
        # Any overlap, including exact equality/touch, is invalid.
        if max(line_lo, body_lo) <= min(line_hi, body_hi):
            return False, j
    return True, None


def _swings(candles, wing=3):
    highs, lows = [], []
    for i in range(wing, len(candles) - wing):
        h, l = candles[i]["high"], candles[i]["low"]
        if h > max(candles[j]["high"] for j in range(i-wing, i)) and h >= max(candles[j]["high"] for j in range(i+1, i+wing+1)):
            highs.append((i, h))
        if l < min(candles[j]["low"] for j in range(i-wing, i)) and l <= min(candles[j]["low"] for j in range(i+1, i+wing+1)):
            lows.append((i, l))
    return highs, lows


def _parse(candles, *, include_forming=False):
    out = {"valid": False, "reason": "No valid setup", "pump1": None, "pump2": None}
    cs = list(candles) if include_forming else [c for c in candles if c.get("closed", True)]
    closed = [c for c in cs if c.get("closed", True)]
    if len(cs) < 30:
        out["reason"] = "Not enough 1H candles"
        return out

    i = len(cs) - 1
    if not red(cs[i]):
        out["reason"] = "Final candle is not red"
        return out
    fr = i
    while fr > 0 and red(cs[fr-1]):
        fr -= 1

    g2e = fr - 1
    if g2e < 0 or not green(cs[g2e]):
        out["reason"] = "No Green #2"
        return out
    g2s = g2e
    while g2s > 0 and green(cs[g2s-1]):
        g2s -= 1
    p2 = pump(cs[g2s:g2e+1])
    out["pump2"] = p2
    if p2 <= 1.0:
        out["reason"] = f"Green #2 pump {p2:.2f}% <= 1%"
        return out

    r2e = g2s - 1
    if r2e < 0 or not red(cs[r2e]):
        out["reason"] = "No red section between green sections"
        return out
    r2s = r2e
    while r2s > 0 and red(cs[r2s-1]):
        r2s -= 1

    g1e = r2s - 1
    if g1e < 0 or not green(cs[g1e]):
        out["reason"] = "No Green #1"
        return out
    g1s = g1e
    while g1s > 0 and green(cs[g1s-1]):
        g1s -= 1
    p1 = pump(cs[g1s:g1e+1])
    out["pump1"] = p1
    if p1 <= 1.0:
        out["reason"] = f"Green #1 pump {p1:.2f}% <= 1%"
        return out

    first = g1s - 1
    if first < 0 or not red(cs[first]):
        out["reason"] = "No first red candle"
        return out

    ok, bad = trendline_clear(cs, first, i)
    if not ok:
        out["reason"] = f"Trendline touches/crosses candle body at index {bad}"
        return out

    out.update(
        valid=True,
        reason="VALID SETUP",
        first_red_idx=first,
        last_red_idx=i,
        signal_time=cs[i]["close_time"],
        final_open=cs[i]["open"],
        final_close=cs[i]["close"],
    )
    return out


def analyze(candles):
    return _parse(candles)


def analyze_preclose(closed, current):
    arr = list(closed) + [current]
    out = _parse(arr, include_forming=True)
    if not out["valid"]:
        return {**out, "reason": "WATCH: " + out["reason"]}
    out["reason"] = "PRE-CLOSE WATCH"
    out["signal_time"] = current["close_time"]
    return out
