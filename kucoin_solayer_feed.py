#!/usr/bin/env python3
"""SOLayer feed: haalt EMA-50/200, RSI-14 & MACD via TAAPI, beoordeelt bias en schrijft naar Gist."""

import os, json, datetime as dt, sys, requests

# ─── Config uit env ────────────────────────────────────────────────────────────
SYMBOL     = os.getenv('SYMBOL', 'LAYER/USDT')
INTERVAL   = os.getenv('INTERVAL', '15m')
FILE_NAME  = os.getenv('FILE_NAME', 'solayer_feed.json')
MACD_EPS   = float(os.getenv('MACD_EPS', '0.0005'))   # ruis-drempel voor MACD-hist
TAAPI_SEC  = os.environ['TAAPI_SECRET']
GIST_ID    = os.environ['GIST_ID']
GIST_TOKEN = os.environ['GIST_TOKEN']

BASE = "https://api.taapi.io/"
REQ  = requests.Session()

# (indicator-pad, extra query-parameters)
INDICS = [
    ("ema",  {"period": 50}),
    ("ema",  {"period": 200}),
    ("rsi",  {"period": 14}),
    ("macd", {}),                       # default 12-26-9
]

# ─── Helpers ───────────────────────────────────────────────────────────────────
def call(endpoint: str, extra: dict):
    """Doe één GET naar bv. /ema, /rsi, /macd …"""
    params = {
        "secret": TAAPI_SEC,
        "exchange": "binance",
        "symbol": SYMBOL,
        "interval": INTERVAL,
        **extra,
    }
    r = REQ.get(BASE + endpoint, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_all():
    data = {}
    for ep, extra in INDICS:
        res = call(ep, extra)
        if ep == "macd":
            # TAAPI retourneert valueMACDHistogram / Signal / MACD
            hist = res.get("valueMACDHistogram") or res.get("valueMACDHist")
            data["macd"] = {
                "macd":     res.get("valueMACD"),
                "signal":   res.get("valueMACDSignal"),
                "hist":     hist,
            }
        elif ep == "rsi":
            data["rsi14"] = res["value"]
        else:          # ema
            key = f"ema{extra['period']}"
            data[key] = res["value"]
    return data

def decide(d):
    up   = d["ema50"] > d["ema200"]
    down = d["ema50"] < d["ema200"]
    hist = d["macd"]["hist"]          # kan None zijn als TAAPI niets teruggeeft
    rsi  = d["rsi14"]

    bull = hist is not None and hist >  MACD_EPS
    bear = hist is not None and hist < -MACD_EPS

    if up and bull and rsi > 55:
        return "long"
    if down and bear and rsi < 45:
        return "short"
    return "flat"

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    try:
        indics = fetch_all()
    except Exception as e:
        print("Error fetching indicators:", e, file=sys.stderr)
        sys.exit(1)

    payload = {
        "symbol":    SYMBOL.replace("/", ""),
        "interval":  INTERVAL,
        "timestamp": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "indicators": indics,
        "bias":      decide(indics),
        "ttl_sec":   900,               # 15 min geldig
    }

    # schrijf lokaal (optioneel handig voor debug)
    with open(FILE_NAME, "w") as fp:
        json.dump(payload, fp, indent=2)

    # patch de Gist
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    r = REQ.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={
            "Authorization": f"token {GIST_TOKEN}",
            "Accept": "application/vnd.github+json",
        },
        data=json.dumps(body),
        timeout=15,
    )
    r.raise_for_status()
    print("Gist updated →", r.json().get("html_url", "ok"))

if __name__ == "__main__":
    main()
