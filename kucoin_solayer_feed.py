#!/usr/bin/env python3
"""
SOLayer feed – multi-TF confluence, ATR-gate, logging & history.
Funding-filters zijn verwijderd.
"""

import os, json, time, datetime as dt, pathlib, requests, sys

# ── Config uit omgevings­variabelen ────────────────────────────────────
SYMBOL           = os.getenv("SYMBOL",       "LAYER/USDT")
TF_FAST          = os.getenv("TF_FAST",      "15m")
TF_SLOW          = os.getenv("TF_SLOW",      "1h")
ATR_FAST_MIN     = float(os.getenv("ATR_PCT_MIN_FAST", "0.005"))   # 0.5 %
ATR_SLOW_MIN     = float(os.getenv("ATR_PCT_MIN_SLOW", "0.007"))   # 0.7 %
MACD_EPS         = float(os.getenv("MACD_EPS",         "0.0005"))
RSI_H            = float(os.getenv("RSI_HIGH", "55"))
RSI_L            = float(os.getenv("RSI_LOW",  "45"))
FILE_NAME        = os.getenv("FILE_NAME",    "solayer_feed.json")
HIST_DIR         = pathlib.Path(os.getenv("HISTORY_DIR", "history"))

TAAPI_SEC  = os.environ["TAAPI_SECRET"]
GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]

BASE_URL = "https://api.taapi.io"
REQ      = requests.Session()

# ── Helper: bulk-body samenstellen ────────────────────────────────────
def bulk_body(tf: str) -> dict:
    return {
        "secret": TAAPI_SEC,
        "construct": {
            "exchange": "binance",
            "symbol": SYMBOL,
            "interval": tf,
            "indicators": [
                {"id": "ema50",  "indicator": "ema",  "period": 50},
                {"id": "ema200", "indicator": "ema",  "period": 200},
                {"id": "rsi",    "indicator": "rsi",  "period": 14},
                {"id": "macd",   "indicator": "macd"},
                {"id": "atr",    "indicator": "atr",  "period": 14},
                {"id": "price",  "indicator": "price"}
            ]
        }
    }

def fetch_tf(tf: str) -> dict:
    """Haalt alle indicator-waarden voor één timeframe op via TAAPI /bulk."""
    r = REQ.post(f"{BASE_URL}/bulk", json=bulk_body(tf), timeout=12)
    r.raise_for_status()
    raw = {item["id"]: item["result"] for item in r.json()["data"]}
    return {
        "ema50":  raw["ema50"]["value"],
        "ema200": raw["ema200"]["value"],
        "rsi":    raw["rsi"]["value"],
        "macd":   raw["macd"]["valueMACDHist"],
        "atr":    raw["atr"]["value"],
        "price":  raw["price"]["value"]
    }

# ── Beslissings­logica ────────────────────────────────────────────────
def decide(d: dict) -> str:
    up   = d["ema50"] > d["ema200"]
    down = d["ema50"] < d["ema200"]
    bull = d["macd"]  >  MACD_EPS
    bear = d["macd"]  < -MACD_EPS
    if up   and bull and d["rsi"] > RSI_H: return "long"
    if down and bear and d["rsi"] < RSI_L: return "short"
    return "flat"

def vol_ok(d: dict, threshold: float) -> bool:
    """True als ATR-percentage ≥ drempel."""
    return d["atr"] / d["price"] >= threshold

# ── Main routine ──────────────────────────────────────────────────────
def main() -> None:
    fast = fetch_tf(TF_FAST)
    time.sleep(0.4)        # rate-limit-marge
    slow = fetch_tf(TF_SLOW)

    bias_fast = decide(fast) if vol_ok(fast, ATR_FAST_MIN) else "flat"
    bias_slow = decide(slow) if vol_ok(slow, ATR_SLOW_MIN) else "flat"
    final     = bias_fast if bias_fast == bias_slow else "flat"

    # Uitleg voor debugging/monitoring
    if bias_fast != bias_slow:
        reason = f"TF disagreement ({bias_fast} vs {bias_slow})"
    elif final == "flat":
        reason = "Volatility gate blocked trade"
    else:
        reason = f"{final} setup confirmed"

    payload = {
        "symbol":     SYMBOL.replace("/", ""),
        "timestamp":  dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "bias15m":    bias_fast,
        "bias1h":     bias_slow,
        "finalBias":  final,
        "biasReason": reason,
        "indicators": {TF_FAST: fast, TF_SLOW: slow},
        "settings": {
            "atrMinFast": ATR_FAST_MIN,
            "atrMinSlow": ATR_SLOW_MIN,
            "macdEps":    MACD_EPS,
            "rsiHigh":    RSI_H,
            "rsiLow":     RSI_L
        },
        "ttl_sec":    900
    }

    # ── Lokaal opslaan + history ──────────────────────────────────────
    with open(FILE_NAME, "w") as fp:
        json.dump(payload, fp, indent=2)

    HIST_DIR.mkdir(exist_ok=True)
    (HIST_DIR / f"{payload['timestamp']}.json").write_text(json.dumps(payload))

    # ── Push naar Gist ────────────────────────────────────────────────
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    r = REQ.patch(f"https://api.github.com/gists/{GIST_ID}",
                  headers=headers, json=body, timeout=12)
    r.raise_for_status()

    # Print hash-vaste raw-URL naar stdout (workflow kan ’m oppakken)
    raw_url = r.json()["files"][FILE_NAME]["raw_url"]
    print(raw_url)

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as err:
        print("HTTP error:", err, file=sys.stderr)
        sys.exit(1)
