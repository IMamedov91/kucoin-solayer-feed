#!/usr/bin/env python3
"""
SOLayer feed – multi-TF confluence, ATR-gate, uitgebreid logging & history,
zonder funding-filters.
"""
import os, json, time, datetime as dt, pathlib, requests, sys

# ── ENV ──────────────────────────────────────────────────────────────
SYMBOL           = os.getenv("SYMBOL",       "LAYER/USDT")
TF_FAST          = os.getenv("TF_FAST",      "15m")
TF_SLOW          = os.getenv("TF_SLOW",      "1h")
ATR_FAST_MIN     = float(os.getenv("ATR_PCT_MIN_FAST", "0.005"))
ATR_SLOW_MIN     = float(os.getenv("ATR_PCT_MIN_SLOW", "0.007"))
MACD_EPS         = float(os.getenv("MACD_EPS",         "0.0005"))
RSI_H, RSI_L     = float(os.getenv("RSI_HIGH", "55")), float(os.getenv("RSI_LOW", "45"))
FILE_NAME        = os.getenv("FILE_NAME",    "solayer_feed.json")
HIST_DIR         = pathlib.Path(os.getenv("HISTORY_DIR", "history"))

TAAPI_SEC  = os.environ["TAAPI_SECRET"]
GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]

BASE = "https://api.taapi.io"
REQ  = requests.Session()

# ── Bulk-construct helper ────────────────────────────────────────────
def bulk_body(tf: str):
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

def fetch_tf(tf: str):
    r = REQ.post(f"{BASE}/bulk", json=bulk_body(tf), timeout=12)
    r.raise_for_status()
    data = {i["id"]: i["result"] for i in r.json()["data"]}
    return {
        "ema50":  data["ema50"]["value"],
        "ema200": data["ema200"]["value"],
        "rsi":    data["rsi"]["value"],
        "macd":   data["macd"]["valueMACDHist"],
        "atr":    data["atr"]["value"],
        "price":  data["price"]["value"]
    }

# ── Beslissings-logica ───────────────────────────────────────────────
def decide(d):
    up, down = d["ema50"] > d["ema200"], d["ema50"] < d["ema200"]
    bull, bear = d["macd"] >  MACD_EPS, d["macd"] < -MACD_EPS
    if up   and bull and d["rsi"] > RSI_H: return "long"
    if down and bear and d["rsi"] < RSI_L: return "short"
    return "flat"

def vol_ok(d, threshold):
    return d["atr"] / d["price"] >= threshold

# ── Main ─────────────────────────────────────────────────────────────
def main():
    fast = fetch_tf(TF_FAST)
    time.sleep(0.4)               # safety delay vs. TAAPI-limit
    slow = fetch_tf(TF_SLOW)

    bias_fast = decide(fast) if vol_ok(fast, ATR_FAST_MIN) else "flat"
    bias_slow = decide(slow) if vol_ok(slow, ATR_SLOW_MIN) else "flat"

    final = bias_fast if bias_fast == bias_slow else "flat"

    # bias-uitleg
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
            "macdEps":    MACD_EPS
        },
        "ttl_sec":    900
    }

    # ── Write & archive ──────────────────────────────────────────────
    with open(FILE_NAME, "w") as fp:
        json.dump(payload, fp, indent=2)

    HIST_DIR.mkdir(exist_ok=True)
    (HIST_DIR / f"{payload['timestamp']}.json").write_text(json.dumps(payload))

    # ── Push naar Gist ──────────────────────────────────────────────
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    r = REQ.patch(f"https://api.github.com/gists/{GIST_ID}",
                  headers={"Authorization": f"token {GIST_TOKEN}",
                           "Accept": "application/vnd.github+json"},
                  json=body, timeout=12)
    r.raise_for_status()
    print("Gist updated →", r.json().get("html_url"))

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print("HTTP error:", e, file=sys.stderr)
        sys.exit(1)
