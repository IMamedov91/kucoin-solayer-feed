#!/usr/bin/env python3
"""
kucoin_solayer_feed.py

Fetches EMA 50, EMA 200, RSI 14 and MACD histogram for LAYER/USDT on Binance via
TAAPI.io’s bulk endpoint, derives a bias (long / short / flat) and updates a
GitHub Gist with a JSON payload that downstream bots can consume.

Environment variables
---------------------
TAAPI_SECRET : TAAPI.io secret token (required)
GIST_TOKEN   : GitHub token with `gist` or `contents:write` scope (required)
GIST_ID      : ID of the target Gist (required)
SYMBOL       : Trading pair, default "LAYER/USDT"
GRANULARITY  : Timeframe, default "15m"
FILE_NAME    : File inside the Gist, default "solayer_feed.json"
"""

import os, json, datetime, sys, requests

TAAPI_SECRET = os.getenv("TAAPI_SECRET")
GIST_TOKEN   = os.getenv("GIST_TOKEN")
GIST_ID      = os.getenv("GIST_ID")
SYMBOL       = os.getenv("SYMBOL", "LAYER/USDT")
INTERVAL     = os.getenv("GRANULARITY", "15m")
FILE_NAME    = os.getenv("FILE_NAME", "solayer_feed.json")

if not all([TAAPI_SECRET, GIST_TOKEN, GIST_ID]):
    sys.exit("TAAPI_SECRET, GIST_TOKEN and GIST_ID must be set as env vars")

# 1️⃣  Fetch indicators via TAAPI bulk endpoint
bulk_url = "https://api.taapi.io/bulk"
construct = {
    "exchange": "binance",
    "symbol": SYMBOL,
    "interval": INTERVAL,
    "indicators": [
        {"id": "ema50",  "indicator": "ema",  "period": 50},
        {"id": "ema200", "indicator": "ema",  "period": 200},
        {"id": "rsi",    "indicator": "rsi",  "period": 14},
        {"id": "macd",   "indicator": "macd"}
    ]
}

r = requests.post(bulk_url, json={"secret": TAAPI_SECRET, "construct": construct}, timeout=10)
r.raise_for_status()
api_data = r.json()["data"]

vals = {}
for item in api_data:
    rid = item["id"]
    res = item["result"]
    if rid.startswith("ema50"):
        vals["ema50"] = res["value"]
    elif rid.startswith("ema200"):
        vals["ema200"] = res["value"]
    elif rid.startswith("rsi"):
        vals["rsi"] = res["value"]
    elif rid.startswith("macd"):
        vals["macd_hist"] = res["valueMACDHist"]

# 2️⃣  Determine trading bias
bias = "flat"
if all(k in vals for k in ("ema50", "ema200", "macd_hist", "rsi")):
    if vals["ema50"] > vals["ema200"] and vals["macd_hist"] > 0 and vals["rsi"] > 55:
        bias = "long"
    elif vals["ema50"] < vals["ema200"] and vals["macd_hist"] < 0 and vals["rsi"] < 45:
        bias = "short"

payload = {
    "symbol": SYMBOL.replace("/", ""),
    "interval": INTERVAL,
    "timestamp": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    "indicators": vals,
    "bias": bias,
}

# 3️⃣  Update (or create) file in the target Gist
update_url = f"https://api.github.com/gists/{GIST_ID}"
headers = {
    "Authorization": f"token {GIST_TOKEN}",
    "Accept": "application/vnd.github+json",
}
gist_body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}

g = requests.patch(update_url, headers=headers, json=gist_body, timeout=10)
g.raise_for_status()

print(f"✔ Updated {FILE_NAME} in gist {GIST_ID} – bias: {bias}")
