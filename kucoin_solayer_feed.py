#!/usr/bin/env python3
"""
kucoin_solayer_feed.py – enhanced
=================================
Push **rich** Solayer‑futures data to a GitHub Gist so the trading‑bot has alles‑in‑één input.

New in this version
-------------------
* **last_20_candles**  – list of the 20 most recent 15 m candles (OHLC‑V)
* **funding_rate**     – current funding rate & next settlement time
* **open_interest**    – current open interest (contracts)
* **order_book_depth** – cum. bid/ask volume within ±0.2 % of mid‑price

JSON layout written to the gist
-------------------------------
```json
{
  "timestamp": 1713967200000,
  "symbol": "SOLAYER-USDT",
  "granularity": "15min",
  "latest_candle": { … },
  "last_20_candles": [ {…}, … ],
  "funding_rate": { "rate": 0.00014, "next_funding_time": 1713974400000 },
  "open_interest": 1245789,
  "order_book": { "bid_vol": 83251, "ask_vol": 79412 }
}
```

Environment vars (unchanged + optional)
---------------------------------------
* SYMBOL, GRANULARITY, GIST_TOKEN, GIST_ID, FILE_NAME
* DEPTH_PCT (default 0.2)  – % around mid‑price to sum order‑book volume

KuCoin public endpoints used
----------------------------
* `/api/v1/market/candles`               – OHLCV
* `/api/v1/funding-rate/symbol`          – funding rate
* `/api/v1/openInterest?symbol=`         – open interest
* `/api/v1/market/orderbook/level2_20`   – order book top‑20 (bids/asks)

The script keeps the request budget low: one call per endpoint.
"""

import os
import json
import datetime as dt
from typing import List, Dict
import requests

BASE = "https://api.kucoin.com"

# ---------- helpers ---------------------------------------------------------

def ts_ms() -> int:
    return int(dt.datetime.utcnow().timestamp() * 1000)


def get(endpoint: str, params: dict | None = None):
    r = requests.get(f"{BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    return r.json()

# ---------- core fetches ----------------------------------------------------

def fetch_candles(symbol: str, tf: str, limit: int = 20) -> List[Dict]:
    data = get("/api/v1/market/candles", {"symbol": symbol, "type": tf})["data"]
    if not data:
        raise RuntimeError("No candles returned")
    candles = []
    for raw in data[:limit][::-1]:  # KuCoin returns newest‑>oldest
        c = {
            "ts": int(raw[0]),
            "open": float(raw[1]),
            "close": float(raw[2]),
            "high": float(raw[3]),
            "low": float(raw[4]),
            "vol": float(raw[5]),
        }
        candles.append(c)
    return candles


def fetch_funding(symbol: str):
    try:
        d = get("/api/v1/funding-rate/symbol", {"symbol": symbol})["data"]
        return {
            "rate": float(d["fundingRate"]),
            "next_time": int(d["nextSettleTime"])
        }
    except Exception:
        return None


def fetch_open_interest(symbol: str):
    try:
        d = get("/api/v1/openInterest", {"symbol": symbol})["data"]
        return float(d["openInterestVolume"])
    except Exception:
        return None


def fetch_depth(symbol: str, pct: float = 0.2):
    try:
        depth = get("/api/v1/market/orderbook/level2_20", {"symbol": symbol})["data"]
        bids = [(float(p), float(q)) for p, q in depth["bids"]]
        asks = [(float(p), float(q)) for p, q in depth["asks"]]
        mid = (bids[0][0] + asks[0][0]) / 2
        thresh = mid * pct / 100
        bid_vol = sum(q for p, q in bids if mid - p <= thresh)
        ask_vol = sum(q for p, q in asks if p - mid <= thresh)
        return {"bid_vol": bid_vol, "ask_vol": ask_vol}
    except Exception:
        return None

# ---------- gist update -----------------------------------------------------

def update_gist(token: str, gist_id: str, file_name: str, payload: dict) -> bool:
    url = f"https://api.github.com/gists/{gist_id}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "solayer-feed-bot"
    }
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    r = requests.patch(url, headers=headers, json=body, timeout=10)
    r.raise_for_status()
    return r.status_code == 200

# ---------- main ------------------------------------------------------------

def main():
    symbol = os.getenv("SYMBOL", "SOLAYER-USDT")
    tf = os.getenv("GRANULARITY", "15min")
    token = os.environ["GIST_TOKEN"]
    gist_id = os.environ["GIST_ID"]
    file_name = os.getenv("FILE_NAME", "solayer_feed.json")
    depth_pct = float(os.getenv("DEPTH_PCT", 0.2))

    candles = fetch_candles(symbol, tf, 20)
    latest = candles[-1]

    payload = {
        "timestamp": ts_ms(),
        "symbol": symbol,
        "granularity": tf,
        "latest_candle": latest,
        "last_20_candles": candles,
        "funding_rate": fetch_funding(symbol),
        "open_interest": fetch_open_interest(symbol),
        "order_book": fetch_depth(symbol, depth_pct)
    }

    if update_gist(token, gist_id, file_name, payload):
        print("Upload success")
    else:
        print("Upload failed")


if __name__ == "__main__":
    main()
