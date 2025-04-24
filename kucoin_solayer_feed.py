#!/usr/bin/env python3
"""
kucoin_solayer_feed.py – enhanced (timestamp‑fix)
=================================================
Same functionality as previous commit, **but timestamps are now 100 % correct** and the deprecation‑warning is removed.

Highlights
----------
* KuCoin returns candle‑time in **seconds**; we convert to **milliseconds** (`*1000`).
* Helper `utc_iso(ts_ms)` gives a readable ISO‑8601 string with explicit **UTC offset**.
* Replaced deprecated `datetime.utcfromtimestamp`.

JSON example
------------
```json
{
  "timestamp": 1745793600000,
  "symbol": "SOLAYER-USDT",
  "latest_candle": {
    "ts": 1745792700000,
    "iso": "2025-04-27T14:25:00+00:00",
    "open": 2.2325,
    …
  },
  …
}
```
"""

import os
import json
import datetime as dt
from typing import List, Dict, Any
import requests

BASE = "https://api.kucoin.com"

# ---------- helpers ---------------------------------------------------------

def ts_ms() -> int:
    """Current UTC timestamp in **milliseconds**."""
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def utc_iso(ts_ms: int) -> str:
    """Return ISO‑8601 string (UTC) from ms‑epoch."""
    return dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).isoformat()


def get(endpoint: str, params: dict[str, Any] | None = None):
    r = requests.get(f"{BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    return r.json()

# ---------- core fetches ----------------------------------------------------

def fetch_candles(symbol: str, tf: str, limit: int = 20) -> List[Dict]:
    data = get("/api/v1/market/candles", {"symbol": symbol, "type": tf})["data"]
    if not data:
        raise RuntimeError("No candles returned")
    candles = []
    for raw in data[:limit][::-1]:  # newest→oldest → reverse
        ts_ms_val = int(raw[0]) * 1000  # KuCoin gives seconds → convert to ms
        candles.append(
            {
                "ts": ts_ms_val,
                "iso": utc_iso(ts_ms_val),
                "open": float(raw[1]),
                "close": float(raw[2]),
                "high": float(raw[3]),
                "low": float(raw[4]),
                "vol": float(raw[5]),
            }
        )
    return candles


def fetch_funding(symbol: str):
    try:
        d = get("/api/v1/funding-rate/symbol", {"symbol": symbol})["data"]
        return {
            "rate": float(d["fundingRate"]),
            "next_time": int(d["nextSettleTime"]),
            "next_time_iso": utc_iso(int(d["nextSettleTime"]))
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

    payload = {
        "timestamp": ts_ms(),
        "iso": utc_iso(ts_ms()),
        "symbol": symbol,
        "granularity": tf,
        "latest_candle": candles[-1],
        "last_20_candles": candles,
        "funding_rate": fetch_funding(symbol),
        "open_interest": fetch_open_interest(symbol),
        "order_book": fetch_depth(symbol, depth_pct)
    }

    print("Upload success" if update_gist(token, gist_id, file_name, payload) else "Upload failed")


if __name__ == "__main__":
    main()
