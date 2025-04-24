#!/usr/bin/env python3
"""
kucoin_solayer_feed.py
======================
Fetch the most recent candlestick for **SOLAYER‑USDT** from KuCoin and push it to a GitHub Gist so that ChatGPT can scrape it every few minutes.

Environment variables
---------------------
SYMBOL      : KuCoin trading pair, default 'SOLAYER-USDT'
GRANULARITY : KuCoin candle timeframe, default '15min'  # KuCoin supports 1min, 5min, 15min, 1hour …
GIST_TOKEN  : GitHub Personal‑Access‑Token (scope: "gist" or "public_repo")
GIST_ID     : ID of the target Gist that will hold the JSON feed
FILE_NAME   : Name of the file inside the Gist, default 'solayer_feed.json'

How it works
------------
1. Calls KuCoin public REST endpoint `/api/v1/market/candles`.
2. Extracts the most recent candle (timestamp, open, high, low, close, volume).
3. Serialises the dict to compact JSON.
4. PATCHes your Gist via GitHub API so the raw‑URL always contains the last candle.
   This raw‑URL is what the ChatGPT automation will poll.

Typical GitHub Actions job (cron every 5 min):
------------------------------------------------
```yaml
name: KuCoin Solayer feed
on:
  schedule:
    - cron: '*/5 * * * *'
  workflow_dispatch:
jobs:
  push-feed:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.x'
      - run: pip install requests
      - run: python kucoin_solayer_feed.py
        env:
          GIST_TOKEN: ${{ secrets.GIST_TOKEN }}
          GIST_ID: <YOUR_GIST_ID>
```
Replace `<YOUR_GIST_ID>` with the real ID.  Add secret **GIST_TOKEN** in repo‑>Settings‑>Secrets‑>Actions.
"""

import os
import json
import datetime as _dt
import requests

KUCOIN_URL = "https://api.kucoin.com/api/v1/market/candles"

def fetch_latest(symbol: str, granularity: str):
    params = {"symbol": symbol, "type": granularity}
    r = requests.get(KUCOIN_URL, params=params, timeout=10)
    r.raise_for_status()
    candles = r.json().get("data", [])
    if not candles:
        raise RuntimeError("No candle data returned from KuCoin")
    # KuCoin returns list: [time, open, close, high, low, volume, turnover]
    c = candles[0]
    ts_ms = int(c[0])
    return {
        "timestamp": ts_ms,
        "datetime_utc": _dt.datetime.utcfromtimestamp(ts_ms // 1000).isoformat(),
        "open": float(c[1]),
        "close": float(c[2]),
        "high": float(c[3]),
        "low": float(c[4]),
        "volume": float(c[5])
    }

def update_gist(token: str, gist_id: str, file_name: str, payload: dict):
    url = f"https://api.github.com/gists/{gist_id}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "solayer-feed-bot"
    }
    body = {
        "files": {
            file_name: {
                "content": json.dumps(payload, separators=(",", ":"))
            }
        }
    }
    resp = requests.patch(url, headers=headers, json=body, timeout=10)
    resp.raise_for_status()
    return resp.status_code == 200

def main():
    symbol = os.getenv("SYMBOL", "SOLAYER-USDT")
    granularity = os.getenv("GRANULARITY", "15min")
    token = os.environ["GIST_TOKEN"]  # must exist
    gist_id = os.environ["GIST_ID"]    # must exist
    file_name = os.getenv("FILE_NAME", "solayer_feed.json")

    candle = fetch_latest(symbol, granularity)
    success = update_gist(token, gist_id, file_name, candle)
    print("Upload success" if success else "Upload failed")

if __name__ == "__main__":
    main()
