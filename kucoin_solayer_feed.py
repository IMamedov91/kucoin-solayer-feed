#!/usr/bin/env python3
"""
kucoin_solayer_feed.py — v1.0

Fetches SOLayer indicators (EMA20/50/200, RSI14, ATR14, VWAP) for the
15‑minute timeframe straight from **TAAPI.io** (exchange = *binance*,
symbol = *LAYER/USDT* by default) and uploads the latest candle plus a
snapshot to a GitHub Gist.  The JSON schema is identical to the BTC feed
that already runs on TAAPI.

Limitations & notes
-------------------
* TAAPI Bulk returns **max 20 candles** per indicator. The snapshot field
  `last_300_candles` will therefore currently contain ≤20 rows. Increase
  granularity (e.g. 1h) if you need a larger historic window.
* All indicators arrive pre‑calculated from TAAPI, so we no longer need
  `pandas` or `ta` in the workflow.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Any, Dict, List

import requests

# ───────────────────────── CONFIG ────────────────────────────────
SECRET     = os.getenv("TAAPI_SECRET")            # required TAAPI key
GIST_ID    = os.getenv("GIST_ID")
GIST_TOKEN = os.getenv("GIST_TOKEN")

SYMBOL     = os.getenv("SYMBOL", "LAYER/USDT")  # TAAPI symbol format
INTERVAL   = os.getenv("GRANULARITY", "15m")     # TAAPI timeframe
SNAP_LEN   = 300                                  # desired snapshot length (≤300)
FILE_NAME  = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_URL = "https://api.taapi.io/bulk"

# ──────────────────────── HELPERS ────────────────────────────────

def iso(ms: int | float) -> str:
    """Unix epoch‑ms → ISO‑8601 (UTC)."""
    return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"

# ───────────────────── TAAPI INTEGRATION ─────────────────────────

def fetch_indicators() -> List[Dict[str, Any]]:
    """Bulk‑request the latest indicator arrays from TAAPI.io."""
    indicators = [
        {"id": "ema20",  "indicator": "ema",  "optInTimePeriod": 20},
        {"id": "ema50",  "indicator": "ema",  "optInTimePeriod": 50},
        {"id": "ema200", "indicator": "ema",  "optInTimePeriod": 200},
        {"id": "rsi14",  "indicator": "rsi",  "optInTimePeriod": 14},
        {"id": "atr14",  "indicator": "atr",  "optInTimePeriod": 14},
        {"id": "vwap",   "indicator": "vwap", "anchorPeriod": "session"},
    ]
    for ind in indicators:
        ind.update({"addResultTimestamp": True, "results": 20})  # TAAPI Bulk max

    payload = {
        "secret": SECRET,
        "construct": {
            "exchange": "binance",
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "indicators": indicators,
        },
    }

    r = requests.post(TAAPI_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["data"]

# ─────────────────────── TRANSFORM DATA ─────────────────────────

def reshape(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge indicator arrays into candle‑centric dicts."""
    bars: Dict[int, Dict[str, Any]] = {}

    for item in data:
        ind_id = item["id"]
        results = item["result"] if isinstance(item["result"], list) else [item["result"]]
        for res in results:
            ts_raw = res.get("timestamp") or res.get("timestampMs")
            if ts_raw is None:
                continue
            ts = int(ts_raw)
            bar = bars.setdefault(ts, {})
            bar[ind_id] = res["value"]
            for k in ("open", "close", "high", "low", "volume"):
                v = res.get(k)
                if v is not None:
                    bar.setdefault(k, v)

    if not bars:
        raise RuntimeError("TAAPI response contained no usable timestamps; check symbol & key.")

    ordered = [{"ts": ts, **vals} for ts, vals in sorted(bars.items())][-SNAP_LEN:]
    last = ordered[-1]

    return {
        "timestamp": last["ts"],
        "datetime_utc": iso(last["ts"]),
        "symbol": SYMBOL.replace("/", ""),
        "granularity": INTERVAL,
        "price": last.get("close"),
        "high": last.get("high"),
        "low": last.get("low"),
        "vol": last.get("volume"),
        "ema20": last.get("ema20"),
        "ema50": last.get("ema50"),
        "ema200": last.get("ema200"),
        "rsi14": last.get("rsi14"),
        "vwap": last.get("vwap"),
        "atr14": last.get("atr14"),
        "last_300_candles": ordered,
        "funding_rate": None,
        "open_interest": None,
        "order_book": None,
        "generated_at": iso(int(time.time() * 1000)),
    }

# ─────────────────────── PUSH TO GIST ───────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}", "Accept": "application/vnd.github+json"}
    r = requests.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers=headers,
        json={"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}},
        timeout=10,
    )
    r.raise_for_status()

# ────────────────────────── MAIN ────────────────────────────────

def main() -> None:
    data = fetch_indicators()
    payload = reshape(data)
    push_gist(payload)
    print("✅ SOLayer TAAPI feed uploaded:", payload["generated_at"])


if __name__ == "__main__":
    main()
