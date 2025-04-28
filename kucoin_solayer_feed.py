#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v1.2  (TAAPI edition)

• Haalt 15-min-indicatoren (EMA20/50/200, RSI14, ATR14, VWAP) via TAAPI.io
  voor Binance-pair LAYER/USDT.
• Levert exact dezelfde JSON-structuur als de BTC-feed.
• Bulk-endpoint ⇒ max 20 candles per indicator (meer kan met Direct).
"""

from __future__ import annotations
import datetime as dt, json, os, sys, time
from typing import Any, Dict, List

import requests

# ──────────────────────────── CONFIG ─────────────────────────────
SECRET     = os.getenv("TAAPI_SECRET") or sys.exit("❌ TAAPI_SECRET ontbreekt")
GIST_ID    = os.getenv("GIST_ID")      or sys.exit("❌ GIST_ID ontbreekt")
GIST_TOKEN = os.getenv("GIST_TOKEN")   or sys.exit("❌ GIST_TOKEN ontbreekt")

SYMBOL       = os.getenv("SYMBOL", "LAYER/USDT")   # TAAPI-formaat
INTERVAL     = os.getenv("GRANULARITY", "15m")     # '15m', '1h', …
SNAP_LEN     = 300                                 # target snapshot (20 geleverd)
FILE_NAME    = os.getenv("FILE_NAME", "solayer_feed.json")
TAAPI_BULK   = "https://api.taapi.io/bulk"

iso = lambda ms: dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds") + "Z"

# ─────────────────────── TAAPI CALL ──────────────────────────────
def fetch_indicators() -> List[Dict[str, Any]]:
    inds = [
        {"id": "ema20",  "indicator": "ema", "optInTimePeriod": 20},
        {"id": "ema50",  "indicator": "ema", "optInTimePeriod": 50},
        {"id": "ema200", "indicator": "ema", "optInTimePeriod": 200},
        {"id": "rsi14",  "indicator": "rsi", "optInTimePeriod": 14},
        {"id": "atr14",  "indicator": "atr", "optInTimePeriod": 14},
        {"id": "vwap",   "indicator": "vwap","anchorPeriod": "session"},
    ]
    for ind in inds:
        ind.update({"addResultTimestamp": True, "results": 20})

    payload = {
        "secret": SECRET,
        "construct": {
            "exchange": "binance",
            "symbol":   SYMBOL,
            "interval": INTERVAL,
            "indicators": inds,
        },
    }

    resp = requests.post(TAAPI_BULK, json=payload, timeout=10)
    if resp.status_code == 401:
        sys.exit("❌ TAAPI 401 Unauthorized – controleer je API-key en plan.")
    resp.raise_for_status()
    return resp.json()["data"]

# ───────────────────────── RESHAPE ───────────────────────────────
def reshape(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Combineert alle indicator-arrays op timestamp-index.
    Werkt voor zowel scalar ('timestamp': 161..) als array ('timestamp': [..]) payloads.
    """
    bars: Dict[int, Dict[str, Any]] = {}

    for item in data:
        ind_id = item["id"]
        res    = item["result"]

        # 1) Wanneer TAAPI arrays terugstuurt  (results >= 2)
        if isinstance(res.get("timestamp"), list):
            for i, ts in enumerate(res["timestamp"]):
                ts = int(ts)
                cell = bars.setdefault(ts, {})
                cell[ind_id] = res["value"][i]
                for k in ("open", "close", "high", "low", "volume"):
                    vlist = res.get(k)
                    if vlist:
                        cell.setdefault(k, vlist[i])

        # 2) Enkelvoudige payload (backwards-compat)
        else:
            ts = int(res.get("timestamp") or res.get("timestampMs"))
            cell = bars.setdefault(ts, {})
            cell[ind_id] = res["value"]
            for k in ("open", "close", "high", "low", "volume"):
                v = res.get(k)
                if v is not None:
                    cell.setdefault(k, v)

    if not bars:
        sys.exit("❌ Geen bruikbare data van TAAPI ontvangen – check symbol/plan.")

    ordered = [{"ts": t, **vals} for t, vals in sorted(bars.items())][-SNAP_LEN:]
    last    = ordered[-1]

    return {
        "timestamp":    last["ts"],
        "datetime_utc": iso(last["ts"]),
        "symbol":       SYMBOL.replace("/", ""),
        "granularity":  INTERVAL,
        "price":        last.get("close"),
        "high":         last.get("high"),
        "low":          last.get("low"),
        "vol":          last.get("volume"),
        "ema20":        last.get("ema20"),
        "ema50":        last.get("ema50"),
        "ema200":       last.get("ema200"),
        "rsi14":        last.get("rsi14"),
        "vwap":         last.get("vwap"),
        "atr14":        last.get("atr14"),
        "last_300_candles": ordered,   # bevat tot 20 candles (TAAPI-limiet)
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None,
        "generated_at":  iso(int(time.time()*1000)),
    }

# ────────────────────── PUSH TO GIST ─────────────────────────────
def push_gist(payload: Dict[str, Any]) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}",
               "Accept": "application/vnd.github+json"}
    body = { "files": { FILE_NAME: { "content": json.dumps(payload, indent=2) } } }
    requests.patch(f"https://api.github.com/gists/{GIST_ID}",
                   headers=headers, json=body, timeout=10).raise_for_status()

# ────────────────────────── MAIN ────────────────────────────────
def main() -> None:
    payload = reshape(fetch_indicators())
    push_gist(payload)
    print("✅ SOLayer TAAPI-feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
