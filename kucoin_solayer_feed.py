#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v1.3.1  🔧 Bug‑fix

* Fix: TAAPI `/candles` returns **a list**, niet `{"data": [... ]}`.
  Daardoor kreeg je `TypeError: list indices must be integers or slices, not str`.
* Functie `taapi_candles()` nu robuust voor beide varianten (list / dict).
* Kleine guard → assert op minimale candle‑aantal.
"""

from __future__ import annotations
import datetime as dt, json, os, sys, time
from typing import Any, Dict, List

import pandas as pd
import requests
import ta

# ─────────────────────────── CONFIG ─────────────────────────────
SECRET      = os.getenv("TAAPI_SECRET") or sys.exit("❌ TAAPI_SECRET ontbreekt")
GIST_ID     = os.getenv("GIST_ID")      or sys.exit("❌ GIST_ID ontbreekt")
GIST_TOKEN  = os.getenv("GIST_TOKEN")   or sys.exit("❌ GIST_TOKEN ontbreekt")

SYMBOL      = os.getenv("SYMBOL", "LAYER/USDT")   # TAAPI‑formaat
INTERVAL    = os.getenv("GRANULARITY", "15m")     # '15m', '1h', …
SNAP_LEN    = int(os.getenv("SNAP_LEN", 300))      # aantal bars dat we in payload willen
FILE_NAME   = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_CANDLES = "https://api.taapi.io/candles"

iso = lambda ms: dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds") + "Z"

# ─────────────────────── TAAPI candles ─────────────────────────

def taapi_candles(length: int = 300) -> pd.DataFrame:
    """Download `length` OHLCV‑bars via TAAPI `/candles`."""
    params = {
        "secret":   SECRET,
        "exchange": "binance",
        "symbol":   SYMBOL.replace("/", ""),
        "interval": INTERVAL,
        "backtrack": length - 1,  # TAAPI telt de huidige candle als 0
    }
    r = requests.get(TAAPI_CANDLES, params=params, timeout=10)
    if r.status_code == 401:
        sys.exit("❌ TAAPI 401 Unauthorized – check API‑key / plan.")
    r.raise_for_status()

    raw = r.json()
    # TAAPI kan list ⟂ dict teruggeven; normaliseer
    if isinstance(raw, dict):
        raw = raw.get("data", [])
    if not isinstance(raw, list):
        sys.exit(f"❌ Onbekend TAAPI‑response‑type: {type(raw)}")

    if len(raw) < length:
        print(f"⚠️ Ontving slechts {len(raw)} candles, minder dan {length}–gevraagd.")

    df = pd.DataFrame(raw)[["timestamp", "open", "close", "high", "low", "volume"]]
    df = df.rename(columns={"timestamp": "ts", "volume": "vol"}).astype(float)
    df["ts"] = df["ts"].astype(int)
    df = df.sort_values("ts").reset_index(drop=True)
    return df.tail(length)

# ───────────────────── Indicators lokaal ───────────────────────

def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)
    df["vwap"]   = (df["vol"] * df["close"]).cumsum() / df["vol"].cumsum()
    return df

# ───────────────── Macro‑data (funding / OI) ───────────────────

def binance_future_metrics() -> Dict[str, float | None]:
    symbol = SYMBOL.replace("/", "").upper() + "T"  # LAYERUSDT
    out: Dict[str, float | None] = {"funding_rate": None, "open_interest": None}
    try:
        fr = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                           params={"symbol": symbol, "limit": 1}, timeout=8).json()[0]
        out["funding_rate"] = float(fr["fundingRate"])
    except Exception:
        pass
    try:
        oi = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                           params={"symbol": symbol, "period": "5m", "limit": 1}, timeout=8).json()[0]
        out["open_interest"] = float(oi["sumOpenInterest"])
    except Exception:
        pass
    return out

# ──────────────────────── Gist push ────────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}",
               "Accept": "application/vnd.github+json"}
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    requests.patch(f"https://api.github.com/gists/{GIST_ID}",
                   headers=headers, json=body, timeout=10).raise_for_status()

# ────────────────────────── MAIN ────────────────────────────────

def main() -> None:
    df = enrich_indicators(taapi_candles(SNAP_LEN))
    assert not df.empty, "Geen candles ontvangen."

    last = df.iloc[-1]
    meta  = binance_future_metrics()

    payload: Dict[str, Any] = {
        "timestamp":    int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol":       SYMBOL.replace("/", ""),
        "granularity":  INTERVAL,
        "price":        float(last.close),
        "high":         float(last.high),
        "low":          float(last.low),
        "vol":          float(last.vol),
        "ema20":        float(last.ema20),
        "ema50":        float(last.ema50),
        "ema200":       float(last.ema200),
        "rsi14":        float(last.rsi14),
        "vwap":         float(last.vwap),
        "atr14":        float(last.atr14),
        "last_300_candles": df.tail(SNAP_LEN).to_dict("records"),
        "funding_rate":  meta["funding_rate"],
        "open_interest": meta["open_interest"],
        "order_book":    None,
        "generated_at":  iso(int(time.time()*1000)),
    }

    push_gist(payload)
    print("✅ SOLayer TAAPI‑feed geüpload:", payload["generated_at"])

if __name__ == "__main__":
    main()
