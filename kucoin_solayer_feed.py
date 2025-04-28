#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v1.5  🟢  stable

* 15‑min LAYER/USDT spot candles from **Binance** via TAAPI `/candles`.
* Chunk‑loop (≤ 99 bars per call) → 300 candles snapshot.
* Indicators local (EMA20/50/200, RSI14, ATR14, VWAP, vol_mean20).
* Macro add‑ons: Binance‑futures funding‑rate, open‑interest & depth‑5 order‑book imbalance.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from typing import Any, Dict, List

import pandas as pd
import requests
import ta

# ────────────────────────── CONFIG ──────────────────────────────
SECRET      = os.getenv("TAAPI_SECRET") or sys.exit("❌ TAAPI_SECRET ontbreekt")
GIST_ID     = os.getenv("GIST_ID")      or sys.exit("❌ GIST_ID ontbreekt")
GIST_TOKEN  = os.getenv("GIST_TOKEN")   or sys.exit("❌ GIST_TOKEN ontbreekt")

EXCHANGE     = os.getenv("EXCHANGE", "binance")        # binance | binancefutures | kucoin
SYMBOL       = os.getenv("SYMBOL", "LAYER/USDT")        # LAYER/USDT (spot)
INTERVAL     = os.getenv("INTERVAL", "15m")
SNAP_LEN     = int(os.getenv("SNAP_LEN", "300"))         # ≤99 bij free‑tier
FILE_NAME    = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_URL    = "https://api.taapi.io/candles"

iso = lambda ms: dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds") + "Z"

# ───────────────────────── HELPER ───────────────────────────────

def _clean(val: Any) -> Any:
    try:
        import math
        if val is None or math.isnan(val) or math.isinf(val):
            return None
    except Exception:
        pass
    return val

# ───────────────── TAAPI CANDLE FETCH ──────────────────────────

def taapi_candles(backtracks: int = SNAP_LEN) -> pd.DataFrame:
    """Return DataFrame with `backtracks` most‑recent candles."""
    CHUNK = 99  # TAAPI free‑tier limit
    frames: List[pd.DataFrame] = []
    fetched = 0

    while fetched < backtracks:
        want = min(CHUNK, backtracks - fetched)
        params = {
            "secret":    SECRET,
            "exchange":  EXCHANGE,
            "symbol":    SYMBOL,
            "interval":  INTERVAL,
            "backtrack": fetched,
            "range":     want,
            "format":    "JSON",
        }
        r = requests.get(TAAPI_URL, params=params, timeout=10)
        if r.status_code == 401:
            sys.exit("❌ TAAPI 401 Unauthorized – check key/plan.")
        data = r.json()
        if isinstance(data, dict):
            if data.get("status") == "error":
                sys.exit(f"❌ TAAPI error: {data.get('message')}")
            data = data.get("data", [])
        if not data:
            break  # nothing returned
        tmp = pd.DataFrame(data)[["timestamp", "open", "close", "high", "low", "volume"]]
        tmp.rename(columns={"timestamp": "ts", "volume": "vol"}, inplace=True)
        tmp = tmp.astype(float)
        frames.append(tmp)
        fetched += len(tmp)
        time.sleep(0.25)
        if len(tmp) < want:
            break  # reached end of history

    if not frames:
        sys.exit("❌ TAAPI returned no data.")

    df = pd.concat(frames).drop_duplicates("ts").sort_values("ts").tail(backtracks).reset_index(drop=True)
    if len(df) < 20:
        sys.exit(f"❌ Only {len(df)} candles received – plan upgrade needed or wrong symbol.")
    return df

# ─────────────── INDICATORS LOCAL ─────────────────────────────

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(df["high"], df["low"], df["close"], df["vol"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=20).mean()
    df.fillna(method="bfill", inplace=True)
    return df

# ───────────── BINANCE MACRO DATA ─────────────────────────────

def fetch_macro(sym_spot: str) -> Dict[str, Any]:
    sym = sym_spot.replace("/", "").upper()  # LAYERUSDT
    out: Dict[str, Any] = {"funding_rate": None, "open_interest": None, "order_book": None}
    try:
        fr = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                          params={"symbol": sym, "limit": 1}, timeout=8).json()[0]
        out["funding_rate"] = float(fr["fundingRate"])
    except Exception:
        pass
    try:
        oi = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                          params={"symbol": sym, "period": "5m", "limit": 1}, timeout=8).json()[0]
        out["open_interest"] = float(oi["sumOpenInterest"])
    except Exception:
        pass
    try:
        ob = requests.get("https://api.binance.com/api/v3/depth",
                          params={"symbol": sym, "limit": 5}, timeout=8).json()
        bids = sum(float(b[1]) for b in ob["bids"])
        asks = sum(float(a[1]) for a in ob["asks"])
        imb = round(100*(bids-asks)/(bids+asks), 2) if (bids+asks) else 0
        out["order_book"] = {"bids_qty": bids, "asks_qty": asks, "imbalance_pct": imb}
    except Exception:
        pass
    return out

# ───────────────── PUSH TO GIST ───────────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}", "Accept": "application/vnd.github+json"}
    files = {FILE_NAME: {"content": json.dumps(payload, indent=2, allow_nan=False)}}
    r = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json={"files": files}, timeout=10)
    r.raise_for_status()

# ───────────────────────── MAIN ───────────────────────────────

def main() -> None:
    df = enrich(taapi_candles())
    last = df.iloc[-1]
    macro = fetch_macro(SYMBOL)

    payload = {
        "timestamp":    int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol":       SYMBOL.replace("/", ""),
        "granularity":  INTERVAL,
        "price":        _clean(round(last.close, 6)),
        "high":         _clean(round(last.high, 6)),
        "low":          _clean(round(last.low, 6)),
        "vol":          _clean(round(last.vol, 6)),
        "ema20":        _clean(round(last.ema20, 6)),
        "ema50":        _clean(round(last.ema50, 6)),
        "ema200":       _clean(round(last.ema200, 6)),
        "rsi14":        _clean(round(last.rsi14, 2)),
        "vwap":         _clean(round(last.vwap, 6)),
        "atr14":        _clean(round(last.atr14, 6)),
        "vol_mean20":   _clean(round(last.vol_mean20, 6)),
        "last_300_candles": [
            {k: (_clean(v) if k != "ts" else int(v)) for k, v in row.items()}
            for row in df.to_dict("records")
        ],
        **macro,
        "macro_updated": iso(int(time.time()*1000)),
        "generated_at":  iso(int(time
