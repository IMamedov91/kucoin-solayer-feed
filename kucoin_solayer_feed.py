#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
solayer_feed_taapi.py — v1.4.2  ✅ indent‑clean version

* 300 × 15‑min candles via **TAAPI `/candles`** (one GET call).
* Indicators (EMA20/50/200, RSI14, ATR14, VWAP, vol_mean20) calculated **locally**.
* Adds Binance‑futures **funding‑rate**, **open‑interest** and 5‑level **order‑book imbalance**.
* Payload schema matches earlier agreed structure; `granularity` is "15m".
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

SYMBOL       = os.getenv("SYMBOL", "LAYER/USDT")
INTERVAL     = os.getenv("GRANULARITY", "15m")
SNAP_LEN     = int(os.getenv("SNAP_LEN", "300"))
FILE_NAME    = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_URL    = "https://api.taapi.io/candles"

iso = lambda ms: dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"

# ────────────────────── HELPER FUNCTIONS ───────────────────────

def _clean(val: Any) -> Any:
    """Map NaN / inf to None so JSON dumps cleanly."""
    if val is None:
        return None
    try:
        import math
        if math.isnan(val) or math.isinf(val):
            return None
    except Exception:
        pass
    return val

# ───────────────────── TAAPI CANDLES FETCH ─────────────────────

def taapi_candles(backtracks: int = SNAP_LEN) -> pd.DataFrame:
    """Fetch up to `backtracks` candles; TAAPI free tier returns max 99 per call.
    We therefore loop in ≤99‑candle chunks until we have enough data.
    """

    CHUNK = 99  # free‑tier ceiling; pro‑tiers allow 500
    frames: List[pd.DataFrame] = []
    fetched = 0

    while fetched < backtracks:
        want = min(CHUNK, backtracks - fetched)
        params = {
            "secret": SECRET,
            "exchange": "binance",
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "backtracks": want,
            "backtrack": fetched,   # offset
            "format": "JSON",
        }
        r = requests.get(TAAPI_URL, params=params, timeout=10)
        if r.status_code == 401:
            sys.exit("❌ TAAPI 401 Unauthorized – controleer je API‑key/plan.")

        payload = r.json()
        if isinstance(payload, dict):
            if payload.get("status") == "error":
                sys.exit(f"❌ TAAPI error: {payload.get('message')}")
            payload = payload.get("data", [])

        if not payload:
            break  # TAAPI stuurde gewoon niks terug → einde historiek

        tmp = pd.DataFrame(payload)[["timestamp", "open", "close", "high", "low", "volume"]]
        tmp.rename(columns={"timestamp": "ts", "volume": "vol"}, inplace=True)
        tmp = tmp.astype(float)
        frames.append(tmp)
        fetched += len(tmp)
        time.sleep(0.25)  # soft‑throttle to avoid 429

        if len(tmp) < want:
            break  # minder bars dan gevraagd → historiek op

    if not frames:
        sys.exit("❌ TAAPI gaf geen data terug.")

    df = pd.concat(frames).sort_values("ts").tail(backtracks).reset_index(drop=True)
    if len(df) < 20:
        sys.exit(f"❌ Te weinig candles ontvangen ({len(df)}) – plan upgrade nodig of verkeerde symbol.")
    return df

# ───────────────────── INDICATOR CALCULATIONS ──────────────────

def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Adds EMA20/50/200, RSI14, ATR14, VWAP, vol_mean20 columns."""
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(
        df["high"], df["low"], df["close"], df["vol"], 14
    )
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=20).mean()

    # fill NaNs caused by warm‑up periods
    ind_cols = [
        "ema20", "ema50", "ema200", "rsi14", "atr14", "vwap", "vol_mean20",
    ]
    df[ind_cols] = df[ind_cols].bfill()
    return df

# ───────────────────── BINANCE MACRO DATA ──────────────────────

def fetch_funding_oi(symbol: str) -> Dict[str, Any]:
    sym = symbol.replace("/", "").upper()
    out: Dict[str, Any] = {"funding_rate": None, "open_interest": None}
    try:
        fr = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": sym, "limit": 1}, timeout=8
        ).json()[0]
        out["funding_rate"] = float(fr["fundingRate"])
    except Exception:
        pass
    try:
        oi = requests.get(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": sym, "period": "5m", "limit": 1}, timeout=8
        ).json()[0]
        out["open_interest"] = float(oi["sumOpenInterest"])
    except Exception:
        pass
    return out


def fetch_order_book(symbol: str) -> Dict[str, Any]:
    """Depth‑5 order book imbalance (Binance spot)."""
    sym = symbol.replace("/", "").upper()
    try:
        ob = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": sym, "limit": 5}, timeout=8
        ).json()
        bids_qty = sum(float(b[1]) for b in ob["bids"])
        asks_qty = sum(float(a[1]) for a in ob["asks"])
        imb_pct = (bids_qty - asks_qty) / (bids_qty + asks_qty) * 100 if (bids_qty + asks_qty) else 0
        return {
            "depth5": {
                "bids_qty": bids_qty,
                "asks_qty": asks_qty,
                "imbalance_pct": round(imb_pct, 2),
            },
            "last_updated": int(time.time() * 1000),
        }
    except Exception:
        return {}

# ─────────────────────── PUSH TO GIST ──────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    body = {
        "files": {FILE_NAME: {"content": json.dumps(payload, indent=2, allow_nan=False)}},
    }
    r = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=body, timeout=10)
    r.raise_for_status()

# ─────────────────────────── MAIN ──────────────────────────────

def main() -> None:
    df_raw = taapi_candles()
    df     = enrich_indicators(df_raw.copy())

    if len(df) < 20:
        sys.exit("❌ Minder dan 20 candles ontvangen – stop.")

    last = df.iloc[-1]

    macro = fetch_funding_oi(SYMBOL)
    order_book = fetch_order_book(SYMBOL)

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
        "order_book":    order_book or None,
        "macro_updated": iso(int(time.time() * 1000)),
        "generated_at":  iso(int(time.time() * 1000)),
    }

    push_gist(payload)
    print("✅ SOLayer feed uploaded:", payload["generated_at"])


if __name__ == "__main__":
    main()
