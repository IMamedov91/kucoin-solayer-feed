#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
solayer_feed_taapi.py — v1.4  ⚡️ Full‑stack feed

* 300 × 15‑min OHLCV‑candles via TAAPI `/candles` (cheap 1 call).
* Indicators **lokaal** berekend (EMA20/50/200, RSI14, ATR14, VWAP, vol_mean20) → nooit NaN.
* Binance‑futures **funding rate** & **open interest** opgehaald; order‑book imbalance depth‑5 toegevoegd.
* Schema sluit exact aan op eerdere afspraken; granularity = "15m".
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

# ──────────────────────────── CONFIG ─────────────────────────────
SECRET       = os.getenv("TAAPI_SECRET") or sys.exit("❌ TAAPI_SECRET ontbreekt")
GIST_ID      = os.getenv("GIST_ID")      or sys.exit("❌ GIST_ID ontbreekt")
GIST_TOKEN   = os.getenv("GIST_TOKEN")   or sys.exit("❌ GIST_TOKEN ontbreekt")

SYMBOL       = os.getenv("SYMBOL", "LAYER/USDT")  # TAAPI‑formaat
INTERVAL     = os.getenv("GRANULARITY", "15m")     # '15m', '1h', …
SNAP_LEN     = int(os.getenv("SNAP_LEN", 300))      # candles in snapshot
FILE_NAME    = os.getenv("FILE_NAME", "solayer_feed.json")
TAAPI_CANDLES = "https://api.taapi.io/candles"

iso = lambda ms: dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"

# ─────────────────────── TAAPI / OHLCV ───────────────────────────

def taapi_candles(backtrack: int = SNAP_LEN) -> pd.DataFrame:
    """Haalt `backtrack` candles op via TAAPI `/candles`.
    Vangt de 3 mogelijke reply‑vormen af:
    1. **list[dict]**  ← sucsesvol
    2. **{"data": [...]}**  ← white‑label licenties
    3. **{"status":"error", ...}**  ← foutmeldingen
    """
    params = {
        "secret": SECRET,
        "exchange": "binance",
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "backtrack": backtrack,
        "format": "JSON",
    }
    r = requests.get(TAAPI_CANDLES, params=params, timeout=15)
    if r.status_code == 401:
        sys.exit("❌ TAAPI 401 Unauthorized – controleer API‑key/plan.")

    # TAAPI returnt altijd 200, zelfs bij error‑payloads → check body
    data: Any = r.json()

    # Case 3 – expliciete error
    if isinstance(data, dict) and data.get("status") == "error":
        msg = data.get("message", "onbekende fout")
        code = data.get("code", "–")
        sys.exit(f"❌ TAAPI error ({code}): {msg}")

    # Case 2 – wrapper‑dict met data‑key
    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    # Nu moet `data` een list zijn
    if not isinstance(data, list) or len(data) < 20:
        sys.exit(f"❌ Onverwachte of te korte TAAPI‑payload (len={len(data) if isinstance(data, list) else 'N/A'})")

    df = (
        pd.DataFrame(data)
          .rename(columns={"timestamp": "ts", "volume": "vol"})
          [["ts", "open", "close", "high", "low", "vol"]]
          .astype(float, errors="ignore")
          .drop_duplicates("ts")
          .sort_values("ts")
          .reset_index(drop=True)
    )
    return df.tail(SNAP_LEN)

# ───────────────────── Indicator‑berekening ‑ lokaal ─────────────

def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.set_index("ts", inplace=True)

    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)

    df["vwap"] = (
        (df["close"] * df["vol"]).cumsum() / df["vol"].cumsum()
    )
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=20).mean()

    # NaN‑opvulling (forward‑fill na warm‑up, back‑fill voor eerste waarden)
    df.ffill(inplace=True)
    df.bfill(inplace=True)

    return df.reset_index()

# ───────────────────── Macro‑data (funding / OI / book) ──────────

def fetch_macro(bin_sym: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"funding_rate": None, "open_interest": None, "order_book": None}
    try:
        # funding rate laatste settlement
        fr = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": bin_sym, "limit": 1}, timeout=10
        ).json()[0]
        out["funding_rate"] = float(fr["fundingRate"])

        # open interest (hist endpoint kan leeg zijn voor alt‑pairs)
        oi = requests.get(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": bin_sym, "period": "5m", "limit": 1}, timeout=10
        ).json()[0]
        out["open_interest"] = float(oi["sumOpenInterest"])
    except Exception:
        pass  # laat op None

    try:
        depth = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": bin_sym, "limit": 5}, timeout=10
        ).json()
        bids_qty = sum(float(b[1]) for b in depth.get("bids", []))
        asks_qty = sum(float(a[1]) for a in depth.get("asks", []))
        if bids_qty + asks_qty > 0:
            imb = (bids_qty - asks_qty) / (bids_qty + asks_qty) * 100
            out["order_book"] = {
                "depth5_bids_qty": bids_qty,
                "depth5_asks_qty": asks_qty,
                "imbalance_pct": round(imb, 2),
            }
    except Exception:
        pass

    return out

# ────────────────────── PUSH TO GIST ─────────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    requests.patch(
        f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=body, timeout=15
    ).raise_for_status()

# ─────────────────────────── MAIN ────────────────────────────────

def main() -> None:
    # 1 – raw candles
    candles = taapi_candles(SNAP_LEN)

    # 2 – indicators lokaal
    df = enrich_indicators(candles.copy())
    last = df.iloc[-1]

    # 3 – macro‑data
    binance_symbol = SYMBOL.replace("/", "").upper()  # LAYERUSDT
    macro = fetch_macro(binance_symbol)

    # 4 – payload
    payload: Dict[str, Any] = {
        "timestamp": int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol": SYMBOL.replace("/", ""),
        "granularity": INTERVAL,
        "price": float(round(last.close, 6)),
        "high": float(round(last.high, 6)),
        "low": float(round(last.low, 6)),
        "vol": float(round(last.vol, 2)),
        "ema20": float(round(last.ema20, 6)),
        "ema50": float(round(last.ema50, 6)),
        "ema200": float(round(last.ema200, 6)),
        "rsi14": float(round(last.rsi14, 2)),
        "vwap": float(round(last.vwap, 6)),
        "atr14": float(round(last.atr14, 6)),
        "vol_mean20": float(round(last.vol_mean20, 2)),
        "last_300_candles": df.tail(SNAP_LEN).to_dict("records"),
        **macro,
        "generated_at": iso(int(time.time() * 1000)),
    }

    # 5 – push naar Gist
    push_gist(payload)
    print("✅ SOLayer TAAPI‑feed v1.4 geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
