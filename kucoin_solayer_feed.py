#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v1.6  🟢 **FULL SOURCE (≈240 regels)**

• Haalt max 300 × 15‑min candles voor LAYER/USDT (Binance‑spot) via TAAPI `/candles` in 99‑bar chunks.
• Indicatoren: EMA20/50/200, RSI14, ATR14, VWAP, vol_mean20 — lokaal berekend (ta‑lib).
• Futures‑macro: funding‑rate, open‑interest & depth‑5 order‑book‑imbalance voor LAYERUSDT perpetual.
• Schrijft payload naar GitHub Gist.

Env‑vars (workflow):
  TAAPI_SECRET  | GIST_TOKEN  | GIST_ID  | EXCHANGE=binance | SYMBOL=LAYER/USDT | INTERVAL=15m | SNAP_LEN=99/300 | FILE_NAME=solayer_feed.json
"""

from __future__ import annotations
import datetime as dt, json, os, sys, time
from typing import Any, Dict, List

import requests
import pandas as pd
import ta

# ───────────────────────── CONFIG ────────────────────────────
SECRET     = os.getenv("TAAPI_SECRET") or sys.exit("❌ TAAPI_SECRET ontbreekt")
GIST_ID    = os.getenv("GIST_ID")      or sys.exit("❌ GIST_ID ontbreekt")
GIST_TOKEN = os.getenv("GIST_TOKEN")   or sys.exit("❌ GIST_TOKEN ontbreekt")

EXCHANGE   = os.getenv("EXCHANGE", "binance")      # binance | binancefutures | kucoin …
SYMBOL     = os.getenv("SYMBOL",   "LAYER/USDT")   # spot‑paar
INTERVAL   = os.getenv("INTERVAL", "15m")
SNAP_LEN   = int(os.getenv("SNAP_LEN", "99"))      # max 99 op free‑tier
FILE_NAME  = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_URL  = "https://api.taapi.io/candles"
iso = lambda ms: dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds")+"Z"

# ─────────────────── TAAPI CANDLES (chunk loop) ──────────────

def taapi_candles(backtracks: int = SNAP_LEN) -> pd.DataFrame:
    """Download `backtracks` candles in 99‑bar batches (free plan limiet)."""
    CHUNK = 99
    frames: List[pd.DataFrame] = []
    fetched = 0

    while fetched < backtracks:
        want = min(CHUNK, backtracks - fetched)
        params = {
            "secret":   SECRET,
            "exchange": EXCHANGE,
            "symbol":   SYMBOL,
            "interval": INTERVAL,
            "backtrack": fetched,
            "range":     want,
            "format":    "JSON",
        }
        r = requests.get(TAAPI_URL, params=params, timeout=10)
        data = r.json()

        if isinstance(data, dict) and data.get("status") == "error":
            sys.exit(f"❌ TAAPI error: {data.get('message')}")
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if not isinstance(data, list) or len(data) == 0:
            sys.exit("❌ TAAPI returned no data. Check symbol/exchange plan.")
        df_chunk = pd.DataFrame(data)
        frames.append(df_chunk)
        fetched += len(df_chunk)
        time.sleep(0.2)  # respect rate‑limit

    df = pd.concat(frames, ignore_index=True).drop_duplicates("timestamp").sort_values("timestamp")
    df.rename(columns={
        "timestamp": "ts", "open": "open", "close": "close",
        "high": "high", "low": "low", "volume": "vol"
    }, inplace=True)
    df["ts"] = df["ts"].astype(int)
    return df.tail(backtracks).reset_index(drop=True)

# ──────────────── INDICATOR‑CALCULATIE ──────────────────────

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

# ─────────────── FUTURES MACRO (funding, OI, OB) ────────────

def futures_macro() -> Dict[str, Any]:
    sym = SYMBOL.replace("/", "")  # LAYERUSDT
    out: Dict[str, Any] = {}
    try:
        fr = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                          params={"symbol": sym, "limit": 1}, timeout=10).json()[0]
        out["funding_rate"] = float(fr["fundingRate"])
    except Exception:
        out["funding_rate"] = None
    try:
        oi = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                          params={"symbol": sym, "period": "5m", "limit": 1}, timeout=10).json()[0]
        out["open_interest"] = float(oi["sumOpenInterest"])
    except Exception:
        out["open_interest"] = None
    try:
        ob = requests.get("https://fapi.binance.com/fapi/v1/depth",
                          params={"symbol": sym, "limit": 5}, timeout=10).json()
        bids = sum(float(b[1]) for b in ob["bids"])
        asks = sum(float(a[1]) for a in ob["asks"])
        out["order_book"] = {
            "bids_qty": bids,
            "asks_qty": asks,
            "imbalance_pct": 100 * (bids - asks) / (bids + asks) if bids + asks else None,
        }
    except Exception:
        out["order_book"] = None
    out["macro_updated"] = iso(int(time.time()*1000))
    return out

# ───────────────────── GIST PUSHEN ──────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}", "Accept": "application/vnd.github+json"}
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=body, timeout=10).raise_for_status()

# ───────────────────────── MAIN ─────────────────────────────

def main() -> None:
    raw = taapi_candles(SNAP_LEN)
    df  = enrich(raw)
    last = df.iloc[-1]

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
        "vol_mean20":   float(last.vol_mean20),
        "last_300_candles": df.to_dict("records"),
        "generated_at": iso(int(time.time()*1000)),
    }
    payload.update(futures_macro())
    push_gist(payload)
    print("✅ Feed uploaded", payload["datetime_utc"], f"({len(df)} candles)")

if __name__ == "__main__":
    main()
