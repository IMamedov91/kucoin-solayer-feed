
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v1.4.3  ✅ stable Binance edition

* Fetches up to 300 × 15‑min candles for LAYER/USDT from Binance spot
  via TAAPI `/candles`, chunked in 99‑bar calls (free‑tier limit).
* Calculates EMA20/50/200, RSI14, ATR14, VWAP & vol_mean20 locally.
* Adds Binance‑futures funding‑rate, open‑interest and 5‑level order‑book
  imbalance for LAYERUSDT perpetual.
* Uploads the payload to a GitHub Gist.
"""

from __future__ import annotations
import datetime as dt, json, os, sys, time
from typing import Any, List, Dict

import requests
import pandas as pd
import ta

# ───────────────────────── CONFIG ────────────────────────────
SECRET     = os.getenv("TAAPI_SECRET") or sys.exit("❌ TAAPI_SECRET fehlt")
GIST_ID    = os.getenv("GIST_ID")      or sys.exit("❌ GIST_ID fehlt")
GIST_TOKEN = os.getenv("GIST_TOKEN")   or sys.exit("❌ GIST_TOKEN fehlt")

EXCHANGE      = os.getenv("EXCHANGE", "binance")      # binance | binancefutures | kucoin …
SYMBOL        = os.getenv("SYMBOL", "LAYER/USDT")     # spot-style with slash
INTERVAL      = os.getenv("INTERVAL", "15m")
SNAP_LEN      = int(os.getenv("SNAP_LEN", "300"))     # nr of bars in final snapshot
FILE_NAME     = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_URL     = "https://api.taapi.io/candles"

iso = lambda ts_ms: dt.datetime.utcfromtimestamp(ts_ms/1000).isoformat(timespec="seconds")+"Z"

# ───────────────────────── TAAPI ─────────────────────────────
def taapi_candles(backtracks: int = SNAP_LEN) -> pd.DataFrame:
    """Fetch `backtracks` candles in 99‑bar chunks (free plan)."""
    CHUNK = 99
    frames: List[pd.DataFrame] = []
    fetched = 0

    while fetched < backtracks:
        want = min(CHUNK, backtracks - fetched)
        params = {
            "secret":     SECRET,
            "exchange":   EXCHANGE,
            "symbol":     SYMBOL,
            "interval":   INTERVAL,
            "backtracks": want,
            "backtrack":  fetched,
            "format":     "JSON",
        }
        r = requests.get(TAAPI_URL, params=params, timeout=10)
        data = r.json()

        # handle error payloads
        if isinstance(data, dict) and data.get("status") == "error":
            sys.exit(f"❌ TAAPI error: {data.get('message')}")
        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        if not isinstance(data, list) or len(data) == 0:
            sys.exit("❌ TAAPI returned no data.")
        df_chunk = pd.DataFrame(data)
        frames.append(df_chunk)
        fetched += len(df_chunk)
        time.sleep(0.2)  # polite pause

    df = pd.concat(frames, ignore_index=True).drop_duplicates("timestamp").sort_values("timestamp")
    df.rename(columns={"timestamp": "ts", "open": "open", "close": "close",
                       "high": "high", "low": "low", "volume": "vol"}, inplace=True)
    df["ts"] = df["ts"].astype(int)
    return df.tail(backtracks).reset_index(drop=True)

# ────────────────── INDICATORS LOCAL ────────────────────────
def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(df["high"], df["low"], df["close"], df["vol"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=20).mean()
    df.fillna(method="bfill", inplace=True)
    return df

# ──────────────── MACRO DATA (FUTURES) ──────────────────────
def fetch_macro() -> Dict[str, Any]:
    sym_fut = SYMBOL.replace("/", "")  # LAYERUSDT
    out: Dict[str, Any] = {}
    try:
        fr = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                          params={"symbol": sym_fut, "limit": 1}, timeout=10).json()[0]
        out["funding_rate"] = float(fr["fundingRate"])
    except Exception:
        out["funding_rate"] = None
    try:
        oi = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                          params={"symbol": sym_fut, "period": "5m", "limit": 1}, timeout=10).json()[0]
        out["open_interest"] = float(oi["sumOpenInterest"])
    except Exception:
        out["open_interest"] = None
    try:
        ob = requests.get("https://fapi.binance.com/fapi/v1/depth",
                          params={"symbol": sym_fut, "limit": 5}, timeout=10).json()
        bids_qty = sum(float(b[1]) for b in ob["bids"])
        asks_qty = sum(float(a[1]) for a in ob["asks"])
        imb = 100 * (bids_qty - asks_qty) / (bids_qty + asks_qty)
        out["order_book"] = {"bids_qty": bids_qty, "asks_qty": asks_qty, "imbalance_pct": imb}
    except Exception:
        out["order_book"] = None
    out["macro_updated"] = iso(int(time.time()*1000))
    return out

# ───────────────────── PUSH TO GIST ─────────────────────────
def push_gist(payload: Dict[str, Any]) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}", "Accept": "application/vnd.github+json"}
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=body, timeout=10).raise_for_status()

# ───────────────────────── MAIN ────────────────────────────
def main() -> None:
    raw = taapi_candles(SNAP_LEN)
    df  = enrich_indicators(raw)
    last = df.iloc[-1]

    payload = {
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
    }
    payload.update(fetch_macro())
    push_gist(payload)
    print("✅ Feed uploaded at", payload["datetime_utc"])

if __name__ == "__main__":
    main()
