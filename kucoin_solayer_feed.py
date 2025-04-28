#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
solayer_feed_taapi.py — v1.3  🚀

* Haalt **300** 15‑min OHLCV‑candles via TAAPI `/candles`.
* Berekent EMA20/50/200, RSI14, ATR14, VWAP lokaal (pandas + ta) zodat er nooit `null`‑waarden zijn.
* Voegt funding‑rate & open‑interest (Binance futures) toe voor LAYERUSDT.
* Uploadt JSON‑payload naar GitHub Gist in het overeengekomen schema.
"""

from __future__ import annotations
import datetime as dt, json, os, sys, time, typing as t

import pandas as pd
import requests
import ta

# ---------- CONFIG ----------
SECRET      = os.getenv("TAAPI_SECRET")  or sys.exit("❌ TAAPI_SECRET ontbreekt")
GIST_ID     = os.getenv("GIST_ID")       or sys.exit("❌ GIST_ID ontbreekt")
GIST_TOKEN  = os.getenv("GIST_TOKEN")    or sys.exit("❌ GIST_TOKEN ontbreekt")

SYMBOL      = os.getenv("SYMBOL", "LAYER/USDT")   # TAAPI‐format
INTERVAL    = os.getenv("GRANULARITY", "15m")
SNAP_LEN    = int(os.getenv("SNAP_LEN", "300"))
FILE_NAME   = os.getenv("FILE_NAME", "solayer_feed.json")

TA_BASE     = "https://api.taapi.io"

def iso(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds") + "Z"


# ---------- TAAPI helpers ----------
def taapi_candles(backtrack: int) -> pd.DataFrame:
    """Download `backtrack` candles via TAAPI."""
    params = {
        "secret":   SECRET,
        "exchange": "binance",
        "symbol":   SYMBOL,
        "interval": INTERVAL,
        "backtrack": backtrack
    }
    r = requests.get(f"{TA_BASE}/candles", params=params, timeout=10)
    if r.status_code == 401:
        sys.exit("❌ TAAPI 401 Unauthorized – controleer je API‑key en plan.")
    r.raise_for_status()
    data = r.json()["data"]
    if not data:
        sys.exit("❌ Geen candle‑data terug van TAAPI – controleer symbol of interval.")
    df = pd.DataFrame(data)
    df.rename(columns={"timestamp": "ts", "volume": "vol"}, inplace=True)
    df = df.astype(float, errors="ignore").sort_values("ts").reset_index(drop=True)
    return df.tail(backtrack)

# ---------- macro ----------
def funding_and_oi() -> tuple[float|None, float|None]:
    try:
        fr = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                          params={"symbol":"LAYERUSDT","limit":1}, timeout=10).json()[0]
        oi = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                          params={"symbol":"LAYERUSDT","period":"5m","limit":1}, timeout=10).json()[0]
        return float(fr["fundingRate"]), float(oi["sumOpenInterest"])
    except Exception:
        return None, None

# ---------- indicator calculation ----------
def enrich_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(df["high"], df["low"],
                                                           df["close"], df["vol"], 14)
    return df

# ---------- Gist push ----------
def push_gist(payload: dict) -> None:
    headers = {"Authorization": f"token {GIST_TOKEN}",
               "Accept": "application/vnd.github+json"}
    body = {"files": {FILE_NAME: {"content": json.dumps(payload, indent=2)}}}
    requests.patch(f"https://api.github.com/gists/{GIST_ID}",
                   headers=headers, json=body, timeout=10).raise_for_status()

# ---------- main ----------
def main() -> None:
    df = enrich_indicators(taapi_candles(SNAP_LEN))
    last = df.iloc[-1]

    funding_rate, open_interest = funding_and_oi()

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
        "last_300_candles": (
            df[["ts","open","close","high","low","vol"]].tail(SNAP_LEN)
              .to_dict("records")
        ),
        "funding_rate":  funding_rate,
        "open_interest": open_interest,
        "order_book":    None,
        "generated_at":  iso(int(time.time()*1000))
    }

    push_gist(payload)
    print("✅ SOLayer TAAPI-feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
