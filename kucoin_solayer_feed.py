#!/usr/bin/env python3
"""
kucoin_solayer_feed.py  –  uitgebreid:
• download 300×15-min candles
• berekent EMA20/50/200, RSI14, VWAP, ATR14
• uploadt   – snapshot van laatste candle
            – complete history (laatste 300 candles)  ==> nodig voor ChatGPT-strategie
"""

import os
import json
import time
import datetime as _dt

import requests
import pandas as pd
import ta        #  pip install ta pandas

KUCOIN_URL   = "https://api.kucoin.com/api/v1/market/candles"
CANDLE_LIMIT = 300          # genoeg voor EMA-200 / ATR / VWAP
FILE_DEFAULT = "solayer_feed.json"


# ---------- helpers ---------------------------------------------------------
def fetch_frame(symbol: str, granularity: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    params = {"symbol": symbol, "type": granularity, "limit": limit}
    r = requests.get(KUCOIN_URL, params=params, timeout=10)
    r.raise_for_status()

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(r.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)          # oldest → newest

    # --------- indicatoren ----
    df["ema20"]   = ta.trend.ema_indicator(df["close"],  20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"],  50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                       df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                       df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20).mean()
    return df


def row_to_dict(row: pd.Series) -> dict:
    """Converteer één candle-row (met essentiële velden) naar dict."""
    return {
        "ts":     int(row.ts),
        "open":   float(row.open),
        "close":  float(row.close),
        "high":   float(row.high),
        "low":    float(row.low),
        "vol":    float(row.vol)
    }


def update_gist(token: str, gist_id: str, file_name: str, payload: dict) -> None:
    url = f"https://api.github.com/gists/{gist_id}"
    headers = {
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github+json",
        "User-Agent":    "solayer-feed-bot"
    }
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    resp = requests.patch(url, headers=headers, json=body, timeout=10)
    resp.raise_for_status()     # → except bij fout


# ---------- main ------------------------------------------------------------
def main() -> None:
    symbol      = os.getenv("SYMBOL",      "SOLAYER-USDT")
    granularity = os.getenv("GRANULARITY", "15min")
    token       = os.environ["GIST_TOKEN"]
    gist_id     = os.environ["GIST_ID"]
    file_name   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, granularity)
    last = df.iloc[-1]

    payload = {
        # ---- snapshot van meest recente candle + indicatoren ----
        "timestamp": int(last.ts),
        "datetime_utc": _dt.datetime.utcfromtimestamp(int(last.ts / 1000)).isoformat(),
        "price":   float(last.close),
        "high":    float(last.high),
        "low":     float(last.low),
        "vol":     float(last.vol),
        "ema20":   float(last.ema20),
        "ema50":   float(last.ema50),
        "ema200":  float(last.ema200),
        "rsi14":   float(last.rsi14),
        "vwap":    float(last.vwap),
        "atr14":   float(last.atr14),
        "vol_mean20": float(last.vol_mean20),

        # ---- complete history nodig voor ChatGPT-analyse ----
        "last_300_candles": [row_to_dict(r) for _, r in df.tail(CANDLE_LIMIT).iterrows()],

        # placeholders voor latere uitbreidingen
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None
    }

    update_gist(token, gist_id, file_name, payload)
    print("Feed updated @", time.strftime("%F %T", time.gmtime()))


if __name__ == "__main__":
    main()
