#!/usr/bin/env python3
"""
kucoin_solayer_feed.py
---------------------------------
• haalt de laatste 300×15-min candles van KuCoin (≈ 3 dagen)
• berekent: EMA-20/50/200, RSI-14, VWAP-14, ATR-14, volume-mean-20
• uploadt naar je Gist:
    – snapshot van de meest recente candle + alle indicatoren
    – volledige candle-historie (last_300_candles)  --> nodig voor de ChatGPT-strategie
    – placeholders voor funding-rate, open-interest, order-book (uitbreidbaar)
"""

import os
import json
import time
import datetime as _dt
from typing import Dict, Any, List

import requests
import pandas as pd
import ta                       #  pip install ta pandas

# ---------- Config ----------------------------------------------------------
KUCOIN_URL   = "https://api.kucoin.com/api/v1/market/candles"
CANDLE_LIMIT = 300                              # genoeg voor EMA-200
FILE_DEFAULT = "solayer_feed.json"

pd.options.mode.copy_on_write = True            # suppress copy-warnings

# ---------- Helpers ---------------------------------------------------------
def fetch_frame(symbol: str, granularity: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    """Download candles en voeg indicator-kolommen toe."""
    params = {"symbol": symbol, "type": granularity, "limit": limit}
    resp   = requests.get(KUCOIN_URL, params=params, timeout=10)
    resp.raise_for_status()

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(resp.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)                     # oldest → newest

    # ---- Indicatoren -------------------------------------------------------
    df["ema20"]   = ta.trend.ema_indicator(df["close"],  20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"],  50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"],         14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                      df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                      df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20).mean()

    return df


def row_to_dict(row: pd.Series) -> Dict[str, float]:
    """Neem één candle-row, retourneer slank dict (ts/open/close/high/low/vol)."""
    return {
        "ts":    int(row.ts),
        "open":  float(row.open),
        "close": float(row.close),
        "high":  float(row.high),
        "low":   float(row.low),
        "vol":   float(row.vol),
    }


def to_datetime(ts_raw: int) -> str:
    """Zet KuCoin-timestamp (ms) om naar ISO-string in UTC."""
    ts = int(ts_raw)
    if ts > 1e12:          # 13-digits  => ms
        ts //= 1000
    return _dt.datetime.utcfromtimestamp(ts).isoformat()


def update_gist(token: str, gist_id: str, file_name: str, payload: Dict[str, Any]) -> None:
    """PATCH de Gist met nieuwe JSON-payload."""
    url = f"https://api.github.com/gists/{gist_id}"
    headers = {
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github+json",
        "User-Agent":    "solayer-feed-bot"
    }
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    resp = requests.patch(url, headers=headers, json=body, timeout=10)
    resp.raise_for_status()


# ---------- Main ------------------------------------------------------------
def main() -> None:
    symbol      = os.getenv("SYMBOL",      "SOLAYER-USDT")
    granularity = os.getenv("GRANULARITY", "15min")
    token       = os.environ["GIST_TOKEN"]          # verplicht in workflow-secret
    gist_id     = os.environ["GIST_ID"]             # idem
    file_name   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, granularity)
    last = df.iloc[-1]

    payload: Dict[str, Any] = {
        # -------- snapshot (laatste candle) --------
        "timestamp":    int(last.ts),
        "datetime_utc": to_datetime(last.ts),
        "price":        float(last.close),
        "high":         float(last.high),
        "low":          float(last.low),
        "vol":          float(last.vol),

        # ---- indicatoren op deze candle ----
        "ema20":        float(last.ema20),
        "ema50":        float(last.ema50),
        "ema200":       float(last.ema200),
        "rsi14":        float(last.rsi14),
        "vwap":         float(last.vwap),
        "atr14":        float(last.atr14),
        "vol_mean20":   float(last.vol_mean20),

        # -------- volledige geschiedenis --------
        "last_300_candles": [row_to_dict(r) for _, r in df.tail(CANDLE_LIMIT).iterrows()],

        # -------- placeholders voor uitbreidingen --------
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None
    }

    update_gist(token, gist_id, file_name, payload)
    print("✅ Feed updated @", time.strftime("%F %T", time.gmtime()))


if __name__ == "__main__":
    main()
