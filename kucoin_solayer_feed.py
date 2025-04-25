#!/usr/bin/env python3
"""
kucoin_solayer_feed.py
----------------------
• download 300 × 15-min candles van KuCoin
• berekent EMA-20/50/200, RSI-14, VWAP, ATR-14
• schrijft een JSON‐payload met:
    – snapshot van de laatste candle + indicatoren
    – volledige reeks laatste 300 candles
"""

import os, time, json, datetime as _dt
import requests, pandas as pd
import ta  #  pip install ta pandas numpy

# ---------------------------------------------------------------------------
KUCOIN_URL   = "https://api.kucoin.com/api/v1/market/candles"
CANDLE_LIMIT = 300
FILE_DEFAULT = "solayer_feed.json"

pd.options.mode.copy_on_write = True           # suppress SettingWithCopyWarning


# ---------------------------------------------------------------------------
def fetch_frame(symbol: str, granularity: str) -> pd.DataFrame:
    """Haalt <CANDLE_LIMIT> candles op en berekent de indicatoren."""
    params = {"symbol": symbol, "type": granularity, "limit": CANDLE_LIMIT}
    r = requests.get(KUCOIN_URL, params=params, timeout=10)
    r.raise_for_status()

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(r.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)                     # oldest → newest

    # ----------------------- indicatoren -------------------------------
    df["ema20"]   = ta.trend.ema_indicator(df["close"],  20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"],  50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                       df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                       df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20).mean()

    # vervang NaN’s aan kop (bv. ema200) door eerste geldige waarde → None is beter dan NaN in JSON
    df.fillna(method="bfill", inplace=True)

    return df


def row_to_dict(row: pd.Series) -> dict:
    """Converteert één candle‐row naar een compact dict (prijzen & volume)."""
    return {
        "ts":    int(row.ts),
        "open":  float(row.open),
        "close": float(row.close),
        "high":  float(row.high),
        "low":   float(row.low),
        "vol":   float(row.vol)
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
    resp.raise_for_status()


def to_iso(ts_raw: int) -> str:
    """Converteert Unix‐tijd in ms of s naar ISO-string (UTC)."""
    if ts_raw > 1e12:                 # ms
        ts_raw //= 1000
    return _dt.datetime.utcfromtimestamp(ts_raw).isoformat()


# ---------------------------------------------------------------------------
def main() -> None:
    symbol      = os.getenv("SYMBOL",      "SOLAYER-USDT")
    granularity = os.getenv("GRANULARITY", "15min")
    token       = os.environ["GIST_TOKEN"]
    gist_id     = os.environ["GIST_ID"]
    file_name   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, granularity)
    last = df.iloc[-1]

    payload = {
        # -------- snapshot --------
        "timestamp": int(last.ts),
        "datetime_utc": to_iso(last.ts),
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

        # -------- volledige reeks --------
        "last_300_candles": [row_to_dict(r) for _, r in df.iterrows()],

        # -------- placeholders --------
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None
    }

    update_gist(token, gist_id, file_name, payload)
    print("[feed] upload success –", time.strftime("%F %T", time.gmtime()))


if __name__ == "__main__":
    main()
