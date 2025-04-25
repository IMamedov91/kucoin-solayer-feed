#!/usr/bin/env python3
"""
kucoin_solayer_feed.py – uitgebreid: schrijft laatste candle + indicator snapshot
"""
import os, json, datetime as _dt, requests, pandas as pd
import ta  #  pip install ta pandas

KUCOIN_URL = "https://api.kucoin.com/api/v1/market/candles"

def fetch_frame(symbol: str, granularity: str, limit: int = 300) -> pd.DataFrame:
    params = {"symbol": symbol, "type": granularity, "limit": limit}
    r = requests.get(KUCOIN_URL, params=params, timeout=10)
    r.raise_for_status()
    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df = pd.DataFrame(r.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)

    # ---- indicatoren ----
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(
                      df["high"], df["low"], df["close"], df["vol"], 14)
    df["vol_mean20"] = df["vol"].rolling(20).mean()
    return df

def update_gist(token: str, gist_id: str, file_name: str, payload: dict):
    url = f"https://api.github.com/gists/{gist_id}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "solayer-feed-bot"
    }
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    resp = requests.patch(url, headers=headers, json=body, timeout=10)
    resp.raise_for_status()
    return resp.ok

def main():
    symbol      = os.getenv("SYMBOL",      "SOLAYER-USDT")
    granularity = os.getenv("GRANULARITY", "15min")
    token       = os.environ["GIST_TOKEN"]
    gist_id     = os.environ["GIST_ID"]
    file_name   = os.getenv("FILE_NAME",   "solayer_feed.json")

    df   = fetch_frame(symbol, granularity)
    last = df.iloc[-1]
    payload = {
        "timestamp": int(last.ts),
        "datetime_utc": _dt.datetime.utcfromtimestamp(int(last.ts/1000)).isoformat(),
        "price":   float(last.close),
        "high":    float(last.high),
        "low":     float(last.low),
        "vol":     float(last.vol),
        "ema20":   float(last.ema20),
        "ema50":   float(last.ema50),
        "ema200":  float(last.ema200),
        "rsi14":   float(last.rsi14),
        "vwap":    float(last.vwap),
        "vol_mean20": float(last.vol_mean20)
    }
    update_gist(token, gist_id, file_name, payload)

if __name__ == "__main__":
    main()
