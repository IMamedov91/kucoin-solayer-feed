#!/usr/bin/env python3
"""
kucoin_solayer_feed.py  –  uitgebreid:
• download 800×15-min candles
• berekent EMA20/50/200, RSI14, VWAP, ATR14
• uploadt snapshot + laatste 300 candles naar je Gist
"""

import os, json, time, datetime as _dt, requests, pandas as pd, ta   #  pip install ta pandas
# ----------------- constante -----------------------------------------------
KUCOIN_URL   = "https://api.kucoin.com/api/v1/market/candles"
CANDLE_LIMIT = 800                # ≥200 voor ema200, beetje marge
FILE_DEFAULT = "solayer_feed.json"

pd.options.mode.copy_on_write = True

# ----------------- helpers --------------------------------------------------
def fetch_frame(symbol: str, granularity: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
    """Haalt candles op en berekent indicatoren."""
    params = {"symbol": symbol, "type": granularity, "limit": limit}
    r = requests.get(KUCOIN_URL, params=params, timeout=10)
    r.raise_for_status()

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(r.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True, ignore_index=True)      # oud → nieuw

    # -------- indicatoren ---------------------------------------------------
    df["ema20"]   = ta.trend.ema_indicator(df["close"],  20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"],  50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200).ffill()
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                       df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                       df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20).mean()
    return df


def row_to_dict(row: pd.Series) -> dict:
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
    """10-cijfer (sec) of 13-cijfer (ms) → ISO-datetime UTC."""
    if ts_raw > 1e12:                           # ms
        ts_raw //= 1000
    return _dt.datetime.utcfromtimestamp(ts_raw).isoformat()


# ----------------- main -----------------------------------------------------
def main() -> None:
    symbol      = os.getenv("SYMBOL",      "SOLAYER-USDT")
    granularity = os.getenv("GRANULARITY", "15min")
    token       = os.environ["GIST_TOKEN"]
    gist_id     = os.environ["GIST_ID"]
    file_name   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, granularity)     # 800 candles
    last = df.iloc[-1]

    payload = {
        # snapshot
        "timestamp":     int(last.ts),
        "datetime_utc":  to_iso(last.ts),
        "price":         float(last.close),
        "high":          float(last.high),
        "low":           float(last.low),
        "vol":           float(last.vol),
        "ema20":         float(last.ema20),
        "ema50":         float(last.ema50),
        "ema200":        float(last.ema200),
        "rsi14":         float(last.rsi14),
        "vwap":          float(last.vwap),
        "atr14":         float(last.atr14),
        "vol_mean20":    float(last.vol_mean20),

        # laatste 300 candles
        "last_300_candles": [row_to_dict(r) for _, r in df.tail(300).iterrows()],

        # placeholders
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None
    }

    update_gist(token, gist_id, file_name, payload)
    print("✔ feed updated", time.strftime("%F %T", time.gmtime()))


if __name__ == "__main__":
    main()
