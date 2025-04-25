#!/usr/bin/env python3
"""
kucoin_solayer_feed.py – v4
• verzamelt gegarandeerd 300 × 15-min candles (KuCoin max = 150 per call)
• berekent EMA20/50/200, RSI14, VWAP, ATR14, vol_mean20
• uploadt snapshot + volledige history naar je Gist
"""

import os, json, time, datetime as _dt
import requests, pandas as pd, ta

API_URL   = "https://api.kucoin.com/api/v1/market/candles"
NEEDED    = 300                  # aantal candles in history
TF        = "15min"              # timeframe voor granularity-mapping
SEC_PER_CANDLE = 900             # 15 min in seconden
DEFAULT_FILE  = "solayer_feed.json"

pd.options.mode.copy_on_write = True

# ---------- helpers --------------------------------------------------------
def fetch_batch(symbol: str, granularity: str, limit: int = 150,
                end_at: int | None = None) -> list[list[float]]:
    """Eén API-call – retourneert lijst met candles (nieuwste eerst)."""
    params = {"symbol": symbol, "type": granularity, "limit": limit}
    if end_at is not None:
        params["endAt"] = end_at
    r = requests.get(API_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()["data"]            # formaat: [[ts,open,close,high,low,vol,turnover], …]

def fetch_frame(symbol: str, granularity: str) -> pd.DataFrame:
    """Haalt minimaal 300 candles op door te pagineren met endAt."""
    batches, got = [], 0
    end_at = None                       # None ⇒ meest recente batch

    while got < NEEDED:
        chunk = fetch_batch(symbol, granularity, limit=150, end_at=end_at)
        if not chunk:
            break                      # fail-safe
        batches.extend(chunk)
        got += len(chunk)
        # volgende iteratie: eindpunt = oudste ts − 1 candle
        oldest_ts_ms = float(chunk[-1][0])
        end_at = int(oldest_ts_ms/1000 - SEC_PER_CANDLE)

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(batches, columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)          # oldest → newest
    df = df.tail(NEEDED)                        # precies 300 laatste

    # ---------------- indicatoren ----------------
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

def iso(ts_ms: int) -> str:
    return _dt.datetime.utcfromtimestamp(ts_ms//1000).isoformat()

def row_dict(row: pd.Series) -> dict:
    return {
        "ts": int(row.ts), "open": float(row.open), "close": float(row.close),
        "high": float(row.high), "low": float(row.low), "vol": float(row.vol),
        "ema20": float(row.ema20), "ema50": float(row.ema50),
        "ema200": float(row.ema200), "rsi14": float(row.rsi14),
        "vwap": float(row.vwap), "atr14": float(row.atr14),
        "vol_mean20": float(row.vol_mean20)
    }

def update_gist(token: str, gist_id: str, file_name: str, payload: dict) -> None:
    hdr  = {"Authorization": f"token {token}",
            "Accept": "application/vnd.github+json"}
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    r = requests.patch(f"https://api.github.com/gists/{gist_id}",
                       headers=hdr, json=body, timeout=10)
    r.raise_for_status()

# ---------- main -----------------------------------------------------------
def main() -> None:
    symbol  = os.getenv("SYMBOL", "SOLAYER-USDT")
    token   = os.environ["GIST_TOKEN"]
    gist_id = os.environ["GIST_ID"]
    f_name  = os.getenv("FILE_NAME", DEFAULT_FILE)

    df   = fetch_frame(symbol, TF)
    last = df.iloc[-1]

    payload = {
        "timestamp": int(last.ts),
        "datetime_utc": iso(last.ts),
        "symbol": symbol, "granularity": TF,

        # snapshot-velden
        "price": float(last.close), "high": float(last.high),
        "low": float(last.low),    "vol": float(last.vol),
        "ema20": float(last.ema20), "ema50": float(last.ema50),
        "ema200": float(last.ema200), "rsi14": float(last.rsi14),
        "vwap": float(last.vwap),   "atr14": float(last.atr14),
        "vol_mean20": float(last.vol_mean20),

        # volledige history (laatste 300 stuks)
        "last_300_candles": [row_dict(r) for _, r in df.iterrows()],

        # placeholders
        "funding_rate": None, "open_interest": None, "order_book": None,

        "generated_at": int(time.time()*1000)
    }

    update_gist(token, gist_id, f_name, payload)
    print("✅  300-candle feed geüpload:", iso(int(time.time()*1000)))

if __name__ == "__main__":
    main()
