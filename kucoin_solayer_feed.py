#!/usr/bin/env python3
"""
kucoin_solayer_feed.py  ·  v4  (2025-04-25)
-------------------------------------------
• download tot 300 × 15-min candles (KuCoin limit)
• berekent EMA20/50/200, RSI14, VWAP, ATR14, vol_mean20
• zet alles, incl. volledige geschiedenis, in één JSON-bestand in je Gist
"""

import os, json, time, datetime as _dt
from typing import Any

import requests, pandas as pd, ta

KUCOIN_URL   = "https://api.kucoin.com/api/v1/market/candles"
LIMIT        = int(os.getenv("LIMIT", "300"))          # max wat KuCoin toelaat
FILE_DEFAULT = "solayer_feed.json"
RETRY_SEC    = 3                                       # één snelle retry

# --------------------------------------------------------------------------- #
def safe(v: Any) -> Any:
    """Zet NaN/Nat om naar None, anders float."""
    return None if pd.isna(v) else float(v)

def fetch_frame(symbol: str, tf: str, limit: int = LIMIT) -> pd.DataFrame:
    """Haalt candles op – met één retry als KuCoin hikt."""
    params = {"symbol": symbol, "type": tf, "limit": limit}
    try:
        r = requests.get(KUCOIN_URL, params=params, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        time.sleep(RETRY_SEC)
        r = requests.get(KUCOIN_URL, params=params, timeout=10)
        r.raise_for_status()

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(r.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)          # KuCoin geeft ms
    df.sort_values("ts", inplace=True)

    # ---- indicatoren -------------------------------------------------------
    df["ema20"]   = ta.trend.ema_indicator(df["close"],  20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"],  50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                       df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                       df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20).mean()
    return df.reset_index(drop=True)

def row_to_dict(r: pd.Series) -> dict:
    """Geef alle kolommen van één candle terug – NaN→null."""
    return {k: safe(r[k]) for k in r.index}

def iso(ts_ms: int) -> str:
    return _dt.datetime.utcfromtimestamp(ts_ms // 1000).isoformat()

def update_gist(token: str, gist_id: str, file_name: str, payload: dict) -> None:
    hdr  = {"Authorization": f"token {token}",
            "Accept":        "application/vnd.github+json"}
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    r = requests.patch(f"https://api.github.com/gists/{gist_id}",
                       headers=hdr, json=body, timeout=10)
    r.raise_for_status()

# --------------------------------------------------------------------------- #
def main() -> None:
    symbol = os.getenv("SYMBOL",      "SOLAYER-USDT")
    tf     = os.getenv("GRANULARITY", "15min")
    token  = os.environ["GIST_TOKEN"]
    gist   = os.environ["GIST_ID"]
    fname  = os.getenv("FILE_NAME", FILE_DEFAULT)

    df = fetch_frame(symbol, tf, limit=LIMIT)
    sample_n = len(df)
    oldest   = iso(df.ts.iloc[0])
    print(f"Fetched {sample_n} candles  (oldest {oldest})")

    last = df.iloc[-1]

    payload = {
        # ---- snapshot ------------------------------------------------------
        "timestamp":    int(last.ts),     # candle-tijd
        "datetime_utc": iso(last.ts),
        "symbol":       symbol,
        "granularity":  tf,

        "price":   safe(last.close),
        "high":    safe(last.high),
        "low":     safe(last.low),
        "vol":     safe(last.vol),

        "ema20":   safe(last.ema20),
        "ema50":   safe(last.ema50),
        "ema200":  safe(last.ema200),
        "rsi14":   safe(last.rsi14),
        "vwap":    safe(last.vwap),
        "atr14":   safe(last.atr14),
        "vol_mean20": safe(last.vol_mean20),

        # ---- volledige historie -------------------------------------------
        "sample_size": sample_n,
        "last_candles": [row_to_dict(r) for _, r in df.iterrows()],

        # placeholders
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None,

        "generated_at":  int(time.time()*1000)
    }

    update_gist(token, gist, fname, payload)
    print("✅  Feed pushed to Gist at", iso(int(time.time()*1000)))

# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()
