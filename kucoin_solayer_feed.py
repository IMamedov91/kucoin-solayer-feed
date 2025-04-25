#!/usr/bin/env python3
"""
kucoin_solayer_feed.py  –  v3
• download 300×15-min candles
• berekent EMA20/50/200, RSI14, VWAP, ATR14, vol_mean20
• uploadt JSON-feed naar GitHub Gist
"""

import os, json, datetime as _dt, time, requests, pandas as pd, ta

KUCOIN_URL   = "https://api.kucoin.com/api/v1/market/candles"
LIMIT        = 300                          # genoeg voor EMA-200
FILE_DEFAULT = "solayer_feed.json"

# ---------- helpers ---------------------------------------------------------
def fetch_frame(symbol: str, tf: str, limit: int = LIMIT) -> pd.DataFrame:
    r = requests.get(KUCOIN_URL, params={"symbol": symbol, "type": tf, "limit": limit}, timeout=10)
    r.raise_for_status()

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(r.json()["data"], columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)                 # KuCoin = milliseconds
    df.sort_values("ts", inplace=True)              # oldest → newest

    # ---------- indicatoren ----------
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

def row_to_dict(r: pd.Series) -> dict:
    return {                     #  ➟ bevat ALLE velden, ook indicatoren
        "ts": int(r.ts),
        "open":  float(r.open),  "close": float(r.close),
        "high":  float(r.high),  "low":  float(r.low),
        "vol":   float(r.vol),

        "ema20": float(r.ema20), "ema50": float(r.ema50), "ema200": float(r.ema200),
        "rsi14": float(r.rsi14), "vwap":  float(r.vwap),
        "atr14": float(r.atr14), "vol_mean20": float(r.vol_mean20)
    }

def iso(ts_ms: int) -> str:
    return _dt.datetime.utcfromtimestamp(ts_ms//1000).isoformat()

def update_gist(token: str, gist_id: str, file_name: str, payload: dict) -> None:
    hdr = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    r = requests.patch(f"https://api.github.com/gists/{gist_id}", headers=hdr, json=body, timeout=10)
    r.raise_for_status()

# ---------- main ------------------------------------------------------------
def main() -> None:
    symbol = os.getenv("SYMBOL",      "SOLAYER-USDT")
    tf     = os.getenv("GRANULARITY", "15min")
    token  = os.environ["GIST_TOKEN"]
    gist   = os.environ["GIST_ID"]
    fname  = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        # ---------- snapshot ----------
        "timestamp":   int(last.ts),             # = candle-tijd!
        "datetime_utc": iso(last.ts),
        "symbol":      symbol,
        "granularity": tf,

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

        # ---------- volledige history ----------
        "last_300_candles": [row_to_dict(r) for _, r in df.tail(LIMIT).iterrows()],

        # ---------- placeholders voor latere afbreiding ----------
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None,

        # optioneel – dit moment van genereren
        "generated_at":  int(time.time()*1000)
    }

    update_gist(token, gist, fname, payload)
    print("✅  Feed updated", iso(int(time.time()*1000)))

if __name__ == "__main__":
    main()
