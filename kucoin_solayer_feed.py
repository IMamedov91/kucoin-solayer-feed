#!/usr/bin/env python3
"""
kucoin_solayer_feed.py – v5  (robuste, JSON-safe 300-candle feed)

• Paginatie over KuCoin-API (max 150 candles per call) tot ≥300 candles
• Berekent EMA20/50/200, RSI14, VWAP, ATR14, vol_mean20
• Verandert alle NaN/inf → None (json-safe)  –  allow_nan=False
• Schrijft snapshot + volledige history naar GitHub Gist
"""

from __future__ import annotations
import os, json, time, math, datetime as dt
import requests, pandas as pd, ta

API_URL        = "https://api.kucoin.com/api/v1/market/candles"
HISTORY        = 300           # gewenste candles
SEC_PER_CANDLE = 15 * 60       # 15-min
TF_DEFAULT     = "15min"
FILE_DEFAULT   = "solayer_feed.json"

pd.options.mode.copy_on_write = True   # suppress pandas warnings


# ────────────────────────── helpers ──────────────────────────
def _fetch_batch(symbol: str, tf: str, *, limit: int = 150,
                 end_at: int | None = None, tries: int = 3) -> list[list[float]]:
    """ Eén API-call met eenvoudige retry-logica (HTTP errors / timeouts). """
    params = {"symbol": symbol, "type": tf, "limit": limit}
    if end_at:
        params["endAt"] = end_at          # KuCoin expects seconds
    for _ in range(tries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            last = e
            time.sleep(1)
    raise RuntimeError(f"KuCoin batch‐request faalt na {tries} pogingen: {last}")


def fetch_frame(symbol: str, tf: str) -> pd.DataFrame:
    """Haalt minstens `HISTORY` candles op via paginatie (nieuw → oud)."""
    raw, got, end_at = [], 0, None
    while got < HISTORY:
        chunk = _fetch_batch(symbol, tf, limit=150, end_at=end_at)
        if not chunk:
            break
        raw.extend(chunk)
        got += len(chunk)
        # volgende iteratie: eindpunt 1 candle vóór oudste
        oldest_ms = float(chunk[-1][0])
        end_at = int(oldest_ms / 1000 - SEC_PER_CANDLE - 1)

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df = pd.DataFrame(raw, columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)
    df.drop_duplicates("ts", keep="last", inplace=True)
    df = df.tail(HISTORY).reset_index(drop=True)   # precies 300

    # ── indicatoren
    df["ema20"]   = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                      df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                      df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()
    return df


def iso(ts_ms: int) -> str:
    # tijdzone-bewust (UTC) → geen DeprecationWarning meer
    return dt.datetime.fromtimestamp(ts_ms / 1000, dt.timezone.utc).isoformat()


def sanitize(v: float) -> float | None:
    return None if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) else v


def row_to_dict(row: pd.Series) -> dict:
    d = {k: sanitize(v) for k, v in row.items()}
    d["ts"] = int(d["ts"])                # ts als int behouden
    return d


def push_gist(token: str, gist_id: str, filename: str, payload: dict) -> None:
    headers = {"Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    body = {"files": {filename: {
            "content": json.dumps(payload, separators=(',', ':'), allow_nan=False)}}}
    resp = requests.patch(f"https://api.github.com/gists/{gist_id}",
                          headers=headers, json=body, timeout=10)
    resp.raise_for_status()


# ─────────────────────────── main ────────────────────────────
def main() -> None:
    symbol   = os.getenv("SYMBOL",      "SOLAYER-USDT")
    tf       = os.getenv("GRANULARITY", TF_DEFAULT)
    gist_id  = os.environ["GIST_ID"]
    token    = os.environ["GIST_TOKEN"]
    filename = os.getenv("FILE_NAME",   FILE_DEFAULT)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        "timestamp":   int(last.ts),
        "datetime_utc": iso(last.ts),
        "symbol":      symbol,
        "granularity": tf,

        # snapshot
        "price": sanitize(last.close),
        "high":  sanitize(last.high),
        "low":   sanitize(last.low),
        "vol":   sanitize(last.vol),
        "ema20": sanitize(last.ema20),
        "ema50": sanitize(last.ema50),
        "ema200":sanitize(last.ema200),
        "rsi14": sanitize(last.rsi14),
        "vwap":  sanitize(last.vwap),
        "atr14": sanitize(last.atr14),
        "vol_mean20": sanitize(last.vol_mean20),

        # volledige history
        "last_300_candles": [row_to_dict(r) for _, r in df.iterrows()],

        # placeholders
        "funding_rate": None,
        "open_interest": None,
        "order_book": None,

        "generated_at": iso(int(time.time() * 1000))
    }

    push_gist(token, gist_id, filename, payload)
    print("✅  300-candle feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
