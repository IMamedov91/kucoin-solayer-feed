#!/usr/bin/env python3
"""
kucoin_solayer_feed.py — v5-stable
• verzamelt ≥300 × 15-min-candles (KuCoin levert max 150 per call)
• berekent EMA-20/50/200, RSI-14, VWAP, ATR-14, vol_mean20
• JSON-safe: NaN/inf ⇒ null (allow_nan=False)
• uploadt snapshot + volledige history naar GitHub Gist
"""

from __future__ import annotations
import os, json, math, time, datetime as dt, typing as t

import requests
import pandas as pd
import ta

API_URL         = "https://api.kucoin.com/api/v1/market/candles"
HISTORY         = 300              # gewenste lengte
SEC_PER_CANDLE  = 15 * 60
TF_DEFAULT      = "15min"
FILE_DEFAULT    = "solayer_feed.json"

pd.options.mode.copy_on_write = True


# ──────────────────────── helpers ────────────────────────
def _fetch_batch(symbol: str, tf: str, *, limit: int = 150,
                 end_at: int | None = None, retries: int = 3) -> list[list[t.Any]]:
    """Eén KuCoin-request met eenvoudige retry-logica."""
    params = {"symbol": symbol, "type": tf, "limit": limit}
    if end_at:
        params["endAt"] = end_at
    exc: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            exc = e
            time.sleep(1)
    raise RuntimeError(f"KuCoin-API bleef falen: {exc}") from exc


def fetch_frame(symbol: str, tf: str) -> pd.DataFrame:
    """Haalt minstens `HISTORY` candles op via paginatie."""
    raw, got, end_at = [], 0, None
    while got < HISTORY:
        chunk = _fetch_batch(symbol, tf, end_at=end_at)
        if not chunk:
            break
        raw.extend(chunk)
        got += len(chunk)
        oldest_ms = float(chunk[-1][0])
        end_at = int(oldest_ms / 1000 - SEC_PER_CANDLE - 1)

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df = pd.DataFrame(raw, columns=cols).astype(float)

    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)
    df.drop_duplicates("ts", keep="last", inplace=True)
    df = df.tail(HISTORY).reset_index(drop=True)

    # indicatoren
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


def iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat(timespec="seconds")


def _clean(v: float | int) -> float | int | None:
    return None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v


def row_dict(row: pd.Series) -> dict:
    d = {k: _clean(v) for k, v in row.items()}
    d["ts"] = int(d["ts"])
    return d


def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    headers = {"Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    body = {"files": {fname: {
            "content": json.dumps(payload, separators=(',', ':'), allow_nan=False)}}}
    r = requests.patch(f"https://api.github.com/gists/{gist_id}",
                       headers=headers, json=body, timeout=10)
    r.raise_for_status()


# ─────────────────────────── main ──────────────────────────
def main() -> None:
    symbol  = os.getenv("SYMBOL",      "SOLAYER-USDT")
    tf      = os.getenv("GRANULARITY", TF_DEFAULT)
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    fname   = os.getenv("FILE_NAME",   FILE_DEFAULT)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        "timestamp":    int(last.ts),
        "datetime_utc": iso(last.ts),
        "symbol":       symbol,
        "granularity":  tf,

        "price": _clean(last.close),  "high": _clean(last.high),
        "low": _clean(last.low),      "vol":  _clean(last.vol),
        "ema20": _clean(last.ema20),  "ema50": _clean(last.ema50),
        "ema200": _clean(last.ema200),"rsi14": _clean(last.rsi14),
        "vwap": _clean(last.vwap),    "atr14": _clean(last.atr14),
        "vol_mean20": _clean(last.vol_mean20),

        "last_300_candles": [row_dict(r) for _, r in df.iterrows()],
        "funding_rate": None, "open_interest": None, "order_book": None,
        "generated_at": iso(int(time.time() * 1000))
    }

    push_gist(token, gist_id, fname, payload)
    print("✅  feed geüpload", payload["generated_at"])


if __name__ == "__main__":
    main()
