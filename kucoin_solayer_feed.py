#!/usr/bin/env python3
"""
kucoin_solayer_feed.py — v8
•   Spot óf futures via env ENDPOINT
•   Zelfde JSON‑structuur & pretty‑print als de BTC‑feed (indent = 2)
•   last_300_candles bevat alléén OHLCV‑velden
•   Indicator‑kolommen zonder NaN/inf (ffill/bfill)
"""

from __future__ import annotations
import datetime as dt
import json, math, os, time, typing as t

import pandas as pd
import requests, ta

# ───────────────────────── CONFIG ──────────────────────────────────
ENDPOINT       = os.getenv("ENDPOINT", "spot")            # spot | futures
TF_DEFAULT     = "15min" if ENDPOINT == "spot" else "15"    # spot = str, futures = int(min)
SYMBOL_DEFAULT = "SOLAYER-USDT" if ENDPOINT == "spot" else "SOLAYERUSDTM"

if ENDPOINT == "futures":                                  # max 500 per request
    API_URL, MAX_LIMIT = (
        "https://api-futures.kucoin.com/api/v1/kline/query", 500)
else:                                                       # max 150 per request
    API_URL, MAX_LIMIT = (
        "https://api.kucoin.com/api/v1/market/candles", 150)

FETCH_LEN    = 550                 # 300 snapshot + buffer voor indicatoren
SNAPSHOT_LEN = 300
MS_PER_BAR   = 15 * 60_000
FILE_DEFAULT = "solayer_feed.json"

pd.options.mode.copy_on_write = True

# ──────────────────────── HELPERS ──────────────────────────────────

def _get(params: dict, retries: int = 3) -> list[list[t.Any]]:
    """Kleine GET‑helper met retry‑logica."""
    err: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            err = e
            time.sleep(1)
    raise RuntimeError(f"KuCoin‑API blijft falen: {err}") from err


def fetch_frame(symbol: str, tf: str | int) -> pd.DataFrame:
    """Haalt ruwe candles op en voegt indicator‑kolommen toe (zonder NaN/inf)."""
    raw: list[list[t.Any]] = []
    end_ms = int(time.time() * 1000)

    while len(raw) < FETCH_LEN:
        params = (
            {"symbol": symbol, "type": tf, "limit": MAX_LIMIT, "endAt": end_ms}  # spot
            if ENDPOINT == "spot"
            else {"symbol": symbol, "granularity": int(tf), "limit": MAX_LIMIT, "to": end_ms}  # futures
        )
        batch = _get(params)
        if not batch:
            break
        raw.extend(batch)
        end_ms = int(batch[-1][0]) - MS_PER_BAR

    # kolom‑lijst afhankelijk van endpoint
    if ENDPOINT == "spot":
        cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
        df = pd.DataFrame(raw, columns=cols)
    else:
        # futures‑volgorde in API: ts, open, high, low, close, vol → herschikken
        cols_api = ["ts", "open", "high", "low", "close", "vol"]
        df = pd.DataFrame(raw, columns=cols_api)
        df = df[["ts", "open", "close", "high", "low", "vol"]]  # gelijke volgorde als BTC

    df = (
        df.astype(float)
          .drop_duplicates("ts")
          .sort_values("ts")
          .reset_index(drop=True)
    )

    # ───────── indicatoren ─────────
    df["ema20"]      = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]      = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"]     = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]      = ta.momentum.rsi(df["close"], 14)
    df["vwap"]       = ta.volume.volume_weighted_average_price(
                        df["high"], df["low"], df["close"], df["vol"], 14)
    df["atr14"]      = ta.volatility.average_true_range(
                        df["high"], df["low"], df["close"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    # Vul ontbrekende indicator‑waarden op
    ind_cols = ["ema20", "ema50", "ema200", "rsi14", "vwap", "atr14"]
    df[ind_cols] = df[ind_cols].ffill().bfill()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)

# ─────────────────────── utilities ────────────────────────────────
iso = lambda ms: dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"
_clean = lambda v: None if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) else v

KEEP = ("open", "close", "high", "low", "vol")
row  = lambda r: {"ts": int(r.ts), **{k: _clean(r[k]) for k in KEEP}}


def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    """Uploadt JSON‑payload naar een Gist (pretty‑printed)."""
    json_content = json.dumps(
        payload,
        indent=2,
        allow_nan=False,
        ensure_ascii=False,
    )
    requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        json={"files": {fname: {"content": json_content}}},
        timeout=10,
    ).raise_for_status()


# ────────────────────────── MAIN ──────────────────────────────────

def main() -> None:
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    symbol  = os.getenv("SYMBOL", SYMBOL_DEFAULT)
    tf      = os.getenv("GRANULARITY", TF_DEFAULT)
    fname   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        "timestamp":   int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol":      symbol,
        "granularity": str(tf),
        "price":       _clean(last.close),
        "high":        _clean(last.high),
        "low":         _clean(last.low),
        "vol":         _clean(last.vol),
        "ema20":       _clean(last.ema20),
        "ema50":       _clean(last.ema50),
        "ema200":      _clean(last.ema200) or "insufficient_data",
        "rsi14":       _clean(last.rsi14),
        "vwap":        _clean(last.vwap),
        "atr14":       _clean(last.atr14),
        "vol_mean20":  _clean(last.vol_mean20),
        "last_300_candles": [row(r) for _, r in df.iterrows()],
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None,
        "generated_at":  iso(int(time.time() * 1000)),
    }

    push_gist(token, gist_id, fname, payload)
    print("✅ SOLayer‑feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
