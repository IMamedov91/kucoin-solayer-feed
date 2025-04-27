#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v9

Fetches a 15‑minute SOLayer candle feed (spot or futures) from KuCoin,
calculates indicators locally with **pandas‑ta**, and uploads the latest
snapshot (300 candles) to a GitHub Gist – identical JSON schema as the
BTC feed.

Key changes v9
--------------
* **Robust KuCoin retries** – `_get()` now backs‑off on 429/5xx.
* **Consistent timestamp handling** – ensures integer ms throughout.
* **Guaranteed clean indicators** – converts `NaN/inf` → `None` everywhere
  (not just the last row) so `json.dumps(allow_nan=False)` never fails.
* **Symbol note for Binance** – when you later switch to Binance the pair
  is simply `LAYERUSDT` (no hyphen). The code keeps KuCoin defaults but
  you can override via `SYMBOL` env‑var.
* Style: matches the BTC v2.4 structure & naming for easy diffing.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import time
import typing as t
from contextlib import suppress

import pandas as pd
import requests
import ta

# ───────────────────────── CONFIG ────────────────────────────────
ENDPOINT       = os.getenv("ENDPOINT", "spot")              # spot | futures
TF_DEFAULT     = "15min" if ENDPOINT == "spot" else "15"     # KuCoin style
SYMBOL_DEFAULT = (
    "SOLAYER-USDT"  if ENDPOINT == "spot"   else  # KuCoin spot
    "SOLAYERUSDTM" if ENDPOINT == "futures" else None
)

if ENDPOINT == "futures":  # KuCoin futures (max 500 candles per request)
    API_URL, MAX_LIMIT = (
        "https://api-futures.kucoin.com/api/v1/kline/query", 5000)
else:                       # KuCoin spot (max 150 candles per request)
    API_URL, MAX_LIMIT = (
        "https://api.kucoin.com/api/v1/market/candles", 1500)

FETCH_LEN    = 550                 # 300 snapshot + buffer for indicators
SNAPSHOT_LEN = 300
MS_PER_BAR   = 15 * 60_000
FILE_DEFAULT = "solayer_feed.json"

pd.options.mode.copy_on_write = True

# ─────────────────────── HELPER FUNCTIONS ───────────────────────

def _iso(ms: int | float) -> str:
    """Epoch‑ms → ISO‑8601 (UTC)."""
    return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"


def _clean(val: t.Any) -> t.Any:
    """Convert NaN/inf (incl. numpy & pandas types) to None for JSON dumps."""
    if val is None:
        return None
    with suppress(TypeError):  # ints will raise here; skip
        if math.isnan(val) or math.isinf(val):
            return None
    return val


# ───────────────────── KuCoin API INTEGRATION ────────────────────

def _get(params: dict, retries: int = 3) -> list[list[t.Any]]:
    """Thin wrapper around requests.get with naive retry/back‑off."""
    err: Exception | None = None
    backoff = 1.0
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            if r.status_code == 429:
                raise RuntimeError("Rate‑limited by KuCoin (429)")
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            err = e
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError(f"KuCoin API keeps failing after retries: {err}") from err


def fetch_frame(symbol: str, tf: str | int) -> pd.DataFrame:
    """Download raw candles, compute indicators (no NaN/inf)."""
    raw: list[list[t.Any]] = []
    end_ms = int(time.time() * 1000)

    while len(raw) < FETCH_LEN:
        params = (
            {"symbol": symbol, "type": tf, "limit": MAX_LIMIT, "endAt": end_ms}
            if ENDPOINT == "spot" else
            {"symbol": symbol, "granularity": int(tf), "limit": MAX_LIMIT, "to": end_ms}
        )
        batch = _get(params)
        if not batch:
            break
        raw.extend(batch)
        end_ms = int(batch[-1][0]) - MS_PER_BAR

    if not raw:
        raise RuntimeError("KuCoin returned no candle data – check symbol & endpoint")

    # Map API rows to a DataFrame
    if ENDPOINT == "spot":
        cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
        df = pd.DataFrame(raw, columns=cols)
    else:
        cols_api = ["ts", "open", "high", "low", "close", "vol"]
        df = pd.DataFrame(raw, columns=cols_api)[["ts", "open", "close", "high", "low", "vol"]]

    df = (
        df.astype(float, errors="ignore")
          .drop_duplicates("ts")
          .sort_values("ts")
          .reset_index(drop=True)
    )

    # ───── indicatoren via ta
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(df["high"], df["low"], df["close"], df["vol"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    # Fill missing values to avoid NaNs in output
    ind_cols = ["ema20", "ema50", "ema200", "rsi14", "vwap", "atr14", "vol_mean20"]
    df[ind_cols] = df[ind_cols].ffill().bfill()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)


# ────────────────────── PUSH TO GIST ─────────────────────────────

def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    """Upload JSON payload to a GitHub Gist (pretty‑printed)."""
    json_content = json.dumps(payload, indent=2, allow_nan=False, ensure_ascii=False)
    r = requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        json={"files": {fname: {"content": json_content}}},
        timeout=10,
    )
    r.raise_for_status()


# ────────────────────────── MAIN ────────────────────────────────

def main() -> None:
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    symbol  = os.getenv("SYMBOL", SYMBOL_DEFAULT)
    tf      = os.getenv("GRANULARITY", TF_DEFAULT)
    fname   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        "timestamp":    int(last.ts),
        "datetime_utc": _iso(int(last.ts)),
        "symbol":       symbol,
        "backtrack":    LIMIT,
        "granularity":  str(tf),
        "price":        _clean(last.close),
        "high":         _clean(last.high),
        "low":          _clean(last.low),
        "vol":          _clean(last.vol),
        "ema20":        _clean(last.ema20),
        "ema50":        _clean(last.ema50),
        "ema200":       _clean(last.ema200) or "insufficient_data",
        "rsi14":        _clean(last.rsi14),
        "vwap":         _clean(last.vwap),
        "atr14":        _clean(last.atr14),
        "vol_mean20":   _clean(last.vol_mean20),
        "last_300_candles": [
            {k: _clean(v) if k != "ts" else int(v) for k, v in r.items()}
            for r in df[["ts", "open", "close", "high", "low", "vol"]].to_dict("records")
        ],
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None,
        "generated_at":  _iso(int(time.time() * 1000)),
    }

    push_gist(token, gist_id, fname, payload)
    print("✅ SOLayer feed uploaded:", payload["generated_at"])


if __name__ == "__main__":
    main()
