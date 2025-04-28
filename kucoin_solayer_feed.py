#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_solayer_feed.py — v11  🚀

* Eén *super‑feed* die in één JSON alle relevante data voor meerdere timeframes opslaat.
* Indicator‑set uitgebreid (MACD, Bollinger, Supertrend, ADX, MFI, OBV).
* Funding‑rate, open‑interest, order‑book‑imbalance, BTC‑correlatie toegevoegd.
* Ondersteunt zowel spot als futures via één script.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import time
import typing as t
from contextlib import suppress

import numpy as np
import pandas as pd
import requests
import ta  # voor EMA/RSI/ATR e.d.

try:
    import pandas_ta as pta  # uitgebreidere set (MACD, Supertrend, …)
except ImportError:  # fallback – sommige indicaties slaan we dan over
    pta = None

# ───────────────────────── CONFIG ────────────────────────────────
ENDPOINT = os.getenv("ENDPOINT", "futures")  # spot | futures
SYMBOL_DEFAULT = (
    "SOLAYER-USDT" if ENDPOINT == "spot" else "SOLAYERUSDTM"
)
BTC_SYMBOL = "BTCUSDTM" if ENDPOINT == "futures" else "BTC-USDT"

# KuCoin endpoints
if ENDPOINT == "futures":
    API_URL, MAX_LIMIT = (
        "https://api-futures.kucoin.com/api/v1/kline/query", 5000,
    )
    FUNDING_URL = "https://api-futures.kucoin.com/api/v1/funding-rate"
    OPEN_INTEREST_URL = "https://api-futures.kucoin.com/api/v1/openInterest"
    ORDERBOOK_URL = (
        "https://api-futures.kucoin.com/api/v1/level2/partOrderBook"
    )  # depth=N
else:
    API_URL, MAX_LIMIT = (
        "https://api.kucoin.com/api/v1/market/candles", 1500,
    )
    FUNDING_URL = OPEN_INTEREST_URL = ORDERBOOK_URL = None  # spot kent dit niet

# Timeframes & lookbacks (bars) – override met ENV `TIMEFRAMES`
_DEFAULT_TF = [
    ("1m", 1440),  # 24 h
    ("5m", 2880),  # 10 d
    ("15m", 3000),  # 31 d
    ("60", 3000),  # 125 d (1 h)
    ("240", 2000),  # 333 d (4 h)
    ("D", 1000),  # 3 j (1 d)
]

TIMEFRAMES: list[tuple[str, int]] = []
_env_tf = os.getenv("TIMEFRAMES")
if _env_tf:
    for tf_part in _env_tf.split(","):
        tf, _, lb = tf_part.partition(":")
        TIMEFRAMES.append((tf.strip(), int(lb) if lb else 3000))
else:
    TIMEFRAMES.extend(_DEFAULT_TF)

# warm‑up om indicatoren te initialiseren
WARMUP = 250  # bars extra naast lookback

FILE_DEFAULT = "solayer_feed.json"
MS_PER_BAR_MAP = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "60": 60 * 60_000,
    "240": 240 * 60_000,
    "D": 24 * 60 * 60_000,
}

pd.options.mode.copy_on_write = True

# ─────────────────────── HELPER FUNCTIONS ───────────────────────

def _iso(ms: int | float) -> str:
    return (
        dt.datetime.utcfromtimestamp(ms / 1000)
        .isoformat(timespec="seconds")
        + "Z"
    )


def _clean(val: t.Any) -> t.Any:
    if val is None:
        return None
    with suppress(TypeError):
        if math.isnan(val) or math.isinf(val):
            return None
    return val


def _sleep_backoff(base: float = 1.0):
    time.sleep(base)
    return base * 2


# ───────────────────── KuCoin API INTEGRATION ────────────────────

def _get(url: str, params: dict, retries: int = 3) -> dict:
    err: Exception | None = None
    backoff = 1.0
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 429:
                raise RuntimeError("Rate‑limited by KuCoin (429)")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            err = e
            backoff = _sleep_backoff(backoff)
    raise RuntimeError(f"KuCoin API keeps failing: {err}") from err


def _tf_to_kucoin(tf: str) -> str | int:
    """Return granularity param expected by KuCoin."""
    if ENDPOINT == "spot":
        return tf  # e.g. "1m" | "15min" ; KuCoin spot accepts both
    return int(tf) if tf.isdigit() else (
        1440 if tf.upper() == "D" else 1  # fallback
    )


def fetch_frame(symbol: str, tf: str, lookback: int) -> pd.DataFrame:
    raw: list[list[t.Any]] = []
    end_ms = int(time.time() * 1000)
    need = lookback + WARMUP

    while len(raw) < need:
        params = (
            {
                "symbol": symbol,
                "type": tf,
                "limit": min(MAX_LIMIT, need - len(raw)),
                "endAt": end_ms,
            }
            if ENDPOINT == "spot"
            else {
                "symbol": symbol,
                "granularity": _tf_to_kucoin(tf),
                "limit": min(MAX_LIMIT, need - len(raw)),
                "to": end_ms,
            }
        )
        batch = _get(API_URL, params)["data"]
        if not batch:
            break
        raw.extend(batch)
        # KuCoin returns ascending or descending? – treat as list of lists, first element is ts
        end_ms = int(batch[-1][0]) - MS_PER_BAR_MAP.get(tf, 60_000)

    if len(raw) < need:
        print(f"⚠️ fetched {len(raw)} < {need} bars for {tf}")

    # Build DataFrame
    if ENDPOINT == "spot":
        cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
        df = pd.DataFrame(raw, columns=cols)
    else:
        cols_api = ["ts", "open", "high", "low", "close", "vol"]
        df = (
            pd.DataFrame(raw, columns=cols_api)[
                ["ts", "open", "close", "high", "low", "vol"]
            ]
        )

    df = (
        df.astype(float, errors="ignore")
        .drop_duplicates("ts")
        .sort_values("ts")
        .reset_index(drop=True)
    )

    return df.tail(lookback).reset_index(drop=True)


# ───────────────────── INDICATORS ───────────────────────────────

def calc_indicators(df: pd.DataFrame) -> dict[str, t.Any]:
    """Return dict with all indicators for *last* row."""
    ind: dict[str, t.Any] = {}

    # Ensure numeric cols
    close, high, low, vol = df["close"], df["high"], df["low"], df["vol"]

    # ta
    ind["ema20"] = ta.trend.ema_indicator(close, 20).iloc[-1]
    ind["ema50"] = ta.trend.ema_indicator(close, 50).iloc[-1]
    ind["ema200"] = ta.trend.ema_indicator(close, 200).iloc[-1]
    ind["rsi14"] = ta.momentum.rsi(close, 14).iloc[-1]
    ind["atr14"] = ta.volatility.average_true_range(high, low, close, 14).iloc[-1]
    ind["vwap"] = ta.volume.volume_weighted_average_price(high, low, close, vol, 14).iloc[-1]
    ind["vol_mean20"] = vol.rolling(20, min_periods=1).mean().iloc[-1]

    if pta is not None:
        macd = pta.macd(close, fast=12, slow=26, signal=9)
        bb = pta.bbands(close, length=20, std=2)
        adx = pta.adx(high, low, close, length=14)
        mfi = pta.mfi(high, low, close, vol, length=14)
        sup = pta.supertrend(high, low, close, length=10, multiplier=3)
        obv = pta.obv(close, vol)

        ind["macd"] = {
            "macd": macd["MACD_12_26_9"].iloc[-1],
            "signal": macd["MACDs_12_26_9"].iloc[-1],
            "hist": macd["MACDh_12_26_9"].iloc[-1],
        }
        ind["bbands"] = {
            "upper": bb["BBU_20_2.0"].iloc[-1],
            "lower": bb["BBL_20_2.0"].iloc[-1],
            "basis": bb["BBM_20_2.0"].iloc[-1],
        }
        ind["adx"] = adx["ADX_14"].iloc[-1]
        ind["mfi"] = mfi.iloc[-1]
        ind["supertrend"] = sup["SUPERT_10_3.0"].iloc[-1]
        ind["obv"] = obv.iloc[-1]

    # Clean NaN/inf
    return {k: _clean(v) for k, v in ind.items()}


# ───────────────────── EXTERNAL DATA (futures) ──────────────────

def get_funding(symbol: str) -> dict[str, t.Any]:
    if not FUNDING_URL:
        return {"current": None, "predicted": None, "history_7d": []}
    res_now = _get(FUNDING_URL, {"symbol": symbol})["data"]
    now_val = float(res_now.get("fundingRate", 0))

    res_hist = _get(
        FUNDING_URL + "/history",
        {"symbol": symbol, "reverse": "true", "pageSize": 56},  # 56×8h ≈ 7 d
    )["data"]
    hist = [float(row["fundingRate"]) for row in res_hist]
    pred = hist[0] if hist else None
    return {"current": now_val, "predicted": pred, "history_7d": hist[:21]}


def get_open_interest(symbol: str) -> dict[str, t.Any]:
    if not OPEN_INTEREST_URL:
        return {"value": None, "change_24h_pct": None}
    res = _get(OPEN_INTEREST_URL, {"symbol": symbol})["data"]
    oi_val = float(res.get("openInterest", 0))
    oi_24h = float(res.get("openInterestValue24h", 0))
    pct = ((oi_val - oi_24h) / oi_24h * 100) if oi_24h else None
    return {"value": oi_val, "change_24h_pct": pct}


def get_order_book_depth(symbol: str, depth: int = 5) -> dict[str, t.Any]:
    if not ORDERBOOK_URL:
        return {}
    res = _get(ORDERBOOK_URL, {"symbol": symbol, "depth": depth})["data"]
    bids = sum(float(b[1]) for b in res.get("bids", []))
    asks = sum(float(a[1]) for a in res.get("asks", []))
    imb = (bids - asks) / (bids + asks) * 100 if bids + asks else None
    return {
        "bids_qty": bids,
        "asks_qty": asks,
        "imbalance_pct": imb,
        "last_updated": int(time.time() * 1000),
    }


def btc_correlation(df_alt: pd.DataFrame) -> float | None:
    """30 d Pearson corr op 1 h basis."""
    try:
        df_btc = fetch_frame(BTC_SYMBOL, "60", 720)  # 30 d * 24 h = 720 bars
        corr = np.corrcoef(df_alt["close"].tail(720), df_btc["close"].tail(720))[0, 1]
        return float(corr)
    except Exception:
        return None


# ────────────────────── PUSH TO GIST ─────────────────────────────

def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    json_content = json.dumps(payload, indent=2, allow_nan=False, ensure_ascii=False)
    r = requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
        },
        json={"files": {fname: {"content": json_content}}},
        timeout=10,
    )
    r.raise_for_status()


# ────────────────────────── MAIN ────────────────────────────────

def main() -> None:
    gist_id = os.environ["GIST_ID"]
    token = os.environ["GIST_TOKEN"]
    symbol = os.getenv("SYMBOL", SYMBOL_DEFAULT)
    fname = os.getenv("FILE_NAME", FILE_DEFAULT)

    payload: dict[str, t.Any] = {
        "generated_at": _iso(int(time.time() * 1000)),
        "symbol": symbol,
        "candles": {},
        "indicators": {},
    }

    for tf, lookback in TIMEFRAMES:
        df = fetch_frame(symbol, tf, lookback)
        payload["candles"][tf] = {
            "lookback": lookback,
            "bars": [
                {k: _clean(v) if k != "ts" else int(v) for k, v in rec.items()}
                for rec in df[["ts", "open", "close", "high", "low", "vol"]].to_dict("records")
            ],
        }
        payload["indicators"][tf] = calc_indicators(df)

        # corr alleen eenmaal bij langste tf gebruiken → 1h referentie
        if tf == "60":
            payload["btc_correlation_30d"] = _clean(btc_correlation(df))

    # Extra futures data
    payload["funding"] = get_funding(symbol)
    payload["open_interest"] = get_open_interest(symbol)
    payload["order_book"] = get_order_book_depth(symbol, depth=5)

    push_gist(token, gist_id, fname, payload)
    print("✅ SOLayer multi‑TF feed uploaded:", payload["generated_at"])


if __name__ == "__main__":
    main()
