#!/usr/bin/env python3
"""
kucoin_solayer_feed.py — v6-fix
•   spot óf futures via env ENDPOINT
•   correcte paginatie-param:  endAt  (spot)  |  to  (futures)
•   granularity in minuten (futures: 15)
"""

from __future__ import annotations
import datetime as dt, json, math, os, time, typing as t

import pandas as pd, requests, ta

# ─── configuratie ────────────────────────────────────────────────────
ENDPOINT       = os.getenv("ENDPOINT", "spot")            # spot | futures
TF_DEFAULT     = "15min" if ENDPOINT == "spot" else "15"  # ← minuten!
SYMBOL_DEFAULT = "SOLAYER-USDT" if ENDPOINT == "spot" else "SOLAYERUSDTM"

if ENDPOINT == "futures":                                 # max 500 :contentReference[oaicite:3]{index=3}
    API_URL, MAX_LIMIT = (
        "https://api-futures.kucoin.com/api/v1/kline/query", 500)
else:                                                     # max 150 :contentReference[oaicite:4]{index=4}
    API_URL, MAX_LIMIT = (
        "https://api.kucoin.com/api/v1/market/candles", 150)

FETCH_LEN     = 550                 # 300 output + 250 buffer
SNAPSHOT_LEN  = 300
MS_PER_BAR    = 15 * 60 * 1000
FILE_DEFAULT  = "solayer_feed.json"

pd.options.mode.copy_on_write = True

# ─── helpers ─────────────────────────────────────────────────────────
def _get(params: dict, retries: int = 3) -> list[list[t.Any]]:
    err: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            err = e
            time.sleep(1)
    raise RuntimeError(f"KuCoin API blijft falen: {err}") from err


def fetch_frame(symbol: str, tf: str) -> pd.DataFrame:
    raw: list[list[t.Any]] = []
    end_ms = int(time.time() * 1000)

    while len(raw) < FETCH_LEN:
        params = (
            # spot
            {"symbol": symbol, "type": tf, "limit": MAX_LIMIT, "endAt": end_ms}
            if ENDPOINT == "spot"
            # futures
            else {"symbol": symbol, "granularity": int(tf),
                  "limit": MAX_LIMIT, "to": end_ms}
        )
        batch = _get(params)
        if not batch:
            break
        raw.extend(batch)
        end_ms = int(batch[-1][0]) - MS_PER_BAR

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df = (
        pd.DataFrame(raw, columns=cols)
        .astype(float)
        .drop_duplicates("ts")
        .sort_values("ts")
        .reset_index(drop=True)
    )

    # ─ indicatoren ─
    df["ema20"] = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"] = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"] = ta.momentum.rsi(df["close"], 14)
    df["vwap"] = ta.volume.volume_weighted_average_price(
        df["high"], df["low"], df["close"], df["vol"], 14
    )
    df["atr14"] = ta.volatility.average_true_range(
        df["high"], df["low"], df["close"], 14
    )
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)


# ─── utils ───────────────────────────────────────────────────────────
iso = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)\
                             .isoformat(timespec="seconds")

_clean = lambda v: None if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) else v

def row(r: pd.Series) -> dict:
    d = {k: _clean(v) for k, v in r.items()}
    d["ts"] = int(d["ts"])
    return d


def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}",
                 "Accept": "application/vnd.github+json"},
        json={"files": {fname: {"content": json.dumps(payload,
                                                      separators=(',', ':'),
                                                      allow_nan=False)}}},
        timeout=10,
    ).raise_for_status()

# ─── main ────────────────────────────────────────────────────────────
def main() -> None:
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    symbol  = os.getenv("SYMBOL", SYMBOL_DEFAULT)
    tf      = os.getenv("GRANULARITY", TF_DEFAULT)
    fname   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    ema200 = _clean(last.ema200) or "insufficient_data"

    payload = {
        "timestamp": int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol": symbol,
        "granularity": tf,
        "price": _clean(last.close),
        "high": _clean(last.high),
        "low": _clean(last.low),
        "vol": _clean(last.vol),
        "ema20": _clean(last.ema20),
        "ema50": _clean(last.ema50),
        "ema200": ema200,
        "rsi14": _clean(last.rsi14),
        "vwap": _clean(last.vwap),
        "atr14": _clean(last.atr14),
        "vol_mean20": _clean(last.vol_mean20),
        "last_300_candles": [row(r) for _, r in df.iterrows()],
        "funding_rate": None,
        "open_interest": None,
        "order_book": None,
        "generated_at": iso(int(time.time() * 1000)),
    }

    push_gist(token, gist_id, fname, payload)
    print("✅ feed geüpload", payload["generated_at"])


if __name__ == "__main__":
    main()
