#!/usr/bin/env python3
"""
kucoin_solayer_feed.py — v5-stable-b2
• haalt ≥1 000 × 15-min-candles (150 per API-call) → betrouwbare EMA-200
• slaat de laatste 300 candles + snapshot op in je Gist
• berekent EMA-20/50/200, RSI-14, VWAP, ATR-14, vol_mean20
• NaN/inf ⇒ null  (json.dumps(..., allow_nan=False))
"""

from __future__ import annotations
import os, json, math, time, datetime as dt, typing as t

import requests, pandas as pd, ta

API_URL         = "https://api.kucoin.com/api/v1/market/candles"
BATCH_LIMIT     = 150          # KuCoin max
FETCH_LEN       = 1_000        # ruim genoeg voor EMA-200
SNAPSHOT_LEN    = 300          # wat er in de Gist gaat
SEC_PER_CANDLE  = 15 * 60
TF_DEFAULT      = "15min"
FILE_DEFAULT    = "solayer_feed.json"

pd.options.mode.copy_on_write = True


# ───────────────────────── helpers ─────────────────────────
def _fetch_batch(symbol: str, tf: str, *, end_at: int | None = None,
                 limit: int = BATCH_LIMIT, retries: int = 3) -> list[list[t.Any]]:
    """ Eén KuCoin-request met eenvoudige retries. """
    params = {"symbol": symbol, "type": tf, "limit": limit}
    if end_at:
        params["endAt"] = end_at           # unix-seconds
    for _ in range(retries):
        try:
            resp = requests.get(API_URL, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()["data"]
        except Exception:
            time.sleep(1)
    raise RuntimeError("KuCoin-API bleef falen na herhaald proberen")


def fetch_frame(symbol: str, tf: str) -> pd.DataFrame:
    """Haalt ≥ FETCH_LEN candles op via paginatie en berekent indicatoren."""
    raw, got, end_at = [], 0, None
    while got < FETCH_LEN:
        batch = _fetch_batch(symbol, tf, end_at=end_at)
        if not batch:
            break                      # KuCoin gaf niets terug
        raw.extend(batch)
        got += len(batch)
        oldest_ms = float(batch[-1][0])
        end_at = int(oldest_ms / 1000 - SEC_PER_CANDLE - 1)

    if got < 200:                      # te weinig data → geen EMA-200
        raise ValueError(f"Slechts {got} candles opgehaald (min 200 vereist)")

    cols = ["ts","open","close","high","low","vol","turnover"]
    df = pd.DataFrame(raw, columns=cols).astype(float)
    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)
    df.drop_duplicates("ts", keep="last", inplace=True)
    df = df.tail(FETCH_LEN).reset_index(drop=True)

    # ─ indicatoren ─
    df["ema20"]   = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                       df["high"], df["low"], df["close"], df["vol"], 14)
    df["atr14"]   = ta.volatility.average_true_range(
                       df["high"], df["low"], df["close"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)


def iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)\
             .isoformat(timespec="seconds")


def _clean(v: float | int) -> float | int | None:
    return None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v


def row_dict(row: pd.Series) -> dict:
    d = {k: _clean(v) for k, v in row.items()}
    d["ts"] = int(d["ts"])
    return d


def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    hdr  = {"Authorization": f"token {token}",
            "Accept": "application/vnd.github+json"}
    body = {"files": {fname: {"content": json.dumps(payload,
                                                   separators=(',', ':'),
                                                   allow_nan=False)}}}
    requests.patch(f"https://api.github.com/gists/{gist_id}",
                   headers=hdr, json=body, timeout=10).raise_for_status()


# ─────────────────────────── main ──────────────────────────
def main() -> None:
    symbol  = os.getenv("SYMBOL",      "SOLAYER-USDT")
    tf      = os.getenv("GRANULARITY", TF_DEFAULT)
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    fname   = os.getenv("FILE_NAME",   FILE_DEFAULT)

    try:
        df = fetch_frame(symbol, tf)
    except ValueError as e:
        print(f"⛔  {e} – upload overgeslagen.")
        return

    last = df.iloc[-1]
    payload = {
        "timestamp":   int(last.ts),
        "datetime_utc": iso(last.ts),
        "symbol":      symbol,
        "granularity": tf,

        "price": _clean(last.close), "high": _clean(last.high),
        "low": _clean(last.low),     "vol":  _clean(last.vol),
        "ema20": _clean(last.ema20), "ema50": _clean(last.ema50),
        "ema200": _clean(last.ema200), "rsi14": _clean(last.rsi14),
        "vwap": _clean(last.vwap),   "atr14": _clean(last.atr14),
        "vol_mean20": _clean(last.vol_mean20),

        "last_300_candles": [row_dict(r) for _, r in df.iterrows()],
        "funding_rate": None, "open_interest": None, "order_book": None,
        "generated_at": iso(int(time.time()*1000))
    }

    push_gist(token, gist_id, fname, payload)
    print("✅  feed geüpload", payload["generated_at"])


if __name__ == "__main__":
    main()
