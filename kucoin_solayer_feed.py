#!/usr/bin/env python3
"""
kucoin_solayer_feed.py — v5-stable-b3
• haalt ≥1 000 × 15-min candles (150 per API-call) → betrouwbare EMA-200
• slaat snapshot + laatste 300 candles op in je Gist
• berekent EMA-20/50/200, RSI-14, VWAP, ATR-14, vol_mean20
• NaN/inf ⇒ null  (json.dumps(..., allow_nan=False))
"""

from __future__ import annotations
import datetime as dt, json, math, os, time, typing as t

import pandas as pd, requests, ta

# ────────── Constante configuratie ──────────
API_URL        = "https://api.kucoin.com/api/v1/market/candles"
BATCH_LIMIT    = 150          # KuCoin max
FETCH_LEN      = 1_000        # minimaal op te halen candles
SNAPSHOT_LEN   = 300          # wat in de Gist wordt bewaard
SEC_PER_BAR    = 15 * 60
MS_PER_BAR     = SEC_PER_BAR * 1000
TF_DEFAULT     = "15min"
FILE_DEFAULT   = "solayer_feed.json"

pd.options.mode.copy_on_write = True  # suppress SettingWithCopy warnings


# ────────── Helpers ──────────
def _fetch_batch(symbol: str, tf: str, *, end_at_ms: int | None = None,
                 retries: int = 3) -> list[list[t.Any]]:
    """ Eén KuCoin-request met eenvoudige retry-logica. """
    params = {"symbol": symbol, "type": tf, "limit": BATCH_LIMIT}
    if end_at_ms:
        params["endAt"] = end_at_ms            # KuCoin verwacht millis
    err: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            err = e
            time.sleep(1)
    raise RuntimeError(f"KuCoin-API bleef falen: {err}") from err


def fetch_frame(symbol: str, tf: str) -> pd.DataFrame:
    """Haalt ≥ FETCH_LEN candles op via paginatie en voegt indicatoren toe."""
    raw: list[list[t.Any]] = []
    end_at_ms: int | None = None

    # ▸ pagineren
    while len(raw) < FETCH_LEN:
        batch = _fetch_batch(symbol, tf, end_at_ms=end_at_ms)
        if not batch:
            print("ℹ️  API gaf lege batch terug — stoppen met ophalen.")
            break
        raw.extend(batch)
        # nieuw eindpunt = (oudste_bar_timestamp − 1 bar)  in ms
        oldest_ms = int(batch[-1][0])
        end_at_ms = oldest_ms - MS_PER_BAR - 1

    # dataframe opbouwen
    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df = (pd.DataFrame(raw, columns=cols)
            .astype(float)
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True))

    if len(df) < 200:              # nog steeds te weinig voor EMA-200
        print(f"⚠️  Slechts {len(df)} candles opgehaald — ema200 wordt 'insufficient_data'.")

    # ▸ indicatoren
    df["ema20"]   = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                      df["high"], df["low"], df["close"], df["vol"], 14)
    df["atr14"]   = ta.volatility.average_true_range(
                      df["high"], df["low"], df["close"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    # ▸ laatste 300 bars teruggeven (indicatoren blijven geldig)
    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)


def iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)\
             .isoformat(timespec="seconds")


def _clean(v: float | int) -> float | int | None:
    return None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v


def row_dict(row: pd.Series) -> dict:        # compact helper voor JSON
    out = {k: _clean(v) for k, v in row.items()}
    out["ts"] = int(out["ts"])
    return out


def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    headers = {"Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    body = {"files": {fname: {"content": json.dumps(payload,
                                                   separators=(',', ':'),
                                                   allow_nan=False)}}}
    requests.patch(f"https://api.github.com/gists/{gist_id}",
                   headers=headers, json=body, timeout=10).raise_for_status()


# ────────── main ──────────
def main() -> None:
    symbol  = os.getenv("SYMBOL", "SOLAYER-USDT")
    tf      = os.getenv("GRANULARITY", TF_DEFAULT)
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    fname   = os.getenv("FILE_NAME", FILE_DEFAULT)

    df = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        "timestamp":   int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol":      symbol,
        "granularity": tf,

        "price": _clean(last.close),   "high": _clean(last.high),
        "low":   _clean(last.low),     "vol":  _clean(last.vol),
        "ema20": _clean(last.ema20),   "ema50": _clean(last.ema50),
        "ema200": ("insufficient_data" if math.isnan(last.ema200)
                   else _clean(last.ema200)),
        "rsi14": _clean(last.rsi14),   "vwap": _clean(last.vwap),
        "atr14": _clean(last.atr14),   "vol_mean20": _clean(last.vol_mean20),

        "last_300_candles": [row_dict(r) for _, r in df.iterrows()],
        "funding_rate": None, "open_interest": None, "order_book": None,
        "generated_at": iso(int(time.time() * 1000))
    }

    push_gist(token, gist_id, fname, payload)
    print("✅  feed geüpload", payload["generated_at"])


if __name__ == "__main__":
    main()
