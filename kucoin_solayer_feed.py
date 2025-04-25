#!/usr/bin/env python3
"""
kucoin_btc_feed.py — v1.2
• 15-min futures-candles (XBTUSDTM) van KuCoin
• indicator-set: EMA-20/50/200, RSI-14, VWAP, ATR-14, vol_mean20
• bewaart laatste 300 candles in een GitHub Gist
"""

from __future__ import annotations
import datetime as dt, json, math, os, time, typing as t
import pandas as pd, requests, ta

# ── CONFIG ──────────────────────────────────────────────────────────
SYMBOL_DEFAULT  = os.getenv("SYMBOL", "XBTUSDTM")           # officiële BTC-USDT-perp :contentReference[oaicite:1]{index=1}
TF_MIN          = int(os.getenv("GRANULARITY", "15"))       # minuten!
API_URL         = "https://api-futures.kucoin.com/api/v1/kline/query"
MAX_LIMIT       = 500

FETCH_LEN       = 550
SNAPSHOT_LEN    = 300
MS_PER_BAR      = TF_MIN * 60_000
FILE_DEFAULT    = os.getenv("FILE_NAME", "btc_feed.json")

pd.options.mode.copy_on_write = True

# ── HELPERS ─────────────────────────────────────────────────────────
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
    raise RuntimeError(f"KuCoin-API bleef falen: {err}") from err


def fetch_frame(symbol: str, tf_min: int) -> pd.DataFrame:
    raw: list[list[t.Any]] = []
    end_ms = int(time.time() * 1000)

    while len(raw) < FETCH_LEN:
        batch = _get({
            "symbol":      symbol,
            "granularity": tf_min,     # ✅ minuten, géén seconden
            "limit":       MAX_LIMIT,
            "to":          end_ms
        })
        if not batch:
            break
        raw.extend(batch)
        end_ms = int(batch[-1][0]) - MS_PER_BAR

    cols = ["ts", "open", "close", "high", "low", "vol"]
    df = (pd.DataFrame(raw, columns=cols)
            .astype(float)
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True))

    # ─ indicatoren ─
    df["ema20"]  = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]  = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200) if len(df) >= 200 else float("nan")
    df["rsi14"]  = ta.momentum.rsi(df["close"], 14)
    df["vwap"]   = ta.volume.volume_weighted_average_price(df["high"], df["low"], df["close"], df["vol"], 14)
    df["atr14"]  = ta.volatility.average_true_range(df["high"], df["low"], df["close"], 14) if len(df) >= 14 else float("nan")
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)

# ── UTILITIES ───────────────────────────────────────────────────────
iso     = lambda ms: dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds") + "Z"
_clean  = lambda v: None if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) else v
row     = lambda r: {**{k: _clean(v) for k, v in r.items()}, "ts": int(r.ts)}

def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
        json={"files": {fname: {"content": json.dumps(payload, separators=(',', ':'), allow_nan=False)}}},
        timeout=10
    ).raise_for_status()

# ── MAIN ────────────────────────────────────────────────────────────
def main() -> None:
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]

    df   = fetch_frame(SYMBOL_DEFAULT, TF_MIN)
    last = df.iloc[-1]

    payload = {
        "timestamp":   int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol":      SYMBOL_DEFAULT,
        "granularity": str(TF_MIN),
        "price": _clean(last.close),   "high": _clean(last.high),
        "low":   _clean(last.low),     "vol":  _clean(last.vol),
        "ema20": _clean(last.ema20),   "ema50": _clean(last.ema50),
        "ema200": _clean(last.ema200) if not math.isnan(last.ema200) else "insufficient_data",
        "rsi14": _clean(last.rsi14),   "vwap": _clean(last.vwap),
        "atr14": _clean(last.atr14),   "vol_mean20": _clean(last.vol_mean20),
        "last_300_candles": [row(r) for _, r in df.iterrows()],
        "funding_rate": None, "open_interest": None, "order_book": None,
        "generated_at": iso(int(time.time()*1000))
    }

    push_gist(token, gist_id, FILE_DEFAULT, payload)
    print("✅ BTC feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
