#!/usr/bin/env python3
"""
kucoin_solayer_feed.py – v4-fix
• vergaart gegarandeerd 300 × 15-min-candles (KuCoin-API max 150/call)
• berekent EMA20/50/200, RSI14, VWAP, ATR14, vol_mean20
• uploadt snapshot + volledige history naar GitHub Gist
"""

import os, json, time, datetime as dt, requests, pandas as pd, ta

API_URL           = "https://api.kucoin.com/api/v1/market/candles"
HISTORY           = 300            # benodigde candles
SEC_PER_CANDLE    = 15 * 60
DEFAULT_FILE_NAME = "solayer_feed.json"
DEFAULT_TF        = "15min"        # KuCoin interval-code

pd.options.mode.copy_on_write = True


# ───────────────────────────────── helpers ──────────────────────────────────
def _fetch_batch(symbol: str, tf: str, limit: int = 150,
                 end_at: int | None = None) -> list[list[float]]:
    """ Eén API-call. KuCoin retourneert nieuw->oud. """
    params = {"symbol": symbol, "type": tf, "limit": limit}
    if end_at is not None:
        params["endAt"] = end_at                           # unix-sec
    r = requests.get(API_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()["data"]


def fetch_frame(symbol: str, tf: str) -> pd.DataFrame:
    """Haalt *minimaal* 300 candles op door te pagineren met endAt."""
    raw, got, end_at = [], 0, None

    while got < HISTORY:
        chunk = _fetch_batch(symbol, tf, 150, end_at)
        if not chunk:
            break                              # safety-escape
        raw.extend(chunk)
        got += len(chunk)
        # volgend eindpunt = starttijd oudste candle – 1 sec
        oldest_ts_ms = float(chunk[-1][0])
        end_at = int(oldest_ts_ms / 1000 - SEC_PER_CANDLE - 1)

    cols = ["ts", "open", "close", "high", "low", "vol", "turnover"]
    df   = pd.DataFrame(raw, columns=cols).astype(float)

    df["ts"] = df["ts"].astype(int)
    df.sort_values("ts", inplace=True)               # oldest → newest
    df.drop_duplicates("ts", keep="last", inplace=True)
    df = df.tail(HISTORY)                            # exact 300

    # ────── indicatoren ──────
    df["ema20"]   = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"]   = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"]  = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"]   = ta.momentum.rsi(df["close"], 14)
    df["vwap"]    = ta.volume.volume_weighted_average_price(
                       df["high"], df["low"], df["close"], df["vol"], window=14)
    df["atr14"]   = ta.volatility.average_true_range(
                       df["high"], df["low"], df["close"], window=14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()
    return df.reset_index(drop=True)


def iso(ts_ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ts_ms // 1000).isoformat()


def row_dict(r: pd.Series) -> dict:
    return {k: float(v) if isinstance(v, (int, float)) else v for k, v in r.items()}


def push_gist(token: str, gist_id: str, file_name: str, payload: dict) -> None:
    hdr  = {"Authorization": f"token {token}",
            "Accept": "application/vnd.github+json"}
    body = {"files": {file_name: {"content": json.dumps(payload, separators=(',', ':'))}}}
    resp = requests.patch(f"https://api.github.com/gists/{gist_id}",
                          headers=hdr, json=body, timeout=10)
    resp.raise_for_status()


# ─────────────────────────────────── main ────────────────────────────────────
def main() -> None:
    symbol  = os.getenv("SYMBOL",       "SOLAYER-USDT")
    tf      = os.getenv("GRANULARITY",  DEFAULT_TF)
    gist_id = os.environ["GIST_ID"]
    token   = os.environ["GIST_TOKEN"]
    f_name  = os.getenv("FILE_NAME",    DEFAULT_FILE_NAME)

    df   = fetch_frame(symbol, tf)
    last = df.iloc[-1]

    payload = {
        # snapshotveld
        "timestamp":   int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol":      symbol,
        "granularity": tf,

        **row_dict(last),                        # price / indics / vol …

        # volledige history
        "last_300_candles": [row_dict(r) for _, r in df.iterrows()],

        # placeholders
        "funding_rate":  None,
        "open_interest": None,
        "order_book":    None,

        "generated_at":  iso(int(time.time() * 1000))
    }

    push_gist(token, gist_id, f_name, payload)
    print("✅  feed uploaded", payload["generated_at"])


if __name__ == "__main__":
    main()
