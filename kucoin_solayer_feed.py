#!/usr/bin/env python3
import os, json, time, datetime as dt, requests, sys

# ── ENV ──────────────────────────────────────────────────
SYMBOL = os.getenv("SYMBOL", "LAYER/USDT")
TF_FAST, TF_SLOW = os.getenv("TF_FAST", "15m"), os.getenv("TF_SLOW", "1h")
ATR_PCT_MIN      = float(os.getenv("ATR_PCT_MIN", "0.005"))
FUND_MAX_LONG    = float(os.getenv("FUNDING_MAX_LONG", "0"))
FUND_MIN_SHORT   = float(os.getenv("FUNDING_MIN_SHORT", "0.0002"))
MACD_EPS         = float(os.getenv("MACD_EPS", "0.0005"))
RSI_H, RSI_L     = float(os.getenv("RSI_HIGH", "55")), float(os.getenv("RSI_LOW", "45"))
FILE_NAME        = os.getenv("FILE_NAME", "solayer_feed.json")

TAAPI_SEC, GIST_ID, GIST_TOKEN = map(os.environ.get,
                                     ["TAAPI_SECRET","GIST_ID","GIST_TOKEN"])
BASE = "https://api.taapi.io"
REQ  = requests.Session()

# ── Indicator-bouwsteen voor bulk ────────────────────────
def bulk_body(tf: str):
    return {
        "secret": TAAPI_SEC,
        "construct": {
            "exchange": "binance",
            "symbol": SYMBOL,
            "interval": tf,
            "indicators": [
                {"id":"ema50",  "indicator":"ema",  "period":50},
                {"id":"ema200", "indicator":"ema",  "period":200},
                {"id":"rsi",    "indicator":"rsi",  "period":14},
                {"id":"macd",   "indicator":"macd"},
                {"id":"atr",    "indicator":"atr",  "period":14},
                {"id":"price",  "indicator":"price"}
            ]
        }
    }

def fetch(tf: str):
    r = REQ.post(f"{BASE}/bulk", json=bulk_body(tf), timeout=12)
    r.raise_for_status()
    data = {item["id"]: item["result"] for item in r.json()["data"]}
    return {
        "ema50":  data["ema50"]["value"],
        "ema200": data["ema200"]["value"],
        "rsi":    data["rsi"]["value"],
        "macd":   data["macd"]["valueMACDHist"],
        "atr":    data["atr"]["value"],
        "price":  data["price"]["value"]
    }

def decide(d):
    up, down = d["ema50"] > d["ema200"], d["ema50"] < d["ema200"]
    bull, bear = d["macd"] >  MACD_EPS, d["macd"] < -MACD_EPS
    if up   and bull and d["rsi"] > RSI_H: return "long"
    if down and bear and d["rsi"] < RSI_L: return "short"
    return "flat"

def funding_ok(bias):
    sym = SYMBOL.replace("/","")
    data = REQ.get("https://fapi.binance.com/fapi/v1/fundingRate",
                   params={"symbol":sym,"limit":1}, timeout=8).json()[0]
    rate = float(data["fundingRate"])
    return not ((bias=="long"  and rate>FUND_MAX_LONG) or
                (bias=="short" and rate<FUND_MIN_SHORT))

def vol_ok(d):        # ATR-percentage filter
    return d["atr"]/d["price"] >= ATR_PCT_MIN

def main():
    fast = fetch(TF_FAST)
    time.sleep(0.4)   # kleine pauze → extra safety
    slow = fetch(TF_SLOW)

    bias_fast = decide(fast) if vol_ok(fast) else "flat"
    bias_slow = decide(slow)
    final     = bias_fast if bias_fast==bias_slow else "flat"
    if final!="flat" and not funding_ok(final):
        final="flat"

    payload = {
        "symbol":     SYMBOL.replace("/",""),
        "timestamp":  dt.datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "bias15m":    bias_fast,
        "bias1h":     bias_slow,
        "finalBias":  final,
        "indicators": {TF_FAST:fast, TF_SLOW:slow},
        "ttl_sec":    900
    }

    with open(FILE_NAME,"w") as fp: json.dump(payload, fp, indent=2)

    body={"files":{FILE_NAME:{"content":json.dumps(payload,indent=2)}}}
    r=REQ.patch(f"https://api.github.com/gists/{GIST_ID}",
                headers={"Authorization":f"token {GIST_TOKEN}",
                         "Accept":"application/vnd.github+json"},
                json=body, timeout=12)
    r.raise_for_status()
    print("Gist updated:", r.json().get("html_url"))

if __name__=="__main__":
    try: main()
    except requests.HTTPError as e:
        print("HTTP error:",e, file=sys.stderr)
        sys.exit(1)
