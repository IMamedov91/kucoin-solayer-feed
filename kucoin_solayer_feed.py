#!/usr/bin/env python3
"""
Multi-TF, volatility- & funding-filtered signal feed for LAYER-USDT.
"""
import os, json, datetime as dt, sys, requests

# ── Config ──────────────────────────────────────────────────────────────────
SYMBOL           = os.getenv('SYMBOL', 'LAYER/USDT')
TF_FAST          = os.getenv('TF_FAST', '15m')
TF_SLOW          = os.getenv('TF_SLOW', '1h')
ATR_PCT_MIN      = float(os.getenv('ATR_PCT_MIN', '0.005'))
FUNDING_MAX_LONG = float(os.getenv('FUNDING_MAX_LONG', '0'))
FUNDING_MIN_SHORT= float(os.getenv('FUNDING_MIN_SHORT', '0.0002'))
MACD_EPS         = float(os.getenv('MACD_EPS', '0.0005'))
RSI_HIGH         = float(os.getenv('RSI_HIGH', '55'))
RSI_LOW          = float(os.getenv('RSI_LOW', '45'))
FILE_NAME        = os.getenv('FILE_NAME', 'solayer_feed.json')

TAAPI_SEC  = os.environ['TAAPI_SECRET']
GIST_ID    = os.environ['GIST_ID']
GIST_TOKEN = os.environ['GIST_TOKEN']

BASE = "https://api.taapi.io/"
REQ  = requests.Session()

# indicator-set
INDICS = [("ema",{"period":50}),("ema",{"period":200}),("rsi",{"period":14}),("macd",{}),("average-true-range",{"period":14}),("price",{})]

def taapi_call(endpoint, extra, tf):
    params = {"secret":TAAPI_SEC,"exchange":"binance","symbol":SYMBOL,"interval":tf,**extra}
    r = REQ.get(BASE+endpoint, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch(tf):
    out={}
    for ep,ex in INDICS:
        res=taapi_call(ep,ex,tf)
        if ep=="macd":
            out["macd"]=res.get("valueMACDHistogram")
        elif ep=="rsi":
            out["rsi"]=res["value"]
        elif ep=="average-true-range":
            out["atr"]=res["value"]
        elif ep=="price":
            out["price"]=res["value"]
        else:
            key=f"ema{ex['period']}"
            out[key]=res["value"]
    return out

def decide(d):
    trend_up   = d["ema50"]>d["ema200"]
    trend_down = d["ema50"]<d["ema200"]
    hist       = d["macd"]
    bull = hist is not None and hist> MACD_EPS
    bear = hist is not None and hist<-MACD_EPS
    if trend_up and bull and d["rsi"]>RSI_HIGH:   return "long"
    if trend_down and bear and d["rsi"]<RSI_LOW:  return "short"
    return "flat"

def funding_ok(bias):
    sym=SYMBOL.replace("/","")
    url="https://fapi.binance.com/fapi/v1/fundingRate"
    r=REQ.get(url, params={"symbol":sym,"limit":1}, timeout=10)
    r.raise_for_status()
    rate=float(r.json()[0]["fundingRate"])
    if bias=="long"  and rate>FUNDING_MAX_LONG:  return False
    if bias=="short" and rate<FUNDING_MIN_SHORT: return False
    return True

def volatility_ok(d):
    return (d["atr"]/d["price"])>=ATR_PCT_MIN

def main():
    fast=fetch(TF_FAST)
    slow=fetch(TF_SLOW)

    bias_fast=decide(fast) if volatility_ok(fast) else "flat"
    bias_slow=decide(slow)

    final=bias_fast if bias_fast==bias_slow else "flat"
    if final!="flat" and not funding_ok(final):
        final="flat"

    payload={
        "symbol":SYMBOL.replace("/",""),
        "ts":dt.datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "bias15m":bias_fast,
        "bias1h":bias_slow,
        "finalBias":final,
        "indicators":{
            TF_FAST:fast,
            TF_SLOW:slow
        },
        "ttl_sec":900
    }

    open(FILE_NAME,"w").write(json.dumps(payload,indent=2))

    body={"files":{FILE_NAME:{"content":json.dumps(payload,indent=2)}}}
    r=REQ.patch(f"https://api.github.com/gists/{GIST_ID}",
                headers={"Authorization":f"token {GIST_TOKEN}",
                         "Accept":"application/vnd.github+json"},
                data=json.dumps(body),timeout=15)
    r.raise_for_status()
    print("Gist updated:",r.json().get("html_url"))

if __name__=="__main__":
    main()
