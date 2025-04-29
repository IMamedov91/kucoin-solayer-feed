#!/usr/bin/env python3
"""Fetch TAAPI indicators for LAYER/USDT (15 m), decide long/short/flat, and PATCH a GitHub Gist."""
import os, json, datetime as dt, requests, sys

SYMBOL      = os.getenv('SYMBOL', 'LAYER/USDT')
INTERVAL    = os.getenv('INTERVAL', '15m')
MACD_EPS    = float(os.getenv('MACD_EPS', '0.0005'))  # neutral zone for MACD hist
FILE_NAME   = os.getenv('FILE_NAME', 'solayer_feed.json')
TAAPI_SEC   = os.environ['TAAPI_SECRET']
GIST_ID     = os.environ['GIST_ID']
GIST_TOKEN  = os.environ['GIST_TOKEN']

SESSION = requests.Session()
BASE_URL = 'https://api.taapi.io/'

INDICATORS = [
    ('ema', 50),
    ('ema', 200),
    ('rsi', 14),
    ('macd', None),  # TAAPI default (12,26,9)
]

def fetch_one(kind: str, period):
    params = {
        'secret': TAAPI_SEC,
        'exchange': 'binance',
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'indicator': kind,
    }
    if period is not None:
        params['period'] = period
    r = SESSION.get(BASE_URL + 'indicator', params=params, timeout=10)
    r.raise_for_status()
    return r.json()['value']


def fetch_all():
    out = {}
    for kind, period in INDICATORS:
        val = fetch_one(kind, period)
        key = f"{kind}{period or ''}"
        out[key] = val if not isinstance(val, dict) else val  # MACD returns dict
    return out


def decide(data):
    ema50, ema200 = data['ema50'], data['ema200']
    macd_hist     = data['macd']['histogram']
    rsi           = data['rsi14']

    uptrend   = ema50 > ema200
    downtrend = ema50 < ema200

    bull_mom  = macd_hist >  MACD_EPS
    bear_mom  = macd_hist < -MACD_EPS

    long  = uptrend   and bull_mom and rsi > 55
    short = downtrend and bear_mom and rsi < 45
    if long:
        return 'long'
    if short:
        return 'short'
    return 'flat'


def main():
    try:
        data = fetch_all()
    except Exception as e:
        print('Error fetching indicators:', e, file=sys.stderr)
        sys.exit(1)

    bias = decide(data)

    payload = {
        'symbol': SYMBOL.replace('/', ''),
        'interval': INTERVAL,
        'timestamp': dt.datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'indicators': data,
        'bias': bias,
        'ttl_sec': 900,  # 15 min validity
    }

    with open(FILE_NAME, 'w') as fp:
        json.dump(payload, fp, indent=2)

    patch_body = {
        'files': {FILE_NAME: {'content': json.dumps(payload, indent=2)}}
    }

    resp = SESSION.patch(
        f'https://api.github.com/gists/{GIST_ID}',
        headers={
            'Authorization': f'token {GIST_TOKEN}',
            'Accept': 'application/vnd.github+json',
        },
        data=json.dumps(patch_body),
        timeout=15,
    )
    resp.raise_for_status()
    print('Gist updated →', resp.json().get('html_url', 'ok'))

if __name__ == '__main__':
    main()
