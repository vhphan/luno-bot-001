# %%
import json
import time
from datetime import datetime

import ccxt

import pandas as pd

from utils.helpers import milli_to_dt
from loguru import logger
import schedule

# %%
exchange = ccxt.binance()

# %%
tickers = pd.DataFrame([v for k, v in exchange.fetch_tickers()])

# %%
symbols_usdt = [v for k, v in exchange.fetch_tickers().items() if k.endswith('/USDT')]

# %%
luno_symbols = ['BCH', 'BTC', 'ETH', 'LTC', 'XRP']
binance_symbols = [symbol for symbol in symbols_usdt if symbol.get('symbol').replace('/USDT', '') in luno_symbols]


# %%
for i, k in enumerate(temp):
    print(i, k)

# %%
def get_all_symbols():
    exchange.tickers()


# %%
def fetch_bars(symbol='ETH/USDT', timeframe='1m'):
    logger.info(datetime.now().isoformat())
    bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=5)
    df = pd.DataFrame(bars[:-1], columns=[c for c in 'tohlcv'])
    df['t'] = df['t'].apply(milli_to_dt)
    print(df)
    return df


def job():
    pass


schedule.every(2).seconds.do(fetch_bars)

while True:
    schedule.run_pending()
    time.sleep(1)
