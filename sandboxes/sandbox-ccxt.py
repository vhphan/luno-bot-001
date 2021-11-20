# %%
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
def fetch_bars():
    logger.info(datetime.now().isoformat())
    bars = exchange.fetch_ohlcv('ETH/USDT', timeframe='1m', limit=5)
    df = pd.DataFrame(bars[:-1], columns=[c for c in 'tohlcv'])
    df['t'] = df['t'].apply(milli_to_dt)
    print(df)


schedule.every(2).seconds.do(fetch_bars)

while True:
    schedule.run_pending()
    time.sleep(1)
