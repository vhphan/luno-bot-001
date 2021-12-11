# %%
import os
import time

import pandas as pd
from binance.client import Client
from dotenv import load_dotenv
from retry import retry
from loguru import logger

# %%
load_dotenv()
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)


# %%
def get_top_tickers(top_n=5):
    tickers_df = pd.DataFrame(client.get_ticker())
    condition1 = tickers_df.symbol.str.endswith('USDT')
    condition2 = (~(tickers_df.symbol.str.contains('UP')) & ~(tickers_df.symbol.str.contains('DOWN')))

    t_usdt = tickers_df[(condition1) & (condition2)]
    sorted_df = t_usdt.sort_values(by='priceChangePercent', ascending=False)
    print(sorted_df[['symbol', 'priceChangePercent', 'lastPrice']].head(5))
    return sorted_df.head(top_n).symbol.to_list()


# %%
@retry(tries=3, delay=61, logger=logger)
def get_minute_data(symbol, interval, look_back):
    frame = pd.DataFrame(client.get_historical_klines(symbol,
                                                      interval,
                                                      look_back + ' min ago UTC'))
    frame = frame.iloc[:, :6]
    frame.columns = 'tohlcv'
    frame.set_index('t')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame.astype(float)
    return frame


# %%
top_symbol = get_top_tickers(1)[0]


# %%
def strategy(symbol, buy_amount, stop_loss, take_profit, open_position=False):
    df = get_minute_data(symbol, '1m', 120)
    qty = round(buy_amount / df['c'].iloc[-1])
    if ((df['c'].pct_change() + 1).cumprod()).iloc[-1] > 1:
        order_dict = dict(symbol=symbol, side='BUY', type='market')
        print(f'create buy order {order_dict}')
        order = client.create_order(**order_dict)
        print(order)
        buy_price = float(order['fills'][0]['price'])
        open_position = True
        while open_position:
            df = get_minute_data(symbol, '1m', '2')
            if df['c'][-1] <= buy_price*stop_loss or df['c'][-1] >=buy_price*take_profit:
                order_dict = dict(symbol=symbol, side='SELL', type='market')
                print(f'create sell order {order_dict}')
                order = client.create_order(**order_dict)
