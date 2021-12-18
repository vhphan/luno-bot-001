##
import asyncio
import os
import time

import pandas as pd
from binance.client import Client, AsyncClient
from binance import BinanceSocketManager
from dotenv import load_dotenv
from loguru import logger
from retry import retry

##
load_dotenv()
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
async_client = AsyncClient(BINANCE_API_KEY, BINANCE_SECRET_KEY)


##
def get_top_tickers(top_n=5):
    tickers_df = pd.DataFrame(client.get_ticker())
    condition1 = tickers_df.symbol.str.endswith('USDT')
    condition2 = (~(tickers_df.symbol.str.contains('UP')) & ~(tickers_df.symbol.str.contains('DOWN')))

    t_usdt = tickers_df[(condition1) & (condition2)]
    sorted_df = t_usdt.sort_values(by='priceChangePercent', ascending=False)
    print(sorted_df[['symbol', 'priceChangePercent', 'lastPrice']].head(5))
    return sorted_df.head(top_n).symbol.to_list()


##
# @retry(tries=3, delay=61, logger=logger)
def get_minute_data(symbol, interval, look_back):
    frame = pd.DataFrame(client.get_historical_klines(symbol,
                                                      interval,
                                                      look_back + ' min ago UTC'))
    frame = frame.iloc[:, :6]
    frame.columns = list('tohlcv')
    frame.set_index('t')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame


##
top_symbol = get_top_tickers(1)[0]


##
def strategy(symbol, buy_amount=20, stop_loss=0.95, take_profit=1.1, open_position=False):
    df = get_minute_data(symbol, '1m', '120')
    qty = round(buy_amount / df['c'].iloc[-1])
    last_cum_change = ((df['c'].pct_change() + 1).cumprod()).iloc[-1]
    print(last_cum_change)
    if last_cum_change > 1:
        order_dict = dict(symbol=symbol, side='BUY', type='market', quantity=qty)
        print(f'create buy order {order_dict}')
        order = client.create_order(**order_dict)
        print(order)
        buy_price = float(order['fills'][0]['price'])
        open_position = True
        while open_position:
            df = get_minute_data(symbol, '1m', '2')
            if df['c'][-1] <= buy_price * stop_loss or df['c'][-1] >= buy_price * take_profit:
                order_dict = dict(symbol=symbol, side='SELL', type='market', quantity=qty)
                print(f'create sell order {order_dict}')
                order = client.create_order(**order_dict)
                print(order)


##
def create_frame(msg):
    df = pd.DataFrame([msg])
    df = df[['s', 'E', 'p']]
    df.columns = ['symbol', 'time', 'price']
    df.price = df.price.astype(float)
    df.time = pd.to_datetime(df.time, unit='ms')
    return df


##


async def aio_strategy(symbol, buy_amount=20, stop_loss=0.95, take_profit=1.1, open_position=False):
    bsm = BinanceSocketManager(async_client)
    socket = bsm.trade_socket(symbol)
    df = get_minute_data(symbol, '1m', '120')
    qty = round(buy_amount / df['c'].iloc[-1])
    last_cum_change = ((df['c'].pct_change() + 1).cumprod()).iloc[-1]
    print(df['c'].pct_change() + 1)
    print(last_cum_change)
    if last_cum_change > 1:
        order_dict = dict(symbol=symbol, side='BUY', type='market', quantity=qty)
        print(f'create buy order {order_dict}')
        order = client.create_order(**order_dict)
        print(order)
        buy_price = float(order['fills'][0]['price'])
        open_position = True
        while open_position:
            async with socket() as s:
                msg = await s.recv()
                df = create_frame(msg)
                if df['c'][-1] <= buy_price * stop_loss or df['c'][-1] >= buy_price * take_profit:
                    order_dict = dict(symbol=symbol, side='SELL', type='market', quantity=qty)
                    print(f'create sell order {order_dict}')
                    order = client.create_order(**order_dict)
                    print(order)


async def main():
    for i in range(10):
        await aio_strategy(symbol=top_symbol)
        time.sleep(5)


if __name__ == '__main__':
    asyncio.run(main())
