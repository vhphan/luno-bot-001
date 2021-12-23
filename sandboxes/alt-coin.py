##
import asyncio
import os
import time

import pandas as pd
from binance import BinanceSocketManager
from binance.client import Client, AsyncClient
from binance.exceptions import BinanceAPIException, BinanceOrderException
from dotenv import load_dotenv
from loguru import logger

from utils.helpers import milli_to_dt, async_retry, TooManyTriesException

logger.add("alt-coin.txt", rotation="1 MB")

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

    t_usdt = tickers_df[condition1 & condition2]
    sorted_df = t_usdt.sort_values(by='priceChangePercent', ascending=False)
    logger.info(sorted_df[['symbol', 'priceChangePercent', 'lastPrice']].head(5))
    return sorted_df.head(top_n).symbol.to_list()


##
# @retry(tries=3, delay=61, logger=logger)
def get_minute_data(symbol, interval, look_back):
    frame = pd.DataFrame(client.get_historical_klines(symbol,
                                                      interval,
                                                      look_back + ' min ago UTC'))
    frame = frame.iloc[:, :6]
    frame.columns = list('tohlcv')
    frame.t = frame.t.apply(milli_to_dt)
    frame = frame.set_index('t')
    # frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame


##
def create_frame(msg):
    df = pd.DataFrame([msg])
    df = df[['s', 'E', 'p']]
    df.columns = ['symbol', 'time', 'price']
    df.price = df.price.astype(float)
    df.time = pd.to_datetime(df.time, unit='ms')
    return df


##
@async_retry(exceptions=(BinanceAPIException, BinanceOrderException), retries=2, logger=logger)
async def aio_strategy(symbol, buy_amount=100, stop_loss=0.95, take_profit=1.1):
    logger.info(f'strategy for {symbol}...')

    df = get_minute_data(symbol, '1m', '120')
    last_closing_price = df['c'].iloc[-1]
    qty = round(buy_amount / last_closing_price)
    if qty == 0:
        logger.info(f'Quantity = 0. Perhaps we cannot afford this {symbol}, Price={last_closing_price}')
        return
    last_cum_change = ((df['c'].pct_change() + 1).cumprod()).iloc[-1]
    # logger.info(df['c'].pct_change() + 1)
    logger.info(f'{symbol} price={last_closing_price}, last cumulative return {last_cum_change}')
    if last_cum_change > 1:
        order_dict = dict(symbol=symbol, side='BUY', type='market', quantity=qty)
        logger.info(f'create buy order {order_dict}')
        order = client.create_order(**order_dict)
        logger.info(order)
        buy_price = float(order['fills'][0]['price'])
        open_position = True
        bsm = BinanceSocketManager(async_client)
        socket = bsm.trade_socket(symbol)
        async with socket() as s:
            while open_position:
                msg = await s.recv()
                df = create_frame(msg)
                last_closing_price = df['c'][-1]
                if last_closing_price <= buy_price * stop_loss or last_closing_price >= buy_price * take_profit:
                    order_dict = dict(symbol=symbol, side='SELL', type='market', quantity=qty)
                    logger.info(f'create sell order {order_dict}')
                    order = client.create_order(**order_dict)
                    logger.info(order)
                    open_position = False
                    time.sleep(60 * 5)  # cooling period


async def main():
    top_symbols = get_top_tickers(5)
    for top_symbol in top_symbols:
        logger.info(f'running strategy for {top_symbol}')
        try:
            await aio_strategy(symbol=top_symbol)
        except TooManyTriesException:
            logger.info(f'Skipping {top_symbol} due to too many tries')
        time.sleep(5)


if __name__ == '__main__':
    asyncio.run(main())
