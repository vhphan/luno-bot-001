##
import os
import time
from datetime import datetime

import binance.enums
import pandas as pd
import schedule
from binance.client import Client, AsyncClient
# from binance.enums import KLINE_timeframe_4HOUR
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv
from loguru import logger
import pandas_ta as ta
# from TAcharts.indicators.ichimoku import Ichimoku
from database.main_db import postgres_db
from utils.helpers import milli_to_dt

logger.add("binance-bot.log", rotation="1 MB")

##

load_dotenv()
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
async_client = AsyncClient(BINANCE_API_KEY, BINANCE_SECRET_KEY)


class BinanceAnalyzer:

    def __init__(self, timeframe, tickers=None, top_n=50):
        self.num_of_candles = 200
        self.timeframe = timeframe
        self.tickers = tickers if tickers is not None else self.get_tickers(top_n=top_n)
        self.signals = {ticker: 0 for ticker in tickers}
        self.last_close = {ticker: None for ticker in tickers}
        self.db = postgres_db(schema='binance')

    def add_ticker_to_analyse(self, ticker):
        if ticker not in self.tickers:
            self.tickers.append(ticker)
        else:
            logger.info("ticker already in analyzer's list.")

    @staticmethod
    def get_tickers(top_n, fmt='list'):
        tickers_df = pd.DataFrame(client.get_ticker())
        condition1 = tickers_df.symbol.str.endswith('USDT')
        condition2 = (~(tickers_df.symbol.str.contains('UP')) & ~(tickers_df.symbol.str.contains('DOWN')))
        t_usdt = tickers_df[condition1 & condition2]
        sorted_df = t_usdt.sort_values(by='priceChangePercent', ascending=False)
        if fmt == 'list':
            return sorted_df.head(top_n).symbol.to_list()
        return sorted_df.head(top_n)

    def get_candles_data(self, symbol, look_back_minutes_ago):
        df = pd.DataFrame(client.get_historical_klines(symbol,
                                                       self.timeframe,
                                                       look_back_minutes_ago + ' min ago UTC'))
        df = df.iloc[:, :6]
        # df.columns = list('tohlcv')
        df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
        df['time'] = df['time'].apply(milli_to_dt)
        # df = df.set_index('time')
        # frame.index = pd.to_datetime(frame.index, unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        return df

    @staticmethod
    def create_dataframe_from_socket(msg):
        # create dataframe from binance websocket's message
        df = pd.DataFrame([msg])
        df = df[['s', 'E', 'p']]
        df.columns = ['symbol', 'time', 'price']
        df.price = df.price.astype(float)
        df.time = pd.to_datetime(df.time, unit='ms')
        return df

    def strategy(self):
        time_unit = self.timeframe[-1]
        time_value = int(self.timeframe[:-1])
        if time_unit == 'm':
            coefficient = 1
        elif time_unit == 'h':
            coefficient = 60
        elif time_unit == 'd':
            coefficient = 24 * 60
        else:
            raise Exception(f'time unit unknown {self.timeframe}')
        minutes_ago = self.num_of_candles * coefficient * time_value

        for ticker in self.tickers:
            candles = self.get_candles_data(ticker, str(minutes_ago))
            # candles.ta.cdl_pattern(name=["engulfing"], append=True)
            # candles.ta.ha(append=True)
            df_indicators, _ = candles.ta.ichimoku(lookahead=True)
            cloud_is_green = df_indicators['ISA_9'] > df_indicators['ISB_26']
            price_above_cloud = candles['close'] > df_indicators[["ISA_9", "ISB_26"]].max(axis=1)
            conversion_above_base = df_indicators['ITS_9'] > df_indicators['IKS_26']
            confirmation = df_indicators['ICS_26'] > candles['close'].shift(26)
            buy_signals = cloud_is_green & price_above_cloud & conversion_above_base & confirmation
            sell_signals = ~confirmation
            buy = buy_signals.iloc[-1]
            sell = sell_signals.iloc[-1]
            self.signals[ticker] = 1 if buy else (-1 if sell else 0)
            self.last_close[ticker] = candles.iloc[-1]['close']

            candles['timeframe'] = self.timeframe
            candles['ticker'] = ticker
            candles['cloud_is_green'] = cloud_is_green
            candles['price_above_cloud'] = price_above_cloud
            candles['conversion_above_base'] = conversion_above_base
            candles['confirmation'] = confirmation
            candles['buy_signals'] = buy_signals
            candles['sell_signals'] = sell_signals
            self.save_results_to_db(candles)
        return self.signals, self.last_close

    def save_results_to_db(self, df):
        delete_query = f"DELETE FROM cctx.binance.temp_prices WHERE TRUE"
        self.db.query(delete_query)
        self.db.df_to_db(df, name=f'temp_prices', if_exists='append', index=False)
        delete_query2 = """
            DELETE
                FROM cctx.binance.prices
                WHERE (time, ticker, timeframe)
                          NOT IN
                      (SELECT time, ticker, timeframe FROM temp_prices);
        """
        self.db.query(delete_query2)
        insert_query = """
            INSERT INTO cctx.binance.prices
                SELECT * FROM cctx.binance.temp_prices;
        """
        self.db.query(insert_query)


class BinanceTrader:

    def __init__(self, analyzer: BinanceAnalyzer, positions=None, position_size=100, stop_loss=0.95, take_profit=1.1):
        self.client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        self.async_client = AsyncClient(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        self.analyzer = analyzer
        self.position_size = position_size
        self.positions = {ticker: False for ticker in analyzer.tickers} if positions is None else positions
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.stop_losses = {}
        self.take_profits = {}

    def run(self):
        in_positions = self.positions
        tickers_to_buy = []
        tickers_to_sell = []
        signals, close_prices = self.analyzer.strategy()
        logger.info(signals)

        for ticker, signal in signals.items():
            logger.info(f"{'buy' if signal == 1 else ('sell' if signal == -1 else 'neutral')} signal for {ticker}")
            logger.info(f'checking positions now for {ticker}. position={in_positions[ticker]}')

            # check stop loss or take profit
            if in_positions.get(ticker) and self.stop_losses[ticker] is not None:
                last_price = close_prices[ticker]
                if last_price <= self.stop_losses[ticker] or last_price >= self.take_profits[ticker]:
                    logger.info(f'stop loss triggered for {ticker} at {last_price}')
                    self.sell([ticker])
                    self.stop_losses[ticker] = None
                    self.take_profits[ticker] = None

            if signal == 0:
                continue
            if signal == 1:
                if not in_positions.get(ticker):
                    logger.info(f"buy {ticker}")
                    tickers_to_buy.append(ticker)
                else:
                    logger.info(f"signal to buy {ticker} but you are already in position")

            elif signal == -1:
                if not in_positions.get(ticker):
                    logger.info(f"signal to sell {ticker} but you are not in position")
                else:
                    tickers_to_sell.append(ticker)
                    logger.info(f"sell {ticker}")

        return {'buy': tickers_to_buy, 'sell': tickers_to_sell}

    def buy(self, tickers_to_buy):
        for ticker in tickers_to_buy:
            if self.positions[ticker]:
                logger.info(f'Already in position for {ticker}. Skipping buy..')
                continue
            order_dict = dict(symbol=ticker, side='BUY', type='market', quantity=self.position_size)
            logger.info(f'create buy order {order_dict}')
            try:
                order = client.create_order(**order_dict)
                logger.info('BUY order:')
                logger.info(order)
                self.positions[ticker] = True
                buy_price = float(order['fills'][0]['price'])
                self.stop_losses[ticker] = buy_price * self.stop_loss
                self.take_profits[ticker] = buy_price * self.take_profit
            except BinanceAPIException as e:
                logger.error(e)
                logger.error(order_dict)

    def sell(self, tickers_to_sell):
        for ticker in tickers_to_sell:
            if not self.positions[ticker]:
                logger.info(f'Not in position for {ticker}. Skipping sell..')
                continue
            order_dict = dict(symbol=ticker, side='SELL', type='market', quantity=self.position_size)
            try:
                order = client.create_order(**order_dict)
                logger.info('SELL order:')
                logger.info(order)
                self.positions[ticker] = False

            except BinanceAPIException as e:
                logger.error(e)
                logger.error(order_dict)


def main():
    logger.info(f'triggered at {datetime.now()}')
    global my_analyzer, my_trader
    tickers_orders = my_trader.run()
    my_trader.buy(tickers_orders.get('buy'))
    my_trader.sell(tickers_orders.get('sell'))


if __name__ == '__main__':
    timeframe = '1m'

    my_analyzer = BinanceAnalyzer(timeframe=timeframe, tickers=['ETHUSDT'])
    # tickers_ = my_analyzer.get_tickers(5, fmt='full')
    my_trader = BinanceTrader(analyzer=my_analyzer)

    schedule.every().minute.do(main)

    while True:
        schedule.run_pending()
        time.sleep(30)
