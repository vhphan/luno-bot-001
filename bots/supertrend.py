import json
import os
import time
from datetime import datetime
from pprint import pprint

import ccxt
import numpy as np

import pandas as pd
from dotenv import load_dotenv

from bots.moon_bot import MoonClient
from database.main_db import postgres_db
from utils.helpers import milli_to_dt
from loguru import logger
import schedule

INITIAL_POSITIONS = {'ETH/USDT': True, 'LTC/USDT': False, 'XRP/USDT': True, 'BCH/USDT': False}
task_id = datetime.now().strftime('%Y%m%d-%H%M%S')
logger.add(f"supertrend_{task_id}.txt", rotation="1 MB")

load_dotenv()
LUNO_API_KEY_TRADE = os.getenv('LUNO_API_KEY_TRADE')
LUNO_SECRET_KEY_TRADE = os.getenv('LUNO_SECRET_KEY_TRADE')
BINANCE_LUNO_MAPPING = list(zip(
    ['BCH', 'BTC', 'ETH', 'LTC', 'XRP'],
    ['BCH', 'XBT', 'ETH', 'LTC', 'XRP'],
))


class Analyzer:
    def __init__(self, atr_window=14, multiplier=5, timeframe='1m'):
        self.multiplier = multiplier
        self.atr_window = atr_window
        self.timeframe = timeframe
        self.exchange = ccxt.binance()
        trade_symbols = ['BCH', 'ETH', 'LTC', 'XRP']
        # trade_symbols = ['BCH', 'BTC', 'ETH', 'LTC', 'XRP']
        # trade_symbols = ['ETH']
        symbols_usdt = [v for k, v in self.exchange.fetch_tickers().items() if k.endswith('/USDT')]
        self.symbols = [symbol for symbol in symbols_usdt if
                        symbol.get('symbol').replace('/USDT', '') in trade_symbols]
        self.latest_results = None

        tickers = [symbol.get('symbol') for symbol in self.symbols]
        self.signals = {ticker: 0 for ticker in tickers}
        self.db = postgres_db()

    def fetch_bars(self, symbol='ETH/USDT', number_of_points=500):
        logger.info(datetime.now().isoformat())
        bars = self.exchange.fetch_ohlcv(symbol, timeframe=self.timeframe, limit=number_of_points)
        df = pd.DataFrame(bars[:-1], columns=[c for c in 'tohlcv'])
        df['t'] = df['t'].apply(milli_to_dt)

        return df

    def add_atr(self, symbol):
        df_ = self.fetch_bars(symbol=symbol)
        df_['pc'] = df_['c'].shift(1)  # pc ~ previous close
        df_['h-l'] = abs(df_['h'] - df_['l'])
        df_['h-pc'] = abs(df_['h'] - df_['pc'])
        df_['l-pc'] = abs(df_['l'] - df_['pc'])
        df_['tr'] = df_[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        df_['atr'] = df_['tr'].rolling(self.atr_window).mean()
        return df_

    def add_supertrend(self, symbol):
        # Reference: https://www.tradinformed.com/calculate-supertrend-indicator-using-excel/
        df_ = self.add_atr(symbol=symbol)
        middle = (df_['h'] + df_['l']) / 2
        scaled_atr = self.multiplier * df_['atr']
        df_['upper'], df_['lower'] = middle + scaled_atr, middle - scaled_atr
        df_ = df_[(self.atr_window - 1):].copy().reset_index(drop=True)

        df_['final_lower'] = None
        df_['final_upper'] = None
        df_['supertrend'] = None
        df_['in_uptrend'] = True

        for i, (lower, upper, close) in enumerate(zip(df_['lower'].values,
                                                      df_['upper'].values,
                                                      df_['c'].values,
                                                      )):

            if i == 0:
                df_.loc[i, 'final_lower'] = lower
                df_.loc[i, 'final_upper'] = upper
                continue

            if upper < df_.iloc[i - 1]['final_upper'] or df_.iloc[i - 1]['c'] > df_.iloc[i - 1]['final_upper']:
                df_.loc[i, 'final_upper'] = upper
            else:
                df_.loc[i, 'final_upper'] = df_.loc[i - 1, 'final_upper']

            if lower > df_.iloc[i - 1]['final_lower'] or df_.iloc[i - 1]['final_lower'] > df_.iloc[i - 1]['c']:
                df_.loc[i, 'final_lower'] = lower
            else:
                df_.loc[i, 'final_lower'] = df_.loc[i - 1, 'final_lower']

            if close > df_.iloc[i - 1]['final_upper']:
                df_.loc[i, 'in_uptrend'] = True
            elif close < df_.iloc[i - 1]['final_lower']:
                df_.loc[i, 'in_uptrend'] = False
            else:
                df_.loc[i, 'in_uptrend'] = df_.loc[i - 1, 'in_uptrend']

            df_.loc[i, 'supertrend'] = df_.loc[i, 'final_upper'] if df_.loc[i, 'in_uptrend'] else df_.loc[
                i, 'final_lower']

        return df_

    def supertrend_symbols(self):
        tickers = [symbol.get('symbol') for symbol in self.symbols]
        self.latest_results = {
            ticker: self.add_supertrend(symbol=ticker) for ticker in tickers
        }
        return self.latest_results

    def save_results_to_db(self):
        delete_query = f"DELETE FROM cctx.public.temp_prices WHERE TRUE"
        self.db.query(delete_query)
        for index, (ticker, df) in enumerate(self.latest_results.items()):
            df['ticker'] = ticker
            df['timeframe'] = self.timeframe
            self.db.df_to_db(df, name=f'temp_prices', if_exists='append', index=False)
        delete_query2 = """
            DELETE
                FROM prices
                WHERE (t, ticker, timeframe)
                          NOT IN
                      (SELECT t, ticker, timeframe FROM temp_prices);
        """
        self.db.query(delete_query2)
        insert_query = """
            INSERT INTO prices
                SELECT * FROM temp_prices;
        """
        self.db.query(insert_query)

    def check_signals(self):
        """
        Check if trend switches in latest price i.e. from data point [-2] to data point [-1]
        :return: dict of symbol & buy/sell signal
        """
        logger.info('check signals')

        self.supertrend_symbols()
        for symbol, df in self.latest_results.items():
            buy = df.iloc[-1]['in_uptrend'] and not df.iloc[-2]['in_uptrend']
            sell = not df.iloc[-1]['in_uptrend'] and df.iloc[-2]['in_uptrend']
            self.signals[symbol] = 1 if buy else (-1 if sell else 0)
        return self.signals

    def check_current_trend(self):
        """
        Check if in uptrend or downtrend on last data point only
        :return: dict of symbol & buy/sell signal
        """
        logger.info('check current trend')
        self.supertrend_symbols()
        for symbol, df in self.latest_results.items():
            buy = df.iloc[-1]['in_uptrend']
            sell = not df.iloc[-1]['in_uptrend']
            self.signals[symbol] = 1 if buy else (-1 if sell else 0)
        return self.signals

    # %%


class Trader:

    def __init__(self, client: MoonClient, analyzer: Analyzer, size=100, positions=None):
        self.client = client
        self.analyzer = analyzer
        self.tickers = [symbol.get('symbol') for symbol in analyzer.symbols]
        self.positions = {ticker: False for ticker in self.tickers} if positions is None else positions
        self.size = size  # position size in MYR
        self.first_run_executed = False

    def run(self):
        in_positions = self.positions
        tickers_to_buy = []
        tickers_to_sell = []
        signals = self.analyzer.check_signals() if self.first_run_executed else self.analyzer.check_current_trend()

        for ticker, signal in signals.items():
            logger.info(f"{'buy' if signal == 1 else ('sell' if signal == -1 else 'neutral')} signal for {ticker}")
            logger.info(f'checking positions now for {ticker}. position={in_positions[ticker]}')
            print(self.analyzer.latest_results.get(ticker).tail())
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
        self.first_run_executed = True
        return {'buy': tickers_to_buy, 'sell': tickers_to_sell}

    def buy(self, tickers_to_buy):
        for ticker in tickers_to_buy:
            if self.positions[ticker]:
                logger.info(f'Already in position for {ticker}. Skipping buy..')
                continue

            luno_ticker = [mapping[1] for mapping in BINANCE_LUNO_MAPPING if mapping[0] == ticker.replace('/USDT', '')][
                0]
            luno_pair = luno_ticker + 'MYR'
            counter_account_id = self.client.get_account_id('MYR')
            base_account_id = self.client.get_account_id(luno_ticker)
            order_params = dict(pair=luno_pair,
                                base_account_id=base_account_id,
                                counter_volume=self.size,
                                counter_account_id=counter_account_id,
                                type='BUY')
            try:
                order = self.client.post_market_order(**order_params)
                self.positions[ticker] = True
                pprint(order)
            except Exception as e:
                logger.error(f'Error when sending order {order_params}')
                logger.error(e)
            break

    def sell(self, tickers_to_sell):
        for ticker in tickers_to_sell:
            if not self.positions[ticker]:
                logger.info(f'Not in position for {ticker}. Skipping sell..')
                continue

            luno_ticker = [mapping[1] for mapping in BINANCE_LUNO_MAPPING if mapping[0] == ticker.replace('/USDT', '')][
                0]
            luno_pair = luno_ticker + 'MYR'
            counter_account_id = self.client.get_account_id('MYR')
            base_account_id = self.client.get_account_id(luno_ticker)
            size = client.get_balances([luno_ticker]).get('balance')[0].get('balance')
            order_params = dict(pair=luno_pair,
                                base_account_id=base_account_id,
                                base_volume=round(float(size), 3),
                                counter_account_id=counter_account_id,
                                type='SELL')
            try:
                order = self.client.post_market_order(**order_params)
                self.positions[ticker] = False
                pprint(order)
            except Exception as e:
                logger.error(f'Error when sending order {order_params}')
                logger.error(e)
            break


def main():
    global analyzer, trader, client
    tickers_orders = trader.run()
    trader.buy(tickers_orders.get('buy'))
    trader.sell(tickers_orders.get('sell'))
    analyzer.save_results_to_db()


if __name__ == '__main__':

    timeframe = '4h'
    analyzer = Analyzer(timeframe=timeframe)
    client = MoonClient(api_key_id=LUNO_API_KEY_TRADE, api_key_secret=LUNO_SECRET_KEY_TRADE)
    trader = Trader(client, analyzer,
                    positions=INITIAL_POSITIONS)
    main()

    schedule.every().day.at("00:05").do(main)
    schedule.every().day.at("04:05").do(main)
    schedule.every().day.at("08:05").do(main)
    schedule.every().day.at("12:05").do(main)
    schedule.every().day.at("16:05").do(main)
    schedule.every().day.at("20:05").do(main)

    while True:
        schedule.run_pending()
        time.sleep(30 * 60)
