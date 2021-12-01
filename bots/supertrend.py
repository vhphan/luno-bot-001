import json
import time
from datetime import datetime

import ccxt
import numpy as np

import pandas as pd

from utils.helpers import milli_to_dt
from loguru import logger
import schedule


class Analyzer:
    def __init__(self, atr_window=14, multiplier=5):
        self.multiplier = multiplier
        self.atr_window = atr_window
        self.exchange = ccxt.binance()
        # trade_symbols = ['BCH', 'BTC', 'ETH', 'LTC', 'XRP']
        trade_symbols = ['BTC', 'ETH']
        symbols_usdt = [v for k, v in self.exchange.fetch_tickers().items() if k.endswith('/USDT')]
        self.symbols = [symbol for symbol in symbols_usdt if
                        symbol.get('symbol').replace('/USDT', '') in trade_symbols]
        self.latest_results = None

    def fetch_bars(self, symbol='ETH/USDT', timeframe='1m', number_of_points=500):
        logger.info(datetime.now().isoformat())
        bars = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=number_of_points)
        df = pd.DataFrame(bars[:-1], columns=[c for c in 'tohlcv'])
        df['t'] = df['t'].apply(milli_to_dt)

        print(df)
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
        symbols = [symbol.get('symbol') for symbol in self.symbols]
        self.latest_results = {
            symbol: self.add_supertrend(symbol=symbol) for symbol in symbols
        }
        return self.latest_results

    def check_signals(self):
        pass
    # %%


class Trader:

    def __init__(self, client, analyzer):
        self.client = client
        self.analyzer = analyzer



if __name__ == '__main__':
    # %%
    analyzer = Analyzer()

    # %%
    results = analyzer.supertrend_symbols()

    # %%
    results['ETH/USDT'].to_clipboard()
