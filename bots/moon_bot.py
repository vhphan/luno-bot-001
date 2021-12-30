# %%
import os

from dotenv import load_dotenv
from luno_python.client import Client

# %% Load env
load_dotenv()
LUNO_API_KEY = os.getenv('LUNO_API_KEY_TRADE')
LUNO_SECRET_KEY = os.getenv('LUNO_SECRET_KEY_TRADE')


class MoonClient(Client):

    def __init__(self, base_url='', timeout=0,
                 api_key_id='', api_key_secret=''):
        super().__init__(base_url, timeout,
                         api_key_id, api_key_secret)
        self.min_volumes = self.get_min_volume()

    def test(self):
        print(self.api_key_id)

    def get_markets_info(self):
        """Makes a call to GET https://api.luno.com/api/exchange/1/markets
        """
        req = {}
        return self.do('GET', '/api/exchange/1/markets', req=req, auth=True)

    def get_min_volume(self):
        markets = self.get_markets_info().get('markets')
        return {
            market.get('market_id'): market.get('min_volume')
            for market in markets
            if market.get('market_id').endswith('MYR')
        }

    def get_candles(self, pair, since, duration):
        """Makes a call to GET /api/exchange/1/candles
            :param pair
            required string
            Example: pair=LTCMYR
            Currency pair

            :param since
            required string <timestamp> Example: since=1470810728478 Filter to candles starting on or after
            this timestamp (Unix milliseconds). Only up to 1000 of the earliest candles are returned.

            :param duration
            required integer <int64> Example: duration=300 Candle duration in seconds. For example,
            300 corresponds to 5m candles. Currently supported durations are: 60 (1m), 300 (5m), 900 (15m),
            1800 (30m), 3600 (1h), 10800 (3h), 14400 (4h), 28800 (8h), 86400 (24h), 259200 (3d), 604800 (7d).
        """
        req = {
            'pair': pair,
            'since': since,
            'duration': duration,
        }
        return self.do('GET', '/api/exchange/1/candles', req=req, auth=True)

    def get_account_id(self, currency: str = 'MYR') -> str:
        return self.get_balances(assets=[currency]).get('balance')[0].get('account_id')

    def get_account_balance(self, currency: str = 'MYR') -> str:
        return self.get_balances(assets=[currency]).get('balance')[0].get('balance')

    def get_myr_pairs(self):
        tickers = self.get_tickers().get('tickers')
        return [
            ticker.get('pair')
            for ticker in tickers
            if ticker.get('pair').endswith('MYR')
        ]



if __name__ == '__main__':
    # %%
    c = MoonClient(api_key_id=LUNO_API_KEY, api_key_secret=LUNO_SECRET_KEY)
    balances = c.get_balances()
    for balance in balances.get('balance'):
        print(balance['asset'], balance['balance'])

    # %%
    c.get_account_id('XBT')

    # %%
    myr_pairs = c.get_myr_pairs()

    # %%
    m = c.get_markets_info()

    # %%
    # buy_dict ={'pair': 'ETHMYR', 'base_account_id': '9185580296327958552', 'counter_volume': 100, 'counter_account_id': '5701716745730703975', 'type': 'BUY'}
    # c.post_market_order(**buy_dict)
