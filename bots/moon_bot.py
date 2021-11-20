# %%
import os

from dotenv import load_dotenv
from luno_python.client import Client

# %% Load env
load_dotenv()
LUNO_API_KEY = os.getenv('LUNO_API_KEY')
LUNO_SECRET_KEY = os.getenv('LUNO_SECRET_KEY')


class MoonClient(Client):

    def test(self):
        print(self.api_key_id)

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
        balance = self.get_balances(assets=[currency])
        return balance.get('balance')[0].get('account_id')


if __name__ == '__main__':
    # %%
    c = MoonClient(api_key_id=LUNO_API_KEY, api_key_secret=LUNO_SECRET_KEY)
    balances= c.get_balances()
    for balance in balances.get('balance'):
        print(balance['asset'], balance['balance'])

    # %%
    c.get_account_id('XBT')

