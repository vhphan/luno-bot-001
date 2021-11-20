# %%
import os
import time
import types
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from luno_python.client import Client

from bots.moon_bot import Moon

# %%
load_dotenv()
LUNO_API_KEY = os.getenv('LUNO_API_KEY')
LUNO_SECRET_KEY = os.getenv('LUNO_SECRET_KEY')

# %%
c = MoonClient(api_key_id=LUNO_API_KEY, api_key_secret=LUNO_SECRET_KEY)
try:
    ticker = c.get_ticker(pair='XBTMYR')
    print(ticker)
    balances = c.get_balances(assets=['MYR', 'ETH'])
    print(balances)
except Exception as e:
    print(e)


# %%



def get_account_balance(currency: str = 'MYR') -> float:
    balance = c.get_balances(assets=[currency])
    return float(balance.get('balance')[0].get('balance'))


# %%
def is_method(obj, name):
    return hasattr(obj, name) and isinstance(getattr(obj, name), types.MethodType)


# %%
for attr in dir(c):
    if attr.startswith('__') and attr.endswith('__'):
        continue
    if is_method(c, attr):
        print(attr)
        # continue
    # else:
    #     print(attr, type(attr))

# %%
tickers = c.get_tickers().get('tickers')

# %%
ticker_base_myr = [ticker for ticker in tickers if ticker.get('pair').endswith('MYR')]

# %%
pairs_myr = [ticker.get('pair') for ticker in ticker_base_myr]


# %%
def milli_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000.0)


def dt_to_milli(dt: str) -> int:
    dt_obj = datetime.strptime(dt, '%Y-%m-%d')
    return int(dt_obj.timestamp() * 1000)


# %%
dt_to_milli('2021-11-01')

# %%
candles = c.get_candles(pair='LTCMYR', since=dt_to_milli('2021-11-05'), duration=300)

# %%
df = pd.DataFrame(candles.get('candles'))
df['datetime'] = df['timestamp'].apply(milli_to_dt)
df.tail()


# %%

def current_milli_time():
    return int(time.time() * 1000)


print(current_milli_time())
