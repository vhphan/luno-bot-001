# %%
import os
import time
import types
from datetime import datetime
from pprint import pprint

import pandas as pd
from dotenv import load_dotenv
from luno_python.client import Client
from pycoingecko import CoinGeckoAPI
from utils.helpers import milli_to_dt

# %%

cg = CoinGeckoAPI()

# %%
coins_id = ['bitcoin', 'litecoin', 'ethereum', 'ripple', 'bitcoin-cash']
prices = cg.get_price(ids=coins_id,
                      vs_currencies='myr',
                      include_last_updated_at=True)
pprint(prices)
## last updated unit = seconds, multiply 1000 to use milli_to_dt


# %%
def search_coin_id(coin_name='bitcoin'):
    coins = cg.get_coins()
    return [coin.get('id') for coin in coins if coin_name in coin.get('name').lower()]


pprint(search_coin_id('bitcoin'))

# %%
df = pd.DataFrame(cg.get_coin_ohlc_by_id(id='bitcoin', vs_currency='myr', days=1))
df.columns = ['t', 'o', 'h', 'l', 'c']
# df['datetime'] = pd.to_datetime(df['t'], unit='ms')
df['datetime'] = df['t'].apply(milli_to_dt) # this gives value in lcoal time
print(df.tail())

# %%
milli_to_dt(1637376588 * 1000) - datetime.now()
