import pandas as pd
from pycoingecko import CoinGeckoAPI

cg = CoinGeckoAPI()

if __name__ == '__main__':
    df = pd.DataFrame(cg.get_coins_markets(vs_currency='USD'))
