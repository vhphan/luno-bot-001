import pandas as pd
import pandas_ta as ta

# %%
candles = pd.read_csv('/home2/eproject/veehuen/python/luno/sandboxes/candles.csv', index_col=0)

# %%
candles.rename(columns={'time': 'date'}, inplace=True)

# %%
df = candles.copy()
df['willr'] = df[['open', 'high', 'low', 'close']].ta.willr()
