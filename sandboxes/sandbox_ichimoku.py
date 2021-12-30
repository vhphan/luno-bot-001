# %%
import pandas as pd
import pandas_ta as ta

# %%
candles = pd.read_csv('/home2/eproject/veehuen/python/luno/sandboxes/candles.csv', index_col=0)

# %%
candles.rename(columns={'time': 'date'}, inplace=True)

# %%
df = candles.copy()

# %%
df1, df2 = df[['open', 'high', 'low', 'close']].ta.ichimoku(lookahead=True)

# %%
green_cloud = df1['ISA_9'] > df1['ISB_26']

# %%
price_above_cloud = df['close'] > df1[["ISA_9", "ISB_26"]].max(axis=1)

# %%
conversion_cross_base = df1['ITS_9'] > df1['IKS_26']

# %% CONFIRMATION lagging span > price 26 days before
confirmation = df1['ICS_26'] > df['close'].shift(26)

# %%
buy = green_cloud & price_above_cloud & conversion_cross_base & confirmation

# %%
df1['green_cloud'] = green_cloud
df1['price_above_cloud'] = price_above_cloud
df1['conversion_cross_base'] = conversion_cross_base
df1['confirmation'] = confirmation

# %%
df1['buy'] = buy
df1['sell'] = ~df1['confirmation']
