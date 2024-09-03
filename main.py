
#%%
import time
from Dune_Util import DuneData
from Bybit_Data_Util import BybitData
import pandas as pd
from Df_Operations_Util import find_closest_earlier

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)  # Show all columns
pd.set_option('display.expand_frame_repr', False)  # Prevent line-breaking
pd.set_option('display.width', None)  # Adjust width to the terminal size

#%%
import pandas as pd

stevenData = pd.read_csv('./CSV_Data/Steven_Data.csv')
stevenData['Datetime'] = stevenData['Date'] + ' ' + stevenData['Time']
stevenData['Datetime'] = pd.to_datetime(stevenData['Datetime'], format='%m/%d/%y %I:%M:%S %p')
stevenData['Datetime'] = stevenData['Datetime'] - pd.Timedelta(hours=8)
stevenData['Timestamp'] = stevenData['Datetime'].astype('int64') // 10**6

btcPriceData = BybitData().getPriceRangeByTime(stevenData['Timestamp'].min(), stevenData['Timestamp'].max(), 'BTCUSDT', interval='1')
btcPriceDf = BybitData().convertKlineDataToDf(btcPriceData)

solPriceData = BybitData().getPriceRangeByTime(stevenData['Timestamp'].min(), stevenData['Timestamp'].max(), 'SOLUSDT', interval='1')
solPriceDf = BybitData().convertKlineDataToDf(solPriceData)

ethPriceData = BybitData().getPriceRangeByTime(stevenData['Timestamp'].min(), stevenData['Timestamp'].max(), 'ETHUSDT', interval='1')
ethPriceDf = BybitData().convertKlineDataToDf(ethPriceData)

stevenData['CEXBTCPrice'] = stevenData.apply(lambda row: find_closest_earlier(row, 'Timestamp', btcPriceDf, 'Open', 'Timestamp'), axis=1)
stevenData['CEXSOLPrice'] = stevenData.apply(lambda row: find_closest_earlier(row, 'Timestamp', solPriceDf, 'Open', 'Timestamp'), axis=1)
stevenData['CEXETHPrice'] = stevenData.apply(lambda row: find_closest_earlier(row, 'Timestamp', ethPriceDf, 'Open', 'Timestamp'), axis=1)

#%%
# Delta Cal
stevenData['BTCAUMDelta'] = stevenData['TVL USD Fees Btc']
stevenData['SOLAUMDelta'] = stevenData['TVL USD Fees Sol']
stevenData['ETHAUMDelta'] = stevenData['TVL USD Fees Eth']

stevenData['BTCOIDiff'] = -1*(stevenData['BTC Long OI USD'] - stevenData['BTC Short OI USD'])
stevenData['SOLOIDiff'] = -1*(stevenData['SOL Long OI USD'] - stevenData['SOL Short OI USD'])
stevenData['ETHOIDiff'] = -1*(stevenData['ETH Long OI USD'] - stevenData['ETH Short OI USD'])

#%%
envData = stevenData[['Datetime', 'Timestamp', 'JLP Price', 'JLP Token Total Supply', 'CEXBTCPrice', 'CEXSOLPrice', 'CEXETHPrice', 'BTCAUMDelta', 'SOLAUMDelta', 'ETHAUMDelta', 'BTCOIDiff', 'SOLOIDiff', 'ETHOIDiff']]
envData = envData.dropna()
envData = envData.sort_values(by='Timestamp')
envData = envData.set_index('Datetime')

# %%
from Backtest_Engine import runBacktest
tokenHoldingAmount = 1000

for asset in ['BTC', 'ETH', 'SOL']:
    runBacktest(asset, envData)
# %%
