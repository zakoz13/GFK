import yfinance as yf
import setuptools
from matplotlib import pyplot as plt
import talib
import pandas as pd
from pandas.plotting import scatter_matrix
import numpy as np

# data = yfinance.download('AAPL', '2021-1-1', '2021-2-1')
# rsi = talib.RSI(data["Close"])
# fig = plt.figure()
# fig.set_size_inches((25, 18))
# ax_rsi = fig.add_axes((0, 0.24, 1, 0.2))
# ax_rsi.plot(data.index, [70] * len(data.index), label="overbought")
# ax_rsi.plot(data.index, [30] * len(data.index), label="oversold")
# ax_rsi.plot(data.index, rsi, label="rsi")
# ax_rsi.plot(data["Close"])
# ax_rsi.legend()
#
# section = None
# sections = []
# for i in range(len(rsi)):
#     if rsi[i] < 30:
#         section = 'oversold'
#     elif rsi[i] > 70:
#         section = 'overbought'
#     else:
#         section = None
#     sections.append(section)
#
# trades = []
# for i in range(1, len(sections)):
#     trade = None
#     if sections[i-1] == 'oversold' and sections[i] == None:
#         trade = True
#     if sections[i-1] == 'overbought' and sections[i] == None:
#         trade = False
#     trades.append(trade)
#
# acp = data['Close'][len(data['Close'])-len(trades):].values
# profit = 0
# qty = 10
# for i in range(len(acp)-1):
#     true_trade = None
#     if acp[i] < acp[i+1]:
#         true_trade = True
#     elif acp[i] > acp[i+1]:
#         true_trade = False
#     if trades[i] == true_trade:
#         profit += abs(acp[i+1] - acp[i]) * qty
#     elif trades[i] != true_trade:
#         profit += -abs(acp[i+1] - acp[i]) * qty
#
# print(round(profit, 3))

#
# data = yf.download('AAPL', '2020-01-01', '2021-01-01')
# data['AdjClose'].plot()
# plt.show()

# ----------------------------------------------------------------------------------------------

# ticker_list = ['LITE', 'VRTX', 'CLF']
# data = pd.DataFrame(columns=ticker_list)
#
# for ticker in ticker_list:
#     data[ticker] = yf.download(ticker, '2018-01-01', '2021-02-01')['Adj Close']
#
# ((data.pct_change()+1).cumprod()).plot(figsize=(10, 7))
# plt.legend()
# plt.title("Adjusted Close Price", fontsize=16)
# plt.ylabel('Price', fontsize=14)
# plt.xlabel('Year', fontsize=14)
# plt.grid(which="major", color='k', linestyle='-.', linewidth=0.5)
# plt.show()

# ----------------------------------------------------------------------------------------------

lite = yf.download('LITE', '2020-01-01')
daily_close = lite[['Adj Close']]
daily_pct_change = daily_close.pct_change()
daily_pct_change.fillna(0, inplace=True)
# print(daily_pct_change.head())

daily_log_returns = np.log(daily_close.pct_change()+1)
# print(daily_log_returns.head())

monthly = lite.resample('BM').apply(lambda x: x[-1])
# print(monthly.pct_change().tail())

quarter = lite.resample('4M').mean()
# print(quarter.pct_change())

daily_pct_change = daily_close / daily_close.shift(1) - 1
# print(daily_pct_change)

daily_pct_change.hist(bins=50)
# plt.show()
# print(daily_pct_change.describe())

cum_daily_return = (1 + daily_pct_change).cumprod()
# print(cum_daily_return)

cum_daily_return.plot(figsize=(12, 8))
# plt.show()

cum_monthly_return = cum_daily_return.resample('M').mean()
# print(cum_monthly_return)

ticker = ['LITE', 'VRTX', 'SPCE', 'SBER.ME']
stock = yf.download(ticker, '2018-01-01', '2021-02-01')
daily_pct_change = stock['Adj Close'].pct_change()
daily_pct_change.hist(bins=50, sharex=True, figsize=(20, 8))
# plt.show()

scatter_matrix(daily_pct_change, diagonal='kde', alpha=0.1, figsize=(20, 20))
plt.show()