# https://brokerchooser.com/broker-reviews/alpaca-trading-review
# https://algotrading101.com/learn/alpaca-trading-api-guide/
# https://manual.arcadetrader.com/
# https://github.com/alpacahq/alpaca-backtrader-api
# https://gbeced.github.io/pyalgotrade/docs/v0.20/html/index.html
# https://github.com/kernc/backtesting.py/blob/master/doc/alternatives.md
# stream -> public message -> backtest ->  Good -> Buy

import os
import backtrader as bt
import alpaca_trade_api

from alpaca_trade_api.common import URL

ALPACA_API_KEY = os.getenv('alpaca_key_id')
ALPACA_SECRET_KEY = os.getenv('alpaca_secret_key')
ALPACA_PAPER = True

alpaca_base_url = URL('https://paper-api.alpaca.markets')
alpaca_ws_url = URL('wss://data.alpaca.markets')
alpaca_key_id = os.getenv('alpaca_key_id')
alpaca_secret_key = os.getenv('alpaca_secret_key')

api = alpaca_trade_api.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, alpaca_base_url, api_version='v2')

aapl = api.get_barset('AAPL', 'day')

data = aapl.df


class SmaCross(bt.SignalStrategy):
    def __init__(self):
        sma1, sma2 = bt.ind.SMA(period=10), bt.ind.SMA(period=30)
        crossover = bt.ind.CrossOver(sma1, sma2)
        self.signal_add(bt.SIGNAL_LONG, crossover)


is_live = False
data = bt.feeds.PandasData(dataname=data)
cerebro = bt.Cerebro()
cerebro.adddata(data)
cerebro.addstrategy(SmaCross)
cerebro.run()
