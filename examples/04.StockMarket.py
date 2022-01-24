# https://brokerchooser.com/broker-reviews/alpaca-trading-review
import os
import datetime
import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream

alpaca_base_url = URL('https://paper-api.alpaca.markets')
alpaca_ws_url = URL('wss://data.alpaca.markets')
alpaca_key_id = os.getenv('alpaca_key_id')
alpaca_secret_key = os.getenv('alpaca_secret_key')

stream = Stream(key_id=alpaca_key_id,
                secret_key=alpaca_secret_key,
                base_url=alpaca_base_url)


async def trade_callback(t):
    print('trade', t)


async def quote_callback(q):
    print('quote', q)


stream.subscribe_trades(trade_callback, 'AAPL')
stream.subscribe_quotes(quote_callback, 'IBM')

stream.run()
