import logging
import os
from datetime import timedelta

import alpaca_trade_api
import faust
from alpaca_trade_api.common import URL
from faust import Worker

app = faust.App(
    'stock-app',
    broker='kafka://localhost:9092',
    value_serializer='json',
    store='rocksdb://',
    version=1,
)

ALPACA_BASE_URL = URL('https://paper-api.alpaca.markets')
alpaca_ws_url = URL('wss://data.alpaca.markets')
ALPACA_API_KEY = os.getenv('alpaca_key_id')
ALPACA_SECRET_KEY = os.getenv('alpaca_secret_key')

alpaca = alpaca_trade_api.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')


class OHLCRecord(faust.Record, serializer='json'):
    Name: str
    datetime: str

    Open: float
    High: float
    Low: float
    Close: float
    Volume: float


stock_market_topic = app.topic('stockmarket-topic', key_type=OHLCRecord, value_type=OHLCRecord, partitions=2)
stock_market_table = app.Table(name='stockmarket-table', default=float, partitions=10).tumbling(
    size=timedelta(seconds=10), expires=timedelta(seconds=60))

stock_order_topic = app.topic('stock-order-topic', key_type=str, value_type=str, partitions=10)


@app.timer(interval=1)
async def get_ohlc():
    FAANG_STOCKS = ['FB', 'AMZN', 'APPL', 'NFLX', 'GOOG']
    df = alpaca.get_latest_bars(symbols=FAANG_STOCKS)
    for name, bar in df.items():
        record = OHLCRecord(
            Name=name,
            datetime=bar.t,

            Open=bar.o,
            Close=bar.c,
            High=bar.h,
            Low=bar.l,
            Volume=bar.v
        )
        # logging.info(record)
        await stock_market_topic.send(key=record, value=record)


@app.timer(interval=10)
async def back_test():
    key = 'stock_name'
    action = 'buy'
    await stock_order_topic.send(key=key, value=action)


@app.agent(stock_order_topic, concurrency=10, isolated_partitions=False)
async def handle_order(orders):
    async for key, order in orders.items():
        logging.info(f'Send Order {key} to {order}')


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
