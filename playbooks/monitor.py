# https://faust.readthedocs.io/en/latest/userguide/sensors.html#sensor-api-reference
import asyncio
from typing import Mapping, Any

import faust
import ssl
import certifi
from dotenv import load_dotenv
import os
import aiohttp
import logging
from faust import Worker, Monitor
from faust.types import AppT
from mode.utils.types.trees import NodeT
from prometheus_client import Counter

from playbooks.prometheusMonitor import PrometheusMonitor

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

APP_NAME = 'monitor'


class MyMonitor(PrometheusMonitor):

    def __init__(self, app: AppT, pattern: str = '/metrics', **kwargs: Any) -> None:
        super().__init__(app, pattern, **kwargs)
        self.kpi = Counter(
            'kpi', 'business_kpi')

    def on_customer_increase(self):
        self.kpi.inc(1)


class MyApp(faust.App):

    def __init__(self, id: str, *, monitor: MyMonitor = None, config_source: Any = None, loop: asyncio.AbstractEventLoop = None, beacon: NodeT = None, **options: Any) -> None:
        super().__init__(id, monitor=monitor, config_source=config_source, loop=loop, beacon=beacon, **options)


app = MyApp(
    id=APP_NAME,
    broker=kafka_broker,
    broker_credentials=faust.SASLCredentials(
        username=kafka_user,
        password=kafka_password,
        ssl_context=ssl_context
    ),
    store='rocksdb://',
    version=1,
    topic_replication_factor=3
)

app.monitor = MyMonitor(app, pattern='/metrics')

livecheck = app.LiveCheck()


@app.page("/customer")
async def increase(web, request):
    app.monitor.on_customer_increase()
    return "hi"


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
