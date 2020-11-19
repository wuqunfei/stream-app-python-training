# https://faust.readthedocs.io/en/latest/userguide/sensors.html#sensor-api-reference
import asyncio
from typing import Mapping

import faust
import ssl
import certifi
from dotenv import load_dotenv
import os
import aiohttp
import logging
from faust import Worker

from playbooks.prometheusMonitor import PrometheusMonitor

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

APP_NAME = 'monitor'

app = faust.App(
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

app.monitor = PrometheusMonitor(app, pattern='/metrics')

livecheck = app.LiveCheck()


@app.page("/customer")
async def increase(web, request):
    pass


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
