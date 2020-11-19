# https://faust.readthedocs.io/en/latest/userguide/agents.html#concurrency

import faust
import ssl
import certifi
from dotenv import load_dotenv
import os
import aiohttp
import logging

from faust import Worker

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

app = faust.App(
    id='concurrency',
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


class MyRecord(faust.Record):
    value: int


topic = app.topic('concurrency', value_type=MyRecord)


@app.agent(topic, concurrency=10)
async def mytask(records):
    session = aiohttp.ClientSession()
    async for record in records:
        await session.get(f'http://www.google.com/?#q={record.value}')
    print("consumed")


@app.timer(interval=60)
async def producer():
    for i in range(10):
        await topic.send(value=MyRecord(value=i))
    print("Message Send")


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
