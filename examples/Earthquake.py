import faust
import ssl
import certifi
from dotenv import load_dotenv
import os
import aiohttp
import logging
import asyncio
import aiomysql

# https://github.com/aio-libs/aiomysql

app = faust.App(
    'hello-world-app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    store='rocksdb://',
    version=1,
)


# Schema Register Case
class EarthQuakeRecord(faust.Record):
    value: int


topic = app.topic('earthquake', value_type=EarthQuakeRecord)


# 1. get earthquake  http -> Produce message
def get_earthquake():
    pass


# 2. Consume the message into database
def persist_database():
    pass


# 3. Expose data into reset api service
def api():
    pass
