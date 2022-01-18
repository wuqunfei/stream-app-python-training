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


class EarthQuakeRecord(faust.Record):
    value: int


topic = app.topic('earthquake', value_type=EarthQuakeRecord)





