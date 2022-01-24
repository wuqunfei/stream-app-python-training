import faust
import pytz
import logging
import aiohttp
import datetime
import sqlalchemy
from notify_run import Notify
from tortoise.models import Model
from tortoise import fields

from faust import Worker

app = faust.App(
    id='earthquake-app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    store='rocksdb://',
    version=1,
)

earthquake_topic = app.topic('earthquake_topic')


@app.task
async def on_started():
    logging.info('Earthquake application started')


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
