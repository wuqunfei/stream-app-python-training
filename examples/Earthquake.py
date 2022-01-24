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


@app.timer(interval=1)
async def get_earthquake_per_second():
    query_start_time = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    query_end_time = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    logging.info(f'Query earthquake at {query_end_time}')
    await earthquake_topic.send(key=query_end_time, value=query_start_time)


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
