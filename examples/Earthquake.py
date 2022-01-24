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


@app.timer(interval=15)
async def get_earthquake_per_second():
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f'Query earthquake at {end_time}')
    # datasets = await query_earthquake(start_time=start_time, end_time=end_time)
    await earthquake_topic.send(key=end_time, value=start_time)


@app.crontab(cron_format='*/1 * * * *', timezone=pytz.timezone('Asia/Shanghai'))
async def persist_record_database():
    async for event in earthquake_topic:
        logging.info(f'event: {event}')
    logging.info(f'persist data into db')


async def query_earthquake(start_time: str, end_time: str):
    parameters = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "alertlevel": "orange"
    }
    server = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    async with aiohttp.ClientSession() as session:
        async with session.get(url=server, params=parameters) as response:
            datasets = await response.json()
    logging.info(datasets)
    return datasets


async def save_event_into_db():
    pass


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
