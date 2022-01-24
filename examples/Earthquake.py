import faust
import pytz
import logging
import aiohttp
from datetime import datetime, timedelta
import sqlalchemy
from notify_run import Notify
from tortoise.models import Model
from tortoise import fields

from faust import Worker

app = faust.App(
    id='earthquake-app',
    broker='kafka://localhost:9092',
    value_serializer='json',
    store='rocksdb://',
    version=1,
)


class EQRecord(faust.Record, serializer='json'):
    title: str
    place: str
    type: str
    mag: float
    magType: str
    time: int


earthquake_topic = app.topic('earthquake_topic', key_type=EQRecord, value_type=EQRecord)


@app.task
async def on_started():
    logging.info('Earthquake application started')


@app.timer(interval=5)
async def get_earthquake_per_five_second():
    local_time_zone = pytz.timezone('Asia/Shanghai')
    local_date_time = local_time_zone.localize(datetime.now() - timedelta(seconds=5), is_dst=None)
    start_time_utc = local_date_time.astimezone(pytz.utc)
    start_time = start_time_utc.strftime('%Y-%m-%dT%H:%M:%S')
    logging.info(f'Query earthquake at {start_time_utc}')
    records = await query_earthquake(start_time=start_time)
    for record in records:
        await earthquake_topic.send(key=record, value=record)


@app.crontab(cron_format='*/1 * * * *', timezone=pytz.timezone('Asia/Shanghai'))
async def persist_record_database():
    async for event in earthquake_topic:
        # await save_event_into_db()
        logging.info(f'event: {event}')
    logging.info(f'persist data into db')


#
#
# @app.agent(channel=earthquake_topic)
# async def send_earthquake_msg(messages):
#     notify = Notify(endpoint='https://notify.run/7qIErxULDNDO4jDYgAca')
#     async for msg in messages:
#         notify.send(f'Dear, there is earthquake now,{msg}')


async def query_earthquake(start_time: str):
    parameters = {
        "format": "geojson",
        "starttime": start_time
    }
    server = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    async with aiohttp.ClientSession() as session:
        async with session.get(url=server, params=parameters) as response:
            response = await response.json()
    datasets = []

    for feature in response['features']:
        record = EQRecord(
            title=feature['properties']['title'],
            place=feature['properties']['place'],
            type=feature['properties']['type'],
            mag=feature['properties']['mag'],
            magType=feature['properties']['magType'],
            time=feature['properties']['time']
        )
        datasets.append(record)
    logging.info(f'Query EarthQuake {len(datasets)} records at {start_time}')
    return datasets


async def save_event_into_db():
    pass


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
