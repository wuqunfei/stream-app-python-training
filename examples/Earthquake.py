import faust
import pytz
import logging
import aiohttp
from datetime import datetime, timedelta
from notify_run import Notify
from tortoise import fields
from tortoise.models import Model
from tortoise import Tortoise

from faust import Worker

app = faust.App(
    id='earthquake-app',
    broker='kafka://localhost:9092',
    value_serializer='json',
    store='rocksdb://',
    version=1,
)

db = {
    'user': 'root',
    'password': 'feifeifei',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'leetcode'
}


class EQRecord(faust.Record, serializer='json'):
    title: str
    place: str
    type: str
    mag: float
    magType: str
    time: int
    latitude: float = 0.0
    longitude: float = 0.0


class EQLocationModel(Model):
    id = fields.IntField(pk=True)
    latitude = fields.FloatField()
    longitude = fields.FloatField()
    occurrence = fields.IntField()
    query_itme = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = 'earthquake'

    def __str__(self):
        return self.id


earthquake_topic = app.topic('earthquake_topic', key_type=EQRecord, value_type=EQRecord, partitions=3)
earthquake_table = app.Table('earthquake_table', default=float, partitions=3)


@app.task
async def on_started():
    logging.info('Earthquake application started')


@app.timer(interval=5, name='QueryTimer')
async def get_earthquake_per_five_second():
    local_time_zone = pytz.timezone('Asia/Shanghai')
    local_date_time = local_time_zone.localize(datetime.now() - timedelta(seconds=5), is_dst=None)
    start_time_utc = local_date_time.astimezone(pytz.utc)
    start_time = start_time_utc.strftime('%Y-%m-%dT%H:%M:%S')
    logging.info(f'Query earthquake at {start_time_utc}')
    records = await query_earthquake(start_time=start_time)
    record = records.pop()
    await earthquake_topic.send(key=record, value=record)


@app.timer(interval=10, name='SyncTimer')
async def sync_window_table_into_db():
    connection_url = f'mysql://{db["user"]}:{db["password"]}@{db["host"]}:{db["port"]}/{db["database"]}'
    await Tortoise.init(db_url=connection_url,
                        modules={"models": ["__main__"]}
                        )
    # await Tortoise.generate_schemas(safe=True)
    for key, value in earthquake_table.items():
        try:
            latitude, longitude = key.split(',')
            logging.info(f'latitude: {latitude}, longitude: {longitude}, value: {value}')
            model = EQLocationModel(latitude=latitude, longitude=longitude, occurrence=value)
            await model.save(force_create=True)
        except Exception as ex:
            logging.error(key)
            logging.error(ex)
    await Tortoise.close_connections()


@app.agent(earthquake_topic)
async def handle_earthquake_msg(messages):
    notify = Notify(endpoint='https://notify.run/7qIErxULDNDO4jDYgAca')
    async for msg in messages:
        notify.send(f'{msg.title}')
        # update kafka table
        key = f'{msg.latitude},{msg.longitude}'
        earthquake_table[key] += 1


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
            time=feature['properties']['time'],
            latitude=feature['geometry']['coordinates'][0],
            longitude=feature['geometry']['coordinates'][1]
        )
        datasets.append(record)
    logging.info(f'Query EarthQuake {len(datasets)} records at {start_time}')
    return datasets


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
