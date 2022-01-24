import faust
import pytz
import logging
import asyncio
import ssl
import certifi
from dotenv import load_dotenv
import os
import aiohttp
import datetime
import sqlalchemy
from notify_run import Notify

# https://github.com/aio-libs/aiomysql
from faust import Worker
from tortoise.models import Model
from tortoise import fields

app = faust.App(
    'earthquake-app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    store='rocksdb://',
    version=1,
)


# Schema Register Case
class EQRecord(faust.Record):
    value: int


class EQModel(Model):
    id = fields.IntField(pk=True)
    create_time = fields.DatetimeField()
    lat = fields.FloatField()
    lang = fields.FloatField()


metadata = sqlalchemy.MetaData()

eq_external_table = sqlalchemy.Table('earthquake', metadata)
eq_internal_table = app.Table(name='earthquake_table')
eq_topic = app.topic('earthquake_queue', value_type=EQRecord)


# 1. get earthquake  http -> Produce message
# https://holypython.com/api-6-earthquake-data/
# https://github.com/abshinn/usgs
# https://earthquake.usgs.gov/fdsnws/event/1/
# https://www.geeksforgeeks.org/send-chrome-notification-using-python/
# https://pawelmhm.github.io/python/aiohttp/python-faust/event-driven/2021/06/13/faust-aiohttp-alert-system.html

@app.timer(interval=2)
async def get_earthquake_per_minute():
    start_time = "2022-01-01"
    end_time = datetime.datetime.now().strftime('%Y-%m-%d')
    parameters = {"format": "geojson",
                  "starttime": start_time,
                  "endtime": end_time,
                  "alertlevel": "orange"}
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url, params=parameters) as response:
            response = await response.json()
    print(response)
    await eq_topic.send(key=end_time, value=response)


# 2. Consume the message into database
@app.crontab(cron_format='*/1 * * * *', timezone=pytz.timezone('Asia/Shanghai'))
async def persist_database():
    messages = eq_topic.get()
    for msg in messages:
        model = EQModel()
        await model.create()


# 3. Send Msg to User's Chrome
# https://pypi.org/project/notify-run/
# Endpoint: https://notify.run/7qIErxULDNDO4jDYgAca
# To subscribe, open: https://notify.run/c/7qIErxULDNDO4jDYgAca

@app.agent(channel=eq_topic)
def send_earthquake_msg(messages):
    notify = Notify(endpoint='https://notify.run/7qIErxULDNDO4jDYgAca')
    async for msg in messages:
        notify.send(f'Dear, there is earthquake now,{msg}')


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
