import faust
import pytz
import logging
import asyncio
import aiomysql
import ssl
import certifi
from dotenv import load_dotenv
import os
import aiohttp
import datetime
import sqlalchemy

# https://github.com/aio-libs/aiomysql
from faust import Worker

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


metadata = sqlalchemy.MetaData()
eq_table = sqlalchemy.Table('earthquake', metadata)
eq_topic = app.topic('earthquake', value_type=EQRecord)


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
    pass


# 3. Expose data into reset api service
def api():
    pass


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
