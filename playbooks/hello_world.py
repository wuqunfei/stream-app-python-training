'''
https://faust.readthedocs.io/en/latest/playbooks/quickstart.html
'''
import faust
import ssl
import certifi
from dotenv import load_dotenv
import os
import logging

from faust import Worker

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

app = faust.App(
    id='hello_world',
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

livecheck = app.LiveCheck()

greetings_topic = app.topic('hello', value_type=str)


@app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)


@app.timer(5)
async def produce():
    for i in range(2):
        await print_greetings.send(value=f'hello {i}')


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
