'''
https://faust.readthedocs.io/en/latest/playbooks/leaderelection.html
'''
import faust
import ssl
import certifi
from dotenv import load_dotenv
import os
import random

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

app = faust.App(
    id='leader',
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


@app.agent()
async def say(greetings):
    async for greeting in greetings:
        print(f"Rec:{greeting}")


@app.timer(interval=5, on_leader=True)
async def public_greetings():
    value = random.random()
    print(f"Publishing:{value}")
    await say.send(value=str(value))
