import logging

import faust
import datetime
from faust import Worker

app = faust.App(
    'hello-world-app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    store='rocksdb://',
    version=1,
)

greetings_topic = app.topic('greetings')


@app.agent(greetings_topic)
async def consumer(greetings):
    async for greeting in greetings:
        print(greeting)


@app.timer(5)
async def producer():
    now = datetime.datetime.now()
    for i in range(2):
        await greetings_topic.send(value=f'hello python kafka stream {now}')


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
