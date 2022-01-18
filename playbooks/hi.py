import logging

import faust
import datetime
from faust import Worker

app = faust.App(
    'hello-world',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('greetings')


@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        print(greeting)


@app.timer(5)
async def produce():
    now = datetime.datetime.now()
    for i in range(2):
        await greetings_topic.send(value=f'hello {now}')

if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()
