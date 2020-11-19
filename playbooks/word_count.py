import logging

import faust
import ssl
import certifi
from dotenv import load_dotenv
import os

from faust import Worker

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

app = faust.App(
    id='word_count',
    broker=kafka_broker,
    broker_credentials=faust.SASLCredentials(
        username=kafka_user,
        password=kafka_password,
        ssl_context=ssl_context
    ),
    store='rocksdb://',
    version=1,
    topic_replication_factor=3,
    value_serializer='raw'
)

posts_topic = app.topic('posts', value_type=str)
word_counts = app.Table('word_counts', default=int, help='Keep count of words (str to int).')

'''
Step 1: do the word, count
'''


@app.agent(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await count_words.send(key=word, value=word)


@app.agent(value_type=str)
async def count_words(words):
    async for word in words:
        word_counts[word] += 1


'''
Step 2: expose into web system
'''


@app.page('/count/{word}')
@app.table_route(table=word_counts, match_info='word')
async def get_count(web, request, word):
    return web.json({word: word_counts[word]})


if __name__ == '__main__':
    worker = Worker(app=app, loglevel=logging.INFO)
    worker.execute_from_commandline()



'''
Step 3. test 

$kafka-console-producer.sh  --broker-list pkc-4r297.europe-west1.gcp.confluent.cloud:9092 --producer.config .config.properties --topic posts
curl http://localhost:6066/count/{word}

'''