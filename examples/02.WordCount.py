import logging

import faust
from faust import Worker

app = faust.App(
    'word-count-app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    store='rocksdb://',
    version=1
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
