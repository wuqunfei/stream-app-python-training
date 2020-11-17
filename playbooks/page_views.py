'''
https://faust.readthedocs.io/en/latest/playbooks/pageviews.html
'''
import faust
import ssl
import certifi
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="../.env")

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile=certifi.where())

kafka_broker = os.getenv("kafka_broker")
kafka_user = os.getenv("kafka_user")
kafka_password = os.getenv("kafka_password")

app = faust.App(
    id='page_views',
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


class PageView(faust.Record):
    id: str
    user: str


page_view_topic = app.topic('page_views', value_type=PageView)
page_views = app.Table('page_views', default=int)


@app.agent(page_view_topic)
async def count_page_view(views: list):
    async for view in views.group_by(PageView.id):
        page_views[view.id] += 1
