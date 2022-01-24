import faust

from pydantic import BaseModel


class UserModel(faust.Record, BaseModel, serializer='json_users'):
    first_name: str
    last_name: str


# codecs.codec.py
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers.faust import FaustJsonSerializer

# create an instance of the `SchemaRegistryClient`
SCHEMA_REGISTRY_URL = ''
client = SchemaRegistryClient(url=SCHEMA_REGISTRY_URL)

json_user_serializer = FaustJsonSerializer(client, "users", UserModel.schema_json())  # usign the method schema_json to get the json schema representation

# function used to register the codec
def json_user_codec():
    return json_user_serializer