# config docker expose rest api port 5432
# https://docs.docker.com/engine/reference/commandline/dockerd/
#
from playbooks.test.KafkaContainer import KafkaContainer
from playbooks.test.ZookeeperContainer import ZookeeperContainer

with ZookeeperContainer(image='confluentinc/cp-zookeeper:5.4.3') as zookeeper_container:
    zookeeper_address = zookeeper_container.get_zookeeper_connect()
    with KafkaContainer(image='confluentinc/cp-kafka:5.4.3', port=9092, zookeeper_connect=zookeeper_address) as kafka_container:
        print(kafka_container)
