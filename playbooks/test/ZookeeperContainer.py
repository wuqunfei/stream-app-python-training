from testcontainers.core.container import DockerContainer
import secrets


class ZookeeperContainer(DockerContainer):
    CLIENT_PORT = '2181'
    TICK_TIME = '200'
    SYNC_LIMIT = '2'
    NAME = 'zookeeper'
    ZOOKEEPER_SERVER_ID = '1'

    def __init__(self, image="confluentinc/cp-zookeeper:5.4.3"):
        super(ZookeeperContainer, self).__init__(image)

        self.with_env('ZOOKEEPER_SERVER_ID', self.ZOOKEEPER_SERVER_ID)
        self.with_env('CLIENT_PORT', self.CLIENT_PORT)
        self.with_env('TICK_TIME', self.TICK_TIME)
        self.with_env('SYNC_LIMIT', self.SYNC_LIMIT)

        self.name = f'{self.NAME}-{secrets.token_hex(6)}'
        self.with_name(self.name)

    def get_zookeeper_name(self):
        return self.name

    def get_zookeeper_port(self):
        return self.CLIENT_PORT

    def get_zookeeper_connect(self):
        return f'{self.name}:{self.CLIENT_PORT}'
