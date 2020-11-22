from testcontainers.core.container import DockerContainer


class KafkaContainer(DockerContainer):

    def __init__(self, image, port_to_expose, zookeeper_connect, **kargs):
        super().__init__(image, **kargs)
        self.port_to_expose = port_to_expose
        self.with_exposed_ports(self.port_to_expose)
        self.with_env('KAFKA_ZOOKEEPER_CONNECT', zookeeper_connect)
