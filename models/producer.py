"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)
BROKER_URL = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "BROKER_URL": BROKER_URL,
            "SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
        {
            'bootstrap.servers': self.broker_properties.get("BROKER_URL"),
            'schema.registry.url': self.broker_properties.get("SCHEMA_REGISTRY_URL")
        },
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        admin_cli = AdminClient({'bootstrap.servers': self.broker_properties.get("BROKER_URL")})

        topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas
        )

        futures_dct = admin_cli.create_topics([topic])

        for topic, future in futures_dct.items():
            try:
                future.result()
                logger.debug(f"Topic {self.topic_name} created")
            except KafkaException as e:
                logger.debug(f"{self.topic_name} creation failed: {e}")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("Producer flushed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
