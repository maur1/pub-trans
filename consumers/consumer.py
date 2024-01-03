"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING, KafkaError
from confluent_kafka.avro import AvroConsumer
from tornado import gen

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
            self,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
            "bootstrap.server": "localhost:9092",
            "group.id": "consumer_1"
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        # return 1 if message was received else 0
        msg = self.consumer.poll(1.0)
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logger.info('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            return 0
        else:
            return 1



    def close(self):
        """Clean up Kafka consumer"""
        self.consumer.close()
        logger.info("Consumer closed")
