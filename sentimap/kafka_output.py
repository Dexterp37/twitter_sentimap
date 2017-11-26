import json
import logging
from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class KafkaOutput:
    """ Send a data packet to Kafka.
    """
    def __init__(self, topic, server="localhost:9092", data_callback=None):
        self._callback = data_callback
        # Pass a JSON serializer so that our packets gets automatically
        # translated to bytes.
        self._producer = KafkaProducer(bootstrap_servers=server,
                                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self._topic = topic

    def process_input(self, data):
        if not data or not isinstance(data, dict):
            logger.error("Empty or invalid data")
            return

        self._producer.send(self._topic, data)
