import json
import logging
from typing import Optional, Dict

import pika


class RabbitQueueProducer:
    """
    Rabbit producer
    """
    logger: logging.Logger = None

    def __init__(self, host: str, publish_queue: str,
                 logger: Optional[logging.Logger] = None):
        """
        Start a rabbit consumer-producer

        :param host: the hostname to connect
        :param publish_queue: the name of the queue to consume
        :param logger: the logger to use
        """
        if logger:
            RabbitQueueProducer.logger = logger
        self.publish_queue = publish_queue
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=publish_queue)

    def publish(self, item: Dict):
        self.channel.basic_publish(exchange='', routing_key=self.publish_queue, body=json.dumps(item).encode())
