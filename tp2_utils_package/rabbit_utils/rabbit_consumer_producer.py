from typing import Callable, NoReturn, Any, Optional, Dict, List
import json
import logging
import pika
from functools import partial
from rabbit_utils.message_set.message_set import MessageSet

class RabbitQueueConsumerProducer:
    """
    Rabbit consumer-producer
    """
    logger: logging.Logger = None

    def consume(self, consume_func: Callable[[Dict], List[Dict]],
                ch, method, properties, body) -> NoReturn:
        responses = []
        messages = []
        try:
            data = json.loads(body)
            if isinstance(data, list):
                for message in data:
                    messages.append(message)
                    if not self.idempotency_set or json.dumps(message).encode() not in self.idempotency_set:
                        resp = consume_func(message)
                        if resp:
                            for r in resp:
                                responses.append(r)
            else:
                if not self.idempotency_set or json.dumps(data).encode() not in self.idempotency_set:
                    messages.append(data)
                    resp = consume_func(data)
                    if resp:
                        for r in resp:
                            responses.append(r)
        except Exception:
            if RabbitQueueConsumerProducer.logger:
                RabbitQueueConsumerProducer.logger.exception("Exception while consuming message")
            ch.basic_nack(delivery_tag = method.delivery_tag)
            return
        try:
            if responses:
                buffer = []
                for response in responses:
                    if self.messages_to_group == 1:
                        ch.basic_publish(exchange='', routing_key=self.response_queue, body=json.dumps(response))
                    else:
                        buffer.append(response)
                        if len(buffer) == self.messages_to_group:
                            ch.basic_publish(exchange='', routing_key=self.response_queue, body=json.dumps(buffer))
                            buffer = []
                if buffer:
                    ch.basic_publish(exchange='', routing_key=self.response_queue, body=json.dumps(buffer))
        except Exception:
            if RabbitQueueConsumerProducer.logger:
                RabbitQueueConsumerProducer.logger.exception("Exception while sending message")
            ch.basic_nack(delivery_tag = method.delivery_tag)
            return
        if self.idempotency_set:
            for message in messages:
                self.idempotency_set.add(json.dumps(message).encode())
        ch.basic_ack(delivery_tag = method.delivery_tag)


    def __init__(self, host: str, consume_queue: str,
                 response_queue: str,
                 consume_func: Callable[[Dict], List[Dict]],
                 messages_to_group: int = 1,
                 idempotency_set: Optional[MessageSet] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Start a rabbit consumer-producer

        :param host: the hostname to connect
        :param consume_queue: the name of the queue to consume
        :param response_queue: the name of the queue in which the response will be sent
        :param consume_func: the function that consumes and creates a response,
                receives a dict and return a list of dicts to respond
        :param messages_to_group: the amount of messages to group
        :param idempotency_set: an object of type DMessageSet to handle the arrival
        of duplicated messages
        :param logger: the logger to use
        """
        if logger:
            RabbitQueueConsumerProducer.logger = logger
        self.idempotency_set = idempotency_set
        self.consume_queue = consume_queue
        self.response_queue = response_queue
        self.messages_to_group = messages_to_group
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=consume_queue)
        self.channel.queue_declare(queue=response_queue)
        consume_func = partial(self.consume, consume_func)
        self.channel.basic_consume(queue=consume_queue,
                                   on_message_callback=consume_func,
                                   auto_ack=False)

    def __call__(self, *args, **kwargs):
        self.channel.start_consuming()
