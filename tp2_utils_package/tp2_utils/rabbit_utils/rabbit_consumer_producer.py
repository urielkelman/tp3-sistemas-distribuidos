import json
import logging
from functools import partial
from typing import Callable, NoReturn, Optional, Dict, List, Tuple

import pika

from tp2_utils.rabbit_utils.publisher_sharding import PublisherSharding
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage
from tp2_utils.interfaces.state_commiter import StateCommiter

PUBLISH_SHARDING_FORMAT = "%s_shard%d"


class RabbitQueueConsumerProducer:
    """
    Rabbit consumer-producer
    """
    logger: logging.Logger = None

    def consume(self, callable_commiter: StateCommiter,
                ch, method, properties, body) -> NoReturn:
        stop_consuming = False
        responses = []
        messages = []
        try:
            data = json.loads(body)
            if isinstance(data, list):
                for message in data:
                    messages.append(message)
                    resp, _stop = callable_commiter.prepare(message)
                    stop_consuming = _stop or stop_consuming
                    if resp:
                        for r in resp:
                            responses.append(r)
            else:
                    messages.append(data)
                    resp, _stop = callable_commiter.prepare(data)
                    stop_consuming = _stop or stop_consuming
                    if resp:
                        for r in resp:
                            responses.append(r)
        except Exception as e:
            if RabbitQueueConsumerProducer.logger:
                RabbitQueueConsumerProducer.logger.exception("Exception while consuming message")
            ch.basic_nack(delivery_tag=method.delivery_tag)
            raise e
        try:
            if responses:
                broadcast_messages = []
                buffer = []
                for response in responses:
                    if isinstance(response, BroadcastMessage):
                        broadcast_messages.append(response)
                        continue
                    if self.messages_to_group == 1:
                        self._publish(ch, response)
                    else:
                        buffer.append(response)
                        if len(buffer) == self.messages_to_group:
                            self._publish(ch, buffer)
                            buffer = []
                if buffer:
                    self._publish(ch, buffer)
                if broadcast_messages:
                    for message in broadcast_messages:
                        self._publish(ch, message)
        except Exception as e:
            if RabbitQueueConsumerProducer.logger:
                RabbitQueueConsumerProducer.logger.exception("Exception while sending message")
            ch.basic_nack(delivery_tag=method.delivery_tag)
            raise e
        try:
            callable_commiter.commit()
        except Exception as e:
            if RabbitQueueConsumerProducer.logger:
                RabbitQueueConsumerProducer.logger.exception("Exception while commiting")
            ch.basic_nack(delivery_tag=method.delivery_tag)
            raise e
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if stop_consuming:
            if RabbitQueueConsumerProducer.logger:
                RabbitQueueConsumerProducer.logger.info("Stopping consumer")
            ch.stop_consuming()
            self.connection.close()

    def _obj_to_bytes(self, obj):
        if isinstance(obj, BroadcastMessage):
            return json.dumps(obj.item)
        elif isinstance(obj, list):
            return json.dumps([elem for elem in obj if elem])
        else:
            return json.dumps(obj)

    def _publish(self, channel, obj):
        if self.publisher_sharding:
            if isinstance(obj, list):
                shard_lists = {}
                for item in obj:
                    shards = self.publisher_sharding.get_shards(item)
                    for shard in shards:
                        if shard not in shard_lists:
                            shard_lists[shard] = [item]
                        else:
                            shard_lists[shard].append(item)
                for k, v in shard_lists.items():
                    for queue in self.response_queues:
                        resp_queue = PUBLISH_SHARDING_FORMAT % (queue, k)
                        channel.basic_publish(exchange='', routing_key=resp_queue,
                                              body=self._obj_to_bytes(v))
            else:
                for queue in self.response_queues:
                    shards = self.publisher_sharding.get_shards(obj)
                    for shard in shards:
                        resp_queue = PUBLISH_SHARDING_FORMAT % (queue, shard)
                        channel.basic_publish(exchange='', routing_key=resp_queue,
                                              body=self._obj_to_bytes(obj))
        else:
            for queue in self.response_queues:
                channel.basic_publish(exchange='', routing_key=queue,
                                      body=self._obj_to_bytes(obj))

    def __init__(self, host: str, consume_queue: str,
                 response_queues: List[str],
                 callable_commiter: StateCommiter,
                 messages_to_group: int = 1,
                 publisher_sharding: Optional[PublisherSharding] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Start a rabbit consumer-producer

        :param host: the hostname to connect
        :param consume_queue: the name of the queue to consume
        :param response_queues: the name of the queue in which the response will be sent
        :param callable_commiter: the function that consumes and creates a response,
                receives a dict and return a list of dicts to respond and a boolean
                indicating whether to stop consuming
        :param messages_to_group: the amount of messages to group
        of duplicated messages
        :param publisher_sharding: object for sharding messages
        :param logger: the logger to use
        """
        if logger:
            RabbitQueueConsumerProducer.logger = logger
        self.consume_queue = consume_queue
        self.response_queues = response_queues
        self.messages_to_group = messages_to_group
        self.publisher_sharding = publisher_sharding
        self.callable_commiter = callable_commiter
        self.host = host

    def __call__(self, *args, **kwargs):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.consume_queue)
        for resp_queue in self.response_queues:
            if self.publisher_sharding:
                for shard in self.publisher_sharding.get_possible_shards():
                    self.channel.queue_declare(queue=PUBLISH_SHARDING_FORMAT % (resp_queue, shard))
            else:
                self.channel.queue_declare(queue=resp_queue)
        callable_commiter = partial(self.consume, self.callable_commiter)
        self.channel.basic_consume(queue=self.consume_queue,
                                   on_message_callback=callable_commiter,
                                   auto_ack=False)
        self.channel.start_consuming()
