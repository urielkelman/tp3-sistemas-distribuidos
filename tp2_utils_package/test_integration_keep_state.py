import json
import os
import shutil
import unittest
from functools import partial
from multiprocessing import Process, Pipe

import pika

from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE, MessagePipeline
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.message_set.disk_message_set import DiskMessageSet
from tp2_utils.rabbit_utils.publisher_sharding import PublisherSharding
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer

DEFAULT_MESSAGES_TO_GROUP = 5


class TestIntegrations(unittest.TestCase):
    """
    This test is a big integration test with one purpose, counting the amount of different messages
    containing certain key and then filtering

    For this purpose we have built to paralellizable pipelines,
    one with sharding (pipeline B) and another one without (pipeline A)
    """

    def _setup_queue(self, queue_name: str):
        self.channel.queue_declare(queue=queue_name)
        self.channel.queue_purge(queue_name)
        self.queues_to_purge.append(queue_name)

    def _setup_message_set(self, location: str):
        shutil.rmtree(location, ignore_errors=True)
        os.mkdir(location)
        message_set = DiskMessageSet(location)
        self.dirs_to_delete.append(location)
        return message_set

    def _setup_start_process(self, func):
        p = Process(target=func)
        p.start()
        self.processes_to_join.append(p)

    @staticmethod
    def _read_process(write_pipe: Pipe, final_queue: str):

        def consume(write_pipe, ch, method, properties, body):
            write_pipe.send(body)
            if json.loads(body) == WINDOW_END_MESSAGE:
                ch.stop_consuming()

        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()
        channel.basic_consume(queue=final_queue,
                              on_message_callback=partial(consume, write_pipe),
                              auto_ack=True)
        channel.start_consuming()

    def _factory_and_start_process(self, pipe_kwargs, cp_kwargs, message_set_args=None):
        message_set = self._setup_message_set('/tmp/message_set1')
        pipe = MessagePipeline([], ends_to_receive=1, ends_to_send=1,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipeline_start',
                                         ['pipelineC_step1'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP,
                                         publisher_sharding=PublisherSharding(by_key='key',
                                                                              shards=3))
        self._setup_start_process(cp)

    def _setup_pipelineC(self):
        message_set = self._setup_message_set('/tmp/message_set1')
        pipe = MessagePipeline([], ends_to_receive=1, ends_to_send=1,
                               idempotency_set=message_set)
        self._setup_queue('pipelineC_step1_shard0')
        self._setup_queue('pipelineC_step1_shard1')
        self._setup_queue('pipelineC_step1_shard2')
        cp = RabbitQueueConsumerProducer("localhost", 'pipeline_start',
                                         ['pipelineC_step1'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP,
                                         publisher_sharding=PublisherSharding(by_key='key',
                                                                              shards=3))
        self._setup_start_process(cp)
        # receive sharded messages and count
        self._setup_queue('pipelineC_step2')

        message_set = self._setup_message_set('/tmp/message_set2')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineC_step1_shard0',
                                         ['pipelineC_step2'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        message_set = self._setup_message_set('/tmp/message_set3')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineC_step1_shard1',
                                         ['pipelineC_step2'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        message_set = self._setup_message_set('/tmp/message_set4')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineC_step1_shard2',
                                         ['pipelineC_step2'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        # filter

        self._setup_queue('pipelineC_result')
        message_set = self._setup_message_set('/tmp/message_set5')
        pipe = MessagePipeline([Operation.factory("Filter", "count", lambda x: x > 2)],
                               ends_to_receive=3, ends_to_send=1,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineC_step2',
                                         ['pipelineC_result'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP, )
        self._setup_start_process(cp)

    def setUp(self) -> None:
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        self.processes_to_join = []
        self.queues_to_purge = []
        self.dirs_to_delete = []
        self.recv_pipe, self.write_pipe = Pipe(False)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = self.connection.channel()
        self._setup_queue('pipeline_start')

    def tearDown(self) -> None:
        for p in self.processes_to_join:
            p.terminate()
        for l in self.dirs_to_delete:
            shutil.rmtree(l, ignore_errors=True)
