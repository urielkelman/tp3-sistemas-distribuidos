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

OPERATION_ARGS = [[]]

PIPELINE_C_ARGS = [({'ends_to_receive':1, 'ends_to_send':1},
                    {'host': "localhost", 'consume_queue': 'pipeline_start',
                     'response_queues': ['pipelineC_step1'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP},
                    ['/tmp/message_set1'],
                    ['key',3])]

MESSAGES_FOR_TESTING_ENDING_1 = [
    {'key': 'A', '_id': 0},
    {'key': 'A', '_id': 0},
    {'key': 'A', '_id': 1},
    [{'key': 'B', '_id': 2}, {'key': 'C', '_id': 3}],
    [{'key': 'B', '_id': 4}, {'key': 'D', '_id': 5}],
    [{'key': 'D', '_id': 6}, {'key': 'C', '_id': 7}],
    [{'key': 'B', '_id': 8}, {'key': 'C', '_id': 9}],
    [{'key': 'B', '_id': 10}, {'key': 'D', '_id': 11}],
    [{'key': 'D', '_id': 12}, {'key': 'C', '_id': 13}],
    [{'key': 'B', '_id': 14}, {'key': 'C', '_id': 15}],
    [{'key': 'B', '_id': 16}, {'key': 'D', '_id': 17}],
    [{'key': 'D', '_id': 18}, {'key': 'C', '_id': 19}],
    [{'key': 'E', '_id': 20}, {'key': 'E', '_id': 21}],
    {'key': 'F', '_id': 22},
    {'key': 'F', '_id': 23},
    {'key': 'G', '_id': 24},
    {'key': 'G', '_id': 25},
    {'key': 'G', '_id': 26},
    [{'key': 'H', '_id': 26}, {'key': 'H', '_id': 27}, {'key': 'H', '_id': 28},
     {'key': 'H', '_id': 29}, {'key': 'H', '_id': 30}, {'key': 'H', '_id': 31}],
    [{'key': 'I', '_id': 32}, {'key': 'J', '_id': 33}, {'key': 'K', '_id': 34},
     {'key': 'L', '_id': 35}, {'key': 'M', '_id': 36}],
    WINDOW_END_MESSAGE
]

MESSAGES_FOR_TESTING_ENDING_2 = [
    {'key': 'A', '_id': 0},
    {'key': 'A', '_id': 0},
    {'key': 'A', '_id': 1},
    [{'key': 'B', '_id': 2}, {'key': 'C', '_id': 3}],
    [{'key': 'B', '_id': 4}, {'key': 'D', '_id': 5}],
    [{'key': 'D', '_id': 6}, {'key': 'C', '_id': 7}],
    [{'key': 'B', '_id': 8}, {'key': 'C', '_id': 9}],
    [{'key': 'B', '_id': 10}, {'key': 'D', '_id': 11}],
    [{'key': 'D', '_id': 12}, {'key': 'C', '_id': 13}],
    [{'key': 'B', '_id': 14}, {'key': 'C', '_id': 15}],
    [{'key': 'B', '_id': 16}, {'key': 'D', '_id': 17}],
    [{'key': 'D', '_id': 18}, {'key': 'C', '_id': 19}],
    [{'key': 'E', '_id': 20}, {'key': 'E', '_id': 21}],
    {'key': 'F', '_id': 22},
    {'key': 'F', '_id': 23},
    {'key': 'G', '_id': 24},
    {'key': 'G', '_id': 25},
    {'key': 'G', '_id': 26},
    [{'key': 'H', '_id': 26}, {'key': 'H', '_id': 27}, {'key': 'H', '_id': 28},
     {'key': 'H', '_id': 29}, {'key': 'H', '_id': 30}, {'key': 'H', '_id': 31}],
    [{'key': 'I', '_id': 32}, {'key': 'J', '_id': 33}, {'key': 'K', '_id': 34},
     {'key': 'L', '_id': 35}, {'key': 'M', '_id': 36}, WINDOW_END_MESSAGE]
]

TESTING_RESULT = {'B': 6, 'C': 6, 'D': 6, 'G': 3, 'H': 6}

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
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if json.loads(body) == WINDOW_END_MESSAGE:
                ch.stop_consuming()
                connection.close()

        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()
        channel.basic_consume(queue=final_queue,
                              on_message_callback=partial(consume, write_pipe))
        channel.start_consuming()

    def _factory_and_start_process(self, pipe_kwargs, cp_kwargs,
                                   message_set_args=None, sharding_args=None,
                                   operations=None):
        message_set = None
        if message_set_args:
            message_set = self._setup_message_set(*message_set_args)
        p_sharding = None
        if sharding_args:
            p_sharding = PublisherSharding(*sharding_args)
        pipe = MessagePipeline(**pipe_kwargs,
                               operations=(operations if operations else []),
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer(**cp_kwargs,
                                         callable_commiter=pipe,
                                         publisher_sharding=p_sharding)
        self._setup_start_process(cp)

    def _setup_pipelineC(self):
        self._setup_queue('pipelineC_step1_shard0')
        self._setup_queue('pipelineC_step1_shard1')
        self._setup_queue('pipelineC_step1_shard2')
        self._setup_queue('pipelineC_step2')
        self._setup_queue('pipelineC_result')
        for ops_args, process_args in zip(OPERATION_ARGS, PIPELINE_C_ARGS):
            self._factory_and_start_process(*process_args)

        # receive sharded messages and count

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
        message_set = self._setup_message_set('/tmp/message_set5')
        pipe = MessagePipeline([Operation.factory("Filter", "count", lambda x: x > 2)],
                               ends_to_receive=3, ends_to_send=1,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineC_step2',
                                         ['pipelineC_result'],
                                         pipe,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP, )
        self._setup_start_process(cp)

    def test_count_pipeline_C_setup_ok(self):
        self._setup_pipelineC()
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineC_result'))
        consume_process.start()
        for element in MESSAGES_FOR_TESTING_ENDING_1:
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(element))
        consume_process.join()
        processed_data = []
        while not processed_data or processed_data[-1] != WINDOW_END_MESSAGE:
            processed_data.append(json.loads(self.recv_pipe.recv()))
        count_result = {}
        for resp in processed_data:
            if resp == WINDOW_END_MESSAGE:
                continue
            if isinstance(resp, list):
                for item in resp:
                    count_result[item['key']] = item['count']
            else:
                count_result[resp['key']] = resp['count']
        self.assertEqual(TESTING_RESULT, count_result)

        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineC_result'))
        consume_process.start()
        for element in MESSAGES_FOR_TESTING_ENDING_2:
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(element))
        consume_process.join()
        processed_data = []
        while not processed_data or processed_data[-1] != WINDOW_END_MESSAGE:
            processed_data.append(json.loads(self.recv_pipe.recv()))
        count_result = {}
        for resp in processed_data:
            if resp == WINDOW_END_MESSAGE:
                continue
            if isinstance(resp, list):
                for item in resp:
                    count_result[item['key']] = item['count']
            else:
                count_result[resp['key']] = resp['count']
        self.assertEqual(TESTING_RESULT, count_result)

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
