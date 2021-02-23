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
            if json.loads(body) == WINDOW_END_MESSAGE:
                ch.stop_consuming()

        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()
        channel.basic_consume(queue=final_queue,
                              on_message_callback=partial(consume, write_pipe),
                              auto_ack=True)
        channel.start_consuming()

    def _setup_pipelineA(self):
        message_set = self._setup_message_set('/tmp/message_set1')
        pipe = MessagePipeline([],
                               ends_to_receive=1, ends_to_send=3, stop_at_window_end=True,
                               idempotency_set=message_set)
        self._setup_queue('pipelineA_step1_queue1')
        cp = RabbitQueueConsumerProducer("localhost", 'pipeline_start',
                                         ['pipelineA_step1_queue1'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)
        # 3 consumers that group and count
        self._setup_queue('pipelineA_step2_queue1')

        message_set = self._setup_message_set('/tmp/message_set2')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineA_step1_queue1',
                                         ['pipelineA_step2_queue1'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        message_set = self._setup_message_set('/tmp/message_set3')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineA_step1_queue1',
                                         ['pipelineA_step2_queue1'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        message_set = self._setup_message_set('/tmp/message_set4')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineA_step1_queue1',
                                         ['pipelineA_step2_queue1'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        # receive and sum counts
        self._setup_queue('pipelineA_step3_queue1')

        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Sum", "count"),
        ]),
                                Operation.factory('Rename', {"count_sum": "count"})],
                               ends_to_receive=3, ends_to_send=1, stop_at_window_end=True)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineA_step2_queue1',
                                         ['pipelineA_step3_queue1'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        # final filter

        self._setup_queue('pipelineA_result')

        message_set = self._setup_message_set('/tmp/message_set6')
        pipe = MessagePipeline([Operation.factory("Filter", "count", lambda x: x > 2)],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineA_step3_queue1',
                                         ['pipelineA_result'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

    def _setup_pipelineB(self):
        message_set = self._setup_message_set('/tmp/message_set1')
        pipe = MessagePipeline([], ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        self._setup_queue('pipelineB_step1_shard0')
        self._setup_queue('pipelineB_step1_shard1')
        self._setup_queue('pipelineB_step1_shard2')
        cp = RabbitQueueConsumerProducer("localhost", 'pipeline_start',
                                         ['pipelineB_step1'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP,
                                         publisher_sharding=PublisherSharding(by_key='key',
                                                                              shards=3))
        self._setup_start_process(cp)
        # receive sharded messages and count
        self._setup_queue('pipelineB_step2')

        message_set = self._setup_message_set('/tmp/message_set2')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineB_step1_shard0',
                                         ['pipelineB_step2'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        message_set = self._setup_message_set('/tmp/message_set3')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineB_step1_shard1',
                                         ['pipelineB_step2'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        message_set = self._setup_message_set('/tmp/message_set4')
        pipe = MessagePipeline([Operation.factory("GroupBy", group_by="key", aggregates=[
            GroupAggregate.factory("Count"),
        ])],
                               ends_to_receive=1, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineB_step1_shard2',
                                         ['pipelineB_step2'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP)
        self._setup_start_process(cp)

        # filter

        self._setup_queue('pipelineB_result')
        message_set = self._setup_message_set('/tmp/message_set5')
        pipe = MessagePipeline([Operation.factory("Filter", "count", lambda x: x > 2)],
                               ends_to_receive=3, ends_to_send=1, stop_at_window_end=True,
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer("localhost", 'pipelineB_step2',
                                         ['pipelineB_result'],
                                         pipe.process,
                                         messages_to_group=DEFAULT_MESSAGES_TO_GROUP,)
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
                                         pipe.process,
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
                                         pipe.process,
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
                                         pipe.process,
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
                                         pipe.process,
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
                                         pipe.process,
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

    def test_count_pipeline_A_ending_1(self):
        self._setup_pipelineA()
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineA_result'))
        consume_process.start()
        for element in MESSAGES_FOR_TESTING_ENDING_1[1:]:
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

    def test_count_pipeline_A_ending_2(self):
        self._setup_pipelineA()
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineA_result'))
        consume_process.start()
        for element in MESSAGES_FOR_TESTING_ENDING_2[1:]:
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

    def test_count_pipeline_B_ending_1(self):
        self._setup_pipelineB()
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineB_result'))
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

    def test_count_pipeline_B_ending_2(self):
        self._setup_pipelineB()
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineB_result'))
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

    def test_count_pipeline_C_process_twice(self):
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
