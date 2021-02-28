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
from tp2_utils.message_pipeline.message_set.disk_message_set_by_commit import DiskMessageSetByLastCommit
from tp2_utils.rabbit_utils.publisher_sharding import PublisherSharding
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
import random
from time import sleep
import numpy as np
import signal
import logging

logger = logging.getLogger('root')

DEFAULT_MESSAGES_TO_GROUP = 1000

OPERATION_ARGS = [([], []),
                  ([["GroupBy", "key"]], [[{'name': "Count"}]]),
                  ([["GroupBy", "key"]], [[{'name': "Count"}]]),
                  ([["GroupBy", "key"]], [[{'name': "Count"}]]),
                  ([["Filter", "count", lambda x: x > 2]], [[]])
                  ]

PIPELINE_C_ARGS = [({'ends_to_receive':1, 'ends_to_send':1, 'data_path': '/tmp/datapath1', 'signature': 'pipe1'},
                    {'host': "localhost", 'consume_queue': 'pipeline_start',
                     'response_queues': ['pipelineC_step1'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP, 'logger': logger},
                    ['/tmp/message_set1'],
                    ['key',3]),
                   ({'ends_to_receive': 1, 'ends_to_send': 1, 'data_path': '/tmp/datapath2', 'signature': 'pipe2'},
                    {'host': "localhost", 'consume_queue': 'pipelineC_step1_shard0',
                     'response_queues': ['pipelineC_step2'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP, 'logger': logger},
                    ['/tmp/message_set2'],
                    None),
                    ({'ends_to_receive': 1, 'ends_to_send': 1, 'data_path': '/tmp/datapath3', 'signature': 'pipe3'},
                    {'host': "localhost", 'consume_queue': 'pipelineC_step1_shard1',
                     'response_queues': ['pipelineC_step2'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP, 'logger': logger},
                    ['/tmp/message_set3'],
                    None),
                   ({'ends_to_receive': 1, 'ends_to_send': 1, 'data_path': '/tmp/datapath4', 'signature': 'pipe4'},
                    {'host': "localhost", 'consume_queue': 'pipelineC_step1_shard2',
                     'response_queues': ['pipelineC_step2'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP, 'logger': logger},
                    ['/tmp/message_set4'],
                    None),
                   ({'ends_to_receive': 3, 'ends_to_send': 1, 'data_path': '/tmp/datapath5'},
                    {'host': "localhost", 'consume_queue': 'pipelineC_step2',
                     'response_queues': ['pipelineC_result'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP, 'logger': logger},
                    ['/tmp/message_set5'],
                    None)
                   ]

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

    def _self_register_dir(self, location, delete_stuff=True):
        if delete_stuff:
            shutil.rmtree(location, ignore_errors=True)
        if not os.path.exists(location):
            os.mkdir(location)
        if location not in self.dirs_to_delete:
            self.dirs_to_delete.append(location)

    def _setup_message_set(self, location: str, delete_stuff=True):
        self._self_register_dir(location, delete_stuff)
        message_set = DiskMessageSetByLastCommit(location)
        return message_set

    def _setup_start_process(self, func, ops_args, process_args):
        p = Process(target=func)
        p.start()
        self.processes_to_join.append((p, ops_args, process_args))

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

    def _factory_operation_list(self, ops_args):
        final_operations = []
        for ops, aggs in zip(ops_args[0], ops_args[1]):
            aggs_objs = []
            if ops[0] == "GroupBy":
                for ag in aggs:
                    aggs_objs.append(GroupAggregate.factory(**ag))
            if aggs_objs:
                final_operations.append(Operation.factory(*ops, aggregates=aggs_objs))
            else:
                final_operations.append(Operation.factory(*ops))
        return final_operations

    def _factory_cp(self, ops_args, process_args, delete_stuff=True):
        pipe_kwargs, cp_kwargs, message_set_args, sharding_args = process_args
        operations = self._factory_operation_list(ops_args)
        message_set = None
        if message_set_args:
            message_set = self._setup_message_set(*message_set_args, delete_stuff=delete_stuff)
        p_sharding = None
        if sharding_args:
            p_sharding = PublisherSharding(*sharding_args)
        if 'data_path' in pipe_kwargs:
            self._self_register_dir(pipe_kwargs['data_path'], delete_stuff)
        pipe = MessagePipeline(**pipe_kwargs,
                               operations=(operations if operations else []),
                               idempotency_set=message_set)
        cp = RabbitQueueConsumerProducer(**cp_kwargs,
                                         callable_commiter=pipe,
                                         publisher_sharding=p_sharding)
        return cp


    def _factory_and_start_process(self, ops_args, process_args):
        self._setup_start_process(self._factory_cp(ops_args, process_args),
                                  ops_args, process_args)

    def _setup_pipelineC(self):
        self._setup_queue('pipelineC_step1_shard0')
        self._setup_queue('pipelineC_step1_shard1')
        self._setup_queue('pipelineC_step1_shard2')
        self._setup_queue('pipelineC_step2')
        self._setup_queue('pipelineC_result')
        for ops_args, process_args in zip(OPERATION_ARGS, PIPELINE_C_ARGS):
            self._factory_and_start_process(ops_args, process_args)

    def _chaos_monkey_process(self, write_chaos):
        np.random.seed(0)
        chaos_count = 0
        while True:
            chaos_count += 1
            i = random.choice(list(range(len(self.processes_to_join))))
            self.processes_to_join[i][0].terminate()
            cp = self._factory_cp(self.processes_to_join[i][1],
                                  self.processes_to_join[i][2],
                                  delete_stuff=False)
            p = Process(target=cp)
            p.start()
            write_chaos.send(p.pid)
            self.processes_to_join[i]=(p,self.processes_to_join[i][1], self.processes_to_join[i][2])
            sleep(np.random.poisson(0.4, size=1)[0]+0.05)

    def test_without_chaos_monkey(self):
        random.seed(0)

        expected_count = {}
        for i in range(100):
            list_of_elements = []
            for j in range(1000):
                element = {'key': chr(ord('A') + random.randint(0,25)), '_id': i*1000+j}
                if element['key'] not in expected_count:
                    expected_count[element['key']]=1
                else:
                    expected_count[element['key']] += 1
                list_of_elements.append(element)
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(list_of_elements))
        self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                   body=json.dumps({}))
        self._setup_pipelineC()
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineC_result'))
        consume_process.start()
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
        self.assertEqual({k:v for k,v in  expected_count.items() if v>2}, count_result)

    def test_with_chaos_monkey(self):
        random.seed(0)

        self._setup_pipelineC()
        chaos_monkey_p = Process(target=self._chaos_monkey_process, args=(self.write_chaos,))
        chaos_monkey_p.start()
        expected_count = {}
        for i in range(1000):
            list_of_elements = []
            for j in range(1000):
                element = {'key': chr(ord('A') + random.randint(0,25)), '_id': i*1000+j}
                if element['key'] not in expected_count:
                    expected_count[element['key']]=1
                else:
                    expected_count[element['key']] += 1
                list_of_elements.append(element)
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(list_of_elements))
        self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                   body=json.dumps({}))
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineC_result'))
        consume_process.start()
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
        self.assertEqual({k:v for k,v in  expected_count.items() if v>2}, count_result)
        self.assertTrue(chaos_monkey_p.is_alive())
        chaos_monkey_p.terminate()

    def test_with_chaos_monkey_and_repetitions(self):
        random.seed(0)

        self._setup_pipelineC()
        chaos_monkey_p = Process(target=self._chaos_monkey_process, args=(self.write_chaos,))
        chaos_monkey_p.start()
        expected_count = {}
        last_list = []
        for i in range(1000):
            list_of_elements = []
            reppeated_messages = []
            for j in range(1000):
                if not last_list or random.random() > 0.05:
                    element = {'key': chr(ord('A') + random.randint(0,25)), '_id': i*1000+j}
                    if element['key'] not in expected_count:
                        expected_count[element['key']]=1
                    else:
                        expected_count[element['key']] += 1
                    list_of_elements.append(element)
                else:
                    reppeated_messages.append(random.choice(last_list))
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(list_of_elements+reppeated_messages))
            last_list = list_of_elements
        self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                   body=json.dumps({}))
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipelineC_result'))
        consume_process.start()
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
        self.assertEqual({k:v for k,v in  expected_count.items() if v>2}, count_result)
        self.assertTrue(chaos_monkey_p.is_alive())
        chaos_monkey_p.terminate()

    def test_without_chaos_monkey_multiple_streams(self):
        random.seed(0)

        self._setup_pipelineC()
        for _ in range(100):
            expected_count = {}
            last_list = []
            reppeated_messages = []
            for i in range(20):
                list_of_elements = []
                for j in range(100):
                    if not last_list or random.random() > 0.05:
                        element = {'key': chr(ord('A') + random.randint(0,25)), '_id': i*1000+j}
                        if element['key'] not in expected_count:
                            expected_count[element['key']]=1
                        else:
                            expected_count[element['key']] += 1
                        list_of_elements.append(element)
                    else:
                        reppeated_messages.append(random.choice(last_list))
                self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                           body=json.dumps(list_of_elements+reppeated_messages))
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps({}))
            consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                       'pipelineC_result'))
            consume_process.start()
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
            self.assertEqual({k:v for k,v in  expected_count.items() if v>2}, count_result)
            for q in self.queues_to_purge:
                self.channel.queue_purge(q)

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
        self.recv_chaos, self.write_chaos = Pipe(False)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = self.connection.channel()
        self._setup_queue('pipeline_start')

    def tearDown(self) -> None:
        while self.recv_chaos.poll(0.1):
            pid=self.recv_chaos.recv()
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                continue
        for p in self.processes_to_join:
            p[0].terminate()
        for l in self.dirs_to_delete:
            shutil.rmtree(l, ignore_errors=True)
        for q in self.queues_to_purge:
            self.channel.queue_purge(q)
        self.channel.close()
        self.connection.close()
