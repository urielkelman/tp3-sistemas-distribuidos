import json
import logging
import os
import random
import shutil
import signal
import unittest
from functools import partial
from multiprocessing import Process, Pipe
from time import sleep

import numpy as np
import pika

from tp2_utils.message_pipeline.message_pipeline import MessagePipeline, message_is_end, \
    signed_end_message
from tp2_utils.message_pipeline.message_set.disk_message_set_by_commit import DiskMessageSetByLastCommit
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.rabbit_utils.publisher_sharding import PublisherSharding
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer

logger = logging.getLogger('root')

DEFAULT_MESSAGES_TO_GROUP = 1000

OPERATION_ARGS = [([["Filter", "count", lambda x: x > 10]], [[]])
                  ]

PIPELINE_C_ARGS = [({'ends_to_receive': 1, 'ends_to_send': 1, 'signature': 'asd'},
                    {'host': "localhost", 'consume_queue': 'pipeline_start',
                     'response_queues': ['pipeline_result'],
                     'messages_to_group': DEFAULT_MESSAGES_TO_GROUP, 'logger': logger},
                    ['/tmp/message_set'],
                    None)
                   ]


class TestIntegrationJUstMessageSet(unittest.TestCase):
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
            load = json.loads(body)
            if isinstance(load, dict) and message_is_end(load):
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

    def _setup_pipeline(self):
        self._setup_queue('pipeline_start')
        self._setup_queue('pipeline_result')
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
            self.processes_to_join[i] = (p, self.processes_to_join[i][1], self.processes_to_join[i][2])
            sleep(np.random.poisson(0.6, size=1)[0] + 0.05)

    def test_with_chaos_monkey(self):
        random.seed(0)

        self._setup_pipeline()
        chaos_monkey_p = Process(target=self._chaos_monkey_process, args=(self.write_chaos,))
        chaos_monkey_p.start()
        expected_list = []
        for i in range(100):
            list_of_elements = []
            for j in range(1000):
                element = {'_id': i * 1000 + j, 'count': random.randint(1, 20)}
                if element['count'] > 10:
                    expected_list.append(element)
                list_of_elements.append(element)
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(list_of_elements))
        self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                   body=json.dumps({}))
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipeline_result'))
        consume_process.start()
        processed_data = []
        while not (processed_data and (isinstance(processed_data[-1], dict) and message_is_end(processed_data[-1]))):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        result_list = []
        for resp in processed_data:
            if isinstance(resp, dict) and message_is_end(resp):
                continue
            if isinstance(resp, list):
                for item in resp:
                    result_list.append(item)
            else:
                result_list.append(resp)
        consume_process.join()
        self.assertEqual(set([json.dumps(r) for r in result_list]), set([json.dumps(r) for r in result_list]))

        self.assertTrue(chaos_monkey_p.is_alive())
        chaos_monkey_p.terminate()

    def test_with_chaos_monkey_and_signature(self):
        random.seed(0)

        self._setup_pipeline()
        chaos_monkey_p = Process(target=self._chaos_monkey_process, args=(self.write_chaos,))
        chaos_monkey_p.start()
        expected_list = []
        for i in range(100):
            list_of_elements = []
            for j in range(1000):
                element = {'_id': i * 1000 + j, 'count': random.randint(1, 20)}
                if element['count'] > 10:
                    expected_list.append(element)
                list_of_elements.append(element)
            self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                       body=json.dumps(list_of_elements))
        self.channel.basic_publish(exchange='', routing_key='pipeline_start',
                                   body=json.dumps(signed_end_message('test')))
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipeline_result'))
        consume_process.start()
        processed_data = []
        while not (processed_data and (isinstance(processed_data[-1], dict) and message_is_end(processed_data[-1]))):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        result_list = []
        for resp in processed_data:
            if isinstance(resp, dict) and message_is_end(resp):
                continue
            if isinstance(resp, list):
                for item in resp:
                    result_list.append(item)
            else:
                result_list.append(resp)
        consume_process.join()
        self.assertEqual(set([json.dumps(r) for r in result_list]), set([json.dumps(r) for r in result_list]))

        self.assertTrue(chaos_monkey_p.is_alive())
        chaos_monkey_p.terminate()

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
            pid = self.recv_chaos.recv()
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
