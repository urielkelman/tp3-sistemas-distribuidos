import json
import os
import shutil
import unittest
from functools import partial
from multiprocessing import Process, Pipe
from typing import Dict, List, Tuple

import pika
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage
from tp2_utils.message_pipeline.message_set.disk_message_set import DiskMessageSet
from tp2_utils.rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from tp2_utils.interfaces.state_commiter import StateCommiter
from tp2_utils.interfaces.dummy_state_commiter import DummyStateCommiter

CONSUME_QUEUE = "consume_example"
RESPONSE_QUEUE = "response_example"


class TestRabbitQueueConsumerProducer(unittest.TestCase):
    @staticmethod
    def consume_filter(message: Dict) -> Tuple[List[Dict], bool]:
        if message["type"] == "A":
            return [message], False
        return [], False

    @staticmethod
    def publish_multiple(message: Dict, idempotency_set=None) -> Tuple[List[Dict], bool]:
        if message and idempotency_set and message in idempotency_set:
            return [], False
        if idempotency_set and message:
            idempotency_set.prepare(message)
            idempotency_set.commit()
        if isinstance(message["value"], int) or isinstance(message["value"], float):
            return [{"type": message["type"]}] * int(message["value"]), False
        return [], False

    @staticmethod
    def republish_and_stop_with_key_z(message: Dict, idempotency_set=None) -> Tuple[List[Dict], bool]:
        if message and idempotency_set and message in idempotency_set:
            return [], False
        if idempotency_set and message:
            idempotency_set.prepare(message)
        if message['key'] != 'Z':
            return [message], False
        else:
            return [message], True

    def _start_process(self, func: StateCommiter, messages_to_group=1):
        RabbitQueueConsumerProducer("localhost", CONSUME_QUEUE, [RESPONSE_QUEUE], func,
                                    messages_to_group=messages_to_group)()

    @staticmethod
    def _read_process(write_pipe: Pipe):

        def consume(write_pipe, ch, method, properties, body):
            write_pipe.send(body)

        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()
        channel.basic_consume(queue=RESPONSE_QUEUE,
                              on_message_callback=partial(consume, write_pipe),
                              auto_ack=True)
        channel.start_consuming()

    def setUp(self) -> None:
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        shutil.rmtree('/tmp/message_set', ignore_errors=True)
        os.mkdir('/tmp/message_set')
        self.message_set = DiskMessageSet('/tmp/message_set', recover_state_on_init = True)
        self.recv_pipe, self.write_pipe = Pipe(False)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.queue_declare(queue=RESPONSE_QUEUE)
        self.channel.queue_purge(CONSUME_QUEUE)
        self.channel.queue_purge(RESPONSE_QUEUE)
        self.test_process = None
        self.consume_process = Process(target=self._read_process, args=(self.write_pipe,))
        self.consume_process.start()

    def tearDown(self) -> None:
        self.channel.queue_purge(CONSUME_QUEUE)
        self.channel.queue_purge(RESPONSE_QUEUE)
        if self.test_process:
            self.test_process.terminate()
        self.consume_process.terminate()
        shutil.rmtree('/tmp/message_set', ignore_errors=True)

    def test_simple_filter(self):
        self.test_process = Process(target=self._start_process, args=(DummyStateCommiter(self.consume_filter),))
        self.test_process.start()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 4.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "B", "value": 5}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "C", "value": "a"}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "D", "value": 4}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 2.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 4.1}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps([{"type": "A", "value": None},
                                                    {"type": "V", "value": None}]))
        processed_data = []
        for _ in range(4):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        self.assertFalse(self.recv_pipe.poll(1))
        self.assertEqual(processed_data[0], {"type": "A", "value": 4.2})
        self.assertEqual(processed_data[1], {"type": "A", "value": 2.2})
        self.assertEqual(processed_data[2], {"type": "A", "value": 4.1})
        self.assertEqual(processed_data[3], {"type": "A", "value": None})

    def test_simple_filter_with_grouping(self):
        self.test_process = Process(target=self._start_process, args=(DummyStateCommiter(self.consume_filter), 2))
        self.test_process.start()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 4.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "B", "value": 85}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "C", "value": "a"}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "D", "value": 4}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 2.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 4.1}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps([{"type": "A", "value": None},
                                                    {"type": "V", "value": None}]))
        processed_data = []
        for _ in range(4):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        self.assertFalse(self.recv_pipe.poll(1))
        self.assertEqual(processed_data[0], [{"type": "A", "value": 4.2}])
        self.assertEqual(processed_data[1], [{"type": "A", "value": 2.2}])
        self.assertEqual(processed_data[2], [{"type": "A", "value": 4.1}])
        self.assertEqual(processed_data[3], [{"type": "A", "value": None}])

    def test_simple_multipy_message_with_grouping(self):
        self.test_process = Process(target=self._start_process, args=(DummyStateCommiter(self.publish_multiple), 2))
        self.test_process.start()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 4.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "B", "value": 7}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "C", "value": "a"}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "D", "value": 1}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps([{"type": "A", "value": None},
                                                    {"type": "V", "value": None}]))
        processed_data = []
        for _ in range(7):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        self.assertFalse(self.recv_pipe.poll(1))
        self.assertEqual(processed_data[0], [{"type": "A"}, {"type": "A"}])
        self.assertEqual(processed_data[1], [{"type": "A"}, {"type": "A"}])
        self.assertEqual(processed_data[2], [{"type": "B"}, {"type": "B"}])
        self.assertEqual(processed_data[3], [{"type": "B"}, {"type": "B"}])
        self.assertEqual(processed_data[4], [{"type": "B"}, {"type": "B"}])
        self.assertEqual(processed_data[5], [{"type": "B"}])
        self.assertEqual(processed_data[6], [{"type": "D"}])

    def test_idempotency_set_integration(self):
        self.test_process = Process(target=self._start_process,
                                    args=(DummyStateCommiter(lambda m: self.publish_multiple(m, self.message_set)), 2))
        self.test_process.start()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "A", "value": 4.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "B", "value": 7}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "C", "value": "a"}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "D", "value": 1}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps([{"type": "A", "value": 4.2},
                                                    {"type": "V", "value": 1}]))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"type": "D", "value": 1}))
        processed_data = []
        for _ in range(8):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        self.assertFalse(self.recv_pipe.poll(1))
        self.assertEqual(processed_data[0], [{"type": "A"}, {"type": "A"}])
        self.assertEqual(processed_data[1], [{"type": "A"}, {"type": "A"}])
        self.assertEqual(processed_data[2], [{"type": "B"}, {"type": "B"}])
        self.assertEqual(processed_data[3], [{"type": "B"}, {"type": "B"}])
        self.assertEqual(processed_data[4], [{"type": "B"}, {"type": "B"}])
        self.assertEqual(processed_data[5], [{"type": "B"}])
        self.assertEqual(processed_data[6], [{"type": "D"}])
        self.assertEqual(processed_data[7], [{"type": "V"}])

    def test_simple_stop(self):
        self.test_process = Process(target=self._start_process,
                                    args=(DummyStateCommiter(lambda m: self.republish_and_stop_with_key_z(m, self.message_set)), 2))
        self.test_process.start()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"key": "A", "value": 4.2}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"key": "Z", "value": 7}))
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"key": "C", "value": "a"}))
        self.test_process.join()
        processed_data = []
        for _ in range(2):
            processed_data.append(json.loads(self.recv_pipe.recv()))
        self.assertFalse(self.recv_pipe.poll(1))
        self.assertEqual(processed_data[0], [{"key": "A", "value": 4.2}])
        self.assertEqual(processed_data[1], [{"key": "Z", "value": 7}])

    def test_stop_on_first_message(self):
        def return_stop(msg):
            return [BroadcastMessage({})]*10, True
        self.test_process = Process(target=self._start_process,
                                    args=(return_stop, 1))
        self.test_process.start()
        self.channel.queue_declare(queue=CONSUME_QUEUE)
        self.channel.basic_publish(exchange='', routing_key=CONSUME_QUEUE,
                                   body=json.dumps({"key": "A", "value": 4.2}))
        self.test_process.join()
