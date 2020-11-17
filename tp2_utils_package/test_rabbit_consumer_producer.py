import json
import unittest
from multiprocessing import Process, Pipe
from typing import Dict, List
from functools import partial
import os
import shutil
import pika
from rabbit_utils.rabbit_consumer_producer import RabbitQueueConsumerProducer
from rabbit_utils.message_set.disk_message_set import DiskMessageSet

CONSUME_QUEUE = "consume_example"
RESPONSE_QUEUE = "response_example"


class TestRabbitQueueConsumerProducer(unittest.TestCase):
    @staticmethod
    def consume_filter(message: Dict) -> List[Dict]:
        if message["type"] == "A":
            return [message]
        return []

    @staticmethod
    def publish_multiple(message: Dict) -> List[Dict]:
        if isinstance(message["value"], int) or isinstance(message["value"], float):
            return [{"type": message["type"]}]*int(message["value"])
        return []

    def _start_process(self, func, messages_to_group=1, idempotency_set=None):
        RabbitQueueConsumerProducer("localhost", CONSUME_QUEUE, RESPONSE_QUEUE, func,
                                    messages_to_group = messages_to_group,
                                    idempotency_set = idempotency_set)()

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
        self.message_set = DiskMessageSet('/tmp/message_set')
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
        self.test_process = Process(target=self._start_process, args=(self.consume_filter,))
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
        self.test_process = Process(target=self._start_process, args=(self.consume_filter, 2))
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
        self.test_process = Process(target=self._start_process, args=(self.publish_multiple, 2))
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
                                    args=(self.publish_multiple, 2, self.message_set))
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
