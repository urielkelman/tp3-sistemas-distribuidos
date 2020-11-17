import json
import unittest
from multiprocessing import Process, Pipe
from functools import partial
import pika
from rabbit_utils.rabbit_producer import RabbitQueueProducer

CONSUME_QUEUE = "consume_example"


class TestRabbitQueueConsumerProducer(unittest.TestCase):
    @staticmethod
    def _read_process(write_pipe: Pipe):

        def consume(write_pipe, ch, method, properties, body):
            write_pipe.send(body)

        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()
        channel.basic_consume(queue=CONSUME_QUEUE,
                              on_message_callback=partial(consume, write_pipe),
                              auto_ack=True)
        channel.start_consuming()

    def setUp(self) -> None:
        self.recv_pipe, self.write_pipe = Pipe(False)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = self.connection.channel()
        self.channel.queue_purge(CONSUME_QUEUE)
        self.test_process = None
        self.consume_process = Process(target=self._read_process, args=(self.write_pipe,))
        self.consume_process.start()

    def tearDown(self) -> None:
        self.channel.queue_purge(CONSUME_QUEUE)
        if self.test_process:
            self.test_process.terminate()
        self.consume_process.terminate()

    def test_simple_produce(self):
        producer = RabbitQueueProducer('localhost', CONSUME_QUEUE)
        producer.publish({"type": "A"})
        producer.publish({"type": "A"})
        producer.publish({"type": "B"})
        producer.publish({"type": "B"})
        producer.publish({"type": "D"})
        consumed = []
        for _ in range(5):
            consumed.append(json.loads(self.recv_pipe.recv()))
        self.assertFalse(self.recv_pipe.poll(1))
        self.assertEqual(consumed[0], {'type': 'A'})
        self.assertEqual(consumed[1], {'type': 'A'})
        self.assertEqual(consumed[2], {'type': 'B'})
        self.assertEqual(consumed[3], {'type': 'B'})
        self.assertEqual(consumed[4], {'type': 'D'})
