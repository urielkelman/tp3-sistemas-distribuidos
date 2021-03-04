import json
import os
import random
import shutil
import unittest
from functools import partial
from multiprocessing import Process, Pipe
from time import sleep

import pika

from business_download_service.__main__ import run_process as run_downloader
from business_joiner_service.__main__ import run_process as run_joiner
from tp2_utils_package.tp2_utils.message_pipeline.message_pipeline import message_is_end, WINDOW_END_MESSAGE


class TestBusinessJoiner(unittest.TestCase):
    PORT_TO_USE = random.randint(2000, 4000)

    def _setup_queue(self, queue_name: str):
        self.channel.queue_declare(queue=queue_name)
        self.channel.queue_purge(queue_name)
        self.queues_to_purge.append(queue_name)

    def _self_register_dir(self, location):
        shutil.rmtree(location, ignore_errors=True)
        if not os.path.exists(location):
            os.mkdir(location)
        self.dirs_to_delete.append(location)

    def _setup_start_process(self, func):
        p = Process(target=func)
        p.start()
        self.processes_to_join.append(p)

    @staticmethod
    def _read_process(write_pipe: Pipe, final_queue: str):

        def consume(write_pipe, ch, method, properties, body):
            desealized = json.loads(body)
            if isinstance(desealized, list):
                for m in desealized:
                    write_pipe.send(m)
                    if message_is_end(m):
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        ch.stop_consuming()
                        connection.close()
                        return
            else:
                write_pipe.send(desealized)
                if message_is_end(desealized):
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    ch.stop_consuming()
                    connection.close()
                    return
            ch.basic_ack(delivery_tag=method.delivery_tag)

        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()
        channel.basic_consume(queue=final_queue,
                              on_message_callback=partial(consume, write_pipe))
        channel.start_consuming()

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
        self._setup_queue('yelp_businesses_news')
        self._setup_queue('pipeline_result')
        self._setup_queue('notify_business_load_end')
        self._setup_queue('queue_to_join')
        self._self_register_dir('/tmp/downloader_data')
        self._self_register_dir('/tmp/joiner_data')

    def tearDown(self) -> None:
        for p in self.processes_to_join:
            p.terminate()
        for l in self.dirs_to_delete:
            shutil.rmtree(l, ignore_errors=True)
        for q in self.queues_to_purge:
            self.channel.queue_purge(q)
        self.channel.close()
        self.connection.close()
        TestBusinessJoiner.PORT_TO_USE += 1

    def test_pipeline_few_messages(self):
        sleep(1)
        QUANTITY_FOR_TESTING = 10000
        consume_process = Process(target=self._read_process, args=(self.write_pipe,
                                                                   'pipeline_result'))
        consume_process.start()
        self._setup_start_process(partial(run_downloader, TestBusinessJoiner.PORT_TO_USE,
                                          1, "localhost", 1,
                                          '/tmp/downloader_data'))
        self._setup_start_process(partial(run_joiner,
                                          "localhost", TestBusinessJoiner.PORT_TO_USE,
                                          "queue_to_join", "pipeline_result", "localhost",
                                          'joiner_signature', '/tmp/joiner_data'))
        for i in range(QUANTITY_FOR_TESTING):
            element = {"city": "Ciudad%d" % i, 'business_id': i}
            self.channel.basic_publish(exchange='', routing_key='yelp_businesses_news',
                                       body=json.dumps(element))
        self.channel.basic_publish(exchange='', routing_key='yelp_businesses_news',
                                   body=json.dumps(WINDOW_END_MESSAGE))
        for i in range(QUANTITY_FOR_TESTING):
            element = {'business_id': i}
            self.channel.basic_publish(exchange='', routing_key='queue_to_join',
                                       body=json.dumps(element))
        self.channel.basic_publish(exchange='', routing_key='queue_to_join',
                                   body=json.dumps(WINDOW_END_MESSAGE))
        messages = []
        while self.recv_pipe.poll(None):
            message = self.recv_pipe.recv()
            if isinstance(message, list):
                for m in message:
                    if not message_is_end(m):
                        messages.append(m)
                    else:
                        break
            else:
                if not message_is_end(message):
                    messages.append(message)
                else:
                    break
        consume_process.join()
        self.assertEqual(len(messages), QUANTITY_FOR_TESTING)
        for i in range(QUANTITY_FOR_TESTING):
            self.assertTrue({"city": "Ciudad%d" % i, 'business_id': i} in messages)
