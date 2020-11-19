import hashlib
import os
import random
import socket
import unittest
from multiprocessing import Barrier, Process

from .tp2_utils.blocking_socket_transferer import BlockingSocketTransferer


def message_sender(barrier, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', port))
    sock.listen(1)
    barrier.wait()
    c, addr = sock.accept()
    transferer = BlockingSocketTransferer(c)
    transferer.send_plain_text("Hola uacho")
    c.close()


def file_sender(barrier, port, input_file):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', port))
    sock.listen(1)
    barrier.wait()
    c, addr = sock.accept()
    transferer = BlockingSocketTransferer(c)
    transferer.send_file(input_file)
    transferer.close()


class TestBlockingSocketTransferer(unittest.TestCase):
    TEST_PORT = random.randint(8000, 9000)

    def setUp(self) -> None:
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        self.barrier = Barrier(2)
        self.p = None

    def tearDown(self) -> None:
        self.p.join()
        TestBlockingSocketTransferer.TEST_PORT += 1

    def test_send_text(self):
        self.p = Process(target=message_sender, args=(self.barrier, TestBlockingSocketTransferer.TEST_PORT))
        self.p.start()
        self.barrier.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestBlockingSocketTransferer.TEST_PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        self.assertEqual(socket_transferer.receive_plain_text(), "Hola uacho")
        socket_transferer.close()

    def test_send_file(self):
        with open('/tmp/big_dummy_file_test', 'wb') as dummy_file:
            for i in range(100000):
                dummy_file.write(("%d%d%d" % (i, i, i)).encode('utf-8'))
        sha256 = hashlib.sha256()
        with open('/tmp/big_dummy_file_test', 'rb') as dummy_file:
            while True:
                data = dummy_file.read(2048)
                if not data:
                    break
                sha256.update(data)
        original_hash = sha256.hexdigest()

        self.p = Process(target=file_sender, args=(self.barrier, TestBlockingSocketTransferer.TEST_PORT,
                                                   '/tmp/big_dummy_file_test'))
        self.p.start()
        self.barrier.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestBlockingSocketTransferer.TEST_PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        with open('/tmp/big_dummy_file_test_out', 'wb') as write_file:
            socket_transferer.receive_file_data(write_file)
        sha256 = hashlib.sha256()
        with open('/tmp/big_dummy_file_test_out', 'rb') as dummy_file:
            while True:
                data = dummy_file.read(2048)
                if not data:
                    break
                sha256.update(data)
        self.assertEqual(sha256.hexdigest(), original_hash)
        os.remove('/tmp/big_dummy_file_test')
        os.remove('/tmp/big_dummy_file_test_out')
        socket_transferer.close()
