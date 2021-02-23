import unittest

from multiprocessing import Process
from time import sleep

from .tp2_utils.leader_election.bully_connection import BullyConnection


class TestBullyConnection(unittest.TestCase):
    @staticmethod
    def _launch_node_process(bully_connection):
        bully_connection.start()

    def test_connection_of_two_process(self):
        node_1_config = {
            2: ("localhost", 9001)
        }

        node_2_config = {
            1: ("locahost", 9000)
        }

        bully_connection_1 = BullyConnection(node_1_config, 9000, 1)
        bully_connection_2 = BullyConnection(node_2_config, 9001, 2)

        bully_2_process = Process(target=TestBullyConnection._launch_node_process, args=(bully_connection_2,))

        bully_2_process.start()
        bully_connection_1.start()

        sleep(1)

        self.assertEqual(bully_connection_1.current_leader(), 2)
