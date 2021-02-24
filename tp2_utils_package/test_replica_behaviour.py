import unittest
import socket

from multiprocessing import Queue, Process
from typing import List

from .tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from .tp2_utils.leader_election.replica_behaviour import ReplicaBehaviour


class TestReplicaBehaviour(unittest.TestCase):
    @staticmethod
    def _launch_process_with_bully(port: int, bully_process_id: int, bully_processes_ids: List[int]):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', port))
        connection, address = sock.accept()


    def test_one_node_initialization(self):
        bully_leader_election = BullyLeaderElection(1, [1])
        replica_behaviour = ReplicaBehaviour({}, bully_leader_election, Queue(), {})
        replica_behaviour.execute_tasks()

        self.assertEqual(bully_leader_election.current_leader(), 1)

    def test_initialization_with_bigger_node(self):
        other_bully_process = Process(target=self._launch_process_with_bully, args=(3450,))
        p
        socket =

        bully_leader_election = BullyLeaderElection(1, [1, 2])
        replica_behaviour = ReplicaBehaviour({}, bully_leader_election, Queue(), {})


