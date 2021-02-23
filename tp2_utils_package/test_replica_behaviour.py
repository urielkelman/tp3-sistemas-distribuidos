import unittest

from .tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from .tp2_utils.leader_election.replica_behaviour import ReplicaBehaviour


class TestReplicaBehaviour(unittest.TestCase):
    def test_one_node_initialization(self):
        bully_leader_election = BullyLeaderElection(1, [1])
        replica_behaviour = ReplicaBehaviour({}, 1, bully_leader_election)
        replica_behaviour.execute_tasks()

        self.assertEqual(bully_leader_election.current_leader(), 1)

    # def test_two_nodes_initialization(self):

