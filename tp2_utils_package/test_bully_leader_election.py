import unittest

from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection


class TestBullyLeaderElection(unittest.TestCase):
    def setUp(self) -> None:
        """
        Creates an arbitrary set up with three nodes, and initialize them with their corresponding messages.
        """
        self.bully_leader_election_1 = BullyLeaderElection(1, [1, 2, 3])
        self.bully_leader_election_2 = BullyLeaderElection(2, [1, 2, 3])
        self.bully_leader_election_3 = BullyLeaderElection(3, [1, 2, 3])

        self.bully_start_election_messages_1 = self.bully_leader_election_1.start_election()
        self.bully_start_election_messages_2 = self.bully_leader_election_2.start_election()
        self.bully_start_election_messages_3 = self.bully_leader_election_3.start_election()

        self.bully_leader_election_2.receive_message(self.bully_start_election_messages_1[0])
        self.bully_leader_election_3.receive_message(self.bully_start_election_messages_1[1])

        self.bully_leader_election_3.receive_message(self.bully_start_election_messages_2[0])

        self.bully_leader_election_1.receive_message(self.bully_start_election_messages_3[0])
        self.bully_leader_election_2.receive_message(self.bully_start_election_messages_3[1])

    def test_two_nodes(self):
        bully_leader_election_1 = BullyLeaderElection(1, [1, 2])
        bully_leader_election_2 = BullyLeaderElection(2, [1, 2])

        bully_start_election_messages_1 = bully_leader_election_1.start_election()
        bully_start_election_messages_2 = bully_leader_election_2.start_election()

        bully_2_response = bully_leader_election_2.receive_message(bully_start_election_messages_1[0])

        bully_leader_election_1.receive_message(bully_start_election_messages_2[0])
        bully_leader_election_1.receive_message(bully_2_response)

        self.assertEqual(len(bully_start_election_messages_2), 1)
        self.assertEqual(bully_leader_election_1.get_current_leader(), 2)
        self.assertEqual(bully_leader_election_2.get_current_leader(), 2)
        self.assertEqual(bully_2_response["message"], "LEADER")

    def test_three_nodes(self):
        self.assertEqual(len(self.bully_start_election_messages_1), 2)
        self.assertEqual(len(self.bully_start_election_messages_2), 1)

        self.assertEqual(self.bully_leader_election_1.get_current_leader(), 3)
        self.assertEqual(self.bully_leader_election_2.get_current_leader(), 3)
        self.assertEqual(self.bully_leader_election_3.get_current_leader(), 3)

    def test_three_nodes_leader_down(self):
        """
        The top layer that manages the algorithm realize that the 3rd node is down.
        """
        self.bully_leader_election_1.notify_leader_down()
        self.bully_leader_election_2.notify_leader_down()

        bully_start_election_messages_1 = self.bully_leader_election_1.start_election()
        bully_start_election_messages_2 = self.bully_leader_election_2.start_election()

        self.bully_leader_election_2.receive_message(bully_start_election_messages_1[0])
        # The first and second bullies are notified by the connection layer that there are messages that weren't delivered.
        new_messages_1 = self.bully_leader_election_1.notify_message_not_delivered(bully_start_election_messages_1[1])
        new_messages_2 = self.bully_leader_election_2.notify_message_not_delivered(bully_start_election_messages_2[0])

        self.bully_leader_election_1.receive_message(new_messages_2[0])

        self.assertIsNone(new_messages_1)
        self.assertEqual(self.bully_leader_election_1.get_current_leader(), 2)
        self.assertEqual(self.bully_leader_election_2.get_current_leader(), 2)

    def test_three_nodes_two_nodes_down_and_one_starts_again(self):
        """
        First, the third node goes down. Node 2 takes the leadership, but then it goes down, so node 1 is alone
        and therefore the leader. Later, node 2 comes up and takes the leadership again.
        """
        self.bully_leader_election_1.notify_leader_down()
        self.bully_leader_election_2.notify_leader_down()
        bully_start_election_messages_1 = self.bully_leader_election_1.start_election()
        bully_start_election_messages_2 = self.bully_leader_election_2.start_election()
        self.bully_leader_election_2.receive_message(bully_start_election_messages_1[0])
        self.bully_leader_election_1.notify_message_not_delivered(bully_start_election_messages_1[1])
        new_messages_2 = self.bully_leader_election_2.notify_message_not_delivered(bully_start_election_messages_2[0])
        self.bully_leader_election_1.receive_message(new_messages_2[0])

        # Node 2 is now the leader. The connection layer of bully 1 realizes that node 2 is not responding anymore.
        self.bully_leader_election_1.notify_leader_down()
        new_messages_1 = self.bully_leader_election_1.start_election()
        self.bully_leader_election_1.notify_message_not_delivered(new_messages_1[0])
        self.bully_leader_election_1.notify_message_not_delivered(new_messages_1[1])
        self.assertEqual(self.bully_leader_election_1.get_current_leader(), 1)

        # Now, node 2 comes up, and recovers the leadership.
        self.bully_leader_election_2 = BullyLeaderElection(2, [1, 2, 3])
        new_messages_2 = self.bully_leader_election_2.start_election()
        leader_messages = self.bully_leader_election_2.notify_message_not_delivered(new_messages_2[0])
        self.bully_leader_election_1.receive_message(leader_messages[0])
        self.assertEqual(self.bully_leader_election_1.get_current_leader(), 2)
        self.assertEqual(self.bully_leader_election_2.get_current_leader(), 2)

    def test_three_nodes_the_second_comes_down_and_starts_again(self):
        # Suppose that the second node came down, so we have to generate a new instance of it. We know node 3 is the leader.
        self.bully_leader_election_2 = BullyLeaderElection(2, [1, 2, 3])
        bully_start_election_messages_2 = self.bully_leader_election_2.start_election()

        bully_3_response = self.bully_leader_election_3.receive_message(bully_start_election_messages_2[0])
        self.bully_leader_election_2.receive_message(bully_3_response)

        self.assertEqual(self.bully_leader_election_2.get_current_leader(), 3)
        self.assertEqual(bully_3_response["message"], "LEADER")

    def test_three_nodes_the_first_comes_down_and_starts_again(self):
        self.bully_leader_election_1 = BullyLeaderElection(1, [1, 2, 3])
        bully_start_election_messages_1 = self.bully_leader_election_1.start_election()

        bully_2_response = self.bully_leader_election_2.receive_message(bully_start_election_messages_1[0])
        bully_3_response_1 = self.bully_leader_election_3.receive_message(bully_start_election_messages_1[1])

        self.bully_leader_election_1.receive_message(bully_2_response)
        self.bully_leader_election_1.receive_message(bully_3_response_1)

        # We know that the second node will hold and election, because it has received and election message and
        # it knows that there ir a bigger node.

        bully_start_election_messages_2 = self.bully_leader_election_2.start_election()
        bully_3_response_2 = self.bully_leader_election_3.receive_message(bully_start_election_messages_2[0])

        self.bully_leader_election_2.receive_message(bully_3_response_2)

        self.assertEqual(self.bully_leader_election_1.get_current_leader(), 3)
        self.assertEqual(self.bully_leader_election_2.get_current_leader(), 3)
        self.assertEqual(self.bully_leader_election_3.get_current_leader(), 3)











