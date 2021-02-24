from abc import abstractmethod
from typing import Dict
from multiprocessing import Queue

from tp2_utils_package.tp2_utils.leader_election.bully_leader_election import BullyLeaderElection

import socket


class NodeBehaviour:
    CONNECTION_LAYER = "CONNECTION"
    ACK_MESSAGE = "ACK"
    BULLY_LAYER = "BULLY"

    def __init__(self, connections: Dict[int, socket.socket], bully_leader_election: BullyLeaderElection,
                 incoming_messages_queue: Queue, outcoming_messages_queues: Dict[int, Queue]):
        """
        Instantiates a NodeBehaviour, an abstract class to inherit common node behaviour.
        :param connections: The connections to send messages of the leader election algorithm.
        :param bully_leader_election: The leader election object that contains the algorithm logic.
        :param incoming_messages_queue: A queue that receives incoming messages of other nodes.
        :param outcoming_messages_queues: A dictionary indexed by the id of a node, that contains queues to respond.
        to incoming messages.
        """
        self._connections = connections
        self._bully_leader_election = bully_leader_election
        self._incoming_messages_queue = incoming_messages_queue
        self._outcoming_messages_queues = outcoming_messages_queues

    @abstractmethod
    def execute_tasks(self):
        pass

    def _generate_ack_message(self) -> Dict:
        return {
            "layer": self.CONNECTION_LAYER,
            "message": self.ACK_MESSAGE,
            "host_id": self._bully_leader_election.get_id()
        }

    def _check_for_incoming_messages(self):
        while not self._incoming_messages_queue.empty():
            received_message = self._incoming_messages_queue.get()
            message = self._bully_leader_election.receive_message(received_message)
            if message:
                self._outcoming_messages_queues[received_message["host_id"]].put(message)
            else:
                self._outcoming_messages_queues[received_message["host_id"]].put(self._generate_ack_message())
