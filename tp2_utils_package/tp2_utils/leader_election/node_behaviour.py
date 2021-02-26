from abc import abstractmethod
from typing import Dict, Optional
from multiprocessing import Queue
from queue import Empty

from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.leader_election.connection import Connection

import socket

OPEN_CONNECTION = "OPEN"
CLOSED_CONNECTION = "CLOSED"

SOCKET_TIMEOUT = 2


class NodeBehaviour:
    CONNECTION_LAYER = "CONNECTION"
    ACK_MESSAGE = "ACK"
    BULLY_LAYER = "BULLY"
    QUEUE_TIMEOUT = 0.1

    def __init__(self, connections: Dict[int, Connection], bully_leader_election: BullyLeaderElection,
                 incoming_messages_queue: Queue, outcoming_messages_queues: Dict[int, Queue]):
        """
        Instantiates a NodeBehaviour, an abstract class to inherit common node behaviour.
        :param connections: The connections with the current status to send messages of the leader election algorithm.
        :param bully_leader_election: The leader election object that contains the algorithm logic.
        :param incoming_messages_queue: A queue that receives incoming messages of other nodes.
        :param outcoming_messages_queues: A dictionary indexed by the id of a node, that contains queues to respond.
        to incoming messages.
        """
        self._connections = connections
        self._bully_leader_election = bully_leader_election
        self._incoming_messages_queue = incoming_messages_queue
        self._outcoming_messages_queues = outcoming_messages_queues

    def execute_tasks(self):
        self._check_for_incoming_messages()

    def _generate_bully_message(self, message):
        return {
            "layer": self.BULLY_LAYER,
            "message": message,
            "host_id": self._bully_leader_election.get_id()
        }

    def _generate_ack_message(self) -> Dict:
        return {
            "layer": self.CONNECTION_LAYER,
            "message": self.ACK_MESSAGE,
            "host_id": self._bully_leader_election.get_id()
        }

    def _check_connection_status(self, host_id):
        """
        Checks if the connection of the host_id is open. If it is not open, creates a connection with it and modifies
        the connections dict.
        :param host_id: The id of the host to check the connection.
        """
        if not self._connections[host_id].socket:
            try:
                host = self._connections[host_id].host
                port = self._connections[host_id].port
                connection = socket.create_connection((host, port), timeout=SOCKET_TIMEOUT)
                self._connections[host_id] = Connection(host, port, connection)
            except ConnectionRefusedError:
                return

    def _check_for_incoming_messages(self):
        while 1:
            try:
                received_message = self._incoming_messages_queue.get(timeout=self.QUEUE_TIMEOUT)
                self._check_connection_status(received_message["host_id"])
                message = self._bully_leader_election.receive_message(received_message["message"])
                if message:
                    self._outcoming_messages_queues[received_message["host_id"]].put(
                        self._generate_bully_message(message))
                else:
                    self._outcoming_messages_queues[received_message["host_id"]].put(self._generate_ack_message())
            except Empty:
                return
