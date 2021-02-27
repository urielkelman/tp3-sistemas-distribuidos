from abc import abstractmethod
from typing import Dict
from multiprocessing import Lock
from queue import Empty

from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection

import socket
import logging


class NodeBehaviour:
    CONNECTION_LAYER = "CONNECTION"
    ACK_MESSAGE = "ACK"
    BULLY_LAYER = "BULLY"
    QUEUE_TIMEOUT = 0.1

    def __init__(self, connections: Dict[int, socket.socket], bully_leader_election_dict: Dict[str, BullyLeaderElection],
                 bully_leader_election_lock: Lock):
        """
        Instantiates a NodeBehaviour, an abstract class to inherit common node behaviour.
        :param connections: The connections to send messages of the leader election algorithm.
        :param bully_leader_election_dict: A concurrent map that wraps the leader election object that contains the algorithm logic.
        :param bully_leader_election_lock: A lock used to access to the bully leader election algorithm wrapped in the concurrent map.
        """
        self._connections = connections
        self._bully_leader_election_dict = bully_leader_election_dict
        self._bully_leader_election_lock = bully_leader_election_lock

    def execute_tasks(self):
        logging.info("executing leaders taks...")

    def _generate_bully_message(self, message):
        return {
            "layer": self.BULLY_LAYER,
            "message": message,
            "host_id": self._bully_leader_election_dict['bully'].get_id()
        }

    def _generate_ack_message(self) -> Dict:
        return {
            "layer": self.CONNECTION_LAYER,
            "message": self.ACK_MESSAGE,
            "host_id": self._bully_leader_election_dict['bully'].get_id()
        }
