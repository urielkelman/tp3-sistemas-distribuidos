import logging
from multiprocessing import Lock
from typing import Dict

from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.leader_election.connection import Connection


class NodeBehaviour:
    CONNECTION_LAYER = "CONNECTION"
    ACK_MESSAGE = "ACK"
    BULLY_LAYER = "BULLY"
    QUEUE_TIMEOUT = 0.1

    def __init__(self, connections: Dict[int, Connection], bully_leader_election_dict: Dict[str, BullyLeaderElection],
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

    def _generate_ack_message(self, destination_host_id) -> Dict:
        return {
            "layer": self.CONNECTION_LAYER,
            "message": self.ACK_MESSAGE,
            "host_id": self._bully_leader_election_dict['bully'].get_id(),
            "destination_host_id": destination_host_id
        }
