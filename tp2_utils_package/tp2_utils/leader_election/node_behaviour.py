import logging
import socket
from abc import abstractmethod
from multiprocessing import Lock
from typing import Dict

from tp2_utils.data_transfering_utils.socket_data_receiver import SocketDataReceiver
from tp2_utils.data_transfering_utils.socket_data_sender import SocketDataSender
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

    def _check_connections(self):
        """
        Checks all the connections that are open. If the leader connection is down, notifies the bully algorithm.
        """
        bully_leader_election = self._bully_leader_election_dict['bully']
        leader_id = bully_leader_election.get_current_leader()
        logging.info("Actual leader id: " + str(leader_id))
        hosts_ids = bully_leader_election.get_hosts_ids()
        for host_id in hosts_ids:
            if host_id != bully_leader_election.get_id():
                connection = self._connections[host_id]
                try:
                    if connection.socket:
                        SocketDataSender.send_json(connection.socket, self._generate_ack_message(host_id))
                        SocketDataReceiver.receive_json(connection.socket)
                except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
                    logging.exception("Connection with host: {} is lost".format(host_id))
                    connection.socket.close()
                    self._connections[host_id] = Connection(connection.host, connection.port, None)
                    if host_id == leader_id:
                        logging.info("Connection with leader: {} is lost".format(host_id))
                        self._bully_leader_election_lock.acquire()
                        bully_leader_election = self._bully_leader_election_dict["bully"]
                        bully_leader_election.notify_leader_down()
                        self._bully_leader_election_dict["bully"] = bully_leader_election
                        self._bully_leader_election_lock.release()

    @abstractmethod
    def execute_tasks(self):
        pass

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
