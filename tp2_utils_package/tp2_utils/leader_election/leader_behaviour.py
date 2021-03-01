import socket
import docker
import logging

from typing import Dict, List
from multiprocessing import Lock

from tp2_utils.leader_election.connection import Connection
from tp2_utils.leader_election.node_behaviour import NodeBehaviour
from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.leader_election.utils import open_sending_socket_connection
from tp2_utils.data_transferin_utils.socket_data_receiver import SocketDataReceiver

DEFAULT_ACK_PORT = 8000
ACK_MESSAGE = "ACK"


class LeaderBehaviour(NodeBehaviour):
    def __init__(self, connections: Dict[int, Connection], bully_leader_election_dict: Dict[str, BullyLeaderElection],
                 bully_leader_election_lock: Lock, workers_config: Dict[str, Dict]):
        """
        Initializes a leader behaviour object. It uses the same parameters of the NodeBehaviour plus a the worker nodes.
        :param workers_config: Dictionary that contains a host to monitor as keys and a dict with all the information
        associated to the host as values.
        """
        super().__init__(connections, bully_leader_election_dict, bully_leader_election_lock)
        self._workers_config = workers_config
        self._docker_client = docker.from_env()

    def _restart_node(self, worker_node):
        logging.info("Leader is about to restart worker: {}".format(worker_node))

        worker_node_config = self._workers_config[worker_node]
        self._docker_client.containers.run(
            image=worker_node_config["image"],
            hostname=worker_node,
            volumes=worker_node_config["volume"],
            environment=worker_node_config["environment"],
            entrypoint=worker_node_config["entrypoint"],
            detach=True
        )

    def _check_nodes_up(self):
        for worker_node in self._workers_config:
            connection = open_sending_socket_connection(worker_node, DEFAULT_ACK_PORT)
            if connection.socket is not None:
                try:
                    SocketDataReceiver.receive_fixed_size(connection.socket, len(ACK_MESSAGE))
                    logging.info("Received ACK from node {}".format(worker_node))
                    connection.socket.close()
                except (OSError, TimeoutError):
                    logging.exception("An exception happened when trying to get ACK from node: {}".format(worker_node))
                    self._restart_node(worker_node)
            else:
                self._restart_node(worker_node)

    def execute_tasks(self):
        logging.info("Execute leader tasks.")
        self._check_connections()
        self._check_nodes_up()
