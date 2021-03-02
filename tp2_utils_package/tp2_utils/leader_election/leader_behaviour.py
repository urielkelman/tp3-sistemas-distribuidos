import logging
from multiprocessing import Lock
from typing import Dict, List

import docker

from tp2_utils.data_transferin_utils.socket_data_receiver import SocketDataReceiver
from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.leader_election.connection import Connection
from tp2_utils.leader_election.node_behaviour import NodeBehaviour
from tp2_utils.leader_election.utils import open_sending_socket_connection

DEFAULT_ACK_PORT = 8000
ACK_MESSAGE = "ACK"
DOCKER_PREFIX = "tp3-sistemas-distribuidos_"


class LeaderBehaviour(NodeBehaviour):
    def __init__(self, connections: Dict[int, Connection], bully_leader_election_dict: Dict[str, BullyLeaderElection],
                 bully_leader_election_lock: Lock, workers_config: Dict):
        """
        Initializes a leader behaviour object. It uses the same parameters of the NodeBehaviour plus a the worker nodes.
        :param workers_config: Dictionary that contains all the configuration about the running workers in the system.
        """
        super().__init__(connections, bully_leader_election_dict, bully_leader_election_lock)
        self._workers_config = workers_config
        self._docker_client = docker.from_env()

    def _get_node_information(self, worker_node) -> Dict:
        worker_node_tree = self._workers_config["services"][worker_node]
        return {
            "name": worker_node_tree["container_name"],
            "image": worker_node_tree["image"],
            "entrypoint": worker_node_tree["entrypoint"],
            "network": DOCKER_PREFIX + worker_node_tree["networks"][0],
            "volumes": {DOCKER_PREFIX + volume.split(":")[0]: {"bind": volume.split(":")[1]} for volume in
                        worker_node_tree["volumes"]},
            "environment": worker_node_tree["environment"] if "environment" in worker_node_tree else []
        }

    def _restart_node(self, worker_node):
        worker_node_information = self._get_node_information(worker_node)
        running_container_names = [volume.name for volume in self._docker_client.containers.list()]
        # We have to do the following check because the node could be restarted recently and not listening yet.
        if worker_node not in running_container_names:
            logging.info("Leader is about to restart worker: {}".format(worker_node))
            self._docker_client.containers.run(
                image=worker_node_information["image"],
                name=worker_node_information["name"],
                volumes=worker_node_information["volumes"],
                environment=worker_node_information["environment"],
                entrypoint=worker_node_information["entrypoint"],
                network=worker_node_information["network"],
                detach=True
            )

    def _get_worker_nodes(self) -> List[str]:
        return [service for service in self._workers_config["services"]
                if "monitor" not in service and "rabbit" not in service]

    def _check_nodes_up(self):
        logging.info(self._get_worker_nodes())
        for worker_node in self._get_worker_nodes():
            connection = open_sending_socket_connection(worker_node, DEFAULT_ACK_PORT)
            if connection.socket is not None:
                try:
                    SocketDataReceiver.receive_fixed_size(connection.socket, len(ACK_MESSAGE))
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
