import socket
import logging

from tp2_utils_package.tp2_utils.leader_election.node_behaviour import NodeBehaviour
from tp2_utils_package.tp2_utils.json_utils.json_receiver import JsonReceiver
from tp2_utils_package.tp2_utils.json_utils.json_sender import JsonSender

from typing import Dict

CONNECTION_LAYER = "CONNECTION"
ACK_MESSAGE = "ACK"
BULLY_LAYER = "BULLY"

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)


class ReplicaBehaviour(NodeBehaviour):
    def __init__(self, connections, replica_id, bully_leader_election):
        self._connections = connections
        self._replica_id = replica_id
        self._bully_leader_election = bully_leader_election

    def _generate_bully_message(self, message):
        return {
            "layer": BULLY_LAYER,
            "message": message,
            "host_id": self._replica_id
        }

    def _generate_ack_message(self) -> Dict:
        return {
            "layer": CONNECTION_LAYER,
            "message": ACK_MESSAGE,
            "host_id": self._replica_id
        }

    def _send_message(self, host_id: int, message: Dict, retry_connection=False):
        """
        Sends a message to the specified host.
        :param host_id: The host_id in the bully structure. Used to look up for the host in the configuration table.
        :param message: The message to be sent.
        :para retry_connection: Boolean indicating if connection has to be retried.
        """
        finish_sending = False
        while not finish_sending:
            try:
                connection = self._connections[host_id]
                JsonSender.send_json(connection, self._generate_bully_message(message))
                response = JsonReceiver.receive_json(connection)
                if response["layer"] == BULLY_LAYER:
                    self._bully_leader_election.receive_message(response["message"])
                finish_sending = True
                connection.close()

            except (socket.timeout, socket.gaierror, ConnectionRefusedError):
                if not retry_connection:
                    self._bully_leader_election.notify_message_not_delivered(message)
                    finish_sending = True

    def _send_election_messages(self, retry_connection=False):
        """
        Requests election messages to the bully algorithm, and send them to other processes.
        """
        election_messages = self._bully_leader_election.start_election()
        for election_message in election_messages:
            self._send_message(election_message["destination_process_number"], election_message, retry_connection)

    def _check_leader_connection(self) -> bool:
        """
        Checks if the connection with the leader is open. Returns True if the connections is open, False otherwise.
        """
        leader_id = self._bully_leader_election.current_leader()
        try:
            JsonSender.send_json(self._connections[leader_id], self._generate_ack_message())
            response = JsonReceiver.receive_json(self._connections)
            logging.info("Received ACK from leader: {}".format(response["host_id"]))
            return True
        except (socket.timeout, socket.gaierror, ConnectionRefusedError):
            logging.info("Connection with leader: {} is lost".format(leader_id))
            return False

    def execute_tasks(self):
        if self._bully_leader_election.current_leader() == -1 or not self._check_leader_connection():
            self._send_election_messages()
