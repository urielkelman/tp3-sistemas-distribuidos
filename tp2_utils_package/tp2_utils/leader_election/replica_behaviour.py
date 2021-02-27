import socket
import logging
import traceback

from tp2_utils.leader_election.node_behaviour import NodeBehaviour
from tp2_utils.json_utils.json_receiver import JsonReceiver
from tp2_utils.json_utils.json_sender import JsonSender
from tp2_utils.leader_election.connection import Connection

from typing import Dict

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)


class ReplicaBehaviour(NodeBehaviour):
    def _set_dead_connection(self, host_id):
        """
        Modifies a connection to indicate that is dead.
        :param host_id: The id of the dead connection node.
        """
        logging.info("Setting dead connection: " + str(host_id))
        host, port = self._connections[host_id].host, self._connections[host_id].port
        self._connections[host_id] = Connection(host, port, None)

    def _send_message(self, host_id: int, message: Dict):
        """
        Sends a message to the specified host.
        :param host_id: The host_id in the bully structure. Used to look up for the host in the configuration table.
        :param message: The message to be sent.
        :para retry_connection: Boolean indicating if connection has to be retried.
        """
        finish_sending = False
        while not finish_sending:
            try:
                connection = self._connections[host_id].socket
                if connection:
                    JsonSender.send_json(connection, self._generate_bully_message(message))
                    logging.info("Comienza a esperar respuesta")
                    response = JsonReceiver.receive_json(connection, with_timeout=True)
                    if response["layer"] == self.BULLY_LAYER:
                        self._bully_leader_election.receive_message(response["message"])
                else:
                    self._bully_leader_election.notify_message_not_delivered(message)
                finish_sending = True

            except (socket.timeout, socket.gaierror, ConnectionRefusedError, TimeoutError, OSError):
                logging.info("ACA")
                traceback.print_exc()
                self._bully_leader_election.notify_message_not_delivered(message)
                finish_sending = True
                self._set_dead_connection(host_id)

    def _send_election_messages(self):
        """
        Requests election messages to the bully algorithm, and send them to other processes.
        """
        election_messages = self._bully_leader_election.start_election()
        for election_message in election_messages:
            self._send_message(election_message["destination_process_number"], election_message)

    def _check_leader_connection(self):
        """
        Checks if the connection with the leader is open. Returns True if the connections is open, False otherwise.
        """
        leader_id = self._bully_leader_election.current_leader()
        logging.info(leader_id)
        if leader_id != -1 and leader_id != self._bully_leader_election.get_id():
            try:
                JsonSender.send_json(self._connections[leader_id].socket, self._generate_ack_message())
                logging.info("Message sent.")
                response = JsonReceiver.receive_json(self._connections[leader_id].socket, with_timeout=True)
                logging.info("Received ACK from leader: {}".format(response["host_id"]))
            except (socket.timeout, socket.gaierror, ConnectionRefusedError, TimeoutError, OSError):
                logging.info("Connection with leader: {} is lost".format(leader_id))
                self._bully_leader_election.notify_leader_down()
                self._set_dead_connection(leader_id)

    def execute_tasks(self):
        self._check_leader_connection()
        if self._bully_leader_election.current_leader() == -1:
            self._send_election_messages()
        self._check_for_incoming_messages()
