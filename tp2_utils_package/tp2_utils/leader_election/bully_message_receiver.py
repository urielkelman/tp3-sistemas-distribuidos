import socket
import logging

from multiprocessing import Lock, Barrier
from typing import Dict

from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.json_utils.json_receiver import JsonReceiver
from tp2_utils.json_utils.json_sender import JsonSender
from tp2_utils.leader_election.connection import Connection
from tp2_utils.leader_election.utils import open_sending_socket_connection

LISTEN_BACKLOG = 5
CONNECTION_LAYER = "CONNECTION"
BULLY_LAYER = "BULLY"
ACK_MESSAGE = "ACK"


class BullyMessageReceiver:
    def __init__(self, host_id: int, port: int, bully_leader_election_dict: Dict[str, BullyLeaderElection],
                 bully_leader_election_lock: Lock, sending_connections: Dict[int, Connection], open_sockets_barrier: Barrier):
        """
        :param host_id: The identifying number of the node that is running the process.
        :param port: The port of a socket that will listen for messages.
        :param bully_leader_election_dict: A concurrent map that wraps the leader election object that contains the algorithm logic.
        :param bully_leader_election_lock: A lock used to access to the bully object that is wrapped in the concurrent map.
        :param sending_connections: A concurrent map with connections. Used to create a connection when a received message.
        :param open_sockets_barrier: A barrier which objective is wait until all the listening sockets are opened.
        of a host id doesn't have a sending connection.
        """
        self._host_id = host_id
        self._port = port
        self._bully_leader_election_dict = bully_leader_election_dict
        self._bully_leader_election_lock = bully_leader_election_lock
        self._sending_connections = sending_connections
        self._open_sockets_barrier = open_sockets_barrier

    @staticmethod
    def _accept_connection(sock: socket.socket):
        connection, address = sock.accept()
        return connection, address

    def create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self._port))
        sock.listen(LISTEN_BACKLOG)
        logging.info("Listening for a connection at port: {}".format(self._port))
        self._open_sockets_barrier.wait()
        connection, address = self._accept_connection(sock)
        return sock, connection, address

    def _generate_bully_message(self, message):
        return {
            "layer": BULLY_LAYER,
            "message": message,
            "host_id": self._host_id
        }

    def _generate_ack_message(self, destination_host_id):
        return {
            "layer": CONNECTION_LAYER,
            "message": ACK_MESSAGE,
            "host_id": self._host_id,
            "destination_host_id": destination_host_id
        }

    def _check_open_sending_connection(self, host_id):
        connection = self._sending_connections[host_id]
        if not connection.socket:
            self._sending_connections[host_id] = open_sending_socket_connection(connection.host, connection.port)

    def start_listening(self):
        sock, connection, address = self.create_socket()
        while True:
            try:
                message = JsonReceiver.receive_json(connection)
                self._check_open_sending_connection(message["host_id"])
                if message["layer"] == CONNECTION_LAYER:
                    JsonSender.send_json(connection, self._generate_ack_message(message["host_id"]))

                elif message["layer"] == BULLY_LAYER:
                    self._bully_leader_election_lock.acquire()
                    bully_leader_election = self._bully_leader_election_dict["bully"]
                    response_message = bully_leader_election.receive_message(message["message"])
                    self._bully_leader_election_dict["bully"] = bully_leader_election
                    self._bully_leader_election_lock.release()

                    if response_message:
                        final_message = self._generate_bully_message(response_message)
                    else:
                        final_message = self._generate_ack_message(message["host_id"])

                    JsonSender.send_json(connection, final_message)
            except (ConnectionResetError, TimeoutError, OSError):
                logging.exception("Exception in message receiver:")
                logging.info("Connection with peer {} was reset. Close and reopen socket in port {} to listen if the node restart.".format(address, self._port))
                connection, address = self._accept_connection(sock)
