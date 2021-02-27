import socket
import logging

from multiprocessing import Lock
from typing import Dict

from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.json_utils.json_receiver import JsonReceiver
from tp2_utils.json_utils.json_sender import JsonSender

LISTEN_BACKLOG = 5
CONNECTION_LAYER = "CONNECTION"
BULLY_LAYER = "BULLY"
ACK_MESSAGE = "ACK"


class BullyMessageReceiver:
    def __init__(self, host_id: int, port: int, bully_leader_election_dict: Dict[str, BullyLeaderElection],
                 bully_leader_election_lock: Lock):
        """
        :param host_id: The identifying number of the node that is running the process.
        :param port: The port of a socket that will listen for messages.
        :param bully_leader_election_dict: A concurrent map that wraps the leader election object that contains the algorithm logic.
        :param bully_leader_election_lock: A lock used to access to the bully object that is wrapped in the concurrent map.
        """
        self._host_id = host_id
        self._port = port
        self._bully_leader_election_dict = bully_leader_election_dict
        self._bully_leader_election_lock = bully_leader_election_lock

    def create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', self._port))
        sock.listen(LISTEN_BACKLOG)
        connection, address = sock.accept()
        return sock, connection, address

    def _generate_bully_message(self, message):
        return {
            "layer": BULLY_LAYER,
            "message": message,
            "host_id": self._host_id
        }

    def _generate_ack_message(self):
        return {
            "layer": CONNECTION_LAYER,
            "message": ACK_MESSAGE,
            "host_id": self._host_id
        }

    def start_listening(self):
        sock, connection, address = self.create_socket()
        while True:
            try:
                message = JsonReceiver.receive_json(connection)
                if message["layer"] == CONNECTION_LAYER:
                    JsonSender.send_json(connection, self._generate_ack_message())

                elif message["layer"] == BULLY_LAYER:
                    self._bully_leader_election_lock.acquire()
                    bully_leader_election = self._bully_leader_election_dict["bully"]
                    response_message = bully_leader_election.receive_message(message["message"])
                    self._bully_leader_election_dict["bully"] = bully_leader_election
                    self._bully_leader_election_lock.release()

                    if response_message:
                        final_message = self._generate_bully_message(response_message)
                    else:
                        final_message = self._generate_ack_message()

                    JsonSender.send_json(connection, final_message)
            except (ConnectionResetError, TimeoutError, OSError):
                logging.info("Connection with peer {} was reset. Open socket to listen if the node restart.".format(address))
                sock.close()
                sock, connection, address = self.create_socket()
