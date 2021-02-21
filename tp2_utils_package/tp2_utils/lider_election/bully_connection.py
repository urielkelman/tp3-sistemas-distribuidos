import socket

from typing import List, Dict, Optional, Tuple
from tp2_utils_package.tp2_utils.blocking_socket_transferer import BlockingSocketTransferer

LISTEN_BACKLOG = 5


class BullyConnection:
    def _open_sending_socket_connection(self, host, port, host_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        self._sockets_to_send_messages[host_id] = BlockingSocketTransferer(sock)

    def _open_receiving_socket_connection(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', port))
        sock.listen(LISTEN_BACKLOG)
        self._sockets_to_receive_messages[port] = sock

    def __init__(self, bully_connections_config: Dict[Tuple], lowest_port: int, host_id: int):
        """
        Initializes connections and bully
        :param bully_connections_config: Dictionary that contains a tuple with a host that will be running the bully algorithm
        and a port that will have a listening socket as keys and the corresponding numerical id of the host as values.
        :param lowest_port: Integer that represents the lowest port to listen from other nodes.
        :param host_id: Numerical value that represents the id of the host for the bully algorithm.
        """
        self._bully_connections_config = bully_connections_config
        self._host_id = host_id
        self._sockets_to_send_messages = {}
        self._sockets_to_receive_messages = {}
        for bully_host, associated_port in self._bully_connections_config.values():
            self._open_sending_socket_connection(bully_host, associated_port, host_id)

        for _ in range(len(bully_connections_config)):
            self._open_receiving_socket_connection(lowest_port)
            lowest_port += 1

        # TODO: Write a method that initializes a Process that listen for each specific host.
        # TODO: Think about ACK messages. Are ACK messages going to be answered at the same port as the algorithm messages?
        # TODO: Perhaps, wrap the message and make a distinction about ACKS (connection layer) and algorithm messages.
