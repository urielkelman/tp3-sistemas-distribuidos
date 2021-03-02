import socket
import logging

from time import sleep

from tp2_utils.leader_election.connection import Connection

SOCKET_TIMEOUT = 2
RETRIES = 5


def open_sending_socket_connection(host, port):
    total_retries = 0
    while total_retries < RETRIES:
        try:
            connection = socket.create_connection((host, port), timeout=SOCKET_TIMEOUT)
            return Connection(host, port, connection)
        except (ConnectionRefusedError, socket.gaierror, socket.timeout):
            total_retries += 1
            sleep(1)
            if total_retries == 5:
                logging.info("Connection with host {} cannot be established.".format(host))
    return Connection(host, port, None)
