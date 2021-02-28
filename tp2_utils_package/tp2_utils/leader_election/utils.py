import socket

from time import sleep

from tp2_utils.leader_election.connection import Connection

SOCKET_TIMEOUT = 2
RETRIES = 5


def open_sending_socket_connection(host, port):
    total_retries = 0
    while total_retries < RETRIES:
        import logging
        logging.info("Trying to establish connection with {} and port {}".format(host, port))
        try:
            connection = socket.create_connection((host, port), timeout=SOCKET_TIMEOUT)
            logging.info("Aca si")
            return Connection(host, port, connection)
        except (ConnectionRefusedError, socket.gaierror):
            total_retries += 1
            sleep(1)
    return Connection(host, port, None)
