import json
import logging

BYTES_AMOUNT_REQUEST_SIZE = 20
SOCKET_TIMEOUT = 4.5


class JsonReceiver:
    @staticmethod
    def _receive_fixed_size(connection, size, with_timeout):
        logging.info("timeout" + str(connection.timeout))
        if with_timeout:
            connection.settimeout(SOCKET_TIMEOUT)
        buffer = ""
        while len(buffer) < size:
            buffer += connection.recv(size).decode('utf-8')
        return buffer

    @staticmethod
    def receive_json(connection, with_timeout=False):
        request_size = int(JsonReceiver._receive_fixed_size(connection, BYTES_AMOUNT_REQUEST_SIZE, with_timeout))
        data = JsonReceiver._receive_fixed_size(connection, request_size, with_timeout)
        logging.info("Json received: {}".format(data))
        logging.info("Address: {}".format(connection.getpeername()))

        return json.loads(data)
