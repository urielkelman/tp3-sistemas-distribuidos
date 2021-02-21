import json
import logging

BYTES_AMOUNT_REQUEST_SIZE = 20


class JsonReceiver:
    @staticmethod
    def _receive_fixed_size(connection, size):
        buffer = ""
        while len(buffer) < size:
            buffer += connection.recv(size).decode('utf-8')
        return buffer

    @staticmethod
    def receive_json(connection):
        request_size = int(JsonReceiver._receive_fixed_size(connection, BYTES_AMOUNT_REQUEST_SIZE))
        data = JsonReceiver._receive_fixed_size(connection, request_size)
        logging.info("Json received: {}".format(data))
        logging.info("Address: {}".format(connection.getpeername()))

        return json.loads(data)