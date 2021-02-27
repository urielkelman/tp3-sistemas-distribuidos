import json
import logging
import select

BYTES_AMOUNT_REQUEST_SIZE = 20
SOCKET_TIMEOUT = 5


class JsonReceiver:
    @staticmethod
    def _receive_fixed_size(connection, size, with_timeout):
        buffer = ""
        if with_timeout:
            while len(buffer) < size:
                try:
                    ready_to_read, ready_to_write, in_error = select.select([connection], [], [], SOCKET_TIMEOUT)
                except select.error:
                    raise TimeoutError
                buffer += connection.recv(size).decode('utf-8')
                if not buffer:
                    raise TimeoutError

        else:
            while len(buffer) < size:
                buffer += connection.recv(size).decode('utf-8')

        return buffer

        # if with_timeout:
        #     poller = select.poll()
        #     poller.register(connection, select.POLLIN)
        #     events = poller.poll(SOCKET_TIMEOUT)
        #     if not events:
        #         raise TimeoutError
        # buffer = ""
        # while len(buffer) < size:
        #     buffer += connection.recv(size).decode('utf-8')
        #     # if not buffer:
        #     #     raise TimeoutError

    @staticmethod
    def receive_json(connection, with_timeout=False):
        request_size = int(JsonReceiver._receive_fixed_size(connection, BYTES_AMOUNT_REQUEST_SIZE, with_timeout))
        data = JsonReceiver._receive_fixed_size(connection, request_size, with_timeout)
        logging.info("Json received: {}".format(data))
        logging.info("Address: {}".format(connection.getpeername()))

        return json.loads(data)
