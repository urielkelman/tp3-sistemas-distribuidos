import json
import select
import socket

BYTES_AMOUNT_REQUEST_SIZE = 20


class SocketDataReceiver:
    @staticmethod
    def receive_fixed_size(connection: socket.socket, size: int):
        buffer = ""

        while len(buffer) < size:
            try:
                ready_to_read, ready_to_write, in_error = select.select([connection], [], [])
            except select.error:
                raise TimeoutError
            buffer += connection.recv(size).decode('utf-8')
            if not buffer:
                raise TimeoutError

        return buffer

    @staticmethod
    def receive_json(connection: socket.socket):
        request_size = int(SocketDataReceiver.receive_fixed_size(connection, BYTES_AMOUNT_REQUEST_SIZE))
        data = SocketDataReceiver.receive_fixed_size(connection, request_size)
        # logging.info("Json received: {}".format(data))
        # logging.info("Address: {}".format(connection.getpeername()))

        return json.loads(data)
