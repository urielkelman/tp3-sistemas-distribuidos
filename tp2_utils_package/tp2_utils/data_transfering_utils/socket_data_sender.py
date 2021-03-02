import json
import socket

BYTES_AMOUNT_REQUEST_SIZE = 20


class SocketDataSender:
    @staticmethod
    def _padd_to_specific_size(bytes_data: str, size: int) -> bytes:
        if len(bytes_data) > size:
            raise ValueError("Final size should be larger than data size to padd.")
        return bytes("0" * (size - len(bytes_data)) + bytes_data, encoding='utf-8')

    @staticmethod
    def _send_fixed_size(connection: socket.socket, json_object):
        connection.sendall(SocketDataSender._padd_to_specific_size(str(len(json_object)), BYTES_AMOUNT_REQUEST_SIZE))

    @staticmethod
    def _to_json(json_object) -> bytes:
        return bytes(json.dumps(json_object), encoding='utf-8')

    @staticmethod
    def send_json(connection: socket.socket, object_to_send):
        json_object = SocketDataSender._to_json(object_to_send)
        SocketDataSender._send_fixed_size(connection, json_object)
        # logging.info("Sending json object: {}".format(json_object))
        connection.sendall(json_object)
