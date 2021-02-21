import json
import logging

BYTES_AMOUNT_REQUEST_SIZE = 20


class JsonSender:
    @staticmethod
    def _padd_to_specific_size(bytes_data, size):
        if len(bytes_data) > size:
            raise ValueError("Final size should be larger than data size to padd.")
        return bytes("0" * (size - len(bytes_data)) + bytes_data, encoding='utf-8')

    @staticmethod
    def _send_fixed_size(connection, json_object):
        connection.sendall(JsonSender._padd_to_specific_size(str(len(json_object)), BYTES_AMOUNT_REQUEST_SIZE))

    @staticmethod
    def _to_json(json_object):
        return bytes(json.dumps(json_object), encoding='utf-8')

    @staticmethod
    def send_json(connection, object_to_send):
        json_object = JsonSender._to_json(object_to_send)
        JsonSender._send_fixed_size(connection, json_object)
        logging.info("Sending json object: {}".format(json_object))
        connection.sendall(json_object)

