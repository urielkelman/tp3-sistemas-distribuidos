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

    ''' @staticmethod
    def size_to_bytes_number(size: int) -> bytes:
        text = str(size)
        return text.zfill(SIZE_NUMBER_SIZE).encode('ascii')

    @staticmethod
    def receive_fixed_size(sock, size) -> str:
        data = ""
        recv_size = 0
        while recv_size < size:
            new_data = sock.recv(size - recv_size)
            recv_size += len(new_data)
            data += new_data.decode('ascii')
        return data

    def send_ok(self):
        self.send_plain_text(OK_MESSAGE)

    def receive_ok(self):
        text = self.receive_plain_text()
        assert text == OK_MESSAGE

    def receive_file_data(self, file):
        self.socket.settimeout(DEFAULT_RECEIVE_TIMEOUT)
        file_size = int(self.receive_fixed_size(self.socket, SIZE_NUMBER_SIZE))
        self.send_ok()
        while file_size > 0:
            buffer = self.socket.recv(DEFAULT_SOCKET_BUFFER_SIZE)
            file.write(buffer)
            file_size -= len(buffer)
        self.send_ok()
        self.socket.settimeout(None)

    def send_file(self, filename):
        file_size = os.stat(filename).st_size
        self.socket.sendall(self.size_to_bytes_number(file_size))
        self.receive_ok()
        with open(filename, "rb") as file:
            while file_size > 0:
                buffer = file.read(DEFAULT_SOCKET_BUFFER_SIZE)
                self.socket.sendall(buffer)
                file_size -= DEFAULT_SOCKET_BUFFER_SIZE
        self.receive_ok()

    def send_plain_text(self, text):
        encoded_text = text.encode('utf-8')
        self.socket.sendall(self.size_to_bytes_number(len(encoded_text)))
        self.socket.sendall(encoded_text)

    def receive_plain_text(self) -> str:
        self.socket.settimeout(DEFAULT_RECEIVE_TIMEOUT)
        size_to_recv = int(self.receive_fixed_size(self.socket, SIZE_NUMBER_SIZE))
        result = ""
        recv_size = 0
        while recv_size < size_to_recv:
            new_data = self.socket.recv(size_to_recv - recv_size)
            result += new_data.decode('utf-8')
            recv_size += len(new_data)
        self.socket.settimeout(None)
        return result

    def abort(self):
        self.send_plain_text("ABORT")

    def close(self):
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()
    '''