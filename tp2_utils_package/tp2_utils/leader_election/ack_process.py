import socket

LISTEN_BACKLOG = 3


class AckProcess:
    def __init__(self, port):
        """
        Initialize a socket that will listen for connections and send ACKs.
        :param port: The port used to open the socket.
        """
        self._ack_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._ack_socket.bind(('', port))
        self._ack_socket.listen(LISTEN_BACKLOG)

    def run(self):
        while True:
            client_sock, addr = self._ack_socket.accept()
            try:
                client_sock.send("ACK".encode('utf-8'))
                client_sock.close()
            except OSError:
                client_sock.close()
