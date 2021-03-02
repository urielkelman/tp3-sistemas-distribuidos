import os
import socket

LISTEN_BACKLOG = 3


class AckProcess:
    def __init__(self, port: int, parent_pid: int):
        """
        Initialize a socket that will listen for connections and send ACKs.
        :param port: The port used to open the socket.
        """
        self._port = port
        self.parent_pid = parent_pid

    @staticmethod
    def pid_is_alive(pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    def run(self):
        _ack_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _ack_socket.bind(('', self._port))
        _ack_socket.listen(LISTEN_BACKLOG)
        while self.pid_is_alive(self.parent_pid):
            client_sock, addr = _ack_socket.accept()
            try:
                client_sock.send("ACK".encode('utf-8'))
                client_sock.close()
            except OSError:
                client_sock.close()
