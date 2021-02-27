import socket

from typing import NamedTuple, Optional


class Connection(NamedTuple):
    host: str
    port: int
    socket: Optional[socket.socket]
