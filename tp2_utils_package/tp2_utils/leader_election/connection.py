from typing import NamedTuple, Optional

import socket


class Connection(NamedTuple):
    host: str
    port: int
    socket: Optional[socket.socket]
