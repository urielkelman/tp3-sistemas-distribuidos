from multiprocessing import Process, Manager, Lock, Barrier
from time import sleep
from typing import Dict, Tuple

from tp2_utils.leader_election.utils import open_sending_socket_connection
from tp2_utils.leader_election.bully_leader_election import BullyLeaderElection
from tp2_utils.leader_election.bully_message_receiver import BullyMessageReceiver
from tp2_utils.leader_election.node_behaviour import NodeBehaviour
from tp2_utils.leader_election.replica_behaviour import ReplicaBehaviour

LISTEN_BACKLOG = 5
CONNECTION_LAYER = "CONNECTION"
BULLY_LAYER = "BULLY"
ACK_MESSAGE = "ACK"


class BullyConnection:
    def __init__(self, bully_connections_config: Dict[int, Tuple], lowest_port: int, host_id: int):
        """
        Initializes connections and bully
        :param bully_connections_config: Dictionary that contains numerical ids of hosts as keys
        and a tuple with the host ip and port that will have a listening socket to receive messages as values.
        :param lowest_port: Integer that represents the lowest port to listen from other nodes.
        :param host_id: Numerical value that represents the id of the host for the bully algorithm.
        """
        self._bully_connections_config = bully_connections_config
        self._host_id = host_id
        self._sockets_to_send_messages = {}

        bully_leader_election = BullyLeaderElection(host_id, list(bully_connections_config.keys()) + [host_id])
        manager = Manager()
        concurrent_dict = manager.dict()
        concurrent_dict['bully'] = bully_leader_election
        self._bully_leader_election_dict = concurrent_dict
        self._bully_leader_election_lock = Lock()

        self._sending_connections = manager.dict()

        open_sockets_barrier = Barrier(len(bully_connections_config) + 1)
        import logging
        logging.info(len(bully_connections_config) + 1)

        for i in range(len(bully_connections_config)):
            bully_message_receiver = BullyMessageReceiver(
                host_id, lowest_port + i, self._bully_leader_election_dict, self._bully_leader_election_lock,
                self._sending_connections, open_sockets_barrier
            )
            listening_process = Process(target=bully_message_receiver.start_listening)
            listening_process.start()

        for h_id, host_and_port in bully_connections_config.items():
            self._sending_connections[h_id] = open_sending_socket_connection(host_and_port[0], host_and_port[1])

        # This barrier exists because a listening process can try to access to the sending connections after they are all initialized.
        logging.info("wait")
        open_sockets_barrier.wait()

    def _run(self):
        """
        Loops doing the connections tasks. Invoke the appropriate behaviour based on who the current leader is.
        """
        while True:
            bully_leader_election = self._bully_leader_election_dict["bully"]
            leader = bully_leader_election.get_current_leader()
            if leader != -1 or leader != self._host_id:
                replica_behaviour = ReplicaBehaviour(self._sending_connections, self._bully_leader_election_dict,
                                                     self._bully_leader_election_lock)
                replica_behaviour.execute_tasks()
            else:
                replica_behaviour = NodeBehaviour(self._sending_connections, self._bully_leader_election_dict,
                                                  self._bully_leader_election_lock)
                replica_behaviour.execute_tasks()
            sleep(3)

    def start(self):
        self._run()
