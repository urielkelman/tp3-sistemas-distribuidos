import logging
import socket
from typing import Dict

from tp2_utils.data_transfering_utils.socket_data_receiver import SocketDataReceiver
from tp2_utils.data_transfering_utils.socket_data_sender import SocketDataSender
from tp2_utils.leader_election.node_behaviour import NodeBehaviour

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)


class ReplicaBehaviour(NodeBehaviour):
    def _send_message(self, host_id: int, message: Dict):
        """
        Sends a message to the specified host.
        :param host_id: The host_id in the bully structure. Used to look up for the host in the configuration table.
        :param message: The message to be sent.
        """
        finish_sending = False
        while not finish_sending:
            try:
                connection = self._connections[host_id].socket
                if not connection:
                    raise ConnectionRefusedError
                SocketDataSender.send_json(connection, self._generate_bully_message(message))
                response = SocketDataReceiver.receive_json(connection)
                if response["layer"] == self.BULLY_LAYER:
                    self._bully_leader_election_lock.acquire()
                    bully_leader_election = self._bully_leader_election_dict["bully"]
                    bully_leader_election.receive_message(response["message"])
                    self._bully_leader_election_dict["bully"] = bully_leader_election
                    self._bully_leader_election_lock.release()
                finish_sending = True

            except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
                logging.exception("An exception happened when sending a message:")
                self._bully_leader_election_lock.acquire()
                bully_leader_election = self._bully_leader_election_dict["bully"]
                possible_leader_messages = bully_leader_election.notify_message_not_delivered(message)
                self._bully_leader_election_dict["bully"] = bully_leader_election
                self._bully_leader_election_lock.release()
                if possible_leader_messages:
                    live_connections_hosts_ids = [host_id_and_conn[0] for host_id_and_conn in self._connections.items()
                                                  if host_id_and_conn[1].socket is not None]
                    filtered_leader_messages = [leader_message for leader_message in possible_leader_messages if
                                                leader_message[
                                                    "destination_process_number"] in live_connections_hosts_ids]
                    # We have to be sure that the message is sent. Otherwise, we can enter an infinite loop of exceptions.
                    for leader_message in filtered_leader_messages:
                        self._send_message(leader_message["destination_process_number"], leader_message)
                finish_sending = True

    def _send_election_messages(self):
        """
        Requests election messages to the bully algorithm, and send them to other processes.
        """
        logging.info("Starting an election!")
        self._bully_leader_election_lock.acquire()
        bully_leader_election = self._bully_leader_election_dict['bully']
        election_messages = bully_leader_election.start_election()
        self._bully_leader_election_dict['bully'] = bully_leader_election
        self._bully_leader_election_lock.release()
        for election_message in election_messages:
            self._send_message(election_message["destination_process_number"], election_message)

    def execute_tasks(self):
        logging.info("Execute replica tasks.")
        self._check_connections()
        if self._bully_leader_election_dict['bully'].get_current_leader() == -1:
            self._send_election_messages()
