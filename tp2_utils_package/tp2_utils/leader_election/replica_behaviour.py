import socket
import logging

from tp2_utils.leader_election.node_behaviour import NodeBehaviour
from tp2_utils.json_utils.json_receiver import JsonReceiver
from tp2_utils.json_utils.json_sender import JsonSender
from tp2_utils.leader_election.connection import Connection

from typing import Dict

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
                JsonSender.send_json(connection, self._generate_bully_message(message))
                response = JsonReceiver.receive_json(connection)
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
                logging.info("possible leader messages: {}".format(possible_leader_messages))
                if possible_leader_messages:
                    live_connections_hosts_ids = [host_id_and_conn[0] for host_id_and_conn in self._connections.items() if host_id_and_conn[1].socket is not None]
                    logging.info("live_conecttions: {}".format(live_connections_hosts_ids))

                    filtered_leader_messages = [leader_message for leader_message in possible_leader_messages if leader_message["destination_process_number"] in live_connections_hosts_ids]

                    logging.info("filtered messages: {}".format(filtered_leader_messages))

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
        logging.info("Election messages: {}".format(election_messages))
        for election_message in election_messages:
            self._send_message(election_message["destination_process_number"], election_message)

    def _check_connections(self):
        """
        Checks all the connections that are open. If the leader connection is down, notifies the bully algorithm.
        """
        bully_leader_election = self._bully_leader_election_dict['bully']
        leader_id = bully_leader_election.get_current_leader()
        logging.info("Actual leader id: " + str(leader_id))
        hosts_ids = bully_leader_election.get_hosts_ids()
        for host_id in hosts_ids:
            if host_id != bully_leader_election.get_id():
                connection = self._connections[host_id]
                try:
                    if connection.socket:
                        JsonSender.send_json(connection.socket, self._generate_ack_message(host_id))
                        JsonReceiver.receive_json(connection.socket)
                except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
                    logging.exception("Connection with host: {} is lost".format(host_id))
                    connection.socket.close()
                    self._connections[host_id] = Connection(connection.host, connection.port, None)
                    if host_id == leader_id:
                        logging.info("Connection with leader: {} is lost".format(host_id))
                        self._bully_leader_election_lock.acquire()
                        bully_leader_election = self._bully_leader_election_dict["bully"]
                        bully_leader_election.notify_leader_down()
                        self._bully_leader_election_dict["bully"] = bully_leader_election
                        self._bully_leader_election_lock.release()

    def execute_tasks(self):
        self._check_connections()
        if self._bully_leader_election_dict['bully'].get_current_leader() == -1:
            self._send_election_messages()
