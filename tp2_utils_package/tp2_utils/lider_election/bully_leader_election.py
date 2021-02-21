from typing import List, Dict, Optional

ELECTION_MESSAGE = "ELECTION"
OK_MESSAGE = "OK"
LEADER_MESSAGE = "LEADER"


class BullyLeaderElection:
    def __init__(self, process_number, other_processes_number):
        """
        Starts a bully leader election algorithm entity, that represents a process.
        :param process_number: the identifying number of the process. Unique for each process.
        :param other_processes_number: the identifying numbers of the other processes that exists in the system.
        """
        self._process_number = process_number
        self._other_processes_number = other_processes_number
        self._current_leader = -1
        self._empty_responses_to_be_leader = -1

    def _generate_election_message(self, destination_process: int) -> Dict:
        """
        Returns an election message to be sent to a process.
        :param destination_process: the identifying number of the process that is going to receive the message.
        """
        return {
            "origin_process_number": self._process_number,
            "destination_process_number": destination_process,
            "message": ELECTION_MESSAGE
        }

    def _generate_ok_message(self, destination_process: int) -> Dict:
        """
        Returns an ok message to be sent to a process.
        :param destination_process: the identifying number of the process that is going to receive the message.
        """
        return {
            "origin_process_number": self._process_number,
            "destination_process_number": destination_process,
            "message": OK_MESSAGE
        }

    def _generate_leader_message(self, destination_process: int) -> Dict:
        """
        Returns a leader message to be sent to a process.
        :param destination_process: the identifying number of the process that is going to receive the message.
        """
        return {
            "origin_process_number": self._process_number,
            "destination_process_number": destination_process,
            "message": LEADER_MESSAGE
        }

    def start_election(self) -> List[Dict]:
        """
        Returns a list with all the messages to be sent to other processes at the start of an election.
        """
        election_messages = [self._generate_election_message(destination_process) for destination_process in
                             self._other_processes_number if destination_process > self._process_number]
        self._empty_responses_to_be_leader = len(election_messages)
        return election_messages

    def receive_message(self, message: Dict) -> Optional[List[Dict]]:
        """
        Processes a message and returns a list of messages to respond.
        :param message: the message to process.
        """
        if message["message"] == ELECTION_MESSAGE:
            return [self._generate_ok_message(message["origin_process_number"])] + self.start_election()
        elif message["message"] == LEADER_MESSAGE:
            self._current_leader = message["origin_process_number"]

    def notify_message_not_delivered(self, message: Dict) -> Optional[List[Dict]]:
        """
        Method to invoke when a message is not delivered. If the message was an election message, check if the process won the election.
        :param message: the message that was not delivered.
        """
        if message["message"] == ELECTION_MESSAGE:
            self._empty_responses_to_be_leader -= 1
            if not self._empty_responses_to_be_leader:
                self._current_leader = self._process_number
                return [self._generate_leader_message(destination_process) for destination_process in
                        self._other_processes_number]

    def current_leader(self):
        return self._current_leader
