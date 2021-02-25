from typing import Dict, List, Tuple, Optional, NoReturn, Any
from tp2_utils.message_pipeline.message_set.message_set import MessageSet
from tp2_utils.rabbit_utils.rabbit_consumer_producer import BroadcastMessage
from tp2_utils.interfaces.state_commiter import StateCommiter
import os
import re
import dill
from pathlib import Path


WINDOW_END_MESSAGE = {}
COMMITS_TO_KEEP = 10
ROTATING_COMMIT_SAVE_PATH = "%s/%d.pickle"
ROTATING_COMMIT_REGEX = "(\d+).pickle"
FLUSH_INDICATOR = "%s/FLUSH"


class MessagePipeline(StateCommiter):
    def __init__(self, operations: List['Operation'],
                 data_path: Optional[str] = None,
                 idempotency_set: Optional[MessageSet] = None,
                 ends_to_receive: int = 1, ends_to_send: int = 1,
                 stop_at_window_end: bool = False):
        """

        :param operations: the operations to use in the pipeline
        :param data_path: data path for saving the state if state needs to be saved
        :param idempotency_set: an object of type MessageSet to handle the arrival
        :param ends_to_receive: the ends to receive for consider that the stream ended
        :param ends_to_send: the ends to send when stream ends
        :param stop_at_window_end: if stop the consumer or not when the stream ends
        """
        super().__init__()
        self.operations = operations
        self.idempotency_set = idempotency_set
        self.data_path = data_path
        self.commit_number = 1
        self.ends_to_receive = ends_to_receive
        self.ends_to_send = ends_to_send
        self.ends_received = 0
        self.stop_at_window_end = stop_at_window_end
        self.flush_at_next_commit = False
        if self.data_path and os.path.exists(FLUSH_INDICATOR % self.data_path):
            self.flush()
        self.recover_state()

    def _change_end_to_broadcast(self, responses: List[Dict]) -> List:
        return [BroadcastMessage(item=r) if r == WINDOW_END_MESSAGE else r for r in responses]

    def flush(self) -> NoReturn:
        """
        Flushes all the state
        """
        if self.data_path:
            Path(FLUSH_INDICATOR % self.data_path).touch()
            for f in os.listdir(self.data_path):
                if f != "FLUSH":
                    os.remove("%s/%s" % (self.data_path, f))
        if self.idempotency_set:
            self.idempotency_set.flush()
        self.recover_state()
        if self.data_path:
            os.remove(FLUSH_INDICATOR % self.data_path)

    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """
        if self.flush_at_next_commit:
            self.flush()
            self.flush_at_next_commit = False
        if self.idempotency_set:
            cn = self.idempotency_set.commit()
            self.commit_number = cn
        else:
            cn = self.commit_number
            self.commit_number += 1
        if self.data_path:
            with open(ROTATING_COMMIT_SAVE_PATH % (self.data_path, self.commit_number), 'wb') as commit_file:
                dill.dump(self.operations, commit_file)
            if (self.commit_number > COMMITS_TO_KEEP and
                os.path.exists(ROTATING_COMMIT_SAVE_PATH % (self.data_path, self.commit_number-10))):
                os.remove(ROTATING_COMMIT_SAVE_PATH % (self.data_path, self.commit_number-10))
        return cn

    def prepare(self, item: Dict) -> Tuple[List, bool]:
        if self.idempotency_set and item and item in self.idempotency_set:
            return [], False
        if item == WINDOW_END_MESSAGE:
            self.ends_received += 1
            if self.ends_received < self.ends_to_receive:
                return [], False
        items_to_process = [item]
        for op in self.operations:
            new_items_to_process = []
            for item in items_to_process:
                new_items_to_process += op.process(item)
            items_to_process = new_items_to_process
        if self.idempotency_set and item:
            self.idempotency_set.prepare(item)
        if self.ends_received == self.ends_to_receive:
            items_to_process += [WINDOW_END_MESSAGE] * (self.ends_to_send - 1)
            if not self.stop_at_window_end:
                self.ends_to_receive = self.ends_received
                self.ends_received = 0
            self.flush_at_next_commit = True
            return self._change_end_to_broadcast(items_to_process), self.stop_at_window_end
        return self._change_end_to_broadcast(items_to_process), False

    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """
        available_commits = []
        if self.data_path:
            for f in os.listdir(self.data_path):
                if re.match(ROTATING_COMMIT_REGEX, f):
                    available_commits.append(int(re.findall(ROTATING_COMMIT_REGEX, f)[0]))
        if available_commits:
            sorted_commits = sorted(available_commits, reverse=True)
            for cn in sorted_commits:
                try:
                    with open(ROTATING_COMMIT_SAVE_PATH % (self.data_path, cn), 'rb') as commit_file:
                        self.operations = dill.load(commit_file)
                except Exception:
                    continue
                self.commit_number = cn
                if self.idempotency_set:
                    try:
                        self.idempotency_set.recover_state(self.commit_number)
                    except Exception as e:
                        raise e
                break
        else:
            if self.idempotency_set:
                self.idempotency_set.recover_state()
            self.commit_number = 1