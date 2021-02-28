from typing import Dict, List, Tuple, Optional, NoReturn, Any
from tp2_utils.message_pipeline.message_set.message_set import MessageSet
from tp2_utils.rabbit_utils.rabbit_consumer_producer import BroadcastMessage
from tp2_utils.interfaces.state_commiter import StateCommiter
import os
import re
import dill
from pathlib import Path
import base64
import json


WINDOW_END_MESSAGE = {}
COMMITS_TO_KEEP = 100
ROTATING_COMMIT_SAVE_PATH = "%s/%d.pickle"
ROTATING_COMMIT_REGEX = "(\d+).pickle"
LOG_LOCATION = "%s/LOG"
FLUSH_INDICATOR = "%s/FLUSH"

ITEM_LOG_LINE = "%s\n"
END_COMMIT_LINE = "END_COMMIT@%d@\n"
END_COMMIT_REGEX = "END_COMMIT@(\d+)@\n"
CANCEL_SECTION = "\n\n@CANCEL@\n\n"
CANCEL_REGEX = "\s*@CANCEL@\s*"


def message_is_end(message) -> bool:
    if not message:
        return True
    if (len(message.keys())==2
            and "type" in message
            and "pipe_signature" in message
            and message['type']=="END"):
        return True
    return False

def signed_end_message(signature):
    return {"type": "END", "pipe_signature": signature}


class MessagePipeline(StateCommiter):
    def __init__(self, operations: List['Operation'],
                 data_path: Optional[str] = None,
                 idempotency_set: Optional[MessageSet] = None,
                 ends_to_receive: int = 1, ends_to_send: int = 1,
                 stop_at_window_end: bool = False,
                 signature: Optional[str] = None):
        """

        :param operations: the operations to use in the pipeline
        :param data_path: data path for saving the state if state needs to be saved
        :param idempotency_set: an object of type MessageSet to handle the arrival
        :param ends_to_receive: the ends to receive for consider that the stream ended
        :param ends_to_send: the ends to send when stream ends
        :param stop_at_window_end: if stop the consumer or not when the stream ends
        :param signature: signature for the end message
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
        self.signature = signature
        self.logfile = None
        if self.data_path and os.path.exists(FLUSH_INDICATOR % self.data_path):
            self.flush()
        self.recover_state()

    def _change_end_to_broadcast(self, responses: List[Dict]) -> List:
        if self.signature:
            return [BroadcastMessage(item=signed_end_message(self.signature))
                    if message_is_end(r) else r for r in responses]
        else:
            return [BroadcastMessage(item=WINDOW_END_MESSAGE)
                    if message_is_end(r) else r for r in responses]

    def _write_item_to_logfile(self, item: Dict):
        if self.logfile:
            item_dump = base64.b64encode(json.dumps(item).encode('utf-8')).decode('utf-8')
            self.logfile.write(ITEM_LOG_LINE % item_dump)
            self.logfile.flush()

    def _recover_item_from_line(self, line: str) -> Dict:
        return json.loads(base64.b64decode(line[:-1]))

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

    def _restore_up_to_last_commit(self, lower_commit_number: int) -> int:
        """

        :param lower_commit_number: the starting point for starting to redo
        :return: the new commit number
        """
        if self.data_path and os.path.exists(LOG_LOCATION % self.data_path):
            logfile = open(LOG_LOCATION % self.data_path, "r")
            cn = lower_commit_number
            temp_items_to_recover = []
            items_to_recover = []
            line = logfile.readline()
            while line:
                if re.match(END_COMMIT_REGEX, line):
                    cn = int(re.findall(END_COMMIT_REGEX, line)[0])
                    if cn > lower_commit_number:
                        items_to_recover += temp_items_to_recover
                        temp_items_to_recover = []
                    else:
                        temp_items_to_recover = []
                elif re.match(CANCEL_REGEX, line):
                    temp_items_to_recover = []
                else:
                    try:
                        temp_items_to_recover.append(self._recover_item_from_line(line))
                    except Exception:
                        pass
                line = logfile.readline()
            logfile.close()
            self.logfile = open(LOG_LOCATION % self.data_path, "a")
            for item in items_to_recover:
                if message_is_end(item):
                    self.ends_received += 1
                items_to_process = [item]
                for op in self.operations:
                    new_items_to_process = []
                    for item in items_to_process:
                        new_items_to_process += op.process(item)
                    items_to_process = new_items_to_process
            self.logfile.write(CANCEL_SECTION)
            self.logfile.flush()
            return cn
        else:
            if self.data_path:
                self.logfile = open(LOG_LOCATION % self.data_path, "w")
            return lower_commit_number

    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """
        if self.idempotency_set:
            cn = self.idempotency_set.commit()
            self.commit_number = cn
        else:
            cn = self.commit_number
            self.commit_number += 1
        if self.logfile:
            self.logfile.write(END_COMMIT_LINE % cn)
            self.logfile.flush()
        if self.data_path and cn % COMMITS_TO_KEEP == 0:
            with open(ROTATING_COMMIT_SAVE_PATH % (self.data_path, cn), 'wb') as commit_file:
                dill.dump((self.operations, self.ends_received), commit_file)
            if (cn > COMMITS_TO_KEEP and
                os.path.exists(ROTATING_COMMIT_SAVE_PATH % (self.data_path, cn-COMMITS_TO_KEEP))):
                os.remove(ROTATING_COMMIT_SAVE_PATH % (self.data_path, cn-COMMITS_TO_KEEP))
            self.logfile = open(LOG_LOCATION % self.data_path, "w")
        return cn

    def prepare(self, item: Dict) -> Tuple[List, bool]:
        if self.idempotency_set and item and item in self.idempotency_set:
            return [], False
        if message_is_end(item):
            self.ends_received += 1
            if self.idempotency_set:
                self.idempotency_set.prepare((item if not message_is_end(item) else WINDOW_END_MESSAGE))
            item = WINDOW_END_MESSAGE
            if self.ends_received < self.ends_to_receive:
                return [], False
        self._write_item_to_logfile(item)
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
            try:
                with open(ROTATING_COMMIT_SAVE_PATH % (self.data_path,
                                                       max(available_commits)),
                          'rb') as commit_file:
                    self.operations, self.ends_received = dill.load(commit_file)
                    self.commit_number = max(available_commits)
            except Exception:
                with open(ROTATING_COMMIT_SAVE_PATH % (self.data_path,
                                                       max(available_commits)-COMMITS_TO_KEEP),
                          'rb') as commit_file:
                    self.operations, self.ends_received = dill.load(commit_file)
                    self.commit_number = max(available_commits)-COMMITS_TO_KEEP
            self.commit_number = self._restore_up_to_last_commit(self.commit_number)
            if self.idempotency_set:
                try:
                    self.idempotency_set.recover_state(self.commit_number)
                except Exception as e:
                    raise e
            self.commit_number += 1
        else:
            if self.data_path:
                self.commit_number = self._restore_up_to_last_commit(0)
            if self.idempotency_set:
                self.idempotency_set.recover_state(self.commit_number)
            self.commit_number += 1

    def commit_done_cleanup(self):
        if self.flush_at_next_commit:
            self.flush()
            self.flush_at_next_commit = False

    def __del__(self):
        if self.logfile:
            self.logfile.close()