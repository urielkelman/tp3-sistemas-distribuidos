import base64
import json
import os
import pickle
import shutil
from typing import NoReturn, Optional, Tuple, Any

message = "Python is fun"
message_bytes = message.encode('ascii')
base64_bytes = base64.b64encode(message_bytes)
base64_message = base64_bytes.decode('ascii')

from .message_set import MessageSet

BUCKET_PATH = "%s/%d"
SAFE_BACKUP_END = ".copy"
# By design we just need the last commit but just in case we allow weaker storage for previous commits
RAM_MESSAGES_FROM_PREVIOUS_COMMITS = 25000


class DiskMessageSetByLastCommit(MessageSet):
    def _safe_pickle_dump(self, obj, path):
        if os.path.exists(path):
            shutil.copy2(path, path + SAFE_BACKUP_END)
        with open(path, "wb") as dumpfile:
            pickle.dump(obj, dumpfile)

    def _safe_pickle_load(self, path) -> Tuple[bool, Any]:
        result = None
        try:
            if os.path.exists(path):
                with open(path, "rb") as dumpfile:
                    result = pickle.load(dumpfile)
        except Exception:
            try:
                if os.path.exists(path + SAFE_BACKUP_END):
                    shutil.copy2(path + SAFE_BACKUP_END, path)
                    with open(path, "rb") as dumpfile:
                        result = pickle.load(dumpfile)
            except Exception:
                pass
        if result != None:
            return True, result
        else:
            return False, None

    def __init__(self, set_data_path: str, commit_number: Optional[int] = None,
                 recover_state_on_init: bool = False):
        super().__init__()
        self.set_data_path = set_data_path
        self.prepare_buffer = []
        self.last_item_set = set()
        self.commit_number = 1
        if recover_state_on_init:
            self.recover_state(commit_number)

    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """
        actual_cn = None
        if commit_number:
            if os.path.exists(BUCKET_PATH % (self.set_data_path, commit_number)):
                success, item_set = self._safe_pickle_load(BUCKET_PATH % (self.set_data_path, commit_number))
                if success:
                    self.last_item_set = item_set
                    actual_cn = commit_number
                elif os.path.exists(BUCKET_PATH % (self.set_data_path, commit_number - 1)):
                    success, item_set = self._safe_pickle_load(BUCKET_PATH % (self.set_data_path, commit_number))
                    if success:
                        self.last_item_set = item_set
                        actual_cn = commit_number - 1
        if actual_cn:
            self.commit_number = actual_cn + 1
        elif commit_number:
            self.commit_number = commit_number + 1
        else:
            self.commit_number = 1

    def __contains__(self, item) -> bool:
        if isinstance(item, dict):
            item = json.dumps(item).encode('utf-8')
        elif isinstance(item, str):
            item = item.encode('utf-8')
        return item in self.last_item_set

    def prepare(self, data: Any) -> NoReturn:
        """
        Prepares the data to be commited
        :param data: the data to be commited
        """
        if isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')
        elif isinstance(data, str):
            data = data.encode('utf-8')
        self.prepare_buffer.append(data)

    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """
        rn = self.commit_number
        if len(self.last_item_set) <= RAM_MESSAGES_FROM_PREVIOUS_COMMITS:
            self.last_item_set = self.last_item_set.union(set(self.prepare_buffer))
        else:
            self.last_item_set = set(self.prepare_buffer)
        self._safe_pickle_dump(self.last_item_set, BUCKET_PATH % (self.set_data_path, rn))
        if os.path.exists(BUCKET_PATH % (self.set_data_path, rn - 5)):
            os.remove(BUCKET_PATH % (self.set_data_path, rn - 5))
        self.prepare_buffer = []
        self.commit_number += 1
        return rn

    def flush(self):
        for f in os.listdir(self.set_data_path):
            os.remove(self.set_data_path + '/' + f)
        self.prepare_buffer = []
        self.last_item_set = set()
        self.recover_state()
