from typing import Any, NoReturn, Optional
import base64
import os
import pickle
import glob
import pyhash
import json
import re

message = "Python is fun"
message_bytes = message.encode('ascii')
base64_bytes = base64.b64encode(message_bytes)
base64_message = base64_bytes.decode('ascii')

from collections import deque
from .message_set import MessageSet

LOGFILE_PATH = "%s/logfile"
END_COMMIT_LINE_START = "@END_COMMIT@"
END_COMMIT_LINE = "{}%d@\n".format(END_COMMIT_LINE_START)
END_COMMIT_REGEX = "@END_COMMIT@(\d+)@"
ADDED_HASH = "@ADDED_HASH@%d_%d@\n"
ADDED_HASH_REGEX = "@ADDED_HASH@(\d+)_(\d+)@"
SETS_PATH = "%s/sets"
BUCKET_PATH = "%s/%d.%d"
LINE_BREAK = '\n'
ADD_LRU = 5000
CONTAINS_CACHE_SIZE = 5000
BUCKET_FILE_MAX_ITEMS = 5
RESET_LOG_EACH_K_COMMITS = 20


class DiskMessageSet(MessageSet):
    def _get_write_bucket_file(self, hash0):
        if hash0 not in self.bucket_numbers:
            self.bucket_numbers[hash0] = 0
        return BUCKET_PATH % (self.set_data_path, hash0, self.bucket_numbers[hash0])

    def _restore_log(self, commit_number: Optional[int] = None):
        undo_adds = []
        actual_cn = None
        if commit_number:
            match_end = END_COMMIT_LINE[:-1] % commit_number
        else:
            match_end = END_COMMIT_REGEX
        with open(LOGFILE_PATH % self.set_data_path, "r") as logfile:
            line = logfile.readline()
            while line:
                if re.match(match_end, line[:-1]):
                    actual_cn = int(re.findall(END_COMMIT_REGEX, line)[0])
                    undo_adds = []
                elif line[-1] == LINE_BREAK:
                    if not re.match(END_COMMIT_REGEX, line[:-1]):
                        undo_adds.append(line)
                line = logfile.readline()
        if undo_adds:
            for line in undo_adds:
                if re.match(ADDED_HASH_REGEX, line[:-1]):
                    set_i, hash_result = re.findall(ADDED_HASH_REGEX, line[:-1])[0]
                    set_i, hash_result = int(set_i), int(hash_result)
                    self.hashing_sets[set_i].remove(hash_result)
                else:
                    item=base64.b64decode(line[:-1])
                    item_hashes = [self.hasher(item, seed=i) % self.hash_mod for i in range(len(self.hashing_sets))]
                    if item_hashes[0] not in self.bucket_numbers:
                        continue
                    for i in range(self.bucket_numbers[item_hashes[0]] + 1):
                        if os.path.exists(BUCKET_PATH % (self.set_data_path, item_hashes[0], i)):
                            with open(BUCKET_PATH % (self.set_data_path, item_hashes[0], i), "rb") as set_file:
                                item_set = pickle.load(set_file)
                            if item in item_set:
                                item_set.remove(item)
                                with open(BUCKET_PATH % (self.set_data_path, item_hashes[0], i), "wb") as set_file:
                                    pickle.dump(item_set, set_file)
                                break
            # This dump could fail and it would be irrecoverable
            with open(SETS_PATH % self.set_data_path, "wb") as sets_file:
                pickle.dump((self.hashing_sets, self.bucket_numbers), sets_file)
        return actual_cn

    def _writeahead_init(self, commit_number: Optional[int] = None):
        actual_cn = None
        if os.path.exists(LOGFILE_PATH % self.set_data_path):
            actual_cn = self._restore_log(commit_number)
        elif commit_number:
            raise AttributeError("Cant roll back to a commit number if there's no log")
        file = open(LOGFILE_PATH % self.set_data_path, "w")
        return file, actual_cn

    def __init__(self, set_data_path: str, hash_mod: int = 10000,
                 number_of_hashes: int = 20, commit_number: Optional[int] = None,
                 recover_state_on_init: bool = True):
        super().__init__()
        self.set_data_path = set_data_path
        self.hash_mod = hash_mod
        self.number_of_hashes = number_of_hashes
        self.contains_lru = deque(maxlen=CONTAINS_CACHE_SIZE)
        self.add_lru = deque(maxlen=ADD_LRU)
        self.writeahead_log = None
        self.hasher = pyhash.murmur3_32()
        self.prepare_buffer = []
        self.hashing_sets = []
        self.commit_number = 1
        self.bucket_numbers = {}
        # This allows an invalid state in the object, sorry fontela :(
        if recover_state_on_init:
            self.recover_state(commit_number)

    def recover_state(self, commit_number: Optional[int] = None):
        """
        Recovers the state, the last one or a determined one

        :param commit_number: the commit number from which to restore the state
        """
        if os.path.exists(SETS_PATH % self.set_data_path):
            with open(SETS_PATH % self.set_data_path, "rb") as sets_file:
                self.hashing_sets, self.bucket_numbers = pickle.load(sets_file)
        else:
            self.hashing_sets = [set() for _ in range(self.number_of_hashes)]
        self.writeahead_log, actual_cn = self._writeahead_init(commit_number)
        if commit_number:
            self.commit_number = commit_number + 1
        elif actual_cn:
            self.commit_number = actual_cn + 1
        else:
            self.commit_number = 1

    def __contains__(self, item) -> bool:
        if isinstance(item, dict):
            item = json.dumps(item).encode('utf-8')
        elif isinstance(item, str):
            item = item.encode('utf-8')
        if item in self.contains_lru:
            return True
        if item in self.add_lru:
            return True
        item_hashes = [self.hasher(item, seed=i) % self.hash_mod for i in range(len(self.hashing_sets))]
        for i in range(len(self.hashing_sets)):
            if item_hashes[i] not in self.hashing_sets[i]:
                return False
        for i in range(self.bucket_numbers[item_hashes[0]] + 1):
            if os.path.exists(BUCKET_PATH % (self.set_data_path, item_hashes[0], i)):
                with open(BUCKET_PATH % (self.set_data_path, item_hashes[0], i), "rb") as set_file:
                    item_set = pickle.load(set_file)
                if item in item_set:
                    self.contains_lru.append(item)
                    return True
        return False

    def _add(self, item):
        base64_item = base64.b64encode(item)
        self.writeahead_log.write(base64_item.decode('ascii') + LINE_BREAK)
        self.writeahead_log.flush()
        item_hashes = [self.hasher(item, seed=i) % self.hash_mod for i in range(len(self.hashing_sets))]
        if os.path.exists(self._get_write_bucket_file(item_hashes[0])):
            with open(self._get_write_bucket_file(item_hashes[0]), "rb") as set_file:
                item_set = pickle.load(set_file)
        else:
            item_set = set()
        item_set.update([item])
        # This dump could fail and it would be irrecoverable
        with open(self._get_write_bucket_file(item_hashes[0]), "wb") as set_file:
            pickle.dump(item_set, set_file)
        if len(item_set) > BUCKET_FILE_MAX_ITEMS:
            self.bucket_numbers[item_hashes[0]] += 1
        log_hash_count = 0
        for i in range(len(self.hashing_sets)):
            if log_hash_count < 1 and item_hashes[i] not in self.hashing_sets[i]:
                self.writeahead_log.write(ADDED_HASH % (i, item_hashes[i]))
                self.writeahead_log.flush()
                log_hash_count += 1
            self.hashing_sets[i].update([item_hashes[i]])
        self.add_lru.append(item)

    def prepare(self, data: Any) -> NoReturn:
        """
        Prepares the data to be commited
        :param data: the data to be commited
        """
        if isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')
        elif isinstance(data, str):
            data = data.encode('utf-8')
        if data in self.add_lru or data in self.contains_lru:
            return
        self.prepare_buffer.append(data)

    def commit(self) -> int:
        """
        Commits the prepared changes
        :return: a commit number
        """
        rn = self.commit_number-1
        if self.prepare_buffer:
            for data in self.prepare_buffer:
                self._add(data)
            # This dump could fail and it would be irrecoverable
            with open(SETS_PATH % self.set_data_path, "wb") as sets_file:
                pickle.dump((self.hashing_sets, self.bucket_numbers), sets_file)
            self.writeahead_log.write(END_COMMIT_LINE % self.commit_number)
            self.writeahead_log.flush()
            self.writeahead_log.close()
            if self.commit_number % RESET_LOG_EACH_K_COMMITS == 0:
                self.writeahead_log = open(LOGFILE_PATH % self.set_data_path, "w")
            else:
                self.writeahead_log = open(LOGFILE_PATH % self.set_data_path, "a")
            self.prepare_buffer = []
            rn = self.commit_number
            self.commit_number += 1
        return rn

    def flush(self):
        if os.path.exists(LOGFILE_PATH % self.set_data_path):
            os.remove(LOGFILE_PATH % self.set_data_path)
        if os.path.exists(SETS_PATH % self.set_data_path):
            os.remove(SETS_PATH % self.set_data_path)
        buckets = glob.glob(self.set_data_path + '/*')
        for b in buckets:
            os.remove(b)
        self.__init__(self.set_data_path, self.hash_mod, len(self.hashing_sets))

