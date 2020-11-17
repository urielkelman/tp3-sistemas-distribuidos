import pyhash
import pickle
import os
import base64

message = "Python is fun"
message_bytes = message.encode('ascii')
base64_bytes = base64.b64encode(message_bytes)
base64_message = base64_bytes.decode('ascii')

from collections import deque
from .message_set import MessageSet

LOGFILE_PATH = "%s/logfile"
COMMIT_LINE = "@COMMIT@\n"
SETS_PATH = "%s/sets"
BUCKET_PATH = "%s/%d.%d"
LINE_BREAK = '\n'
MAX_UNCOMMITED = 5000
CONTAINS_CACHE_SIZE = 5000
BUCKET_FILE_MAX_ITEMS = 5

class DiskMessageSet(MessageSet):
    def _get_write_bucket_file(self, hash0):
        if hash0 not in self.bucket_numbers:
            self.bucket_numbers[hash0] = 0
        return BUCKET_PATH % (self.set_data_path, hash0, self.bucket_numbers[hash0])

    def _redo_log(self):
        redo_adds = []
        with open(LOGFILE_PATH % self.set_data_path, "r") as logfile:
            line = logfile.readline()
            while line:
                if line == COMMIT_LINE:
                    redo_adds = []
                elif line[-1] == LINE_BREAK:
                    redo_adds.append(line)
                line = logfile.readline()
        if redo_adds:
            for encoded_item in redo_adds:
                item = base64.b64decode(encoded_item[:-1])
                self.add(item, lazy=False)
            # This dump could fail and it would be irrecoverable
            with open(SETS_PATH % self.set_data_path, "wb") as sets_file:
                pickle.dump((self.hashing_sets, self.bucket_numbers), sets_file)

    def _writeahead_init(self):
        if os.path.exists(LOGFILE_PATH % self.set_data_path):
            self._redo_log()
        file = open(LOGFILE_PATH % self.set_data_path, "w")
        file.write(COMMIT_LINE)
        file.flush()
        return file

    def __init__(self, set_data_path: str, hash_mod: int=10000, number_of_hashes: int = 20):
        self.set_data_path = set_data_path
        self.hasher = pyhash.murmur3_32()
        self.bucket_numbers = {}
        if os.path.exists(SETS_PATH % self.set_data_path):
            with open(SETS_PATH % self.set_data_path, "rb") as sets_file:
                self.hashing_sets, self.bucket_numbers = pickle.load(sets_file)
        else:
            self.hashing_sets = [set() for _ in range(number_of_hashes)]
        self.hash_mod = hash_mod
        self.contains_lru = deque(maxlen=CONTAINS_CACHE_SIZE)
        self.add_lru = deque(maxlen=MAX_UNCOMMITED)
        self.writeahead_log = self._writeahead_init()
        self.log_uncommited = []

    def __contains__(self, item) -> bool:
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

    def add(self, item, lazy=True):
        if lazy and item in self:
            return
        item_hashes = [self.hasher(item, seed=i) % self.hash_mod for i in range(len(self.hashing_sets))]
        if lazy and len(self.log_uncommited) == MAX_UNCOMMITED - 1:
            for uncommited_item in self.log_uncommited:
                self.add(uncommited_item, lazy=False)
            self.log_uncommited = []
            # This dump could fail and it would be irrecoverable
            with open(SETS_PATH % self.set_data_path, "wb") as sets_file:
                 pickle.dump((self.hashing_sets, self.bucket_numbers), sets_file)
            self.writeahead_log.write(COMMIT_LINE)
            self.writeahead_log.flush()
            self.writeahead_log.close()
            self.writeahead_log = open(LOGFILE_PATH % self.set_data_path, "w")
        if lazy:
            base64_item = base64.b64encode(item)
            self.writeahead_log.write(base64_item.decode('ascii') + LINE_BREAK)
            self.writeahead_log.flush()
            self.log_uncommited.append(item)
            self.add_lru.append(item)
        else:
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
            for i in range(len(self.hashing_sets)):
                self.hashing_sets[i].update([item_hashes[i]])
            self.add_lru.append(item)