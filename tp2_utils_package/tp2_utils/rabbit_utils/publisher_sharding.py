from typing import List
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage
import pyhash

class PublisherSharding:
    def __init__(self, by_key: str, shards: int):
        self.by_key = by_key
        self.shards = shards
        self.hasher = pyhash.murmur3_32()

    def get_shards(self, item) -> List[int]:
        if isinstance(item, BroadcastMessage):
            return list(range(self.shards))
        return [self.hasher(item[self.by_key]) % self.shards]

    def get_possible_shards(self) -> List[int]:
        return list(range(self.shards))
