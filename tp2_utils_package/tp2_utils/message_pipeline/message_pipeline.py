from typing import Dict, List, Tuple

from tp2_utils.rabbit_utils.rabbit_consumer_producer import BroadcastMessage

WINDOW_END_MESSAGE = {}


class MessagePipeline:
    def __init__(self, operations: List['Operation'],
                 ends_to_receive: int = 1,
                 ends_to_send: int = 1):
        self.operations = operations
        self.ends_to_receive = ends_to_receive
        self.ends_to_send = ends_to_send
        self.ends_received = 0

    def _change_end_to_broadcast(self, responses: List[Dict]) -> List:
        return [BroadcastMessage(item=r) if r == WINDOW_END_MESSAGE else r for r in responses]

    def process(self, item: Dict) -> Tuple[List, bool]:
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
        if self.ends_received >= self.ends_to_receive:
            items_to_process += [WINDOW_END_MESSAGE] * (self.ends_to_send - 1)
            return self._change_end_to_broadcast(items_to_process), True
        return self._change_end_to_broadcast(items_to_process), False
