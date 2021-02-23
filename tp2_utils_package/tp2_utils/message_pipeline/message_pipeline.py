from typing import Dict, List, Tuple, Optional
from tp2_utils.message_pipeline.message_set.message_set import MessageSet
from tp2_utils.rabbit_utils.rabbit_consumer_producer import BroadcastMessage

WINDOW_END_MESSAGE = {}


class MessagePipeline:
    def __init__(self, operations: List['Operation'],
                 idempotency_set: Optional[MessageSet] = None,
                 ends_to_receive: int = 1,
                 ends_to_send: int = 1,
                 stop_at_window_end: bool = False):
        """

        :param operations: the operations to use in the pipeline
        :param idempotency_set: an object of type MessageSet to handle the arrival
        :param ends_to_receive: the ends to receive for consider that the stream ended
        :param ends_to_send: the ends to send when stream ends
        :param stop_at_window_end: if stop the consumer or not when the stream ends
        """
        self.operations = operations
        self.idempotency_set = idempotency_set
        self.ends_to_receive = ends_to_receive
        self.ends_to_send = ends_to_send
        self.ends_received = 0
        self.stop_at_window_end = stop_at_window_end

    def _change_end_to_broadcast(self, responses: List[Dict]) -> List:
        return [BroadcastMessage(item=r) if r == WINDOW_END_MESSAGE else r for r in responses]

    def process(self, item: Dict) -> Tuple[List, bool]:
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
            self.idempotency_set.add(item)
        if self.ends_received == self.ends_to_receive:
            items_to_process += [WINDOW_END_MESSAGE] * (self.ends_to_send - 1)
            if not self.stop_at_window_end:
                self.ends_to_receive = self.ends_received
                self.ends_received = 0
            if self.idempotency_set:
                self.idempotency_set.flush()
            return self._change_end_to_broadcast(items_to_process), self.stop_at_window_end
        return self._change_end_to_broadcast(items_to_process), False
