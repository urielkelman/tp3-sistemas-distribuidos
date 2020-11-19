from typing import Dict, List

from tp2_utils.message_pipeline.operations.operation import Operation

WINDOW_END_MESSAGE = {}


class MessagePipeline:
    def __init__(self, operations: List[Operation]):
        self.operations = operations

    def process(self, item: Dict) -> List[Dict]:
        items_to_process = [item]
        for op in self.operations:
            new_items_to_process = []
            for item in items_to_process:
                new_items_to_process += op.process(item)
            items_to_process = new_items_to_process
        return items_to_process
