from typing import Dict, List, Callable, Any

from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.operation import Operation


class Filter(Operation):
    def __init__(self, filter_by: str,
                 keep_cond: Callable[[Any], bool]):
        """
        Creates a filter operation

        :param filter_by: the field to use for filtering
        :param keep_cond: the function of the condition that must hold to keep the message
        """
        self.filter_by = filter_by
        self.keep_cond = keep_cond

    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """
        if item == WINDOW_END_MESSAGE:
            return [item]
        if self.filter_by not in item:
            raise UnexistentField(self.filter_by)
        if self.keep_cond(item[self.filter_by]):
            return [item]
        return []
