import heapq
from typing import Dict, List

from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.operation import Operation


class TopN(Operation):
    def __init__(self, top_key: str, value_name: str, n: int):
        """
        Creates a top n operation

        :param top_key: the key to keep along with the max value
        :param value_name: the value to look for top
        """
        self.top_key = top_key
        self.value_name = value_name
        self.heap = []
        self.n = n

    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """
        if item != WINDOW_END_MESSAGE:
            if self.top_key not in item:
                raise UnexistentField(self.top_key)
            if self.value_name not in item:
                raise UnexistentField(self.value_name)
            heapq.heappush(self.heap, (item[self.value_name], item[self.top_key]))
            return []
        else:
            response = []
            for v, k in heapq.nlargest(self.n, self.heap):
                response.append({self.top_key: k, self.value_name: v})
            response = sorted(response, key=lambda x: x[self.value_name], reverse=True)
            response.append(WINDOW_END_MESSAGE)
            self.heap = []
            return response
