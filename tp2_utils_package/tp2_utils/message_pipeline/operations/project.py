from typing import Dict, List, Callable, Any

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE


class Project(Operation):
    def __init__(self, attributes: List[str]):
        """
        Creates a project operation

        :param attributes: the attributes to keep
        """
        self.attributes = attributes

    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """
        if item == WINDOW_END_MESSAGE:
            return [item]
        for attr in self.attributes:
            if attr not in item:
                raise UnexistentField(attr)
        return [{k: v for k, v in item.items() if k in self.attributes}]
