from typing import Dict, List, Callable, Any

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE


class Rename(Operation):
    def __init__(self, attr_renames: Dict[str, str]):
        """
        Creates a rename operation

        :param attr_renames: the dict for renaming
        """
        self.attr_renames = attr_renames

    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """
        if item == WINDOW_END_MESSAGE:
            return [item]
        for attr in self.attr_renames.keys():
            if attr not in item:
                raise UnexistentField(attr)
        new_item = {}
        for k, v in item.items():
            if k in self.attr_renames:
                new_item[self.attr_renames[k]] = v
            else:
                new_item[k] = v
        return [new_item]
