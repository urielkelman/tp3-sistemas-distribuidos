from typing import Dict, List, Callable, Any

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE


class Transform(Operation):
    def __init__(self, attribute_name: str,
                 transform_func: Callable[[Any], Any]):
        """
        Creates a transform operation

        :param attribute_name: the attribute to transform
        :param transform_func: the function used to transform
        """
        self.attribute_name = attribute_name
        self.transform_func = transform_func

    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """
        if item == WINDOW_END_MESSAGE:
            return [item]
        if self.attribute_name not in item:
            raise UnexistentField(self.attribute_name)
        item[self.attribute_name] = self.transform_func(item[self.attribute_name])
        return [item]
