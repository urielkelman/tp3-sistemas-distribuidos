from typing import Dict, NoReturn, Any

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate


class ValueUnique(GroupAggregate):
    def __init__(self, value_name: str, unique_suffix: str = '_is_unique'):
        """

        :param value_name: the value to see if its unique
        :param unique_prefix: the suffix to add to the output key
        """
        self.unique_dict = {}
        self.value_name = value_name
        self.unique_suffix = unique_suffix

    def add(self, key: str, values: Dict) -> NoReturn:
        """
        Adds an element to the group statistic

        :param key: the key of the element
        :param values: the values associated
        """
        if self.value_name not in values:
            raise UnexistentField(self.value_name)
        if key not in self.unique_dict:
            self.unique_dict[key] = (True, values[self.value_name])
        else:
            if self.unique_dict[key][0] and self.unique_dict[key][1] == values[self.value_name]:
                return
            else:
                self.unique_dict[key] = (False, values[self.value_name])

    def dump(self) -> Dict[Any, Dict]:
        """
        Dumps all the statistics to a dict
        :return: the dict with the group value as key and a dict of statistics
        """
        result = {k: {self.value_name + self.unique_suffix: v[0]} for k, v in self.unique_dict.items()}
        self.unique_dict = {}
        return result
