from typing import Dict, NoReturn, Any

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate


class Sum(GroupAggregate):
    def __init__(self, sum_value: str, sum_suffix: str = '_sum'):
        """

        :param sum_value: the value to sum
        :param sum_suffix: the suffix to add to the output key
        """
        self.sum_by_key = {}
        self.sum_value = sum_value
        self.sum_suffix = sum_suffix

    def add(self, key: str, values: Dict) -> NoReturn:
        """
        Adds an element to the group statistic

        :param key: the key of the element
        :param values: the values associated
        """
        if self.sum_value not in values:
            raise UnexistentField(self.sum_value)
        if key not in self.sum_by_key:
            self.sum_by_key[key] = values[self.sum_value]
        else:
            self.sum_by_key[key] += values[self.sum_value]

    def dump(self) -> Dict[Any, Dict]:
        """
        Dumps all the statistics to a dict
        :return: the dict with the group value as key and a dict of statistics
        """
        result = {k: {self.sum_value + self.sum_suffix: v} for k, v in self.sum_by_key.items()}
        self.sum_by_key = {}
        return result
