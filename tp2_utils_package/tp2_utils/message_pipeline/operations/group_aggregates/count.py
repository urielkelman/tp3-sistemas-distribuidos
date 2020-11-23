from typing import Dict, NoReturn, Any

from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate


class Count(GroupAggregate):
    def __init__(self, count_name: str = 'count'):
        """

        :param count_name: the name of the output key
        """
        self.count_by_key = {}
        self.count_name = count_name

    def add(self, key: str, values: Dict) -> NoReturn:
        """
        Adds an element to the group statistic

        :param key: the key of the element
        :param values: the values associated
        """
        if key not in self.count_by_key:
            self.count_by_key[key] = 1
        else:
            self.count_by_key[key] += 1

    def dump(self) -> Dict[Any, Dict]:
        """
        Dumps all the statistics to a dict
        :return: the dict with the group value as key and a dict of statistics
        """
        return {k: {self.count_name: v} for k, v in self.count_by_key.items()}
