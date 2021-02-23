from typing import Dict, NoReturn, Any

from tp2_utils.message_pipeline.operations.group_aggregates.count import Count
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate
from tp2_utils.message_pipeline.operations.group_aggregates.sum import Sum


class Mean(GroupAggregate):
    def __init__(self, mean_value: str, mean_suffix: str = '_mean'):
        """

        :param mean_value: the value to calculate the mean
        :param mean_suffix: the suffix to add to the output key
        """
        self.mean_value = mean_value
        self.mean_suffix = mean_suffix
        self.count = Count()
        self.sum = Sum(mean_value)

    def add(self, key: str, values: Dict) -> NoReturn:
        """
        Adds an element to the group statistic

        :param key: the key of the element
        :param values: the values associated
        """
        self.count.add(key, values)
        self.sum.add(key, values)

    def dump(self) -> Dict[Any, Dict]:
        """
        Dumps all the statistics to a dict
        :return: the dict with the group value as key and a dict of statistics
        """
        counts = self.count.dump()
        sums = self.sum.dump()
        result = {k: {self.mean_value + self.mean_suffix: v[self.mean_value + '_sum'] / counts[k]['count']}
                for k, v in sums.items()}
        self.count = Count()
        self.sum = Sum(self.mean_value)
        return result
