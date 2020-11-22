from typing import Dict, List, Callable, Any

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate


class GroupBy(Operation):
    def __init__(self, group_by: str, aggregates: List[GroupAggregate]):
        """
        Creates a filter operation

        :param group_by: the field to use for grouping
        :param aggregates: the aggregates to compute
        """
        self.group_by = group_by
        self.aggregates = aggregates

    def process(self, item: Dict) -> List[Dict]:
        """
        Processes an item

        :param item: The dict item to process
        :return: A list with the results
        """
        if item != WINDOW_END_MESSAGE:
            if self.group_by not in item:
                raise UnexistentField(self.group_by)
            for agg in self.aggregates:
                agg.add(item[self.group_by], item)
            return []
        else:
            results = {}
            for agg in self.aggregates:
                for k, v in agg.dump().items():
                    if k not in results:
                        results[k] = v
                    else:
                        results[k].update(v)
            response = []
            for k, v in results.items():
                v[self.group_by] = k
                response.append(v)
            response.append(WINDOW_END_MESSAGE)
            return response
