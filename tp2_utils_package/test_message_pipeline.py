import unittest

from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.filter import Filter
from tp2_utils.message_pipeline.operations.group_aggregates.count import Count
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate
from tp2_utils.message_pipeline.operations.group_aggregates.mean import Mean
from tp2_utils.message_pipeline.operations.group_aggregates.sum import Sum
from tp2_utils.message_pipeline.operations.group_aggregates.value_unique import ValueUnique
from tp2_utils.message_pipeline.operations.group_by import GroupBy
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE, MessagePipeline


class TestDiskMessagePipeline(unittest.TestCase):

    def test_simple_filter(self):
        filter_op = Filter(filter_by="value", keep_cond=lambda x: x % 2 == 0)
        self.assertEqual(filter_op.process({"value": 2}), [{"value": 2}])
        self.assertEqual(filter_op.process({"value": 1}), [])
        self.assertEqual(filter_op.process({"key": "A", "value": 2}), [{"key": "A", "value": 2}])
        self.assertEqual(filter_op.process({"key": "A", "value": 1}), [])

    def test_filter_unexistent_field(self):
        filter_op = Filter(filter_by="value", keep_cond=lambda x: x % 2 == 0)
        with self.assertRaises(UnexistentField):
            filter_op.process({"key": "A"})

    def test_filter_end(self):
        filter_op = Filter(filter_by="value", keep_cond=lambda x: False)
        self.assertEqual(filter_op.process(WINDOW_END_MESSAGE), [WINDOW_END_MESSAGE])

    def test_simple_groupby_count(self):
        group_op = GroupBy(group_by='key', aggregates=[Count()])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 3}), [])
        self.assertEqual(group_op.process({"key": "B", "value": 0}), [])
        self.assertEqual(group_op.process(WINDOW_END_MESSAGE), [{"key": "A", "count": 3},
                                                                {"key": "B", "count": 1},
                                                                WINDOW_END_MESSAGE])

    def test_groupby_unexistent_key(self):
        group_op = GroupBy(group_by='key', aggregates=[Count()])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        with self.assertRaises(UnexistentField):
            group_op.process({"value": 2})

    def test_simple_groupby_sum(self):
        group_op = GroupBy(group_by='key', aggregates=[Sum("value")])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 3}), [])
        self.assertEqual(group_op.process({"key": "B", "value": 0}), [])
        self.assertEqual(group_op.process(WINDOW_END_MESSAGE), [{"key": "A", "value_sum": 7},
                                                                {"key": "B", "value_sum": 0},
                                                                WINDOW_END_MESSAGE])

    def test_simple_groupby_sum_unexistent_value(self):
        group_op = GroupBy(group_by='key', aggregates=[Sum("value2")])
        with self.assertRaises(KeyError):
            group_op.process({"key": "A", "value": 2})

    def test_simple_groupby_mean(self):
        group_op = GroupBy(group_by='key', aggregates=[Mean("value")])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 3}), [])
        self.assertEqual(group_op.process({"key": "B", "value": 0}), [])
        self.assertEqual(group_op.process(WINDOW_END_MESSAGE), [{"key": "A", "value_mean": 7 / 3},
                                                                {"key": "B", "value_mean": 0.0},
                                                                WINDOW_END_MESSAGE])

    def test_simple_groupby_mean_unexistent_value(self):
        group_op = GroupBy(group_by='key', aggregates=[Mean("value2")])
        with self.assertRaises(KeyError):
            group_op.process({"key": "A", "value": 2})

    def test_simple_groupby_value_unique(self):
        group_op = GroupBy(group_by='key', aggregates=[ValueUnique("value")])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(group_op.process({"key": "A", "value": 3}), [])
        self.assertEqual(group_op.process({"key": "B", "value": 0}), [])
        self.assertEqual(group_op.process({"key": "B", "value": 0}), [])
        self.assertEqual(group_op.process({"key": "C", "value": 1}), [])
        self.assertEqual(group_op.process(WINDOW_END_MESSAGE), [{"key": "A", "value_is_unique": False},
                                                                {"key": "B", "value_is_unique": True},
                                                                {"key": "C", "value_is_unique": True},
                                                                WINDOW_END_MESSAGE])

    def test_simple_groupby_value_unique_unexistent_value(self):
        group_op = GroupBy(group_by='key', aggregates=[Mean("value2")])
        with self.assertRaises(KeyError):
            group_op.process({"key": "A", "value": 2})

    def test_complex_pipeline(self):
        pipe = MessagePipeline([Operation.factory("Filter", "key", lambda x: x != "Z"),
                                Operation.factory("GroupBy", group_by="key", aggregates=[
                                    GroupAggregate.factory("Count"),
                                    GroupAggregate.factory("Sum", "value"),
                                    GroupAggregate.factory("ValueUnique", "comment"),
                                    GroupAggregate.factory("Mean", "time"),
                                ]),
                                Operation.factory("Filter", "value_sum", lambda x: x > 5)])
        self.assertEqual(pipe.process({"key": "A", "value": 2, "comment": "test", "time": 0.2}), [])
        self.assertEqual(pipe.process({"key": "Z", "value": 2, "comment": "test", "time": 0.2}), [])
        self.assertEqual(pipe.process({"key": "A", "value": 1, "comment": "test", "time": -0.2}), [])
        self.assertEqual(pipe.process({"key": "Z", "value": 1, "comment": "test", "time": -0.2}), [])
        self.assertEqual(pipe.process({"key": "A", "value": 0, "comment": "test", "time": 0.1}), [])
        self.assertEqual(pipe.process({"key": "Z", "value": 0, "comment": "test", "time": 0.1}), [])
        self.assertEqual(pipe.process({"key": "A", "value": 0, "comment": "test", "time": -0.1}), [])
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test", "time": 0.0}), [])
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test", "time": 0.0}), [])
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test2", "time": 0.5}), [])
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test3", "time": 0.5}), [])
        self.assertEqual(pipe.process({"key": "C", "value": 7, "comment": "test", "time": 0.0}), [])
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE),
                         [{"key": "B", "count": 4,"value_sum": 8, "comment_is_unique": False, "time_mean": 0.25},
                          {"key": "C", "count": 1,"value_sum": 7, "comment_is_unique": True, "time_mean": 0.0},
                          WINDOW_END_MESSAGE])
