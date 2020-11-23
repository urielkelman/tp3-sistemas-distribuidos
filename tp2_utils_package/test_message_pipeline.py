import unittest

from tp2_utils.message_pipeline.message_pipeline import WINDOW_END_MESSAGE, MessagePipeline
from tp2_utils.message_pipeline.operations.exceptions.unexistent_field import UnexistentField
from tp2_utils.message_pipeline.operations.filter import Filter
from tp2_utils.message_pipeline.operations.group_aggregates.count import Count
from tp2_utils.message_pipeline.operations.group_aggregates.group_aggregate import GroupAggregate
from tp2_utils.message_pipeline.operations.group_aggregates.mean import Mean
from tp2_utils.message_pipeline.operations.group_aggregates.sum import Sum
from tp2_utils.message_pipeline.operations.group_aggregates.value_unique import ValueUnique
from tp2_utils.message_pipeline.operations.group_by import GroupBy
from tp2_utils.message_pipeline.operations.operation import Operation
from tp2_utils.message_pipeline.operations.project import Project
from tp2_utils.message_pipeline.operations.rename import Rename
from tp2_utils.message_pipeline.operations.top_n import TopN
from tp2_utils.message_pipeline.operations.transform import Transform
from tp2_utils.rabbit_utils.special_messages import BroadcastMessage


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

    def test_simple_transform(self):
        transform_op = Transform(attribute_name="value", transform_func=lambda x: x * 2)
        self.assertEqual(transform_op.process({"value": 2}), [{"value": 4}])
        self.assertEqual(transform_op.process({"value": 1}), [{"value": 2}])
        self.assertEqual(transform_op.process({"key": "A", "value": 2}), [{"key": "A", "value": 4}])
        self.assertEqual(transform_op.process({"key": "A", "value": 0}), [{"key": "A", "value": 0}])

    def test_transform_unexistent_field(self):
        transform_op = Transform(attribute_name="value", transform_func=lambda x: x * 2)
        with self.assertRaises(UnexistentField):
            transform_op.process({"key": "A"})

    def test_transform_end(self):
        transform_op = Filter(filter_by="value", keep_cond=lambda x: False)
        self.assertEqual(transform_op.process(WINDOW_END_MESSAGE), [WINDOW_END_MESSAGE])

    def test_simple_project(self):
        project_op = Project(["text", "value"])
        self.assertEqual(project_op.process({"value": 2, "text": "asd"}), [{"value": 2, "text": "asd"}])
        self.assertEqual(project_op.process({"key": "A", "value": 0, "text": "asd"}),
                         [{"value": 0, "text": "asd"}])

    def test_project_unexistent_field(self):
        project_op = Project(["text", "value"])
        with self.assertRaises(UnexistentField):
            project_op.process({"key": "A", "value": 2})

    def test_project_end(self):
        project_op = Project(["text", "value"])
        self.assertEqual(project_op.process(WINDOW_END_MESSAGE), [WINDOW_END_MESSAGE])

    def test_simple_rename(self):
        rename_op = Rename({"text": "comment", "value": "rating"})
        self.assertEqual(rename_op.process({"value": 2, "text": "asd"}), [{"rating": 2, "comment": "asd"}])
        self.assertEqual(rename_op.process({"key": "A", "value": 0, "text": "asd"}),
                         [{"key": "A", "rating": 0, "comment": "asd"}])

    def test_rename_unexistent_field(self):
        rename_op = Rename({"text": "comment", "value": "rating"})
        with self.assertRaises(UnexistentField):
            rename_op.process({"key": "A", "value": 2})

    def test_rename_end(self):
        rename_op = Rename({"text": "comment", "value": "rating"})
        self.assertEqual(rename_op.process(WINDOW_END_MESSAGE), [WINDOW_END_MESSAGE])

    def test_simple_topn(self):
        top_op = TopN(top_key='key', value_name='value', n=3)
        self.assertEqual(top_op.process({"key": "A", "value": 2}), [])
        self.assertEqual(top_op.process({"key": "D", "value": 0}), [])
        self.assertEqual(top_op.process({"key": "B", "value": 4}), [])
        self.assertEqual(top_op.process({"key": "C", "value": 3}), [])
        self.assertEqual(top_op.process(WINDOW_END_MESSAGE), [{"key": "B", "value": 4},
                                                              {"key": "C", "value": 3},
                                                              {"key": "A", "value": 2},
                                                              WINDOW_END_MESSAGE])

    def test_topn_unexistent_key(self):
        top_op = TopN(top_key='key', value_name='value', n=3)
        self.assertEqual(top_op.process({"key": "A", "value": 2}), [])
        with self.assertRaises(UnexistentField):
            top_op.process({"value": 2})
        with self.assertRaises(UnexistentField):
            top_op.process({"key": "A"})

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
        self.assertEqual(pipe.process({"key": "A", "value": 2, "comment": "test", "time": 0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "Z", "value": 2, "comment": "test", "time": 0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "A", "value": 1, "comment": "test", "time": -0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "Z", "value": 1, "comment": "test", "time": -0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "A", "value": 0, "comment": "test", "time": 0.1}), ([], False))
        self.assertEqual(pipe.process({"key": "Z", "value": 0, "comment": "test", "time": 0.1}), ([], False))
        self.assertEqual(pipe.process({"key": "A", "value": 0, "comment": "test", "time": -0.1}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test", "time": 0.0}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test", "time": 0.0}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test2", "time": 0.5}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test3", "time": 0.5}), ([], False))
        self.assertEqual(pipe.process({"key": "C", "value": 7, "comment": "test", "time": 0.0}), ([], False))
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE),
                         ([{"key": "B", "count": 4, "value_sum": 8, "comment_is_unique": False, "time_mean": 0.25},
                           {"key": "C", "count": 1, "value_sum": 7, "comment_is_unique": True, "time_mean": 0.0},
                           BroadcastMessage(WINDOW_END_MESSAGE)], True))

    def test_complex_pipeline_multiple_ends(self):
        pipe = MessagePipeline([Operation.factory("Filter", "key", lambda x: x != "Z"),
                                Operation.factory("GroupBy", group_by="key", aggregates=[
                                    GroupAggregate.factory("Count"),
                                    GroupAggregate.factory("Sum", "value"),
                                    GroupAggregate.factory("ValueUnique", "comment"),
                                    GroupAggregate.factory("Mean", "time"),
                                ]),
                                Operation.factory("Filter", "value_sum", lambda x: x > 5)],
                               ends_to_receive=2, ends_to_send=2)
        self.assertEqual(pipe.process({"key": "A", "value": 2, "comment": "test", "time": 0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "Z", "value": 2, "comment": "test", "time": 0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "A", "value": 1, "comment": "test", "time": -0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "Z", "value": 1, "comment": "test", "time": -0.2}), ([], False))
        self.assertEqual(pipe.process({"key": "A", "value": 0, "comment": "test", "time": 0.1}), ([], False))
        self.assertEqual(pipe.process({"key": "Z", "value": 0, "comment": "test", "time": 0.1}), ([], False))
        self.assertEqual(pipe.process({"key": "A", "value": 0, "comment": "test", "time": -0.1}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test", "time": 0.0}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test", "time": 0.0}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test2", "time": 0.5}), ([], False))
        self.assertEqual(pipe.process({"key": "B", "value": 2, "comment": "test3", "time": 0.5}), ([], False))
        self.assertEqual(pipe.process({"key": "C", "value": 7, "comment": "test", "time": 0.0}), ([], False))
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE), ([], False))
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE),
                         ([{"key": "B", "count": 4, "value_sum": 8, "comment_is_unique": False, "time_mean": 0.25},
                           {"key": "C", "count": 1, "value_sum": 7, "comment_is_unique": True, "time_mean": 0.0},
                           BroadcastMessage(WINDOW_END_MESSAGE), BroadcastMessage(WINDOW_END_MESSAGE)], True))

    def test_pipeline_ends(self):
        pipe = MessagePipeline([],
                               ends_to_receive=3, ends_to_send=1)
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE), ([], False))
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE), ([], False))
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE),
                         ([BroadcastMessage(WINDOW_END_MESSAGE)], True))
        pipe = MessagePipeline([],
                               ends_to_receive=1, ends_to_send=3)
        self.assertEqual(pipe.process(WINDOW_END_MESSAGE),
                         ([BroadcastMessage(WINDOW_END_MESSAGE),
                           BroadcastMessage(WINDOW_END_MESSAGE),
                           BroadcastMessage(WINDOW_END_MESSAGE)], True))
