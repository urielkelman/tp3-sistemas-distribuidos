import os
import random
import shutil
import unittest

from tp2_utils.message_pipeline.message_set.disk_message_set import DiskMessageSet


class TestDiskMessageSet(unittest.TestCase):

    def setUp(self) -> None:
        shutil.rmtree('/tmp/message_set', ignore_errors=True)
        os.mkdir('/tmp/message_set')
        self.message_set = DiskMessageSet('/tmp/message_set',
                                          recover_state_on_init = True)

    def tearDown(self) -> None:
        shutil.rmtree('/tmp/message_set', ignore_errors=True)

    def test_simple_add(self):
        self.assertFalse(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)
        self.message_set.prepare(b"test")
        self.message_set.commit()
        self.assertTrue(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)

    def test_add_twice(self):
        self.assertFalse(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)
        self.message_set.prepare(b"test")
        self.message_set.commit()
        self.assertTrue(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)
        self.message_set.prepare(b"test")
        self.message_set.commit()
        self.assertTrue(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)

    def test_thousands_add(self):
        # worst case for cache
        random.seed(0)
        for i in range(100000):
            text = "%d" % i
            self.assertFalse(text.encode() in self.message_set)
            if random.random() > 0.5:
                self.message_set.prepare(text.encode())
        random.seed(0)
        for i in range(100000):
            text = "%d" % i
            if random.random() > 0.5:
                self.message_set.commit()
                self.assertTrue(text.encode() in self.message_set)
            else:
                self.assertFalse(text.encode() in self.message_set)

    def test_big_add_w_recovery(self):
        random.seed(0)
        for i in range(10000):
            text = "%d" % i
            self.assertFalse(text.encode() in self.message_set)
            if random.random() < 0.05:
                self.message_set.commit()
                self.message_set = DiskMessageSet('/tmp/message_set',
                                                  recover_state_on_init = True)
            if random.random() > 0.5:
                self.message_set.prepare(text.encode())
        random.seed(0)
        for i in range(10000):
            text = "%d" % i
            if random.random() < 0.1:
                self.message_set.commit()
                self.message_set = DiskMessageSet('/tmp/message_set',
                                                  recover_state_on_init = True)
            if random.random() > 0.5:
                self.message_set.commit()
                self.assertTrue(text.encode() in self.message_set)
            else:
                self.message_set.commit()
                self.assertFalse(text.encode() in self.message_set)

    def test_realistic_case(self):
        """
        This test will be performed under the assumption just 1% messages are in the set.
        We want to answer fast when the items are not, we may allow answering slow if the item is in the set.
        """
        random.seed(0)
        for i in range(100000):
            text = "%d" % i
            self.message_set.prepare(text.encode())
            if random.random() < 0.01:
                self.message_set.commit()
                query = "%d" % random.randint(0, i - 1)
                self.assertTrue(query.encode() in self.message_set)
            else:
                query = "%d" % random.randint(i + 1, 100001)
                self.assertFalse(query.encode() in self.message_set)

    def test_simple_commit_no_changes(self):
        self.assertFalse(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)
        self.message_set.prepare(b"test")
        cm_1 = self.message_set.commit()
        self.assertTrue(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)
        self.message_set.prepare(b"test")
        cm_2 = self.message_set.commit()
        self.assertTrue(b"test" in self.message_set)
        self.assertFalse(b"test2" in self.message_set)
        self.assertEqual(cm_1, cm_2)

    def test_simple_commit_rollback(self):
        self.assertFalse(b"1" in self.message_set)
        self.assertFalse(b"2" in self.message_set)
        self.message_set.prepare(b"1")
        self.message_set.prepare(b"2")
        cm_1 = self.message_set.commit()
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.message_set.prepare(b"3")
        cm_2 = self.message_set.commit()
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.assertTrue(b"3" in self.message_set)
        self.message_set = DiskMessageSet('/tmp/message_set',
                                          commit_number=cm_1,
                                          recover_state_on_init = True)
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.assertFalse(b"3" in self.message_set)

    def test_double_commit_rollback(self):
        self.assertFalse(b"1" in self.message_set)
        self.assertFalse(b"2" in self.message_set)
        self.message_set.prepare(b"1")
        self.message_set.prepare(b"2")
        cm_1 = self.message_set.commit()
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.message_set.prepare(b"3")
        self.message_set.commit()
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.assertTrue(b"3" in self.message_set)
        self.message_set.prepare(b"4")
        self.message_set.prepare(b"5")
        self.message_set.commit()
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.assertTrue(b"3" in self.message_set)
        self.assertTrue(b"4" in self.message_set)
        self.assertTrue(b"5" in self.message_set)
        self.message_set = DiskMessageSet('/tmp/message_set',
                                          commit_number=cm_1,
                                          recover_state_on_init = True)
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.assertFalse(b"3" in self.message_set)
        self.assertFalse(b"4" in self.message_set)
        self.assertFalse(b"5" in self.message_set)
