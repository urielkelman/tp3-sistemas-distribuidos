import os
import shutil
import unittest

from tp2_utils.message_pipeline.message_set.disk_message_set_by_commit import DiskMessageSetByLastCommit


class TestDiskMessageSetByLastCommit(unittest.TestCase):

    def setUp(self) -> None:
        shutil.rmtree('/tmp/message_set', ignore_errors=True)
        os.mkdir('/tmp/message_set')
        self.message_set = DiskMessageSetByLastCommit('/tmp/message_set',
                                                      recover_state_on_init=True)

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
        for i in range(100000):
            text = "%d" % i
            self.assertFalse(text.encode() in self.message_set)
            if i > 0:
                self.assertTrue(("%d" % (i - 1)).encode() in self.message_set)
            self.message_set.prepare(text.encode())
            if i > 0:
                self.assertTrue(("%d" % (i - 1)).encode() in self.message_set)
            self.message_set.commit()

    def test_simple_commit_rollback(self):
        self.assertFalse(b"1" in self.message_set)
        self.assertFalse(b"2" in self.message_set)
        self.message_set.prepare(b"1")
        self.message_set.prepare(b"2")
        cm_1 = self.message_set.commit()
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.message_set.prepare(b"3")
        self.message_set.commit()
        self.assertFalse(b"1" in self.message_set)
        self.assertFalse(b"2" in self.message_set)
        self.assertTrue(b"3" in self.message_set)
        self.message_set = DiskMessageSetByLastCommit('/tmp/message_set', commit_number=cm_1,
                                                      recover_state_on_init=True)
        self.assertTrue(b"1" in self.message_set)
        self.assertTrue(b"2" in self.message_set)
        self.assertFalse(b"3" in self.message_set)
