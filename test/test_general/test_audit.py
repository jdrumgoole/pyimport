"""
Created on 13-Apr-2018

@author: jdrumgoole
"""
import unittest

import pymongo
from dateutil.parser import parse

from pyimport.audit import Audit


class Test_Audit(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost/TEST_AUDIT")
        self._database = self._client["TEST_AUDIT"]
        self._audit = Audit(self._database)

    def tearDown(self):
        self._client.drop_database("TEST_AUDIT")

    # @unittest.skip
    def test_get_current_batch_id(self):
        self.assertFalse(self._audit.in_batch())

        batch_id = self._audit.start_batch(doc={"test": "doc"})
        self.assertTrue(self._audit.in_batch())
        self._audit.end_batch(batch_id)

        self.assertTrue(self._audit.get_batch(batch_id))

        self.assertFalse(self._audit.in_batch())
        self.assertEqual(batch_id, self._audit.get_last_valid_batch_id())

    def test_get_valid_batches(self):
        id1 = self._audit.start_batch(doc={"test": "doc"})
        id2 = self._audit.start_batch(doc={"test": "doc"})

        self.assertTrue(self._audit.in_batch())
        self._audit.end_batch(id2)
        self.assertTrue(self._audit.in_batch())
        self._audit.end_batch(id1)
        batch = self._audit.get_batch_end(id1)
        self.assertGreaterEqual(batch['end'], parse("1-Jun-2017", ))
        self.assertFalse(self._audit.in_batch())

        idlist = list(self._audit.get_valid_batch_ids())
        self.assertTrue(id1 in idlist)
        self.assertTrue(id2 in idlist)

    def test_get_last_batch_id(self):
        id1 = self._audit.start_batch(doc={"test": "doc"})
        id2 = self._audit.start_batch(doc={"test": "doc"})
        self.assertEqual(2, self._audit.get_last_batch_id())
        self._audit.end_batch(id2)
        self.assertEqual(2, self._audit.get_last_batch_id())
        self._audit.end_batch(id1)

        id1 = self._audit.start_batch(doc={"test": "doc"})
        self.assertEqual(3, self._audit.get_last_batch_id())
        self._audit.end_batch(id1)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
