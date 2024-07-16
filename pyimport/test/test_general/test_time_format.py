import os
import unittest
from csv import DictReader

import pymongo

from pyimport.argparser import ArgMgr
from pyimport.importcommand import ImportCommand


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._db = self._client["TEST_FORMAT"]
        self._collection = self._db["format"]
        self._dir = os.path.dirname(os.path.realpath(__file__))
        self._args = ArgMgr.default_args()
        self._args.add_arguments(host="mongodb://localhost:27017", database="TEST_FORMAT", collection="format")

    def tearDown(self):
        self._client.drop_database(self._db)

    def test_data_format(self):

        # MOT delimiter=| fieldfile=mot_time_format_test.tff filenames=mot_time_format_test.txt
        args = ArgMgr.default_args().add_arguments(filenames=["mot_time_format_test.txt"])
        fc = ImportCommand.prep_field_file(args.ns)
        fmt = fc.format_value("test_date")
        self.assertEqual(fmt, "%Y-%m-%d")
        self.assertTrue(fc)

        data = {}
        with open("mot_time_format_test.txt") as csvfile:
            reader = DictReader(csvfile, fieldnames=fc.fields())
            count = 0
            for i in reader:
                count = count + 1
                data[count] = i
                if count > 10:
                    break

        projection = {}
        for i in fc.fields():
            projection[i] = 1

        # print(projection)
        first_rec = self._collection.find_one({"locator.n": 1}, projection)

        # print( first_rec)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
