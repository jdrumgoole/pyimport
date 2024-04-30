"""
Created on 7 Aug 2017

@author: jdrumgoole
"""

import os
import unittest

import pymongo
import pymongo.errors

from pyimport.fileprocessor import FileProcessor
from pyimport.filesplitter import LineCounter

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost:27017")
        self._database = self._client["TEST_FP"]
        self._col = self._database["test_fp"]

    def tearDown(self):
        self._client.drop_database(self._database)

    def test_fileprocessor(self):
        fp = FileProcessor(self._col, ",")

    def test_property_prices(self):

        start_count = self._col.count_documents({})
        fp = FileProcessor(self._col, ',')
        try:
            fp.processOneFile(f("data/uk_property_prices.csv"))
        except pymongo.errors.BulkWriteError as e:
            print(e)
            raise;
        lines = LineCounter(f("data/uk_property_prices.csv")).line_count
        self.assertEqual(lines, self._col.count_documents({}) - start_count)

        self.assertTrue(self._col.find_one({"Postcode": "NG10 5NN"}))

    def test_mot_data(self):

        col = self._database["mot"]
        start_count = col.count_documents({})
        fp = FileProcessor(col, '|')
        fp.processOneFile(f("data/10k.txt"))
        lines = LineCounter(f("data/10k.txt")).line_count
        self.assertEqual(lines, col.count_documents({}) - start_count)
        self.assertTrue(col.find_one({"test_id": 114624}))

    def test_date_format(self):

        col = self._database["mot"]
        start_count = col.count_documents({})
        fp = FileProcessor(col, delimiter='|')
        fp.processOneFile(f("data/mot_time_format_test.txt"))
        lines = LineCounter(f("data/mot_time_format_test.txt")).line_count
        self.assertEqual(lines, col.count_documents({}) - start_count)
        self.assertTrue(col.find_one({"test_id": 1077}))

    def test_A_and_E_data(self):

        col = self._database["AandE"]
        start_count = col.count_documents({})
        fp = FileProcessor(col, ',', onerror="ignore")
        fp.processOneFile(input_filename=f("data/AandE_Data_2011-04-10.csv"), hasheader=True)
        lines = LineCounter(f("data/AandE_Data_2011-04-10.csv")).line_count
        self.assertEqual(lines, col.count_documents({}) - start_count + 1)
        self.assertTrue(col.find_one({"Code": "RA4"}))

    def test_gdelt_data(self):
        col = self._database["GDELT"]
        start_count = col.count_documents({})
        fp = FileProcessor(col, onerror="ignore", delimiter="tab")
        fp.processOneFile(input_filename=f("data/gdelt.tsv"),
                          hasheader=False,
                          field_filename=f("data/GDELT_columns.tff"))
        lines = LineCounter(f("data/gdelt.tsv")).line_count
        self.assertEqual(lines, col.count_documents({}) - start_count)
        self.assertTrue(col.find_one(
            {"SOURCEURL": "https://www.standardspeaker.com/news/dream-factory-director-retiring-1.2467094"}))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_fileprocessor']
    unittest.main()
