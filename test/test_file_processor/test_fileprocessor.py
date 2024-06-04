"""
Created on 7 Aug 2017

@author: jdrumgoole
"""

import os
import unittest

import pymongo
import pymongo.errors

from pyimport.argparser import ArgMgr
from pyimport.csvreader import CSVReader
from pyimport.filesplitter import LineCounter
from pyimport.importcommand import ImportCommand


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost:27017")
        self._database = self._client["TEST_FP"]
        self._col = self._database["test_fp"]
        self._args = ArgMgr.default_args()
        self._args.add_arguments(host="mongodb://localhost:27017", database="TEST_FP", collection="test_fp")

    def tearDown(self):
        self._client.drop_database(self._database)

    def test_sniff(self):
        self.assertFalse(CSVReader.sniff_header("uk_property_prices.csv"))
        self.assertFalse(CSVReader.sniff_header("10k.txt"))
        self.assertTrue(CSVReader.sniff_header("AandE_Data_2011-04-10.csv"))
        self.assertFalse(CSVReader.sniff_header("gdelt.tsv"))

    def test_property_prices(self):

        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["uk_property_prices.csv"], delimiter=",", hasheader=True)
        ImportCommand(args=args.ns).run(args.ns)
        lines = LineCounter("uk_property_prices.csv").line_count - 1
        self.assertEqual(lines, self._col.count_documents({}) - start_count)
        self.assertTrue(self._col.find_one({"Postcode": "NG10 5NN"}))

    def test_mot_data(self):

        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["10k.txt"], delimiter="|", hasheader=True)
        ImportCommand(args=args.ns).run(args.ns)
        lines = LineCounter("10k.txt").line_count - 1
        self.assertEqual(lines, self._col.count_documents({}) - start_count)
        self.assertTrue(self._col.find_one({"test_id": 114624}))

    def test_date_format(self):

        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["mot_time_format_test.txt"], delimiter="|", hasheader=True)
        ImportCommand(args=args.ns).run(args.ns)
        lines = LineCounter("mot_time_format_test.txt").line_count - 1
        self.assertEqual(lines, self._col.count_documents({}) - start_count)
        self.assertTrue(self._col.find_one({"test_id": 1077}))

    def test_A_and_E_data(self):

        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["AandE_Data_2011-04-10.csv"], delimiter=",", hasheader=True)
        ImportCommand(args=args.ns).run(args.ns)
        lines = LineCounter("AandE_Data_2011-04-10.csv").line_count - 1
        self.assertEqual(lines, self._col.count_documents({}) - start_count)
        self.assertTrue(self._col.find_one({"Code": "RA4"}))

    def test_gdelt_data(self):
        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["gdelt.tsv"], fieldfile="GDELT_columns.tff", delimiter="tab", hasheader=False)
        ImportCommand(args=args.ns).run(args.ns)
        lines = LineCounter("gdelt.tsv").line_count
        self.assertEqual(lines, self._col.count_documents({}) - start_count)
        self.assertTrue(self._col.find_one(
            {"SOURCEURL": "https://www.standardspeaker.com/news/dream-factory-director-retiring-1.2467094"}))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_fileprocessor']
    unittest.main()
