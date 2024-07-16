"""
Created on 8 Aug 2017

@author: jdrumgoole
"""

import os
import shutil
import unittest


import pymongo
import dateutil

from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.filesplitter import LineCounter
from pyimport.logger import Log
from pyimport.importcommand import ImportCommand
from pyimport.argparser import ArgMgr
import pytest

from test.mdbtest import MDBTestDB


def test_generate_fieldfile():
    with MDBTestDB() as tr:
        FieldFile.generate_field_file("inventory.csv", ext="testff")
        assert os.path.exists("inventory.testff")
        start_count = tr.test_col.count_documents({})
        args = tr.args.add_arguments(filenames=["inventory.csv"], fieldfile="inventory.testff", hasheader=True)
        results = ImportCommand(args=args.ns).run()
        result = results.filename_results("inventory.csv")
        line_count = LineCounter.count_now("inventory.csv")
        new_inserted_count = tr.test_col.count_documents({}) - start_count
        assert new_inserted_count == result.total_written
        assert new_inserted_count == (line_count - 1) # header must be subtracted
        os.unlink("inventory.testff")


def test_delimiter_header():
    with MDBTestDB() as tr:
        args = tr.args.add_arguments(filenames=["AandEData_300.csv"], fieldfile="AandE_Data_2011-04-10.tff", hasheader=True)
        results = ImportCommand(args=args.ns).run()
        assert results.total_errors == 1
        assert results.total_results == 0


class TestFieldFile(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestFieldFile, self).__init__(*args, **kwargs)
        Log.add_null_hander()

    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost:27017")
        self._db = self._client["FC_TEST"]
        self._col = self._db["FC_TEST"]
        self._args = ArgMgr.default_args()
        self._args.add_arguments(host="mongodb://localhost:27017", database="FC_TEST", collection="FC_TEST")

    def tearDown(self):
        self._client.drop_database("FC_TEST")

    def test_error(self):
        with self.assertRaises(FieldFileException):
            _ = FieldFile.load("error.tff")

    def test_FieldConfig(self):
        fc = FieldFile.load("test_fieldconfig.tff")
        self.assertEqual(len(fc.fields()), 4)

        self.assertEqual(fc.fields()[0], "Test 1")
        self.assertEqual(fc.fields()[3], "Test 4")

        fc = FieldFile.load("uk_property_prices.tff")
        self.assertEqual(len(fc.fields()), 16)

        self.assertEqual(fc.fields()[0], "txn")
        self.assertEqual(fc.fields()[2], "Date of Transfer")
        self.assertEqual(fc.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["10k.txt"], delimiter="|", hasheader=False)
        results = ImportCommand(args=args.ns).run()
        self.assertEqual(self._col.count_documents({}) - start_count, results.total_written)

    def test_fieldfile_nomatch(self):
        args = self._args.add_arguments(filenames=["inventory.csv"], fieldfile="AandE_Data_2011-04-10.tff")
        results = ImportCommand(args=args.ns).run()
        assert results.total_errors > 0
        assert results.total_results == 0
        assert results.total_written is None

    def test_fieldfile_autogen(self):
        with MDBTestDB() as tr:
            args = tr.args.add_arguments(filenames=["inventory.csv"], hasheader=True)
            results = ImportCommand(args=args.ns).run()
            assert results.total_errors == 0
            assert results.total_results == 1
            assert results.total_written == 4
            assert os.path.exists("inventory.tff")
            os.unlink("inventory.tff")

    def test_new_delimiter_and_timeformat_header(self):
        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["mot_test_set_small.csv"], fieldfile="mot.tff", hasheader=False, delimiter="|")
        results = ImportCommand(args=args.ns).run()
        lines = LineCounter.count_now('mot_test_set_small.csv')
        result = results.filename_results("mot_test_set_small.csv")
        inserted_count = self._col.count_documents({}) - start_count
        self.assertEqual(inserted_count, result.total_written)
        self.assertEqual(inserted_count, lines)



    def test_simple_generate(self):
        gfc = FieldFile.generate_field_file('inventory.csv', ext="xx")
        rfc = FieldFile.load("inventory.xx")
        self.assertTrue("Inventory Item" in rfc.fields())
        self.assertTrue("Amount" in rfc.fields())
        self.assertTrue("Last Order", rfc.fields())
        self.assertEqual(len(rfc.fields()), 3)
        os.unlink("inventory.xx")

    def test_generate_field_filename(self):

        fc = FieldFile.generate_field_file('inventory.csv')
        self.assertTrue(os.path.exists("inventory.tff"))
        os.unlink("inventory.tff")

        fc = FieldFile.generate_field_file('inventory.csv.1')
        self.assertTrue(os.path.exists("inventory.csv.tff"))
        os.unlink("inventory.csv.tff")

        fc = FieldFile.generate_field_file('yellow_tripdata_2015-01-06-200k.csv.1')
        self.assertTrue(os.path.exists("yellow_tripdata_2015-01-06-200k.csv.tff"))
        os.unlink("yellow_tripdata_2015-01-06-200k.csv.tff")

        fc = FieldFile.generate_field_file('yellow_tripdata_2015-01-06-200k.csv.10')
        self.assertTrue(os.path.exists("yellow_tripdata_2015-01-06-200k.csv.tff"))
        os.unlink("yellow_tripdata_2015-01-06-200k.csv.tff")

        fc = FieldFile.generate_field_file('test_results_2016_10.txt.1')
        self.assertTrue(os.path.exists("test_results_2016_10.txt.tff"))
        os.unlink("test_results_2016_10.txt.tff")

    def test_nyc_2018_genfieldfile(self):

        fc = FieldFile.generate_field_file('2018_Yellow_Taxi_Trip_Data_1000.csv', delimiter=";")
        fc_new = FieldFile.load('2018_Yellow_Taxi_Trip_Data_1000.tff')
        self.assertEqual(fc.fields(), fc_new.fields())
        self.assertEqual(list(fc.field_dict.values()), list(fc_new.field_dict.values()))
        os.unlink("2018_Yellow_Taxi_Trip_Data_1000.tff")



    def test_date(self):
        start_count = self._col.count_documents({})
        args= self._args.add_arguments(filenames=["inventory.csv"], fieldfile="inventory_dates.tff", hasheader=True)
        results = ImportCommand(args=args.ns).run()
        result = results.filename_results("inventory.csv")
        lines_count = LineCounter.count_now("inventory.csv") - 1  # header
        end_count = self._col.count_documents({})
        self.assertEqual(end_count - start_count, lines_count)
        self.assertEqual(lines_count, result.total_written)

        nuts_doc = self._col.find_one({"Last Order": dateutil.parser.parse("29-Feb-2016")})
        self.assertTrue(nuts_doc)

    def testFieldDict(self):
        d = FieldFile.load("testresults.tff").field_dict
        self.assertTrue("TestID" in d)
        self.assertTrue("FirstUseDate" in d)
        self.assertTrue("Colour" in d)
        self.assertTrue(d["TestID"]["type"] == "int")

    def test_duplicate_id(self):
        self.assertRaises(ValueError, FieldFile.load, "duplicate_id.tff")

    def test_process_blank_fields(self):
        if os.path.exists("AandE_Data_Blank_Test.tff"):
            os.unlink("AandE_Data_Blank_Test.tff")

        shutil.copy("AandE_Data_2011-04-10.csv", "AandE_Data_Blank_Test.csv")
        fc1 = FieldFile.generate_field_file("AandE_Data_Blank_Test.csv")
        self.assertTrue(os.path.isfile("AandE_Data_Blank_Test.tff"))
        fc2 = FieldFile.load( "AandE_Data_Blank_Test.tff")
        self.assertEqual(len(fc2.fields()), 20)
        self.assertEqual(len(fc1.fields()), len(fc2.fields()))
        self.assertEqual(fc2.fields()[0], "Blank-1")
        self.assertEqual(fc2.fields()[1], "Blank-2")
        self.assertEqual(fc2.fields()[19], "Number of patients spending >12 hours from decision to admit to admission")
        os.unlink("AandE_Data_Blank_Test.tff")
        os.unlink("AandE_Data_Blank_Test.csv")


if __name__ == "__main__":
    unittest.main()
