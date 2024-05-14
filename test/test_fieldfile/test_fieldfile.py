"""
Created on 8 Aug 2017

@author: jdrumgoole
"""
import os
import unittest
from typing import Dict
from datetime import datetime

import pymongo
import dateutil

from pyimport.fieldfile import FieldFile
from pyimport.filewriter import FileWriter
from pyimport.filereader import FileReader
from pyimport.filesplitter import LineCounter
from pyimport.logger import Logger
from pyimport.type_converter import Converter
from pyimport.csvlinetodictparser import CSVLineToDictParser
#
# path_dir = os.path.dirname(os.path.realpath(__file__))

class Test(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(Test, self).__init__(*args, **kwargs)
        Logger.add_null_hander()

    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost:27017")
        self._db = self._client["FC_TEST"]
        self._col = self._db["FC_TEST"]

    def tearDown(self):
        self._db.drop_collection("FC_TEST")

    def test_FieldConfig(self):
        fc = FieldFile("test_fieldconfig.tff")
        self.assertEqual(len(fc.fields()), 4)

        self.assertEqual(fc.fields()[0], "Test 1")
        self.assertEqual(fc.fields()[3], "Test 4")

        fc = FieldFile("uk_property_prices.tff")
        self.assertEqual(len(fc.fields()), 16)

        self.assertEqual(fc.fields()[0], "txn")
        self.assertEqual(fc.fields()[2], "Date of Transfer")
        self.assertEqual(fc.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count_documents({})
        fc = FieldFile("10k.tff")
        parser = CSVLineToDictParser(fc)
        reader = FileReader("10k.txt", has_header=False, delimiter="|")
        bw = FileWriter(self._col, reader=reader, parser=parser)
        bw.write()
        self.assertEqual(self._col.count_documents({}) - start_count, 10000)

    def test_fieldfile_nomatch(self):
        fc = FieldFile("AandE_Data_2011-04-10.tff")
        parser = CSVLineToDictParser(fc)
        reader = FileReader('inventory.csv', has_header=True)
        bw = FileWriter(self._col, reader=reader, parser=parser)
        with self.assertRaises(ValueError):
            bw.write()

    def test_new_delimiter_and_timeformat_header(self):
        start_count = self._col.count_documents({})
        fc = FieldFile("mot.tff")
        parser = CSVLineToDictParser(fc)
        reader = FileReader('mot_test_set_small.csv', has_header=False, delimiter="|")
        self.assertTrue(type(reader.name) == str)
        bw = FileWriter(self._col, reader=reader, parser=parser)
        total, elapsed = bw.write()
        lines = LineCounter('mot_test_set_small.csv').line_count
        inserted_count = self._col.count_documents({}) - start_count
        self.assertEqual(inserted_count, total)
        self.assertEqual(inserted_count, lines)

    def test_delimiter_header(self):
        start_count = self._col.count_documents({})
        fc = FieldFile("AandE_Data_2011-04-10.tff")
        parser = CSVLineToDictParser(fc)
        reader = FileReader('AandE_Data_2011-04-10.csv', has_header=True)
        bw = FileWriter(self._col, reader=reader, parser=parser)
        bw.write()
        self.assertEqual(self._col.count_documents({}) - start_count, 300)

    def test_generate_field_filename(self):
        gfc = FieldFile.generate_field_file('inventory.csv', ext="xx")
        self.assertEqual(gfc.field_filename, "inventory.xx")
        rfc = FieldFile(gfc.field_filename)
        self.assertTrue("Inventory Item" in rfc.fields())
        self.assertTrue("Amount" in rfc.fields())
        self.assertTrue("Last Order", rfc.fields())
        self.assertEqual(len(rfc.fields()), 3)
        os.unlink(gfc.field_filename)

        fc = FieldFile.generate_field_file('inventory.csv')
        self.assertEqual(fc.field_filename, "inventory.tff")
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file('inventory.csv.1')
        self.assertEqual(fc.field_filename, "inventory.csv.tff", fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file('yellow_tripdata_2015-01-06-200k.csv.1')
        self.assertEqual(fc.field_filename, "yellow_tripdata_2015-01-06-200k.csv.tff", fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file('yellow_tripdata_2015-01-06-200k.csv.10')
        self.assertEqual(fc.field_filename, "yellow_tripdata_2015-01-06-200k.csv.tff", fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file('test_results_2016_10.txt.1')
        self.assertEqual(fc.field_filename, "test_results_2016_10.txt.tff", fc.field_filename)
        os.unlink(fc.field_filename)

    def test_nyc_2018_genfieldfile(self):

        fc = FieldFile.generate_field_file('2018_Yellow_Taxi_Trip_Data_1000.csv', delimiter=";")
        fc_new = FieldFile(fc.field_filename)

        self.assertEqual(fc.fields(), fc_new.fields())

        os.unlink(fc.field_filename)

    def test_generate_fieldfile(self):
        fc = FieldFile.generate_field_file("inventory.csv", ext="testff")
        self.assertEqual(fc.field_filename, "inventory.testff", fc.field_filename)
        self.assertTrue(os.path.isfile("inventory.testff"), "inventory.testff")
        parser = CSVLineToDictParser(fc)
        reader = FileReader("inventory.csv", has_header=True)
        start_count = self._col.count_documents({})
        writer = FileWriter(self._col, reader=reader, parser=parser)
        write_count, elapsed = writer.write()
        line_count = LineCounter("inventory.csv").line_count
        new_inserted_count = self._col.count_documents({}) - start_count
        self.assertEqual(new_inserted_count, write_count)  # header must be subtracted
        self.assertEqual(new_inserted_count, line_count - 1)  # header must be subtracted
        os.unlink("inventory.testff")

    def test_date(self):
        config = FieldFile("inventory_dates.tff")
        parser = CSVLineToDictParser(config, locator=False)  # screws up comparison later if locator is true
        reader = FileReader("inventory.csv", has_header=True)
        start_count = self._col.count_documents({})
        writer = FileWriter(self._col, reader=reader, parser=parser)
        docs_written, elapsed = writer.write()
        line_count = LineCounter("inventory.csv").line_count
        self.assertEqual(self._col.count_documents({}) - start_count, line_count - 1)  # header must be subtracted
        self.assertEqual(self._col.count_documents({}), docs_written)

        nuts_doc = self._col.find_one({"Last Order": dateutil.parser.parse("29-Feb-2016")})
        self.assertTrue(nuts_doc)

    def testFieldDict(self):
        d = FieldFile("testresults.tff").field_dict
        self.assertTrue("TestID" in d)
        self.assertTrue("FirstUseDate" in d)
        self.assertTrue("Colour" in d)
        self.assertTrue(d["TestID"]["type"] == "int")

    def test_duplicate_id(self):
        self.assertRaises(ValueError, FieldFile, "duplicate_id.tff")


if __name__ == "__main__":
    unittest.main()
