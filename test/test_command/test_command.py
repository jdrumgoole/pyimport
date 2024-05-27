import os
import shutil
import unittest
from logging import getLogger

import pymongo

from pyimport.audit import Audit
from pyimport.command import GenerateFieldfileCommand
from pyimport.dropcollectioncommand import DropCollectionCommand
from pyimport.importcommand import ImportCommand
from pyimport.fieldfile import FieldFile
from pyimport.filesplitter import LineCounter


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._database = self._client["TEST_CMD"]
        self._collection = self._database["test"]
        self._collection.insert_one({"hello": "world"})

    def tearDown(self):
        self._client.drop_database( "TEST_CMD")

    def test_Drop_Command(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})

        cmd = DropCollectionCommand(database=self._database,
                                    audit=self._audit,
                                    id=batch_id)

        self.assertTrue(self._collection.find_one({"hello": "world"}))

        cmd.run("test")

        self.assertFalse(self._collection.find_one({"hello": "world"}))

        self._audit.end_batch(batch_id)

    def test_Generate_Fieldfile_Command(self):
        cmd = GenerateFieldfileCommand()
        shutil.copy("yellow_tripdata_2015-01-06-200k.csv",
                    "test_generate_ff.csv")
        cmd.run(f"test_generate_ff.csv")
        self.assertTrue(os.path.isfile("test_generate_ff.tff"))
        ff = FieldFile.load("test_generate_ff.tff")
        self.assertTrue(os.path.exists("test_generate_ff.tff"))
        self.assertEqual(ff.field_dict["pickup_latitude"]["type"], "float")
        os.unlink("test_generate_ff.tff")
        os.unlink("test_generate_ff.csv")

    def test_Import_Command(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})
        collection = self._database["import_test"]

        start_size = collection.count_documents({})
        size_10k = LineCounter("10k.txt").line_count
        size_120 = LineCounter("120lines.txt").line_count
        cmd = ImportCommand(audit=self._audit,
                            id=batch_id,
                            collection=collection,
                            field_filename="10k.tff",
                            delimiter="|",
                            has_header=False,
                            onerror="warn",
                            limit=0)

        cmd.run("10k.txt", "120lines.txt")
        new_size = collection.count_documents({})
        self.assertEqual(size_10k + size_120, new_size - start_size)

        self._audit.end_batch(batch_id)

    def test_Import_Command_new(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})
        collection = self._database["import_test"]

        start_size = collection.count_documents({})
        size_test = LineCounter("test_date_data.csv").line_count - 1
        cmd = ImportCommand(audit=self._audit,
                            id=batch_id,
                            collection=collection,
                            field_filename="10k.tff",
                            delimiter=",",
                            has_header=True,
                            onerror="warn",
                            limit=0)

        cmd.run("test_date_data.csv")
        new_size = collection.count_documents({})
        self.assertEqual(size_test, new_size - start_size)

        self._audit.end_batch(batch_id)

    def test_Import_Command_NYC(self):
        self._audit = Audit(database=self._client["nyc_test"])
        batch_id = self._audit.start_batch({"nyc": "test_batch"})
        collection = self._database["nyc_test"]

        start_size = collection.count_documents({})
        size_test = LineCounter("yellow_trip_data_10.csv").line_count - 1
        cmd = ImportCommand(audit=self._audit,
                            id=batch_id,
                            collection=collection,
                            field_filename="yellow_trip_data_10.tff",
                            delimiter=",",
                            has_header=True,
                            onerror="warn",
                            limit=0)

        cmd.run("yellow_trip_data_10.csv")
        new_size = collection.count_documents({})
        self.assertEqual(size_test, new_size - start_size)

        self._audit.end_batch(batch_id)

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()
