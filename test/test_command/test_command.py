import argparse
import os
import shutil
import unittest

import pymongo
from motor.motor_asyncio import AsyncIOMotorClient

from pyimport.argparser import ArgMgr
from pyimport.asyncimport import AsyncImportCommand
from pyimport.audit import Audit
from pyimport.asyncaudit import AsyncAudit
from pyimport.generatefieldfilecommand import GenerateFieldfileCommand
from pyimport.dropcollectioncommand import DropCollectionCommand
from pyimport.enrichtypes import EnrichTypes
from pyimport.importcommand import ImportCommand
from pyimport.fieldfile import FieldFile
from pyimport.filesplitter import LineCounter
import pytest
import pytest_asyncio

import random


def get_random_line(file_path: str) -> str:
    # we ignore the header line
    selected_line = None
    with open(file_path, 'r') as file:
        for i, line in enumerate(file):
            if i == 0:
                continue
            if i == 1:
                selected_line = line
            else:
                # Randomly replace the selected line with a probability of 1/(i+1)
                if random.randint(1, i) == 1:
                    selected_line = line
    return selected_line.strip()


def get_random_field(csvfile: str, ff: FieldFile, delimiter: str) -> [str, str]:
    line = get_random_line(csvfile)
    vals = line.split(delimiter)
    doc = dict(zip(ff.fields(), vals))
    doc = EnrichTypes(field_file=ff, locator=False).enrich_doc(doc)
    k = random.choice(list(doc.keys()))
    return k, doc[k]


# # Example usage
# file_path = 'path_to_your_text_file.txt'
# random_line = get_random_line(file_path)
# print(f"Random line: {random_line.strip()}")


@pytest_asyncio.fixture
async def setup_async():
    client = AsyncIOMotorClient()
    db = client["TEST_CMD"]
    col = db["test"]
    audit = AsyncAudit(database=client["TEST_AUDIT"])
    batch_id = await audit.start_batch({"test": "test_batch"})
    return {"col": col, "audit": audit, "batch_id": batch_id, 'db': db, 'client': client}


@pytest.mark.asyncio
async def test_async_import_command(setup_async):
    col = setup_async["col"]
    size_10k = LineCounter("10k.txt").line_count
    size_120 = LineCounter("120lines.txt").line_count
    args = ArgMgr.default_args().add_arguments(database="TEST_CMD",
                                               collection="test",
                                               fieldfile="10k.tff",
                                               filenames=["10k.txt", "120lines.txt"],
                                               delimiter="|")

    cmd = AsyncImportCommand(audit=setup_async["audit"], args=args.ns)
    start_size = await col.count_documents({})
    await cmd.reader_writer()
    end_size = await col.count_documents({})
    assert (size_10k + size_120) == (end_size - start_size)
    for i in range(20):
        k, v = get_random_field("10k.txt", cmd.field_info, cmd.delimiter)
        assert await col.find_one({k: v}) is not None, f"Field '{k}' with value '{v}' not found in collection i={i}"
    await setup_async['client'].drop_database("TEST_ASYNC_INSERTER")

class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._database = self._client["TEST_CMD"]
        self._collection = self._database["test"]
        self._collection.insert_one({"hello": "world"})
        self._default_args = ArgMgr.default_args()
        self._default_args.add_arguments(database="TEST_CMD", collection="import_test")

    def tearDown(self):
        self._client.drop_database("TEST_CMD")

    def test_Drop_Command(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})
        args = ArgMgr.default_args().add_arguments(database="TEST_CMD", collection="test")
        self.assertTrue(self._collection.find_one({"hello": "world"}))
        DropCollectionCommand(audit=self._audit, client=self._client, args=args.ns).run()
        self.assertFalse(self._collection.find_one({"hello": "world"}))

        self._audit.end_batch(batch_id)

    def test_Generate_Fieldfile_Command(self):

        shutil.copy("yellow_tripdata_2015-01-06-200k.csv",
                    "test_generate_ff.csv")
        args = self._default_args.add_arguments(delimiter=",", fieldfile=None, filenames=["test_generate_ff.csv"])
        GenerateFieldfileCommand(args=args.ns).run()
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
        args = self._default_args.add_arguments(fieldfile="10k.tff", filenames=["10k.txt", "120lines.txt"],
                                                delimiter="|")

        ImportCommand(audit=self._audit, args=args.ns).run()

        new_size = collection.count_documents({})
        self.assertEqual(size_10k + size_120, new_size - start_size)

        self._audit.end_batch(batch_id)

    def test_import_command_small(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})
        collection = self._database["import_test"]

        start_size = collection.count_documents({})
        size_test = LineCounter("test_date_data.csv").line_count - 1
        args = self._default_args.add_arguments(fieldfile="10k.tff", filenames=["test_date_data.csv"], hasheader=True)
        docs_written = ImportCommand(audit=self._audit, args=args.ns).run()
        assert size_test == docs_written

        new_size = collection.count_documents({})
        self.assertEqual(size_test, new_size - start_size)
        self._audit.end_batch(batch_id)

    def test_Import_Command_NYC(self):
        self._audit = Audit(database=self._client["nyc_test"])
        batch_id = self._audit.start_batch({"nyc": "test_batch"})
        collection = self._database["nyc_test"]

        start_size = collection.count_documents({})
        size_test = LineCounter("yellow_trip_data_10.csv").line_count - 1
        args = self._default_args.add_arguments(fieldfile="yellow_trip_data_10.tff",
                                                collection="nyc_test",
                                                filenames=["yellow_trip_data_10.csv"], hasheader=True)
        ImportCommand(audit=self._audit, args=args.ns).run()
        new_size = collection.count_documents({})
        self.assertEqual(size_test, new_size - start_size)

        self._audit.end_batch(batch_id)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()
