
import os
import unittest


import pymongo
import dateutil
import pytest

from dotenv import load_dotenv
from pyimport.argparser import ArgMgr
from pyimport.fieldfile import FieldFile, FieldNames
from pyimport.filesplitter import LineCounter, split_files, FileSplitter
from pyimport.importcommand import ImportCommand
from pyimport.multiimportcommand import MultiImportCommand
from test.mdbtest import MDBTestDB


# take environment variables from .env.
@pytest.fixture
def setup(scope="module"):
    client = pymongo.MongoClient()
    db = client["E2E_TEST"]
    col = db["TEST"]
    args = ArgMgr.default_args().add_arguments(host="mongodb://localhost:27017", database="E2E_TEST", collection="TEST")
    yield col
    client.drop_database("E2E_TEST")


def test_multi_split(setup):
    #  --splitfile --multi --poolsize 2   --delimiter '|' --fieldfile ./test/test_mot/10k.tff ./test/test_mot/10k.txt
    with MDBTestDB() as tr:
        args = tr.args.add_arguments(filenames=["10k.txt"], delimiter="|", fieldfile="10k.tff", splitfile=True, multi=True, poolsize=2)
        files = split_files(args.ns)
        split_files_list = [split[0] for split in files]
        args.add_arguments(filenames=split_files_list)
        MultiImportCommand(args=args.ns).run()
        assert FileSplitter.compare_concatenated_files("10k.txt", split_files_list) is True
        for i in split_files_list:
            assert os.path.isfile(i) is True
            os.unlink(i)


def test_std_with_audit():
    args = ArgMgr.default_args().add_arguments(filenames=["10lines.txt"], audit=True, delimiter="|", fieldfile="10k.tff")
    assert args.ns.audit is True
    ImportCommand(args=args.ns).run()


class TestEndToEnd(unittest.TestCase):
    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost:27017")
        self._db = self._client["E2E_TEST"]
        self._col = self._db["TEST"]
        self._args = ArgMgr.default_args()
        self._args.add_arguments(host="mongodb://localhost:27017", database="E2E_TEST", collection="TEST")

    def tearDown(self):
        self._client.drop_database("E2E_TEST")

    def test_in(self):
        #x = list(FieldNames.values)
        self.assertFalse(FieldNames.is_valid("hell"))
        self.assertTrue(FieldNames.is_valid("type"))
        self.assertTrue(FieldNames.is_valid("format"))
        self.assertTrue(FieldNames.is_valid("name"))

    def test_mot(self):
        fc = FieldFile.load("mot.tff")
        self.assertEqual(len(fc.fields()), 14)
        self.assertEqual(fc.fields()[0], "TestID")
        self.assertEqual(fc.fields()[13], "FirstUseDate")

        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["mot.txt"], delimiter="|", fieldfile="mot.tff", has_header=False)
        ImportCommand(args=args.ns).run()
        file_size = LineCounter.count_now("mot.txt")
        end_count = self._col.count_documents({})

        self.assertEqual(end_count - start_count, file_size)
        d1 = dateutil.parser.parse("1993-08-11")
        d2 = dateutil.parser.parse("2000-05-12")
        l1 = list(self._col.find({"FirstUseDate": d1}))
        l2 = list(self._col.find({"FirstUseDate": d2}))
        self.assertEqual(len(l1), 1)
        self.assertEqual(len(l2), 2)
        self.assertEqual(l1[0]["FirstUseDate"], d1)
        self.assertEqual(l2[0]["FirstUseDate"], d2)
        self.assertEqual(l1[0]["TestID"], 17)
        self.assertEqual(l2[0]["TestID"], 65)

    def test_aande(self):
        fc = FieldFile.generate_field_file('AandEData.csv', delimiter=",")
        self.assertTrue(os.path.exists("AandEData.tff"))
        self.assertEqual(len(fc.fields()), 20)
        self.assertEqual(fc.fields()[0], "Blank-1")
        self.assertEqual(fc.fields()[1], "Blank-2")
        self.assertEqual(fc.fields()[2], "SHA")
        self.assertEqual(fc.fields()[19], "Number of patients spending >12 hours from decision to admit to admission")
        start_count = self._col.count_documents({})
        args = self._args.add_arguments(filenames=["AandEData.csv"], delimiter=",", fieldfile="AandEData.tff", hasheader=True)
        ImportCommand(args=args.ns).run()
        file_size = LineCounter.count_now("AandEData.csv") - 1
        end_count = self._col.count_documents({})
        self.assertEqual(end_count - start_count, file_size)

    def test_plants(self):
        FieldFile.generate_field_file('plants.txt', delimiter="tab")
        self.assertTrue(os.path.exists("plants.tff"))
        fc = FieldFile.load("plants.tff")
        self.assertEqual(len(fc.fields()), 29)
        os.unlink("plants.tff")
