import unittest
import os

import pymongo
import requests

from pyimport.argparser import ArgMgr
from pyimport.csvreader import CSVReader
from pyimport.fieldfile import FieldFile
from pyimport.enrichtypes import EnrichTypes
from pyimport.fileprocessor import FileProcessor
from pyimport.filereader import FileReader
from pyimport.databasewriter import DatabaseWriter
from pyimport.fieldfile import FieldFile
from pyimport.importcommand import ImportCommand

path_dir = os.path.dirname(os.path.realpath(__file__))

def check_internet():
    url='http://www.google.com/'
    timeout=2
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return True
    except requests.ConnectionError:
        pass
    return False


class TestHTTPImport(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._db = self._client[ "PYIM_HTTP_TEST"]
        self._collection = self._db["PYIM_HTTP_TEST"]
        self._ff = FieldFile.load("2018_Yellow_Taxi_Trip_Data_1000.tff")
        self._parser = EnrichTypes(self._ff)
        self._args = ArgMgr.default_args()
        self._args.add_arguments(host="mongodb://localhost:27017",
                           database="PYIM_HTTP_TEST",
                           collection="PYIM_HTTP_TEST",
                           fieldfile="2018_Yellow_Taxi_Trip_Data_1000.tff")

    def tearDown(self):
        self._db.drop_collection("PYIM_HTTP_TEST")

    def test_limit(self):
        #
        # TODO:need to test limit with a noheader file
        #

        with open("2018_Yellow_Taxi_Trip_Data_1000.csv", "r") as f:
            ff = FieldFile.load("2018_Yellow_Taxi_Trip_Data_1000.tff")
            reader = CSVReader(file=f,
                               delimiter=";",
                               limit=10,
                               field_file=ff,
                               has_header=True)

            for i, doc in enumerate(reader, 1):
                pass

            self.assertEqual(i, 10)

    def test_http_generate_fieldfile(self):
        if check_internet():
            # Demographic_Statistics_By_Zip_Code.csv
            url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"

            ff_file = FieldFile.generate_field_file(url,
                                                    delimiter=";",
                                                    ff_filename="yellow-trip-data.tff")

            self.assertTrue("VendorID" in ff_file.fields(), ff_file.fields())
            self.assertEqual(len(ff_file.fields()), 17)
            self.assertTrue("fare_amount" in ff_file.fields())

            os.unlink("yellow-trip-data.tff")

        else:
            print("Warning:No internet: Skipping test for generating field files from URLs")

    def test_http_import(self):
        if check_internet():
            url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"
            args = self._args.add_arguments(filenames=[url], delimiter=";", hasheader=True)
            ff_file = FieldFile.generate_field_file(url,
                                                    delimiter=";",
                                                    ff_filename="yellow-trip-data.tff")
            before_doc_count = self._collection.count_documents({})
            total_written = ImportCommand(args=args.ns).run(args.ns)
            after_doc_count = self._collection.count_documents({})
            self.assertEqual(after_doc_count - before_doc_count, 999)
        else:
            print("Warning:No internet: test_http_import() skipped")
        os.unlink("yellow-trip-data.tff")




if __name__ == '__main__':
    unittest.main()
