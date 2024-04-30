import unittest
import os

import pymongo
import requests

from pyimport.fieldfile import FieldFile
from pyimport.csvlinetodictparser import CSVLineToDictParser
from pyimport.filereader import FileReader
from pyimport.filewriter import FileWriter
from pyimport.fieldfile import FieldFile

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

def f(path):
    return os.path.join(path_dir, path)

class TestHTTPImport(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._db = self._client[ "PYIM_HTTP_TEST"]
        self._collection = self._db["PYIM_HTTP_TEST"]
        self._ff = FieldFile(f("data/2018_Yellow_Taxi_Trip_Data_1000.ff"))
        self._parser = CSVLineToDictParser(self._ff)

    def tearDown(self):
        self._db.drop_collection("PYIM_HTTP_TEST")

    def test_limit(self):
        #
        # need to test limit with a noheader file
        #

        reader = FileReader(f("data/2018_Yellow_Taxi_Trip_Data_1000.csv"),
                            delimiter=";",
                            limit=10,
                            has_header=True)
        count = 0
        for doc in reader.readline(limit=10):
            count = count + 1

        self.assertEqual(count, 10)

    def test_local_import(self):
        reader = FileReader(f("data/2018_Yellow_Taxi_Trip_Data_1000.csv"),
                            has_header=True,
                            delimiter=";")

        before_doc_count = self._collection.count_documents({})

        writer = FileWriter(self._collection, reader=reader,parser=self._parser)
        writer.write(10)

        after_doc_count = self._collection.count_documents({})

        self.assertEqual( after_doc_count - before_doc_count, 10)

    def test_http_generate_fieldfile(self):
        if check_internet():
            # Demographic_Statistics_By_Zip_Code.csv
            url = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"

            ff_file = FieldFile.generate_field_file(url,
                                                    delimiter=",",
                                                    ff_filename=f("data/Demographic_Statistics_By_Zip_Code.tff"))

            self.assertTrue("JURISDICTION NAME" in ff_file.fields(), ff_file.fields())
            self.assertEqual(len(ff_file.fields()), 46)
            self.assertTrue("PERCENT PUBLIC ASSISTANCE TOTAL" in ff_file.fields())

            os.unlink(f("data/Demographic_Statistics_By_Zip_Code.tff"))

        else:
            print("Warning:No internet: Skipping test for generating field files from URLs")

    def test_http_import(self):
        if check_internet():
            csv_parser = CSVLineToDictParser(self._ff)
            reader = FileReader("https://data.cityofnewyork.us/api/views/biws-g3hs/rows.csv?accessType=DOWNLOAD&bom=true&format=true&delimiter=%3B",
                                has_header=True,
                                delimiter=';')

            writer = FileWriter(self._collection, reader, csv_parser)
            before_doc_count = self._collection.count_documents({})
            after_doc_count, elapsed = writer.write(1000)
            self.assertEqual(after_doc_count - before_doc_count, 1000)
        else:
            print("Warning:No internet: test_http_import() skipped")




if __name__ == '__main__':
    unittest.main()
