import unittest
import os

import pymongo

from pyimport.pyimport_main import pyimport_main

path_dir = os.path.dirname(os.path.realpath(__file__))

def f(path):
    return os.path.join(path_dir, path)


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self._client = pymongo.MongoClient()
        self._db = self._client["test"]

    def test_main(self):
        collection = self._db["inventory"]
        self._db.drop_collection("inventory")
        pyimport_main(["--genfieldfile",
                            "--loglevel", "CRITICAL", # suppress output for test
                            f("data/inventory.csv")])

        pyimport_main(["--database", "test",
                            "--loglevel", "CRITICAL", # suppress output for test
                            "--hasheader",
                            "--collection", "inventory",
                            f("data/inventory.csv")])
        self.assertTrue(os.path.isfile(f("data/inventory.tff")))
        os.unlink(f("data/inventory.tff"))

        results = list(collection.find())
        self.assertEqual(len(results), 4)

        pyimport_main(["--genfieldfile",
                            "--loglevel", "CRITICAL", # suppress output for test
                            "--fieldfile", f("data/Demographic_Statistics_By_Zip_Code.tff"),
                            "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"])

        pyimport_main(["--database", "test",
                            "--collection", "nyc_demographics",
                            "--hasheader",
                            "--limit", "150",
                            "--loglevel", "CRITICAL",  # suppress output for test
                            "--fieldfile", f("data/Demographic_Statistics_By_Zip_Code.tff"),
                            "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"])

        collection = self._db["nyc_demographics"]
        results=list(collection.find())
        self.assertTrue(len(results), 150)
        self._db.drop_collection("nyc_demographics")
        self.assertTrue(os.path.isfile(f("data/Demographic_Statistics_By_Zip_Code.tff")))
        os.unlink(f("data/Demographic_Statistics_By_Zip_Code.tff"))



    def tearDown(self) -> None:
        collection = self._db["inventory"]
        self._db.drop_collection("inventory")

if __name__ == '__main__':
    unittest.main()
