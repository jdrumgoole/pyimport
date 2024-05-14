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

    def test_inventory_csv(self):
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

    def test_mot_csv(self):
        collection = self._db["mot"]
        self._db.drop_collection("mot")




    def tearDown(self) -> None:
        collection = self._db["inventory"]
        self._db.drop_collection("inventory")

if __name__ == '__main__':
    unittest.main()
