import unittest

import pymongo

from pyimport.pyimport_main import pyimport_main


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self._client = pymongo.MongoClient()
        self._db = self._client["test"]

    def test_mot_csv(self):
        collection = self._db["mot"]
        self._db.drop_collection("mot")

        pyimport_main(["--database", "test",
                       "--loglevel", "CRITICAL",  # suppress output for test
                       "--delimiter", "|",
                       "--fieldfile", "10k.tff",
                       "--collection", "mot",
                       "10.txt"])

        results = list(collection.find())
        self.assertEqual(len(results), 10)

    def test_mot_csv_isodate(self):
        collection = self._db["motiso"]
        self._db.drop_collection("motiso")

        pyimport_main(["--database", "test",
                       "--loglevel", "CRITICAL",  # suppress output for test
                       "--delimiter", "|",
                       "--fieldfile", "10kiso.tff",
                       "--collection", "motiso",
                       "10.txt"])

        results = list(collection.find())
        self.assertEqual(len(results), 10)

    def x_test_mot_csv(self):
        collection = self._db["mot"]
        self._db.drop_collection("mot")

        pyimport_main(["--database", "test",
                       "--loglevel", "CRITICAL",  # suppress output for test
                       "--delimiter", "|",
                       "--fieldfile", "10k.tff",
                       "--collection", "mot",
                       "10k.txt"])

        results = list(collection.find())
        self.assertEqual(len(results), 10000)

if __name__ == '__main__':
    unittest.main()
