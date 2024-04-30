import unittest

import pymongo

from pyimport.threadwriter import ThreadWriter


class TestThreadWriter(unittest.TestCase):

    def test_writer(self):
        client = pymongo.MongoClient()
        db = client["testdb"]
        col = db["testcol"]
        tw = ThreadWriter(col, timeout=0.1)
        tw.start()
        tw.put({"hello": "world"})
        tw.put({"goodbye": "world"})
        tw.stop()


if __name__ == '__main__':
    unittest.main()
