'''
Created on 31 Jul 2017

@author: jdrumgoole
'''
import unittest


class Test(unittest.TestCase):
    pass

    #
    # def setUp(cls):
    #     cls._client = pymongo.MongoClient()
    #     cls._client.drop_database( "RESTART_TEST")
    #     cls._db = cls._client[ "RESTART_TEST"]
    #     cls._collection = cls._db[ "RESTART_TEST"]
    #     cls._root = Root()
    #
    # def tearDown(cls):
    #     pass #cls._client.drop_database( "RESTART_TEST")
    #
    # def get_last_doc(cls, col ):
    #     last_doc = col.find().sort([ ("_id", pymongo.DESCENDING )]).limit( 1 )
    #
    #     for i in last_doc :
    #         return i
    #
    # def test_Restart(cls):
    #     fc = FieldFile( cls._root.root_path( "data", "10k.ff"), has_header=False, delimiter="|")
    #     bw = FileWriter( cls._collection, fc)
    #     bw.write( cls._root.root_path( "data", "10k.txt"), restart=True )
    #     audit = cls._db[ "audit"]
    #     cls.assertEqual( audit.thread_id(), 1 )
    #     audit_doc = audit.find_one()
    #
    #     cls.assertEqual( audit_doc[ "state" ], "completed")
    #
    #     last_doc = cls.get_last_doc( cls._collection )
    #     cls.assertEqual( audit_doc[ "last_doc_id"], last_doc[ "_id"])
    #     cls.assertEqual( audit_doc[ "thread_id"], 10000 )
    #
    #     bw.write( cls._root.root_path( "data", "10k.txt"), restart=True )
    #
    #
    #     cls.assertEqual( audit.thread_id(), 2 )
    #     last_audit_doc = cls.get_last_doc( audit )
    #
    #     cls.assertEqual( last_audit_doc[ "state" ], "completed")
    #
    #     last_doc = cls.get_last_doc( cls._collection )
    #
    #     cls.assertEqual( last_audit_doc[ "last_doc_id"], last_doc[ "_id"])
    #     cls.assertEqual( last_audit_doc[ "thread_id"], 10000 )


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_Restart']
    unittest.main()
