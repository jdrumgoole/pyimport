from pyimport.argmgr import ArgMgr
from pyimport.db.dbwriter import DBWriter


def test_pg_writer():
    args = ArgMgr.default_args()
    del args['mdburi']
    w = DBWriter(args.ns)


def test_mdb_writer():
    args = ArgMgr.default_args()
    del args['pguri']
    w = DBWriter(args.ns)
