from datetime import datetime

from pyimport.argmgr import ArgMgr
from pyimport.db.rdbmanager import RDBManager
from pyimport.db.syncrdbwriter import SyncRDBWriter


class RDBTestDB:

    test_schema_dict = {
        "id": int,
        "name": str,
        "age": int,
        "email": str,
        "salary": float,
        "hire_date": datetime
    }

    def __init__(self, args):
        self._test_table_name = "pyimport_test"
        self._args = args.ns
        default_args = ArgMgr.default_args().ns
        if self._args.pguri is None:
            self._uri = default_args.pguri
        else:
            self._uri = self._args.pguri

        self._databases = {}
        self._drop_db = True
        self._mgr = RDBManager(self._uri)
        self._writer = None
        if self._args.pgtable is None:
            self._table_name = self._args.pgtable
        else:
            self._table_name = default_args.pgtable

    @property
    def test_table_name(self):
        return self._test_table_name

    @property
    def writer(self):
        return self._writer

    @property
    def total_written(self):
        return self._writer.total_written

    @property
    def mgr(self):
        return self._mgr

    def __enter__(self):
        if self._mgr.is_table(self.test_table_name):
            self._mgr.drop_table(self.test_table_name)
        self._mgr.create_table(self.test_table_name, self.test_schema_dict)
        self._writer = SyncRDBWriter(self._args, self._mgr.get_table(self.test_table_name))
        return self

    def insert(self, table, data):
        return self._writer.insert(table, data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._drop_db:
            self._mgr.drop_table(self.test_table_name)

    @property
    def test_schema(self):
        return RDBTestDB.test_schema_dict
