import os
from datetime import datetime

from pyimport.argparser import ArgMgr
from pyimport.rdbmanager import RDBManager
from pyimport.rdbwiter import RDBWriter


class RDBTestDB:

    test_schema_dict = {
        "id": int,
        "name": str,
        "age": int,
        "email": str,
        "salary": float,
        "hire_date": datetime
    }

    test_table_name = "pyimport_test"

    def __init__(self, uri, table_name=None, drop_db=True):
        if uri is None:
            args = ArgMgr.default_args()
            self._uri = args.ns.pguri
        else:
            self._uri = uri
        self._databases = {}
        self._drop_db = drop_db
        self._tables_registry = {}
        if table_name is None:
            self._table_name = self.test_table_name
        else:
            self._table_name = table_name

    @property
    def writer(self):
        return self._writer

    @property
    def mgr(self):
        return self._mgr

    def __enter__(self):
        self._mgr = RDBManager(self._uri)
        if self._mgr.is_table(self.test_table_name):

            self._mgr.drop_table(self.test_table_name)
        self.create_table(self.test_table_name, self.test_schema_dict)
        self._writer = RDBWriter(self._mgr, self._table_name)
        return self

    def create_database(self, db_name):
        if db_name in self._databases:
            return self._databases[db_name]
        else:
            db = self._writer.create_database(db_name)
            self._databases[db_name] = db
            return db

    def create_table(self, table_name, table_dict):
        if table_name in self._tables_registry:
            return self._tables_registry[table_name]
        else:
            tbl = self._mgr.create_table(table_name, table_dict)
            self._tables_registry[table_name] = tbl
            return tbl

    def drop_table(self, table_name):
        if table_name in self._tables_registry:
            tbl = self._tables_registry.pop(table_name)
            self._mgr.drop_table(tbl)
            return True
        else:
            return False

    def get_test_table(self):
        if len(self._tables_registry) > 0:
            return self._tables_registry[self.test_table_name]
        else:
            return None

    @property
    def table_names(self):
        return list(self._tables_registry.keys())

    def insert(self, table, data):
        return self._writer.insert(table, data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._drop_db:
            db_names = list(self._tables_registry.keys())
            for n in db_names:
                self._mgr.drop_table(n)

    @property
    def test_schema(self):
        return RDBTestDB.test_schema_dict
