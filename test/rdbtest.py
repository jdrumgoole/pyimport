import os
from datetime import datetime

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

    def __init__(self,  url=None, db_name=None, table_dict=None, drop_db=True):
        if url is None:
            self._uri = os.getenv("PGHOST", "postgresql://@localhost/")
        else:
            self._url = url
        self._databases = {}
        self._drop_db = drop_db
        self._writer = RDBWriter(self._uri)
        self._tables_registry = {}

    @property
    def writer(self):
        return self._writer

    def __enter__(self):
        self._writer = RDBWriter(self._uri)
        if self._writer.is_table(self.test_table_name):
            table = self._writer.get_table(self.test_table_name)
            self._writer.drop_table(table)
        self.create_table(self.test_table_name, self.test_schema_dict)
        return self

    def create_table(self, table_name, table_dict):
        if table_name in self._tables_registry:
            return self._tables_registry[table_name]
        else:
            tbl = self._writer.create_table(table_name, table_dict)
            self._tables_registry[table_name] = tbl
            return tbl

    def drop_table(self, table_name):
        if table_name in self._tables_registry:
            tbl = self._tables_registry.pop(table_name)
            self._writer.drop_table(tbl)
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
                self.drop_table(n)

    @property
    def test_schema(self):
        return RDBTestDB.test_schema_dict
