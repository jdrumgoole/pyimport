from typing import List, Dict, Any

from pyimport.db.rdbmanager import RDBManager


class RDBWriter:

    def __init__(self, args):
        self._total_written = 0
        self._table_name = args.pgtable
        self._mgr = RDBManager(args.pguri)

    @classmethod
    def make_rdb_writer(cls, args):
        return cls(args)
    @property
    def total_written(self):
        return self._total_written

    @property
    def table_name(self):
        return self._table_name

    @property
    def mgr(self):
        return self._mgr

    @property
    def table(self):
        return self._table

    def insert(self, table, list_of_dicts: List[Dict[str, Any]]) -> int:
        raise NotImplementedError("Subclasses must implement this method")
