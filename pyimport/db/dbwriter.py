from pyimport.db.syncmdbwriter import SyncMDBWriter
from pyimport.db.syncrdbwriter import SyncRDBWriter


class DBWriter:
    def __init__(self, args):

        if "mdburi" in args:
            self._sync_mdb_writer = SyncMDBWriter(args)
            self._sync_rdb_writer = None
        elif "pguri" in args:
            # self._sync_rdb_writer = SyncRDBWriter(args)
            self._sync_mdb_writer = None
        else:
            raise ValueError("No uri provided (eithe --pguri or --mdburi)")

    @property
    def sync_mdb_writer(self):
        return self._sync_mdb_writer

    @property
    def sync_rdb_writer(self):
        return self._sync_rdb_writer

    @property
    def writer(self):
        if self._sync_mdb_writer:
            return self._sync_mdb_writer
        elif self._sync_rdb_writer:
            return self._sync_rdb_writer
        else:
            raise ValueError("No writer created")

    def write(self, doc):
        try:
            self.writer.write(doc)
        except StopIteration:
            return self._total_written

    @property
    def total_written(self):
        return self.writer.total_written

    def close(self):
        self.writer.close()



