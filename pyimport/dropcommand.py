import argparse
import logging

from pyimport.db.syncmdbwriter import SyncMDBWriter
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.db.rdbmanager import RDBManager


class DropCommand:

    def __init__(self, args: argparse.Namespace):
        self._args = args
        self._log = logging.getLogger(__name__)

    def drop(self):
        raise NotImplementedError("Drop method not implemented")


class DropCollectionCommand(DropCommand):

    def drop(self):

        writer = SyncMDBWriter(self._args)
        self._log.info(f"Dropping collection '{self._args.collection}'")
        result = writer.database.drop_collection(self._args.collection)
        if result["ok"] == 1:
            self._log.info(f"Collection '{self._args.collection}' dropped")
        else:
            self._log.error(f"Error dropping collection '{self._args.collection}'")
            self._log.error(f"Result: {result}")
        return result


class DropTableCommand(DropCommand):

    def __init__(self, args: argparse.Namespace):
        super().__init__(args)
        self._mgr = RDBManager(args)

    def drop(self):
        self._mgr.drop_table(self._args.pgtable)

