import argparse
import logging

from pyimport.importcommand import ImportCommand


class DropCollectionCommand:

    def __init__(self, args: argparse.Namespace):
        self._name = "drop"
        self._args = args
        self._log = logging.getLogger(__name__)

    def run(self):

        database = ImportCommand.prep_database(self._args)
        self._log.info(f"Dropping collection '{self._args.collection}'")
        result = database.drop_collection(self._args.collection)
        if result["ok"] == 1:
            self._log.info(f"Collection '{self._args.collection}' dropped")
        else:
            self._log.error(f"Error dropping collection '{self._args.collection}'")
            self._log.error(f"Result: {result}")
        return result
