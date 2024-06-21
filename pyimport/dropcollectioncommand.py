import argparse
import logging

import pymongo

from pyimport import commandutils
from pyimport.command import Command


class DropCollectionCommand:

    def __init__(self, args: argparse.Namespace, audit=None):
        self._audit = audit
        self._name = "drop"
        self._args = args
        self._log = logging.getLogger(__name__)

    def run(self):

        database = commandutils.prep_database(self._args)
        self._log.info(f"Dropping collection '{self._args.collection}'")
        result = database.drop_collection(self._args.collection)
        if result["ok"] == 1:
            self._log.info(f"Collection '{self._args.collection}' dropped")
        else:
            self._log.error(f"Error dropping collection '{self._args.collection}'")
            self._log.error(f"Result: {result}")
        return result
