import argparse
import logging

import pymongo

from pyimport.command import Command


class DropCollectionCommand:

    def __init__(self, client: pymongo.MongoClient, args: argparse.Namespace, audit=None):
        self._audit = audit
        self._name = "drop"
        self._args = args
        self._log = logging.getLogger(__name__)
        self._database = args
        self._collection_name = args.collection
        self._database_name = args.database
        self._client = client

    def run(self):

        self._database = self._client[self._database_name]
        self._log.info(f"Dropping collection '{self._args.collection}'")
        result = self._database.drop_collection(self._args.collection)
        if result["ok"] == 1:
            self._log.info(f"Collection '{self._args.collection}' dropped")
        else:
            self._log.error(f"Error dropping collection '{self._args.collection}'")
            self._log.error(f"Result: {result}")
        return result
