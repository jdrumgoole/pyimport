"""
The audit collection is used to track a batch process that has a distinct start and finish.
Each process has a start and end document that is linked by a batchID. BatchIDs are unique.

An invalid batch is any batch with a start batch and no corresponding end batch. Batch documents
are never updated so that the atomic properties of document writes ensure that batch creation
and batch completion are all or nothing affairs.

Start Batch Document
{ "batchID" :  13
  "start"    : October 10, 2016 9:16 PM
  "info"     : { "args"  : { ... }
                 "MUGS" : { ... }
                }
   "version" : "Program version"
}

End Batch Document
{ "batchID"  :  13
  "end"      : October 10, 2016 9:20 PM
}

There is an index on batchID.


"""

import getpass
import os
import socket
import time
from datetime import datetime, timezone

from typing import Generator

from bson import CodecOptions
from pymongo.database import Database
import pymongo

from pyimport.monotonicid import MonotonicID


class Audit(object):
    name = "audit"

    def __init__(self, host, database_name: str, collection_name: str):

        client = pymongo.MongoClient(host)
        database = client[database_name]
        options = CodecOptions(tz_aware=True)
        self._col = database.get_collection(collection_name, options)

    def add_batch_info(self, info: dict) -> pymongo.results.InsertOneResult:
        info["timestamp"] = datetime.now(timezone.utc)
        return self._col.insert_one(info)

    def audit_collection(self) -> pymongo.collection.Collection:
        return self._col




