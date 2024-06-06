"""
The audit collection is used to track a batch process that has a distinct start and finish.
Each process has a start and end document that is linked by a batchID. BatchIDs are unique.

Batch creation (specifically batch ID increment) is protected by a lock to make it thread safe.

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
from threading import Lock
from typing import Generator

from bson import CodecOptions
from pymongo.database import Database
import pymongo

from pyimport.monotonicid import MonotonicID


class Audit(object):
    name = "audit"

    def __init__(self, database: Database, collection_name: str = "audit"):

        self._database = database
        self._lock = Lock()
        options = CodecOptions(tz_aware=True)
        self._col = database.get_collection(collection_name, options)
        self._open_batch_count = 0
        self._current_batch_id :MonotonicID = None
        indexes = list(self._col.list_indexes())
        if "batch_id" not in indexes:
            self._col.create_index("batch_id")

    @property
    def collection(self):
        return self._col

    def drop_collection(self):
        self._col.drop()

    def start_batch(self, info: dict) -> MonotonicID:

        self._open_batch_count = self._open_batch_count + 1
        self._current_batch_id = MonotonicID()
        self._col.insert_one({"batch_id": self._current_batch_id.id,
                              "username": getpass.getuser(),
                              "start": datetime.now(timezone.utc),
                              "host": socket.getfqdn(),
                              "pid": os.getpid(),
                              "info": info})

        return self._current_batch_id

    @property
    def current_batch_id(self):
        return self._current_batch_id

    def add_batch_info(self, batch_id:MonotonicID, info: dict) -> pymongo.results.InsertOneResult:
        return self._col.insert_one({"batch_id": batch_id.id,
                                     "timestamp": datetime.now(timezone.utc),
                                     "info": info})

    def end_batch(self, batch_id: MonotonicID, info: dict | None = None) -> dict:

        batch = self._col.find_one({"batch_id": batch_id.id})
        if batch is None:
            raise ValueError("batch_id does not exist: %s" % batch_id.id)
        if "end" in batch:
            raise ValueError("batch_id already ended: %s" % batch_id.id)
        else:
            info = info if info else {}
            end_doc = {"end": datetime.now(timezone.utc),
                       "info": info}
            result = self._col.update_one(
                {"batch_id": batch_id.id},
                {"$set": end_doc}
            )

            if result.matched_count != 1:
                raise ValueError("batch_id does not exist: %s" % batch_id.id)
            elif result.modified_count < 1:
                raise ValueError("Update operation failed to change any docs: %s" % batch_id.id)
            elif result.modified_count > 1:
                raise ValueError("Update operation changed more than one doc: %s" % batch_id.id)
            else:
                self._open_batch_count = self._open_batch_count - 1

        return end_doc

    def in_batch(self):
        with self._lock:
            return self._open_batch_count > 0

    def get_batch(self, batch_id: MonotonicID):
        batch = self._col.find_one({"batch_id": batch_id.id})
        if batch is None:
            raise ValueError("batch_id does not exist: %s" % batch_id.id)
        else:
            return batch

    def get_batch_end(self, batch_id: MonotonicID) -> dict:
        batch = self._col.find_one({"batch_id": batch_id.id,
                                    "end": {"$exists": 1}})
        if batch is None:
            raise ValueError("{ batch_id, end } does not exist: %s" % batch_id.id)
        return batch

    def is_batch(self, batch_id: MonotonicID) -> bool:
        return self._col.find_one({"batch_id": batch_id.id})

    def is_complete(self, batch_id: MonotonicID) -> bool:
        end_doc = self._col.find_one({"batch_id": batch_id.id, "end": {"$exists": 1}})
        if end_doc is None:
            raise ValueError("batch_id does not exist: %s" % batch_id.id)
        else:
            return end_doc

    def audit_collection(self) -> pymongo.collection.Collection:
        return self._col

    def get_last_batch_id(self) -> MonotonicID:
        d = self._col.find_one(sort=[("batch_id", pymongo.DESCENDING)])
        return MonotonicID(d["batch_id"])

    def get_last_batch(self) -> dict:
        return self._col.find_one(sort=[("batch_id", pymongo.DESCENDING)])

    def get_last_valid_batch_id(self) -> MonotonicID:
        d = self._col.find_one({}, sort=[("end", pymongo.DESCENDING)])
        return MonotonicID(d["batch_id"])

    def get_last_valid_batch(self) -> dict:
        return self._col.find_one({"end": {"$exists": 1}}, sort=[("end", pymongo.DESCENDING)])

    def get_batches(self) -> Generator[dict, None, None]:
        # If we included documents with an end field, we would have to filter them out other wise we would have
        # duplicate batch_id's for start and end documents.
        return (i for i in self._col.find({"batch_id": {"$exists": 1},
                                           "start": {"$exists": 1},
                                           "end": {"$exists": 0}}).sort("start", pymongo.DESCENDING))

    def get_batch_ids(self) -> Generator[MonotonicID, None, None]:
        return (MonotonicID(i["batch_id"]) for i in self.get_batches())

    def get_valid_batches(self, start: datetime = None, end: datetime = None) -> Generator[dict, None, None]:

        if start is None and end is None:
            query = {}
        elif start and not isinstance(start, datetime):
            raise ValueError("start is not a datetime object")
        elif end and not isinstance(end, datetime):
            raise ValueError("end is not a datetime object")
        elif start > end:
            raise ValueError("start is greater than end")
        else:
            query = {"end": {"$gte": start, "$lte": end}}

        projection = {"_id": 0, "batch_id": 1, "start": 1, "end": 1}
        return (i for i in self._col.find(query, projection).sort("end", pymongo.DESCENDING))

    def get_valid_batch_ids(self) -> Generator[MonotonicID, None, None]:
        return (MonotonicID(i["batch_id"]) for i in self.get_valid_batches())
