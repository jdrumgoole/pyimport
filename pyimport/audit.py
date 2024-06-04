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
from datetime import datetime, timezone
from threading import Lock

import pymongo


class Audit(object):
    name = "audit"

    def __init__(self, client=None, database=None, collection="audit"):

        self._lock = Lock()

        if database is not None:
            self._database = database
        elif client is not None:
            self._database = client["AUDIT"]
        else:
            raise ValueError("Neither database nor client defined")

        self._auditCollection = self._database[collection]
        self._open_batch_count = 0
        self._auditCollection.create_index("batchID")

    def collection(self):
        return self._auditCollection

    def drop_collection(self):
        self._auditCollection.drop()

    def getBatchIDs(self):
        cursor = self._auditCollection.find({"batchID": {"$exists": 1}}, {"_id": 0, "batchID": 1})
        for i in cursor:
            if i["batchID"] == 0:
                continue
            yield i['batchID']

    def start_batch(self, doc):

        updated_doc = self._auditCollection.find_one_and_update({"batchID": 0,
                                                                 "filename": "Current Batch"},
                                                                {"$inc": {"currentID": 1}},
                                                                upsert=True,
                                                                return_document=pymongo.ReturnDocument.AFTER)

        assert updated_doc

        self._open_batch_count = self._open_batch_count + 1
        self._auditCollection.insert_one({"batchID": updated_doc["currentID"],
                                          "username": getpass.getuser(),
                                          "start": datetime.now(timezone.utc),
                                          "host": socket.getfqdn(),
                                          "pid": os.getpid(),
                                          "info": doc})

        return updated_doc["currentID"]

    def add_batch_info(self, batchID, field_name, doc):
        self._auditCollection.insert_one({"batchID": batchID,
                                          "timestamp": datetime.now(timezone.utc),
                                          field_name: doc})

    def add_command(self, batchID, cmd_name, args):

        self.add_batch_info(batchID, "command", {"filename": cmd_name,
                                                 "args": args})

    def end_batch(self, batchID):

        if not self.is_batch(batchID):
            raise ValueError("BatchID does not exist: %s" % batchID)

        start = self._auditCollection.find_one({"batchID": batchID,
                                                "start": {"$type": 9}})  # is a timestamp

        assert (start)
        self._auditCollection.insert_one({"batchID": batchID,
                                          "start": start["start"],
                                          "end": datetime.now(timezone.utc)})

        self._open_batch_count = self._open_batch_count - 1
        return batchID

    def in_batch(self):
        with self._lock:
            return self._open_batch_count > 0

    def get_batch(self, batchID):
        batch = self._auditCollection.find_one({"batchID": batchID})
        if batch is None:
            raise ValueError("BatchID does not exist: %s" % batchID)
        else:
            return batch

    def get_batch_end(self, batchID):
        batch = self._auditCollection.find_one({"batchID": batchID,
                                                "end": {"$exists": 1}})
        if batch is None:
            raise ValueError("{ BatchID, end } does not exist: %s" % batchID)

        return batch

    def is_batch(self, batchID):
        return self._auditCollection.find_one({"batchID": batchID})

    def is_complete(self, batchID):
        if self._auditCollection.find_one({"batchID": batchID}) is None:
            raise ValueError("BatchID does not exist: %s" % batchID)
        else:
            return self._auditCollection.find_one({"batchID": batchID, "end": {"$exists": 1}})

    def auditCollection(self):
        return self._auditCollection

    def get_last_batch_id(self):
        curBatch = self._auditCollection.find_one({"filename": 'Current Batch'})
        return curBatch["currentID"]

    def get_batches(self):

        batches = self._auditCollection.find({"batchID": {"$exists": 1},
                                              "start": {"$exists": 1}}).sort("start", pymongo.DESCENDING)
        for i in batches:
            yield i

    def get_batch_ids(self):
        for i in self.get_batches():
            yield i["batchID"]

    def count_to_end(self):
        for i in self.get_batch_ids():
            return i

    def get_batch_zero(self):
        return self._auditCollection.find_one({"batchID": 0})

    def get_valid_batches(self, start=None, end=None):

        if start and not isinstance(start, datetime):
            raise ValueError("start is not a datetime object")
        if end and not isinstance(end, datetime):
            raise ValueError("end is not a datetime object")

        batches = self._auditCollection.find(  # { "start" : { "$exists" : 0},
            {"end": {"$type": 9}},
            {"_id": 0, "batchID": 1, "start": 1, "end": 1}).sort("end", pymongo.DESCENDING)

        for i in batches:
            batch_date = i['end']
            # print( batch_date )
            if start and end:
                if start <= batch_date <= end:
                    yield i
            elif start:
                if batch_date >= start:
                    yield i
            elif end:
                if batch_date <= end:
                    yield i
            else:
                yield i

    def get_valid_batch_ids(self):
        for i in self.get_valid_batches():
            yield i["batchID"]

    def get_last_valid_batch_id(self):
        ids = self.get_valid_batch_ids()
        for i in ids:
            return i
