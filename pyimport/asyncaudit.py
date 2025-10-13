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

Progress Document (for restart capability)
{ "batchID"  :  13
  "progress" : { "filename"          : "data.csv.1"
                 "docs_written"      : 125000
                 "last_line_number"  : 125000
                 "file_position"     : 5242880
                 "status"            : "in_progress"  # or "completed"
                }
  "timestamp" : October 10, 2016 9:18 PM
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


class AsyncAudit(object):
    name = "audit"

    def __init__(self, database: Database, collection_name: str = "audit"):

        self._database = database
        options = CodecOptions(tz_aware=True)
        self._col = database.get_collection(collection_name, options)
        self._open_batch_count = 0
        self._current_batch_id: MonotonicID = None

    @staticmethod
    def _get_batch_id_value(batch_id):
        """Helper to extract batch ID value from either MonotonicID object or string/int."""
        if isinstance(batch_id, MonotonicID):
            return batch_id.id
        return batch_id

    async def create_batch_index(self):
        indexes = await self._col.index_information()
        if "batch_id" not in indexes:
            await self._col.create_index("batch_id")

    @property
    def collection(self):
        return self._col

    async def drop_collection(self):
        await self._col.drop()

    async def start_batch(self, info: dict) -> MonotonicID:

        self._open_batch_count = self._open_batch_count + 1
        self._current_batch_id = MonotonicID()
        await self._col.insert_one({"batch_id": self._current_batch_id.id,
                                    "username": getpass.getuser(),
                                    "start": datetime.now(timezone.utc),
                                    "host": socket.getfqdn(),
                                    "pid": os.getpid(),
                                    "info": info})

        return self._current_batch_id

    @property
    def current_batch_id(self):
        return self._current_batch_id

    async def add_batch_info(self, info: dict) -> pymongo.results.InsertOneResult:
        """
        Add batch information document to the audit collection.
        Compatible with sync Audit signature.
        """
        info["timestamp"] = datetime.now(timezone.utc)
        return await self._col.insert_one(info)

    async def end_batch(self, batch_id: MonotonicID, info: dict | None = None) -> dict:

        batch = await self._col.find_one({"batch_id": batch_id.id})
        if batch is None:
            raise ValueError("batch_id does not exist: %s" % batch_id.id)
        if "end" in batch:
            raise ValueError("batch_id already ended: %s" % batch_id.id)
        else:
            info = info if info else {}
            end_doc = {"end": datetime.now(timezone.utc),
                       "info": info}
            result = await self._col.update_one(
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
        return self._open_batch_count > 0

    async def get_batch(self, batch_id: MonotonicID):
        batch = await self._col.find_one({"batch_id": batch_id.id})
        if batch is None:
            raise ValueError("batch_id does not exist: %s" % batch_id.id)
        else:
            return batch

    async def get_batch_end(self, batch_id: MonotonicID) -> dict:
        batch = await self._col.find_one({"batch_id": batch_id.id,
                                    "end": {"$exists": 1}})
        if batch is None:
            raise ValueError("{ batch_id, end } does not exist: %s" % batch_id.id)
        return batch

    async def is_batch(self, batch_id: MonotonicID) -> bool:
        return await self._col.find_one({"batch_id": batch_id.id})

    async def is_complete(self, batch_id: MonotonicID) -> bool:
        end_doc = await self._col.find_one({"batch_id": batch_id.id, "end": {"$exists": 1}})
        if end_doc is None:
            raise ValueError("batch_id does not exist: %s" % batch_id.id)
        else:
            return end_doc

    def audit_collection(self) -> pymongo.collection.Collection:
        return self._col

    async def get_last_batch_id(self) -> MonotonicID:
        d = await self._col.find_one(sort=[("batch_id", pymongo.DESCENDING)])
        return MonotonicID(d["batch_id"])

    async def get_last_batch(self) -> dict:
        return await self._col.find_one(sort=[("batch_id", pymongo.DESCENDING)])

    async def get_last_valid_batch_id(self) -> MonotonicID:
        d = await self._col.find_one({}, sort=[("end", pymongo.DESCENDING)])
        return MonotonicID(d["batch_id"])

    async def get_last_valid_batch(self) -> dict:
        return await self._col.find_one({"end": {"$exists": 1}}, sort=[("end", pymongo.DESCENDING)])

    async def get_batches(self) -> Generator[dict, None, None]:
        # If we included documents with an end field, we would have to filter them out other wise we would have
        # duplicate batch_id's for start and end documents.
        return (i async for i in await self._col.find({"batch_id": {"$exists": 1},
                                                       "start": {"$exists": 1},
                                                       "end": {"$exists": 0}}).sort("start", pymongo.DESCENDING))

        # return (i for i in self._col.find({"batch_id": {"$exists": 1},
        #                                    "start": {"$exists": 1},
        #                                    "end": {"$exists": 0}}).sort("start", pymongo.DESCENDING))

    async def get_batch_ids(self) -> Generator[MonotonicID, None, None]:
        return (MonotonicID(i["batch_id"]) async for i in await self.get_batches())

    async def get_valid_batches(self, start: datetime = None, end: datetime = None) -> Generator[dict, None, None]:

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
        cursor = self._col.find(query, projection).sort("end", pymongo.DESCENDING)
        async for d in cursor:
            yield d

    async def get_valid_batch_ids(self) -> Generator[MonotonicID, None, None]:
        return (MonotonicID(i["batch_id"]) async for i in await self.get_valid_batches())

    # Progress tracking methods for restart capability

    async def record_progress(self, batch_id, filename: str, docs_written: int,
                             last_line_number: int = None, file_position: int = None,
                             status: str = "in_progress") -> pymongo.results.InsertOneResult:
        """
        Record progress for a file being imported. This allows restart capability.
        Should be called periodically (e.g., every 10K documents) and when file completes.

        Args:
            batch_id: The batch ID for this import (MonotonicID, str, or int)
            filename: Name of the file being processed
            docs_written: Number of documents written so far
            last_line_number: Last line number processed (optional)
            file_position: Byte position in file (optional)
            status: "in_progress" or "completed"
        """
        progress_doc = {
            "batch_id": self._get_batch_id_value(batch_id),
            "progress": {
                "filename": filename,
                "docs_written": docs_written,
                "status": status
            },
            "timestamp": datetime.now(timezone.utc)
        }

        if last_line_number is not None:
            progress_doc["progress"]["last_line_number"] = last_line_number
        if file_position is not None:
            progress_doc["progress"]["file_position"] = file_position

        return await self._col.insert_one(progress_doc)

    async def get_file_progress(self, batch_id, filename: str) -> dict | None:
        """
        Get the latest progress record for a specific file in a batch.

        Args:
            batch_id: The batch ID (MonotonicID, str, or int)
            filename: Name of the file

        Returns the progress document or None if not found.
        """
        return await self._col.find_one(
            {
                "batch_id": self._get_batch_id_value(batch_id),
                "progress.filename": filename
            },
            sort=[("timestamp", pymongo.DESCENDING)]
        )

    async def get_batch_progress(self, batch_id) -> list[dict]:
        """
        Get all progress records for a batch, sorted by most recent first.

        Args:
            batch_id: The batch ID (MonotonicID, str, or int)

        Returns a list of progress documents.
        """
        cursor = self._col.find(
            {
                "batch_id": self._get_batch_id_value(batch_id),
                "progress": {"$exists": True}
            }
        ).sort("timestamp", pymongo.DESCENDING)
        return [doc async for doc in cursor]

    async def get_completed_files(self, batch_id) -> list[str]:
        """
        Get list of filenames that have been completed for a batch.

        Args:
            batch_id: The batch ID (MonotonicID, str, or int)

        Returns a list of filenames.
        """
        cursor = self._col.find({
            "batch_id": self._get_batch_id_value(batch_id),
            "progress.status": "completed"
        })
        return [doc["progress"]["filename"] async for doc in cursor]

    async def get_incomplete_files(self, batch_id) -> list[dict]:
        """
        Get list of files that are in progress or pending for a batch.

        Args:
            batch_id: The batch ID (MonotonicID, str, or int)

        Returns a list of progress documents for incomplete files.
        """
        cursor = self._col.find({
            "batch_id": self._get_batch_id_value(batch_id),
            "progress.status": {"$ne": "completed"}
        }).sort("timestamp", pymongo.DESCENDING)
        return [doc async for doc in cursor]

    async def mark_file_completed(self, batch_id, filename: str,
                                  total_docs: int) -> pymongo.results.InsertOneResult:
        """
        Mark a file as completed for restart tracking.

        Args:
            batch_id: The batch ID (MonotonicID, str, or int)
            filename: Name of the completed file
            total_docs: Total documents written for this file
        """
        return await self.record_progress(
            batch_id=batch_id,
            filename=filename,
            docs_written=total_docs,
            status="completed"
        )

    async def get_last_incomplete_batch(self) -> dict | None:
        """
        Find the most recent batch that has not been completed (no end document).
        Useful for auto-restart functionality.

        Returns the batch start document or None.
        """
        # Find batches that have a start but no end
        cursor = self._col.find(
            {"batch_id": {"$exists": True}, "start": {"$exists": True}}
        ).sort("start", pymongo.DESCENDING)

        batches_with_start = [doc async for doc in cursor]

        for batch in batches_with_start:
            batch_id = batch["batch_id"]
            # Check if there's an end document for this batch
            end_doc = await self._col.find_one({"batch_id": batch_id, "end": {"$exists": True}})
            if end_doc is None:
                return batch

        return None
