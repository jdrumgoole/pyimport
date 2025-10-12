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

    # Batch lifecycle methods

    def start_batch(self, batch_id: str, info: dict = None) -> pymongo.results.InsertOneResult:
        """
        Record the start of a batch import.

        Args:
            batch_id: Unique batch identifier
            info: Optional dictionary with batch information (args, etc.)
        """
        start_doc = {
            "batchID": batch_id,
            "start": datetime.now(timezone.utc)
        }
        if info:
            start_doc["info"] = info

        return self._col.insert_one(start_doc)

    def end_batch(self, batch_id: str) -> pymongo.results.InsertOneResult:
        """
        Record the end of a batch import.

        Args:
            batch_id: Unique batch identifier matching the start_batch call
        """
        end_doc = {
            "batchID": batch_id,
            "end": datetime.now(timezone.utc)
        }

        return self._col.insert_one(end_doc)

    # Progress tracking methods for restart capability

    def record_progress(self, batch_id: int, filename: str, docs_written: int,
                       last_line_number: int = None, file_position: int = None,
                       status: str = "in_progress") -> pymongo.results.InsertOneResult:
        """
        Record progress for a file being imported. This allows restart capability.
        Should be called periodically (e.g., every 10K documents) and when file completes.

        Args:
            batch_id: The batch ID for this import
            filename: Name of the file being processed
            docs_written: Number of documents written so far
            last_line_number: Last line number processed (optional)
            file_position: Byte position in file (optional)
            status: "in_progress" or "completed"
        """
        progress_doc = {
            "batchID": batch_id,
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

        return self._col.insert_one(progress_doc)

    def get_file_progress(self, batch_id: int, filename: str) -> dict | None:
        """
        Get the latest progress record for a specific file in a batch.

        Returns the progress document or None if not found.
        """
        return self._col.find_one(
            {
                "batchID": batch_id,
                "progress.filename": filename
            },
            sort=[("timestamp", pymongo.DESCENDING)]
        )

    def get_batch_progress(self, batch_id: int) -> list[dict]:
        """
        Get all progress records for a batch, sorted by most recent first.

        Returns a list of progress documents.
        """
        return list(self._col.find(
            {
                "batchID": batch_id,
                "progress": {"$exists": True}
            }
        ).sort("timestamp", pymongo.DESCENDING))

    def get_completed_files(self, batch_id: int) -> list[str]:
        """
        Get list of filenames that have been completed for a batch.

        Returns a list of filenames.
        """
        completed_docs = self._col.find({
            "batchID": batch_id,
            "progress.status": "completed"
        })
        return [doc["progress"]["filename"] for doc in completed_docs]

    def get_incomplete_files(self, batch_id: int) -> list[dict]:
        """
        Get list of files that are in progress or pending for a batch.

        Returns a list of progress documents for incomplete files.
        """
        return list(self._col.find({
            "batchID": batch_id,
            "progress.status": {"$ne": "completed"}
        }).sort("timestamp", pymongo.DESCENDING))

    def mark_file_completed(self, batch_id: int, filename: str,
                           total_docs: int) -> pymongo.results.InsertOneResult:
        """
        Mark a file as completed for restart tracking.

        Args:
            batch_id: The batch ID
            filename: Name of the completed file
            total_docs: Total documents written for this file
        """
        return self.record_progress(
            batch_id=batch_id,
            filename=filename,
            docs_written=total_docs,
            status="completed"
        )

    def get_last_incomplete_batch(self) -> dict | None:
        """
        Find the most recent batch that has not been completed (no end document).
        Useful for auto-restart functionality.

        Returns the batch start document or None.
        """
        # Find batches that have a start but no end
        batches_with_start = list(self._col.find(
            {"batchID": {"$exists": True}, "start": {"$exists": True}}
        ).sort("start", pymongo.DESCENDING))

        for batch in batches_with_start:
            batch_id = batch["batchID"]
            # Check if there's an end document for this batch
            end_doc = self._col.find_one({"batchID": batch_id, "end": {"$exists": True}})
            if end_doc is None:
                return batch

        return None




