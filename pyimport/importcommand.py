import _csv
import logging
import os
import pprint
import sys
import time
from datetime import datetime, timezone

import pymongo
from pymongo import errors
from requests import exceptions

from pyimport.command import Command, seconds_to_duration
from pyimport.csvreader import CSVReader
from pyimport.enrichtypes import ErrorResponse, EnrichTypes
from pyimport.databasewriter import DatabaseWriter
from pyimport.doctimestamp import DocTimeStamp
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.timer import Timer
from pyimport.linereader import RemoteLineReader, is_url

class ImportCommand(Command):

    def __init__(self, audit=None, batch_id=None, args=None):

        super().__init__(audit, batch_id)
        self._log = logging.getLogger(__name__)
        self._host = args.host
        self._database_name = args.database
        self._collection_name = args.collection
        self._filenames: list[str] = args.filenames
        self._name: str = "import"
        self._field_filename: str = args.fieldfile
        self._delimiter: str = args.delimiter
        self._has_header: bool = args.hasheader

        self._onerror = args.onerror
        self._limit: int = args.limit
        self._locator = args.locator
        self._batch_size: int = args.batchsize
        self._timestamp = args.addtimestamp
        self._total_written: int = 0
        self._batch_timestamp: datetime = datetime.now(timezone.utc)
        self._fsync = args.fsync
        self._write_concern = args.writeconcern
        self._journal = args.journal
        self._field_filename = args.fieldfile

        self._writer: DatabaseWriter = None
        self._field_file: FieldFile = None

    @staticmethod
    def time_stamp(d):
        d["timestamp"] = datetime.now(timezone.utc)
        return d

    def batch_time_stamp(self, d):
        d["timestamp"] = self._batch_timestamp
        return d

    def pre_execute(self, args):
        # print(f"'{arg}'")
        super().pre_execute(args)
        self._log.info("Using collection:'{}'".format(self._collection_name))
        self._log.info(f"Write concern : {self._write_concern}")
        self._log.info(f"journal       : {self._journal}")
        self._log.info(f"fsync         : {self._fsync}")
        self._log.info(f"has header    : {self._has_header}")

        if self._field_filename is None:
            self._field_filename = FieldFile.make_default_tff_name(self._filenames[0])

        self._log.info(f"Using field file:'{self._field_filename}'")

        if not os.path.isfile(self._field_filename):
            raise OSError(f"No such field file:'{self._field_filename}'")

        if args.fieldfile is None:
            self._field_filename = FieldFile.make_default_tff_name(args.filenames[0])
        else:
            self._field_filename = args.fieldfile
        self._field_file = FieldFile.load(self._field_filename)
        self._log.info(f"Using field file:'{self._field_filename}'")
        if self._write_concern == 0:  # pymongo won't allow other args with w=0 even if they are false
            client = pymongo.MongoClient(self._host, w=self._write_concern)
        else:
            client = pymongo.MongoClient(self._host, w=self._write_econcern, fsync=self._fsync, j=self._journal)

        database = client[self._database_name]
        collection = database[self._collection_name]
        self._writer = DatabaseWriter(collection)

    def process_file(self, filename: str):

        total_written = 0
        timer = Timer()
        inserted_this_quantum = 0
        total_read = 0
        insert_list = []
        time_period = 1.0
        time_start = timer.start()
        is_url_file = is_url(filename)
        csv_file = None

        if is_url_file:
            self._log.info(f"Reading from URL:'{filename}'")
            csv_file  = RemoteLineReader(url=filename)
        else:
            self._log.info(f"Reading from file:'{filename}'")
            csv_file = open(filename, "r")

        try:
            reader = CSVReader(file=csv_file,
                               limit=self._limit,
                               field_file=self._field_file,
                               has_header=self._has_header,
                               delimiter=self._delimiter)
            ts_func = None
            if self._timestamp == DocTimeStamp.DOC_TIMESTAMP:
                ts_func = self.time_stamp
            elif self._timestamp == DocTimeStamp.BATCH_TIMESTAMP:
                ts_func = self.batch_time_stamp

            parser = EnrichTypes(self._field_file,
                                 locator=self._locator,
                                 timestamp_func=ts_func,
                                 onerror=self._onerror,
                                 filename=filename)

            for i, doc in enumerate(reader, 1):
                d = parser.enrich_doc(doc, i)
                insert_list.append(d)
                if len(insert_list) >= self._batch_size:
                    results = self._writer.write(insert_list)
                    total_written = total_written + len(results)
                    inserted_this_quantum = inserted_this_quantum + len(results)
                    insert_list = []
                    elapsed = timer.elapsed()
                    if elapsed >= time_period:
                        docs_per_second = inserted_this_quantum / elapsed
                        timer.reset()
                        inserted_this_quantum = 0
                        self._log.info(
                            f"Input:'{filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")
            if not is_url_file:
                csv_file.close()
            if len(insert_list) > 0:
                try:
                    results = self._writer.write(insert_list)
                    total_written = total_written + len(results)
                    self._log.info("Input: '%s' : Inserted %i records", filename, total_written)
                except errors.BulkWriteError as e:
                    self._log.error(f"pymongo.errors.BulkWriteError: {e.details}")
                    raise

        finally:
            if not is_url_file:
                csv_file.close()

        time_finish = time.time()
        elapsed_time = time_finish - time_start
        return total_written, elapsed_time

    def execute(self, args):

        for i in self._filenames:
            self._log.info(f"Processing:'{i}'")
            try:
                total_written_this_file, elapsed_time = self.process_file(i)
                self._total_written = self._total_written + total_written_this_file
                if self._audit:
                    audit_doc = {"filename": i, "total_written": total_written_this_file}
                    self._audit.add_command(self._id, self.name(), audit_doc)
                self._log.info(f"imported file: '{i}' ({total_written_this_file} rows)")
                self._log.info(f"Total elapsed time to upload '{i}' : {seconds_to_duration(elapsed_time)}")
                self._log.info(f"Average upload rate per second: {round(self._total_written / elapsed_time)}")
            except OSError as e:
                self._log.error(f"{e}")
            except exceptions.HTTPError as e:
                self._log.error(f"{e}")
            except FieldFileException as e:
                self._log.error(f"{e}")
            except _csv.Error as e:
                self._log.error(f"{e}")
            except ValueError as e:
                self._log.error(f"{e}")
            except KeyboardInterrupt:
                self._log.error(f"Keyboard interrupt... exiting")
                sys.exit(1)

        return self._total_written

    def total_written(self):
        return self._total_written

    @property
    def field_info(self):
        return self._field_file

    def post_execute(self, arg):
        super().post_execute(arg)
