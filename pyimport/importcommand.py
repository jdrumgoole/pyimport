import logging
import os
import pprint
import time
from datetime import datetime, timezone

import pymongo
from pymongo import errors

from pyimport.command import Command, seconds_to_duration
from pyimport.csvlinetodictparser import ErrorResponse, CSVLineToDictParser
from pyimport.databasewriter import DatabaseWriter
from pyimport.doctimestamp import DocTimeStamp
from pyimport.fieldfile import FieldFile
from pyimport.filereader import FileReader
from pyimport.timer import Timer


class ImportCommand(Command):

    def __init__(self,
                 collection:pymongo.collection,
                 field_filename: str = None,
                 delimiter:str = ",",
                 has_header:bool = True,
                 onerror: ErrorResponse = ErrorResponse.Warn,
                 limit: int = 0,
                 locator=False,
                 timestamp: DocTimeStamp = DocTimeStamp.NO_TIMESTAMP,
                 audit:bool= None,
                 id:object= None,
                 batch_size=1000):

        super().__init__(audit, id)

        self._log = logging.getLogger(__name__)
        self._filename: str = None
        self._collection: pymongo.collection = collection
        self._name: str = "import"
        self._field_filename: str = field_filename
        self._delimiter: str = delimiter
        self._has_header: bool = has_header
        self._parser: CSVLineToDictParser = None
        self._reader: FileReader = None
        self._writer: DatabaseWriter = None
        self._onerror = onerror
        self._limit: int = limit
        self._locator = locator
        self._batch_size: int = batch_size
        self._timestamp = timestamp
        self._total_written: int = 0
        self._elapsed_time: int = 0
        self._batch_timestamp: datetime = datetime.now(timezone.utc)

    @staticmethod
    def time_stamp(d):
        d["timestamp"] = datetime.now(timezone.utc)
        return d

    def batch_time_stamp(self, d):
        d["timestamp"] = self._batch_timestamp
        return d

    def pre_execute(self, arg):
        # print(f"'{arg}'")
        self._filename = arg
        super().pre_execute(self._filename)
        self._log.info("Using collection:'{}'".format(self._collection.full_name))

        if self._field_filename is None:
            self._field_filename = FieldFile.make_default_tff_name(arg)

        self._log.info(f"Using field file:'{self._field_filename}'")

        if not os.path.isfile(self._field_filename):
            raise OSError(f"No such field file:'{self._field_filename}'")

        self._fieldinfo = FieldFile.load(self._field_filename)

        ts_func = None
        if self._timestamp == DocTimeStamp.DOC_TIMESTAMP:
            ts_func = self.time_stamp
        elif self._timestamp == DocTimeStamp.BATCH_TIMESTAMP:
            ts_func = self.batch_time_stamp

        self._reader = FileReader(arg,
                                  limit=self._limit,
                                  fields=self._fieldinfo.fields(),
                                  has_header=self._has_header,
                                  delimiter=self._delimiter)
        self._parser = CSVLineToDictParser(self._fieldinfo,
                                           locator=self._locator,
                                           timestamp_func=ts_func,
                                           onerror=self._onerror,
                                           filename=self._filename)
        self._writer = DatabaseWriter(self._collection, filename=self._filename)

    def execute(self, arg):

        total_written = 0
        timer = Timer()
        inserted_this_quantum = 0
        total_read = 0
        insert_list = []
        time_period = 1.0
        time_start = timer.start()
        for i, doc in enumerate(self._reader.readline(limit=self._limit), 1):
            d = self._parser.parse_line(doc, i)
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
                            f"Input:'{self._reader.filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")
        if len(insert_list) > 0:
                # print(insert_list)
                try:
                    results = self._writer.write(insert_list)
                    total_written = total_written + len(results)
                    self._log.info("Input: '%s' : Inserted %i records", self._reader.filename, total_written)
                except errors.BulkWriteError as e:
                    self._log.error(f"pymongo.errors.BulkWriteError: {e.details}")
                    raise

        time_finish = time.time()
        #cls._logger.info("Total elapsed time to upload '%s' : %s", cls._reader.filename, seconds_to_duration(finish - time_start))
        #cls._logger.info(f"Total elapsed time to upload '{cls._reader.filename}' : {seconds_to_duration(time_finish - time_start)}")

        self._total_written = total_written
        self._elapsed_time = time_finish - time_start
        return total_written

    def total_written(self):
        return self._total_written

    @property
    def fieldinfo(self):
        return self._fieldinfo

    def post_execute(self, arg):
        super().post_execute(arg)
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"filename": arg})

        self._log.info(f"imported file: '{arg}' ({self._total_written} rows)")
        self._log.info(f"Total elapsed time to upload '{arg}' : {seconds_to_duration(self._elapsed_time)}")
        self._log.info(f"Average upload rate per second: {round(self._total_written/self._elapsed_time)}")
