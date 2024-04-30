"""

Author: joe@joedrumgoole.com

5-May-2018

"""
import os
import logging
from datetime import datetime, timedelta, timezone

import pymongo

from pyimport.fieldfile import FieldFile
from pyimport.filewriter import FileWriter, WriterType
from pyimport.csvlinetodictparser import CSVLineToDictParser
from pyimport.csvlinetodictparser import ErrorResponse
from pyimport.filereader import FileReader
from pyimport.doctimestamp import DocTimeStamp


def seconds_to_duration(seconds):
    result=""
    delta = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + delta
    if d.day - 1 > 0:
        result =f"{d.day -1} day(s)"
    result = result + "%02d:%02d:%02d.%02d" % (d.hour, d.minute, d.second, d.microsecond)
    return result

class Command:

    def __init__(self, audit=None, id=None):
        self._name = None
        self._log = logging.getLogger(__name__)
        self._audit = audit
        self._id = id

    def name(self):
        return self._name

    def pre_execute(self, arg):
        pass

    def execute(self, arg):
        pass

    def post_execute(self, arg):
        pass

    def run(self, *args):
        for i in args:
            self.pre_execute(i)
            self.execute(i)
            self.post_execute(i)


class Drop_Command(Command):

    def __init__(self, database, audit=None, id=None):
        super().__init__(audit, id)
        self._name = "drop"
        self._log = logging.getLogger(__name__)
        self._database = database

    def post_execute(self, arg):
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"database": self._database.name,
                                                            "collection_name": arg})
        self._log.info("dropped collection: %s.%s", self._database.name, arg)

    def execute(self, arg):
        # print( "arg:'{}'".format(arg))
        self._database.drop_collection(arg)


class GenerateFieldfileCommand(Command):

    def __init__(self, audit=None, field_filename=None, id=None,delimiter=","):
        super().__init__(audit, id)
        self._name = "generate"
        self._log = logging.getLogger(__name__)
        self._field_filename = field_filename
        self._delimiter = delimiter

    def field_filename(self):
        return self._field_filename

    def execute(self, arg):
        ff = FieldFile.generate_field_file(csv_filename=arg, ff_filename=self._field_filename)
        self._field_filename = ff.field_filename
        return self._field_filename

    def post_execute(self, arg):
        self._log.info(f"Created field filename \n'{self._field_filename}' from '{arg}'")


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
        self._collection = collection
        self._name = "import"
        self._field_filename = field_filename
        self._delimiter = delimiter
        self._has_header = has_header
        self._parser = None
        self._reader = None
        self._writer = None
        self._onerror = onerror
        self._limit = limit
        self._locator = locator
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._total_written = 0
        self._elapsed_time = 0
        self._batch_timestamp = datetime.now(timezone.utc)
    @staticmethod
    def time_stamp(d):
        d["timestamp"] = datetime.now(timezone.utc)
        return d

    def batch_time_stamp(self, d):
        d["timestamp"] = self._batch_timestamp
        return d

    def pre_execute(self, arg):
        # print(f"'{arg}'")
        super().pre_execute(arg)
        self._log.info("Using collection:'{}'".format(self._collection.full_name))

        if self._field_filename is None:
            self._field_filename = FieldFile.make_default_tff_name(arg)

        self._log.info(f"Using field file:'{self._field_filename}'")

        if not os.path.isfile(self._field_filename):
            raise OSError(f"No such field file:'{self._field_filename}'")

        self._fieldinfo = FieldFile(self._field_filename)

        ts_func = None
        if self._timestamp == DocTimeStamp.DOC_TIMESTAMP:
            ts_func = self.time_stamp
        elif self._timestamp == DocTimeStamp.BATCH_TIMESTAMP:
            ts_func = self.batch_time_stamp

        self._reader = FileReader(arg,
                                  limit=self._limit,
                                  has_header=self._has_header,
                                  delimiter=self._delimiter)
        self._parser = CSVLineToDictParser(self._fieldinfo,
                                           locator=self._locator,
                                           timestamp_func=ts_func,
                                           onerror=self._onerror)
        self._writer = FileWriter(self._collection, self._reader, self._parser, batch_size=self._batch_size)

    def execute(self, arg):

        self._total_written, self._elapsed_time = self._writer.write(writer=WriterType.direct) #locked_write()

        return self._total_written

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
