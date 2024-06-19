import _csv
import logging
import os
import pprint
import sys
import time
import asyncio
from asyncio import Queue, TaskGroup
from datetime import datetime, timezone

import aiofile
import pymongo
from pymongo import errors
from requests import exceptions
from asyncstdlib import enumerate as aenumerate
from motor.motor_asyncio import AsyncIOMotorClient

from pyimport.asyncinserter import AsyncInserter
from pyimport.command import Command, seconds_to_duration
from pyimport.csvreader import AsyncCSVReader
from pyimport.enrichtypes import EnrichTypes

from pyimport.doctimestamp import DocTimeStamp
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.timer import Timer
from pyimport.linereader import is_url, AsyncRemoteLineReader


class AsyncImportCommand:

    def __init__(self, audit=None, args=None):

        self._audit = audit
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

        self._collection: pymongo.collection.Collection = None
        self._field_file: FieldFile = None
        self._queue: Queue = asyncio.Queue()
        self._inserter = AsyncInserter(self._collection_name, self._queue)

        self._log.info("Using collection:'{}'".format(self._collection_name))
        self._log.info(f"Write concern : {self._write_concern}")
        self._log.info(f"journal       : {self._journal}")
        self._log.info(f"fsync         : {self._fsync}")
        self._log.info(f"has header    : {self._has_header}")

        if self._field_filename is None:
            self._field_filename = FieldFile.make_default_tff_name(self._filenames[0])

        if not os.path.isfile(self._field_filename):
            raise OSError(f"No such field file:'{self._field_filename}'")

        if args.fieldfile is None:
            self._field_filename = FieldFile.make_default_tff_name(args.filenames[0])
        else:
            self._field_filename = args.fieldfile
        self._field_file = FieldFile.load(self._field_filename)
        self._log.info(f"Using field file:'{self._field_filename}'")
        if self._write_concern == 0:  # pymongo won't allow other args with w=0 even if they are false
            client = AsyncIOMotorClient(self._host, w=self._write_concern)
        else:
            client = AsyncIOMotorClient(self._host, w=self._write_econcern, fsync=self._fsync, j=self._journal)

        database = client[self._database_name]
        self._collection = database[self._collection_name]

    @staticmethod
    def time_stamp(d):
        d["timestamp"] = datetime.now(timezone.utc)
        return d

    @property
    def delimiter(self):
        return self._delimiter

    def batch_time_stamp(self, d):
        d["timestamp"] = self._batch_timestamp
        return d

    async def read_file(self, filename: str):

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
            csv_file = AsyncRemoteLineReader(url=filename)
        else:
            csv_file = await aiofile.async_open(filename, "r")

        try:
            async_reader = AsyncCSVReader(file=csv_file,
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

            async for i, doc in aenumerate(async_reader, 1):
                d = parser.enrich_doc(doc, i)
                await self._queue.put(d)

            await self._queue.put(None)

            if not is_url_file:
                await csv_file.close()

        finally:
            if not is_url_file:
                await csv_file.close()

        time_finish = time.time()
        elapsed_time = time_finish - time_start
        return i, elapsed_time

    async def reader_writer(self):

        total_written_this_file = 0

        for i in self._filenames:
            inserter = AsyncInserter(self._collection, self._queue, filename=i)
            print(f"Processing:'{i}'")
            self._log.info(f"Processing:'{i}'")
            try:
                async with asyncio.TaskGroup() as tg:
                    t1 = tg.create_task(self.read_file(i))
                    t2 = tg.create_task(inserter())

                total_written_this_file, elapsed_time = t1.result()
                self._total_written = self._total_written + total_written_this_file

                if self._audit:
                    audit_doc = { "command": "import",
                                  "filename": i,
                                  "elapsed_time": elapsed_time,
                                  "total_written": total_written_this_file}
                    await self._audit.add_batch_info(self._audit.current_batch_id, audit_doc)
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

        await self._queue.join()
        return self._total_written, elapsed_time

    async def _runner(self):
        async with TaskGroup() as tg:
            t1 = tg.create_task(self.reader_writer())
        self._total_written, self._elapsed_time = t1.result()

        return self._total_written, self._elapsed_time

    def run(self):

        asyncio.run(self._runner())

    def total_written(self):
        return self._total_written

    @property
    def field_info(self):
        return self._field_file

