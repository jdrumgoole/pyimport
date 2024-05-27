"""
Created on 23 Jul 2017

@author: jdrumgoole


"""
import time
from datetime import datetime

import logging
from enum import Enum

import pymongo
from pymongo import errors

from pyimport.poolwriter import PoolWriter
from pyimport.timer import Timer


class WriterType(Enum):
    direct = 1
    threaded = 2
    pool = 3


class DatabaseWriterException(Exception):
    pass


class DatabaseWriter:

    def __init__(self,
                 doc_collection: pymongo.collection,
                 audit_collection: pymongo.collection = None,
                 filename: str | None = None):

        self._logger = logging.getLogger(__name__)
        self._collection = doc_collection
        self._audit_collection = audit_collection
        self._totalWritten = 0
        self._buffer: list[dict] = []
        #
        # Need to work out stat manipulation for mongodb insertion
        #

        if filename is None:
            filename = 'Unknown file'
        if self._audit_collection:
            self._audit_collection.insert_one({"filestamp": filename,
                                               "timestamp": datetime.now(datetime.UTC)})

    #
    # def locked_write(self, limit=0, restart=False):
    #
    #     timer = Timer()
    #     thread_writer = ThreadWriter(self._collection, timeout=0.01)
    #     total_read = 0
    #
    #     thread_writer.start()
    #     try:
    #         time_start = timer.start()
    #         previous_count = 0
    #         for i, line in enumerate(self._reader.readline(limit=limit), 1):
    #             thread_writer.put(self._parser.parse_line(line, i))
    #             elapsed = timer.elapsed()
    #             if elapsed >= 1.0:
    #                 inserted_to_date = thread_writer.thread_id
    #                 this_insert = inserted_to_date - previous_count
    #                 previous_count = inserted_to_date
    #                 docs_per_second = this_insert / elapsed
    #                 timer.reset()
    #                 self._logger.info(
    #                         f"Input:'{self._reader.name}': docs per sec:{docs_per_second:7.0f}, total docs:{inserted_to_date:>10}")
    #         thread_writer.stop()
    #     except UnicodeDecodeError as exp:
    #         if self._logger:
    #             self._logger.error(exp)
    #             self._logger.error("Error on line:%i", total_read + 1)
    #             thread_writer.stop()
    #         raise;
    #
    #     except KeyboardInterrupt:
    #         thread_writer.stop()
    #         raise KeyboardInterrupt
    #
    #     time_finish = time.time()
    #
    #     return thread_writer.thread_id, time_finish - time_start
    #

    def pool_write(self, limit=0, restart=False, worker_count=4):
        '''
        TODO: clean pool writer, may just replace it with the async version
        '''

        total_written = 0
        timer = Timer()
        pool_writer = PoolWriter(self._collection, worker_count=worker_count, timeout=0.1)
        total_read = 0
        insert_list = []

        pool_writer.start()
        try:
            time_start = timer.start()
            previous_count = 0
            for i, line in enumerate(self._reader.readline(limit=limit), 1):
                pool_writer.put(self._parser.parse_line(line, i))
                elapsed = timer.elapsed()
                if elapsed >= 1.0:
                    inserted_to_date = pool_writer.count
                    this_insert = inserted_to_date - previous_count
                    previous_count = inserted_to_date
                    docs_per_second = this_insert / elapsed
                    timer.reset()
                    self._logger.info(
                        f"Input:'{self._reader.filename}': docs per sec:{docs_per_second:7.0f}, total docs:{inserted_to_date:>10}")
            pool_writer.stop()
        except UnicodeDecodeError as exp:
            if self._logger:
                self._logger.error(exp)
                self._logger.error("Error on line:%i", total_read + 1)
                pool_writer.stop()
            raise;

        except KeyboardInterrupt:
            pool_writer.stop()
            raise KeyboardInterrupt;

        time_finish = time.time()

        return pool_writer.count, time_finish - time_start

    def write(self, buffer: list[dict], writer=WriterType.direct, worker_count=2) -> list[str]:
        if writer is WriterType.direct:
            return self.direct_write(buffer)
        elif writer is WriterType.pool:
            return self.pool_write(buffer, limit=limit, restart=restart, worker_count=worker_count)

    def direct_write(self, buffer: list[dict]) -> list[str]:

        try:
            results = self._collection.insert_many(buffer)
            return results.inserted_ids

        except errors.BulkWriteError as e:
            self._logger.error(f"pymongo.errors.BulkWriteError: {e.details}")
            raise DatabaseWriterException(f"pymongo.errors.BulkWriteError: {e.details}")

