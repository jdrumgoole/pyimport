"""
Created on 23 Jul 2017

@author: jdrumgoole


"""
import time
from datetime import datetime
import os
import logging
import stat
from enum import Enum

import pymongo
from pymongo import errors

from pyimport.filereader import FileReader
from pyimport.csvlinetodictparser import CSVLineToDictParser
from pyimport.poolwriter import PoolWriter
from pyimport.timer import Timer


class WriterType(Enum):
    direct = 1
    threaded = 2
    pool = 3


class FileWriter(object):

    def __init__(self,
                 doc_collection: pymongo.collection,
                 reader: FileReader,
                 parser: CSVLineToDictParser,
                 audit_collection : pymongo.collection =None,
                 batch_size: int = 1000):

        self._logger = logging.getLogger(__name__)
        self._collection = doc_collection
        self._audit_collection = audit_collection
        self._batch_size = batch_size
        self._totalWritten = 0
        self._reader = reader
        self._parser = parser
        #
        # Need to work out stat manipulation for mongodb insertion
        #

        if self._audit_collection:
            self._audit_collection.insert_one({"filestamp": self._reader.name,
                                               "timestamp": datetime.now(datetime.UTC)})

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, size: int) -> None:
        if size < 1:
            raise ValueError(f"Invalid batchsize: {size}")
        self._batch_size = size

    @staticmethod
    def skipLines(f, skip_count: int):
        """
        >>> f = open( "test_set_small.txt", "r" )
        >>> skipLines( f , 20 )
        20
        """

        line_count = 0
        if skip_count > 0:
            # print( "Skipping")
            dummy = f.readline()  # skipcount may be bigger than the number of lines i  the file
            while dummy:
                line_count = line_count + 1
                if line_count == skip_count:
                    break
                dummy = f.readline()
        return line_count
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

        total_written = 0
        timer = Timer()
        pool_writer = PoolWriter(self._collection, worker_count=worker_count,  timeout=0.1)
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
                            f"Input:'{self._reader.name}': docs per sec:{docs_per_second:7.0f}, total docs:{inserted_to_date:>10}")
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

    def write(self, limit=0, restart=False, writer=WriterType.direct, worker_count=2):
        if writer is WriterType.direct:
            return self.direct_write(limit, restart)
        elif writer is WriterType.pool:
            return self.pool_write(limit=limit, restart=restart, worker_count=worker_count)

    def direct_write(self, limit=0, restart=False):

        total_written = 0
        timer = Timer()
        inserted_this_quantum = 0
        total_read = 0
        insert_list = []
        time_period = 1.0
        try:
            time_start = timer.start()
            for i, line in enumerate(self._reader.readline(limit=limit), 1):
                insert_list.append(self._parser.parse_line(line, i))
                if len(insert_list) >= self._batch_size:
                    results = self._collection.insert_many(insert_list)
                    total_written = total_written + len(results.inserted_ids)
                    inserted_this_quantum = inserted_this_quantum + len(results.inserted_ids)
                    insert_list = []
                    elapsed = timer.elapsed()
                    if elapsed >= time_period:
                        docs_per_second = inserted_this_quantum / elapsed
                        timer.reset()
                        inserted_this_quantum = 0
                        self._logger.info(
                                f"Input:'{self._reader.name}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")

        except UnicodeDecodeError as exp:
            if self._logger:
                self._logger.error(exp)
                self._logger.error("Error on line:%i", total_read + 1)
            raise;

        if len(insert_list) > 0:
            # print(insert_list)
            try:
                results = self._collection.insert_many(insert_list)
                total_written = total_written + len(results.inserted_ids)
                self._logger.info("Input: '%s' : Inserted %i records", self._reader.name, total_written)
            except errors.BulkWriteError as e:
                self._logger.error(f"pymongo.errors.BulkWriteError: {e.details}")

        time_finish = time.time()
        #cls._logger.info("Total elapsed time to upload '%s' : %s", cls._reader.filename, seconds_to_duration(finish - time_start))
        #cls._logger.info(f"Total elapsed time to upload '{cls._reader.filename}' : {seconds_to_duration(time_finish - time_start)}")

        return total_written, time_finish - time_start
