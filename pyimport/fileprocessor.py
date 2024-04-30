"""
Created on 24 Jul 2017

@author: jdrumgoole
"""

import logging
import os
from datetime import datetime

from pyimport.command import ImportCommand
from pyimport.fieldfile import FieldFile
from pyimport.logger import Logger


class InputFileException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class FileProcessor(object):

    def __init__(self, collection, delimiter, onerror="warn", batchsize=500, limit=0):
        self._logger = logging.getLogger(Logger.LOGGER_NAME)
        self._collection = collection
        self._delimiter = delimiter
        self._onerror = onerror
        self._batchsize = batchsize
        self._files = []
        self._limit = limit

    def processOneFile(self, input_filename, field_filename=None, hasheader=False, restart=False, batchID=None):

        if not field_filename:
            field_filename = FieldFile.make_default_tff_name(input_filename)

        cmd = ImportCommand(collection=self._collection,
                            field_filename=field_filename,
                            delimiter=self._delimiter,
                            has_header=hasheader,
                            onerror=self._onerror,
                            limit=self._limit)

        cmd.run(input_filename)
        return cmd.total_written()

    def get_files(self):
        return self._files

    def processFiles(self, filenames, field_filename=None, hasheader=False, restart=False, audit=None, batchID=None):

        totalCount = 0
        lineCount = 0
        results = []
        failures = []
        new_name = None

        for i in filenames:
            self._files.append(i)
            try:
                self._logger.info("Processing : %s", i)
                #                 if field_filename :
                #                     new_name = field_filename
                #                     cls._logger.info( "using field file: '%s'", new_name )
                #                 else:
                #                     new_name = os.path.splitext(os.path.basename( i ))[0] + ".ff"
                lineCount = self.processOneFile(i, field_filename, hasheader, restart)
                size = os.path.getsize(i)
                path = os.path.abspath(i)
                if audit and batchID:
                    audit.add_batch_info(batchID, "file_data", {"os_size": size,
                                                                "collection": self._collection.full_name,
                                                                "path": path,
                                                                "records": lineCount,
                                                                "timestamp": datetime.now(datetime.UTC)})

                totalCount = lineCount + totalCount
            except FieldConfigException as e:
                self._logger.info("FieldFile error for %s : %s", i, e)
                failures.append(i)
                if self._onerror == "fail":
                    raise
            except InputFileException as e:
                self._logger.info("Input file error for %s : %s", i, e)
                failures.append(i)
                if self._onerror == "fail":
                    raise

        if len(results) > 0:
            self._logger.info("Processed  : %i files", len(results))
        if len(failures) > 0:
            self._logger.info("Failed to process : %s", failures)
