"""
Created on 24 Jul 2017

@author: jdrumgoole
"""

import logging
import os
from datetime import datetime

from pyimport.importcommand import ImportCommand
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.logger import Logger


class InputFileException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class AbortException(Exception):
    pass


class FileProcessor(object):

    def __init__(self, collection, delimiter=",", onerror="warn", _batch_size=500):
        self._logger = logging.getLogger(Logger.LOGGER_NAME)
        self._collection = collection
        self._delimiter = delimiter
        self._onerror = onerror
        self._batch_size = _batch_size
        self._files = []

    def process_one_file(self, input_filename:str, field_filename:str|None = None, has_header: bool = False, limit=0,  restart=False, batchID=None):

        if not field_filename:
            if has_header:
                field_filename = FieldFile.make_default_tff_name(input_filename)
            else:
                self._logger.error("No field file specified and no header in input file")
                self._logger.error("Cannot continue without a field file")
                raise AbortException(f"Aborted processing:' '{input_filename}'")

        cmd = ImportCommand(collection=self._collection,
                            field_filename=field_filename,
                            delimiter=self._delimiter,
                            has_header=has_header,
                            onerror=self._onerror,
                            limit=limit)

        total_written = cmd.run(input_filename)
        return total_written

    def get_files(self):
        return self._files

    def process_files(self, filenames, field_filename=None, hasheader=False, restart=False, audit=None, batchID=None):

        total_count: int = 0
        results = []
        failures = []

        for i in filenames:
            self._files.append(i)
            try:
                self._logger.info("Processing : %s", i)
                #                 if field_filename :
                #                     new_name = field_filename
                #                     cls._logger.info( "using field file: '%s'", new_name )
                #                 else:
                #                     new_name = os.path.splitext(os.path.basename( i ))[0] + ".ff"
                line_count = self.process_one_file(i, field_filename, hasheader, restart)
                size = os.path.getsize(i)
                path = os.path.abspath(i)
                if audit and batchID:
                    audit.add_batch_info(batchID, "file_data", {"os_size": size,
                                                                "collection": self._collection.full_name,
                                                                "path": path,
                                                                "records": line_count,
                                                                "timestamp": datetime.now(datetime.UTC)})

                total_count = line_count + total_count
            except FieldFileException as e:
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
