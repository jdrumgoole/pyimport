import _csv
import csv
import logging
from typing import TextIO

import aiocsv
import aiofile

from pyimport.fieldfile import FieldFile
from pyimport.linereader import LocalLineReader


class CSVReader:

    def __init__(self, file: TextIO, field_file: FieldFile, delimiter=",", skip_lines=0, has_header=True, limit=0):
        #
        # limit is the limit to the number of the data lines read. it ignores the header.
        # if limit is 0, all lines are read.
        #
        self._file = file
        self._delimiter = delimiter
        self._skip_lines = skip_lines
        self._field_file = field_file
        self._has_header = has_header
        self._limit = limit
        self._header_line = None
        self._log = logging.getLogger(__name__)
        if delimiter == "tab":
            self._delimiter = "\t"

        if self._has_header and limit > 0:
            self._limit += 1

    @property
    def delimiter(self):
        return self._delimiter

    @property
    def field_file(self):
        return self._field_file

    @property
    def has_header(self):
        return self._has_header

    @property
    def limit(self):
        return self._limit

    @property
    def skip_lines(self):
        return self._skip_lines


    def __iter__(self):
        # TODO: handle reading URLs
        reader = csv.reader(self._file, delimiter=self._delimiter)
        # we use Reader rather than DictReader because it is more straightforward to use when we may
        # or may not have a header line in the file. We can always use the field_file to map the fields

        for i, row in enumerate(reader, 1):
            if self._has_header and i == 1:
                self._header_line = row
                continue
            if (self._limit > 0) and (i > self._limit):
                break
            else:
                if len(self._field_file.fields()) != len(row):
                    self._log.error(f"Row {i} has {len(row)} fields but field file has {len(self._field_file.fields())}")
                    self._log.error(f"Are you using the right fieldfile and delimiter?")
                    raise ValueError("CSVReader error")
                yield {k: v for k, v in zip(self._field_file.fields(), row)}

    @staticmethod
    def sniff_header(filename: str) -> bool:
        sample = LocalLineReader.read_first_lines(filename)
        sniffer = csv.Sniffer()  # Create a Sniffer object
        has_header = sniffer.has_header(sample)  # Use Sniffer to detect header
        return has_header


class AsyncCSVReader(CSVReader):

    def __init__(self, file: aiofile, field_file: FieldFile, delimiter=",", skip_lines=0, has_header=True, limit=0):
        super().__init__(file, field_file, delimiter, skip_lines, has_header, limit)
        self._file = file
        self._delimiter = delimiter
        self._skip_lines = skip_lines
        self._field_file = field_file
        self._has_header = has_header
        self._limit = limit
        self._header_line = None
        self._log = logging.getLogger(__name__)
        if delimiter == "tab":
            self._delimiter = "\t"

        if self._has_header and limit > 0:
            self._limit += 1

    async def __aiter__(self):
        reader = aiocsv.AsyncReader(self._file, delimiter=self._delimiter)
        async for i, row in enumerate(reader, 1):
            if self._has_header and i == 1:
                self._header_line = row
                continue
            if (self._limit > 0) and (i > self._limit):
                break
            else:
                if len(self._field_file.fields()) != len(row):
                    self._log.error(f"Row {i} has {len(row)} fields but field file has {len(self._field_file.fields())}")
                    self._log.error(f"Are you using the right fieldfile and delimiter?")
                    raise ValueError("CSVReader error")
                yield {k: v for k, v in zip(self._field_file.fields(), row)}
