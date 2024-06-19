import pprint
from datetime   import datetime
import csv
from enum import Enum
import logging
from typing import List, Callable

from pyimport.fieldfile import FieldFile
from pyimport.type_converter import Converter
from pyimport.doctimestamp import DocTimeStamp


class ErrorResponse(Enum):
    Ignore = "ignore"
    Warn   = "warn"
    Fail   = "fail"

    def __str__(self):
        return self.value


class EnrichTypes:

    def __init__(self,
                 field_file: FieldFile,
                 locator: bool = True,
                 timestamp_func: Callable = None,
                 onerror: ErrorResponse = ErrorResponse.Warn,
                 filename: str = None):

        self._logger = logging.getLogger(__name__)

        self._onerror = onerror
        self._line_count = 0
        self._timestamp = None
        self._idField = None  # section on which filename == _id
        self._log = logging.getLogger(__name__)
        self._converter = Converter(self._log)
        self._field_file = field_file
        self._locator = locator
        if timestamp_func is None:
            self._timestamp_func = lambda d : d
        else:
            self._timestamp_func = timestamp_func
        if filename is None:
            self._filename = "Unknown"
        else:
            self._filename = filename

    def map_types(self, k:str, v:str) -> type:
        pass

    def enrich_value(self, k, v, line_number: int, line: str) -> str:
        new_doc = {}
        if v is None:
            msg = f"Value for field '{k}' at line {line_number}:{self._filename} is '{v}' which is not valid\n"
            msg = f"{msg}\t\t\tline:{line_number}:'{line}'"
            if self._onerror == ErrorResponse.Fail:
                if self._log:
                    self._log.error(msg)
                raise ValueError(msg)

        if k.startswith("blank-") and self._onerror == ErrorResponse.Warn:  # ignore blank- columns
            if self._log:
                self._log.info(f"Field {k} is blank [blank-] : ignoring")
            return None

        # try:
        t = self._field_file.type_value(k)
        try:
            return self._converter.convert(t, v, self._field_file.format_value(k))

        except ValueError:
            if self._onerror == ErrorResponse.Fail:
                if self._log:
                    self._log.error(f"Parse failure at line {line_number}:{self._filename} at field '{k}'\n"
                                    f"type conversion error: Cannot convert '{v}' to type {type_field}")
                raise
            elif self._onerror == ErrorResponse.Warn:
                self._log.warning(f"Parse failure at line {line_number}:{self._filename} at field '{k}'\n"
                                  f"type conversion error: Cannot convert '{v}' to type {t} using string type instead")
                new_doc[k] = str(v)
            elif self._onerror == ErrorResponse.Ignore:
                new_doc[k] = str(v)
            else:
                raise ValueError(f"Invalid value for onerror: {self._onerror}")

    def enrich_doc(self, csv_doc: dict, line_number: int = None) -> dict:
        """
        Make a new doc from a dictEntry generated by the csv.DictReader.

        :param csv_doc: the line to be parsed (dict of strs)
        :param line_number: the location of the line in the input file
        :return: the new doc

        WIP
        Do we make gen id generate a compound key or another field instead of ID
        TODO: Need to get the filename being parsed in at this level to allow use to report the right fil
        when an error occurs.
        """

        if line_number is None:
            line_number = "Unknown"
        try:
            line = ",".join(csv_doc.values())
        except TypeError as e:
            self._logger.error(f"TypeError: {e}")
            self._logger.error(f"At line: {line_number}:{self._filename}")
            self._logger.error(f"Does the fieldfile match the csv file content?")
            raise

        fields = self._field_file.fields()

        if len(csv_doc) == 1:
            self._logger.warning("Warning: only one field in "
                                 "input line. Do you have the "
                                 "right delimiter set ?")
            self._logger.warning(f"input line : {line}")

        if len(csv_doc) != len(self._field_file):
            self._logger.error(f"\nrecord: at line {line_number}:{line}(len={len(csv_doc)}) and fields required\n"
                             f"{self._field_file.fields()}(len={len(fields)})"
                             f"don't match in length")
            raise ValueError

        new_doc = {k: self.enrich_value(k, v, line_number, line) for k, v in csv_doc.items()}

        if self._locator:
            new_doc['locator'] = {"line": line_number}

        return self._timestamp_func(new_doc)

