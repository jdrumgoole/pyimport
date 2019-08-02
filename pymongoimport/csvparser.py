

from datetime   import datetime
import csv
from enum import Enum
import logging

import requests

from pymongoimport.fieldfile import FieldFile
from pymongoimport.type_converter import Converter


class ErrorResponse(Enum):
    Ignore = "ignore"
    Warn   = "warn"
    Fail   = "fail"

    def __str__(self):
        return self.value


class CSVParser:

    def __init__(self,
                 field_file : FieldFile,
                 has_header: bool = "False",
                 delimiter:str = ",",
                 onerror: ErrorResponse = ErrorResponse.Warn):

        self._logger = logging.getLogger(__name__)
        if delimiter == "tab":
            self._delimiter = "\t"
        else:
            self._delimiter = delimiter
        self._hasheader = has_header
        self._onerror = onerror
        self._record_count = 0
        self._line_count = 0
        self._timestamp = None
        self._idField = None  # section on which name == _id
        self._log = logging.getLogger(__name__)
        self._converter = Converter(self._log)
        self._field_file = field_file

    # def add_timestamp(self, timestamp):
    #     '''
    #     timestamp = "now" generate time once for all docs
    #     timestamp = "gen" generate a new timestamp for each doc
    #     timestamp = "none" don't add timestamp field
    #     '''
    #     self._timestamp = timestamp
    #     if timestamp == "now":
    #         self._doc_template["timestamp"] = datetime.utcnow()
    #     return self._doc_template

    def hasheader(self):
        return self._hasheader

    def delimiter(self):
        return self._delimiter

    def parse_csv_line(self, csv_line:list, record_number):
        """
        Make a new doc from a dictEntry generated by the csv.DictReader.

        :param dict_entry: the corresponding dictEntry for the column
        :return: the new doc

        WIP
        Do we make gen id generate a compound key or another field instead of ID
        """

        doc = {}

        if len(csv_line) == 1:
            self._logger.warning("Warning: only one field in "
                                 "input line. Do you have the "
                                 "right delimiter set ? "
                                 "( current delimiter is : '%s')",
                                 self._delimiter)
            self._logger.warning(f"input line : {csv_line}")

        if len(csv_line) != len(self._field_file.fields()):
            raise ValueError(f"\nrecord:{record_number}:{csv_line}(len={len(csv_line)}) and fields required\n"
                             f"{self._field_file.fields()}(len={len(self._field_file.fields())})"
                             f"don't match in length")

        # print( "dictEntry: %s" % dictEntry )
        field_count = 0

        for i,k in enumerate(self._field_file.fields()):
            # print( "field: %s" % k )
            # print( "value: %s" % dictEntry[ k ])
            field_count = field_count + 1

            if csv_line[i] is None:
                if self._hasheader:
                    self._line_count = self._record_count + 1
                else:
                    self._line_count = self._record_count

                msg = f"Value for field '{k}' at line {self._line_count} is 'None' which is not valid\n"
                # print(dictEntry)
                msg = msg + f"\t\t\tline:{self._record_count}:'{self._delimiter.join([str(v) for v in csv_line])}'"
                if self._onerror == ErrorResponse.Fail:
                    if self._log:
                        self._log.error(msg)
                    raise ValueError(msg)
                elif self._onerror == ErrorResponse.Warn:
                    if self._log:
                        self._log.warning(msg)
                    continue
                else:
                    continue

            if k.startswith("blank-") and self._onerror == ErrorResponse.Warn:  # ignore blank- columns
                if self._log:
                    self._log.info("Field %i is blank [blank-] : ignoring", field_count)
                continue

            # try:
            try:
                type_field = self._field_file.type_value(k)
                if type_field in ["date", "datetime"]:
                    fmt = self._field_file.format_value(k)
                    v = self._converter.convert_time(type_field, csv_line[i], fmt)
                else:
                    v = self._converter.convert(type_field, csv_line[i])

            except ValueError:

                if self._onerror == ErrorResponse.Fail:
                    if self._log:
                        self._log.error("Error at line %i at field '%s'", self._record_count, k)
                        self._log.error("type conversion error: Cannot convert '%s' to type %s", csv_line[i],
                                        type_field)
                    raise
                elif self._onerror == ErrorResponse.Warn:
                    msg = "Parse failure at line {} at field '{}'".format(self._record_count, k)
                    msg = msg + " type conversion error: Cannot convert '{}' to type {} using string type instead".format(
                        csv_line[i], type_field)
                    v = str(csv_line[i])
                elif self._onerror == ErrorResponse.Ignore:
                    v = str(csv_line[i])
                else:
                    raise ValueError("Invalid value for onerror: %s" % self._onerror)

            if self._field_file.has_new_name(k):
                assert (self._field_file.name_value(k) is not None)
                doc[self._field_file.name_value(k)] = v
            else:
                doc[k] = v

        return doc





