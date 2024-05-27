"""
Created on 2 Mar 2016

@author: jdrumgoole
"""
import itertools
import os
import pprint

import toml
from enum import Enum
from datetime import datetime, timezone

from pyimport.linereader import LineReader
from pyimport.type_converter import Converter
from pyimport.filereader import FileReader


class FieldFileException(Exception):
    pass


def dict_to_fields(d):
    f = []
    for k, v in d.items():
        if type(v) == dict:
            f.extend(dict_to_fields(v))
        else:
            f.append(k)
    return f


class FieldNames(Enum):
    NAME = "name"
    TYPE = "type"
    FORMAT = "format"

    def __str__(self):
        return self.value

    @classmethod
    def is_valid(cls, lhs: str) -> bool:
        return (lhs == cls.NAME.value) or (lhs == cls.TYPE.value) or (lhs == cls.FORMAT.value)


class FieldFile(object):
    """
      Each field is represented by a section in the config parser
      For each field there are a set of configurations:

      type = the type of this field, int, float, str, date,
      format = the way the content will be formatted for now really only used to date
      filename = an optional filename field. If not present the section filename will be used.

      If the filename field is "_id" then this will be used as the _id field in the collection.
      Only one filename =_id can be present in any fieldConfig file.

      The values in this column must be unique in the source data file otherwise loading will fail
      with a duplicate key error.

      YAML
      =====

      Each field is represented by a top level field filename. Each field has a nested dict
      called `_config`. That config defines the following values for the field:

        type : int|str|bool|float|datetime|dict
        format : <a valid format string for the type this field is optional>
        <other nested fields> :
            _config : <as above>
            format  : <as above>
            <other nested fields>:
              _config : <as above>
              format  : <as above>

    """

    DEFAULT_EXTENSION = ".tff"

    def __init__(self, field_dict:dict, id_field=None):

        if type(field_dict) is not dict:
            raise TypeError(f"FieldFile expects a dict type for the field_dict parameter, not {type(field_dict)}")
        self._fields = None
        self._field_dict = field_dict
        self._fields = list(self._field_dict.keys())
        self._id_field = id_field



    @staticmethod
    def make_default_tff_name(name):
        return f"{os.path.splitext(name)[0]}{FieldFile.DEFAULT_EXTENSION}"

    @property
    def field_filename(self):
        return None

    # @staticmethod
    # def clean_kv( k: str, v: str) -> [str, str]:
    #     if v.startswith('"'):  # strip out quotes if they exist
    #         v = v.strip('"')
    #         if v == "":
    #             v = "blank"
    #     if v.startswith("'"):
    #         v = v.strip("'")
    #     k = k.replace('$', '_')  # not valid keys for mongodb
    #     k = k.replace('.', '_')  # not valid keys for mongodb
    #     return (k.strip(), v.strip())  # remove any white space inside quotes
    #
    # @staticmethod
    # def clean_field_name(k: str) -> str:
    #     k = k.replace('$', '_')  # not valid keys for mongodb
    #     k = k.replace('.', '_')  # not valid keys for mongodb
    #     return k.strip()

    @staticmethod
    def clean_data_fields(v:str) -> str:
        if v.startswith('"'):  # strip out quotes if they exist
            v = v.strip('"')
            if v == "":
                v = "blank"
        if v.startswith("'"):
            v = v.strip("'")
        return v.strip()  # remove any white space inside quotes

    @staticmethod
    def clean_keys(k: str, i: int) -> str:
        if k == "":
            return f"Blank-{i}"
        else:
            k = k.replace('$', '_')  # not valid keys for mongodb
            k = k.replace('.', '_')
            return k

    @staticmethod
    def clean_field_names(fn:list[str]) ->list[str]:
        new_fn = []
        id_field = None
        for i, k in enumerate(fn, 1):
            if k == "_id":
                if id_field is None:
                    id_field = k
                    new_fn.append(k)
                else:
                    raise ValueError(
                        f"Duplicate _id field:{k} appears more than once as _id see field:{id_field} and {i}")
            elif k == "":
                new_fn.append( f"Blank-{i}")
            else:
                nk = k.replace('$', '_')  # not valid keys for mongodb
                nk = nk.replace('.', '_')
                new_fn.append(nk)

        return new_fn

    @staticmethod
    def generate_field_file(csv_filename, ff_filename=None, ext=DEFAULT_EXTENSION, delimiter=","):

        toml_dict: dict = {}
        if not ext.startswith("."):
            ext = f".{ext}"

        if delimiter == "tab":
            delimiter = "\t"

        if ff_filename is None:
            if csv_filename.startswith("http://") or csv_filename.startswith("https://"):
                ff_filename = csv_filename.split('/')[-1]
                ff_filename = os.path.splitext(ff_filename)[0] + ext
            else:
                ff_filename = os.path.splitext(csv_filename)[0] + ext

        with LineReader(csv_filename) as lr:

            for line in lr:
                if not line.startswith("#"):
                    break

            field_names = [n for n in line.split(delimiter)]
            data_line = next(iter(lr)).strip()
            data_fields = [f.strip() for f in data_line.split(delimiter)]

            if len(field_names) > len(data_fields):
                raise ValueError(f"Header line has more columns than first "
                                 f"line: {len(field_names)} > {len(data_fields)}")
            elif len(field_names) < len(data_fields):
                raise ValueError(f"Header line has less columns"
                                 f"than first line: {len(field_names)} < {len(data_fields)}")
            # else:
            #     header_line = ["" for i in range(len(first_line))]

            # TODO: write a test for multiple ID fields
            field_names = FieldFile.clean_field_names(field_names)
            data_fields = [FieldFile.clean_data_fields(f) for f in data_fields]
            data_field_types = [Converter.guess_type(v) for v in data_fields]
            toml_dict = {v: {"type": k, "name": v} for k, v in zip(data_field_types, field_names)}

        with open(ff_filename, "w") as ff_file:
            ff_file.write("#\n")
            ff_file.write(f"# Created '{ff_filename}'\n")
            ff_file.write(f"# at UTC: {datetime.now(timezone.utc)} by class {__name__}\n")
            ff_file.write(f"# Parameters:\n")
            ff_file.write(f"#    csv        : '{csv_filename}'\n")
            ff_file.write(f"#    delimiter  : '{delimiter}'\n")
            ff_file.write("#\n")
            toml_string = toml.dumps(toml_dict)
            ff_file.write(toml_string)
            ff_file.write(f"#end\n")

        return FieldFile(toml_dict)

    @staticmethod
    def load(filename:str) -> "FieldFile":

        toml_dict = {}

        if not os.path.exists(filename):
            raise OSError(f"No such file: '{filename}'")
        try:
            toml_dict = toml.load(filename)
        except toml.decoder.TomlDecodeError as e:
            raise FieldFileException(f"Error: Failed to parse Field File: '{filename}'\n"
                                     f"TOML Decode Error : {e}")
        # result = cls._cfg.read(filename)

        #print(toml_dict
        id_field = None
        for column_name, column_value in toml_dict.items():
            # print( "section: '%s'" % s )
            for field_name, field_value in column_value.items():
                # print("option : '%s'" % o )
                if FieldNames.is_valid(field_name):
                    if field_name == FieldNames.NAME.value:
                        if field_value == "_id":
                            if id_field is None:
                                id_field = column_name
                            else:
                                raise ValueError(f"Duplicate _id field:{column_name} appears more than once as _id")
                else:
                    raise ValueError(f"Invalid field name: '{field_name}' in section: '{column_name}'")

            if FieldNames.NAME.value not in column_value.keys():
                toml_dict[column_name][FieldNames.NAME.value] = column_name
            #
            # format is optional for datetime input fields. It is used if present.
            #

        return FieldFile(toml_dict, id_field)

    @property
    def field_dict(self):
        if self._field_dict is None:
            raise ValueError("trying retrieve a field_dict which has a 'None' value")
        else:
            return self._field_dict

    def fields(self):
        return self._fields

    def __len__(self):
        return len(self._field_dict)

    def has_new_name(self, section):
        return section != self._field_dict[section][FieldNames.NAME.value]

    def type_value(self, field_name):
        return self._field_dict[field_name][FieldNames.TYPE.value]
        # return cls._cfg.get(fieldName, "type")

    def format_value(self, field_name):
        if FieldNames.FORMAT.value not in self._field_dict[field_name]:
            return None
        else:
            return self._field_dict[field_name][FieldNames.FORMAT.value]
        # return cls._cfg.get(fieldName, "format")

    def name_value(self, field_name):
        return self._field_dict[field_name][FieldNames.NAME.value]
        # return cls._cfg.get(fieldName, "filename")

    def __repr__(self):
        return f"FieldFile({self._field_dict})"
