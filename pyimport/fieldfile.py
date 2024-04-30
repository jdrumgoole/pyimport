"""
Created on 2 Mar 2016

@author: jdrumgoole
"""

import os
import toml
from enum import Enum
from datetime import datetime, timezone

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

    DEFAULT_EXTENSION=".tff"

    def __init__(self, name):

        self._name = name
        self._fields = None
        self._field_dict = {}
        self._idField = None

        if os.path.exists(self._name):
            self.read(self._name)
        else:
            raise OSError(f"No such file {self._name}")

    @staticmethod
    def make_default_tff_name(name):
        return f"{os.path.splitext(name)[0]}{FieldFile.DEFAULT_EXTENSION}"

    @property
    def field_filename(self):
        return self._name

    @staticmethod
    def generate_field_file(csv_filename, ff_filename=None, ext=DEFAULT_EXTENSION, delimiter=",", has_header=True):

        toml_dict: dict = {}
        if not ext.startswith("."):
            ext = f".{ext}"

        if ff_filename is None:
            if csv_filename.startswith("http://") or csv_filename.startswith("https://"):
                ff_filename = csv_filename.split('/')[-1]
                ff_filename = os.path.splitext(ff_filename)[0] + ext
            else:
                ff_filename = os.path.splitext(csv_filename)[0] + ext

        reader = FileReader(csv_filename, has_header=has_header, delimiter=delimiter)
        first_line = next(reader.readline())
        if has_header:
            header_line = reader.header_line
            if len(first_line) > len(header_line):
                raise ValueError(f"Header line has more columns than first "
                                 f"line: {len(first_line)} > {len(header_line)}")
            elif len(first_line) < len(header_line):
                    raise ValueError(f"Header line has less columns"
                                     f"than first line: {len(first_line)} < {len(header_line)}")
        else:
            header_line = ["" for i in range(len(first_line))]

        for i, (key, value) in enumerate(zip(header_line, first_line)):
            value=value.strip()
            if value == "":
                value = f"blank-{i}"
            # print( i )

            if value.startswith('"'):  # strip out quotes if they exist
                value = value.strip('"')
            if value.startswith("'"):
                value = value.strip("'")
            key = key.replace('$', '_')  # not valid keys for mongodb
            key = key.replace('.', '_')  # not valid keys for mongodb
            t = Converter.guess_type(value)
            key = key.strip()  # remove any white space inside quotes
            if key == "":
                key = f"No Header {i+1}"
            toml_dict[key] = {}
            toml_dict[key]["type"] = t
            toml_dict[key]["name"] = key
            # ff_file.write(f"[{filename}]\n")
            # ff_file.write(f"type={t}\n")
            # ff_file.write(f"filename={filename}")

        # with open("mongoimport.ff", "w") as ff_file:
        #     #print(toml_dict)
        #     for k,v in toml_dict.items():
        #         ff_file.write(f"{k}.{v['type']}()\n")

        with open(ff_filename, "w") as ff_file:
            #print(toml_dict)
            ff_file.write("#\n")
            ff_file.write(f"# Created '{ff_filename}'\n")
            ff_file.write(f"# at UTC: {datetime.now(timezone.utc)} by class {__name__}\n")
            ff_file.write(f"# Parameters:\n")
            ff_file.write(f"#    csv        : '{csv_filename}'\n")
            ff_file.write(f"#    delimiter  : '{delimiter}'\n")
            ff_file.write(f"#    has_header : {has_header}\n")
            ff_file.write("#\n")
            toml_string = toml.dumps(toml_dict)
            ff_file.write(toml_string)
            ff_file.write(f"#end\n")
        return FieldFile(ff_filename)

    def read(self, filename):

        toml_data = ""

        if not os.path.exists(filename):
            raise OSError(f"No such file: '{filename}'")
        with open(filename) as toml_file:
            for i in toml_file.readlines():
                toml_data = toml_data + i

        try:
            toml_dict = toml.loads(toml_data)
        except toml.decoder.TomlDecodeError as e:
            raise FieldFileException(f"Error: \'{e}\' in {filename}")
        # result = cls._cfg.read(filename)

        if len(toml_data) == 0:
            raise OSError(f"No data in TOML file: '{filename}'")

        self._fields = [k for k in toml_dict.keys()]

        #print(toml_dict)
        for column_name, column_value in toml_dict.items():
            # print( "section: '%s'" % s )
            for field_name, field_value in column_value.items():
                # print("option : '%s'" % o )
                if FieldNames.is_valid(field_name):
                    if field_name == FieldNames.NAME.value:
                        if field_value == "_id":
                            if self._idField is None:
                                self._idField = column_name
                            else:
                                raise ValueError(f"Duplicate _id field:{column_name} appears more than once as _id")
                else:
                    raise ValueError(f"Invalid field name: '{field_name}' in section: '{column_name}'")

            if FieldNames.NAME.value not in column_value.keys():
                toml_dict[column_name][FieldNames.NAME.value] = column_name
            #
            # format is optional for datetime input fields. It is used if present.
            #
            if "format" not in column_value.keys():
                toml_dict[column_name]["format"] = None

        self._field_dict = toml_dict

        return self._field_dict

    @property
    def field_dict(self):
        if self._field_dict is None:
            raise ValueError("trying retrieve a field_dict which has a 'None' value")
        else:
            return self._field_dict

    def fields(self):
        return self._fields

    def has_new_name(self, section):
        return section != self._field_dict[section][FieldNames.NAME.value]

    def type_value(self, fieldName):
        return self._field_dict[fieldName][FieldNames.TYPE.value]
        # return cls._cfg.get(fieldName, "type")

    def format_value(self, fieldName):
        return self._field_dict[fieldName][FieldNames.FORMAT.value]
        # return cls._cfg.get(fieldName, "format")

    def name_value(self, fieldName):
        return self._field_dict[fieldName][FieldNames.NAME.value]
        # return cls._cfg.get(fieldName, "filename")

    def __repr__(self):
        return f"filename:{self._name}\ndict:\n{self._field_dict}\n"
