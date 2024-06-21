import argparse
import logging

from pyimport.fieldfile import FieldFile


class GenerateFieldfileCommand:

    def __init__(self, audit=None, args=None):
        self._audit = audit
        self._name = "generate"
        self._log = logging.getLogger(__name__)
        self._field_files: list[str] = []
        self._args = args

    def field_filename(self):
        return self._field_filename

    def run(self):
        if self._args:
            for i in self._args.filenames:
                self._log.info(f"Generating field file from '{i}'")
                if self._args.fieldfile is None:
                    field_filename = FieldFile.make_default_tff_name(i)
                else:
                    field_filename = self._args.fieldfile
                FieldFile.generate_field_file(csv_filename=i, ff_filename=field_filename, delimiter=self._args.delimiter)
                self._field_files.append(field_filename)
            field_list = ",".join([f"'{i}'" for i in self._field_files])
            self._log.info(f"Created field filename(s) {field_list} from {self._args.filenames}")
            return self._field_files
        else:
            return None
