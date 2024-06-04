"""

Author: joe@joedrumgoole.com

5-May-2018

"""
import logging
from datetime import datetime, timedelta

from pyimport.fieldfile import FieldFile


def seconds_to_duration(seconds):
    result=""
    delta = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + delta
    if d.day - 1 > 0:
        result =f"{d.day -1} day(s)"
    result = result + "%02d:%02d:%02d.%02d" % (d.hour, d.minute, d.second, d.microsecond)
    return result


class Command:

    def __init__(self, audit=None, id=None):
        self._name = None
        self._log = logging.getLogger(__name__)
        self._audit = audit
        self._id = id
        self._pre_result = None
        self._execute_result = None
        self._post_result = None

    def name(self):
        return self._name

    def pre_execute(self, args):
        pass

    def execute(self, args):
        pass

    def post_execute(self, args):
        pass

    def run(self, args):
        self._pre_result = self.pre_execute(args)
        self._execute_result = self.execute(args)
        self._post_result = self.post_execute(args)
        return self._execute_result


class GenerateFieldfileCommand(Command):

    def __init__(self, audit=None, id=None):
        super().__init__(audit, id)
        self._name = "generate"
        self._log = logging.getLogger(__name__)
        self._field_files: list[str] = []

    def field_filename(self):
        return self._field_filename

    def execute(self, args):
        for i in args.filenames:
            if args.fieldfile is None:
                field_filename = FieldFile.make_default_tff_name(i)
            else:
                field_filename = args.fieldfile
            FieldFile.generate_field_file(csv_filename=i, ff_filename=field_filename, delimiter=args.delimiter)
            self._field_files.append(field_filename)
        return self._field_files

    def post_execute(self, args):
        field_list = ",".join([f"'{i}'" for i in self._field_files])
        self._log.info(f"Created field filename(s) {field_list} from '{args.filenames}'")

