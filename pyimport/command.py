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

    def name(self):
        return self._name

    def pre_execute(self, arg):
        pass

    def execute(self, arg):
        pass

    def post_execute(self, arg):
        pass

    def run(self, *args):
        for i in args:
            self.pre_execute(i)
            retVal = self.execute(i)
            self.post_execute(i)
        return retVal


class GenerateFieldfileCommand(Command):

    def __init__(self, audit=None, field_filename=None, id=None,delimiter=","):
        super().__init__(audit, id)
        self._name = "generate"
        self._log = logging.getLogger(__name__)
        self._field_filename = field_filename
        self._delimiter = delimiter

    def field_filename(self):
        return self._field_filename

    def execute(self, arg):
        ff = FieldFile.generate_field_file(csv_filename=arg, ff_filename=self._field_filename)
        self._field_filename = ff.field_filename
        return self._field_filename

    def post_execute(self, arg):
        self._log.info(f"Created field filename \n'{self._field_filename}' from '{arg}'")


