
import logging
from datetime import datetime, timezone

from pyimport import commandutils
from pyimport.fieldfile import FieldFile


class ImportCommand:

    def __init__(self, audit=None, args=None):

        self._audit = audit
        self._args = args
        self._log = logging.getLogger(__name__)
        self._total_written: int = 0
        self._field_filename = args.fieldfile
        self._field_file: FieldFile = None

        commandutils.print_args(self._log, args)

    def run(self):
        return commandutils.process_files(self._log, self._args, self._audit)

    def total_written(self):
        return self._total_written

    @property
    def field_info(self):
        return self._field_file
