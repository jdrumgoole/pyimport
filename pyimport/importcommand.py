
import logging
from datetime import datetime, timezone

from pyimport import commandutils
from pyimport.fieldfile import FieldFile


class ImportCommand:

    def __init__(self, audit=None, args=None):

        self._args = args
        self._audit = audit

        self._log = logging.getLogger(__name__)

        commandutils.print_args(self._log, args)

    def run(self):
        return commandutils.process_files(self._log, self._args, self._audit)


