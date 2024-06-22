
import logging
import asyncio
from datetime import datetime, timezone

from pyimport import asynccommandutils, commandutils


class AsyncImportCommand:

    def __init__(self, audit=None, args=None):

        self._args = args
        self._audit = audit
        self._log = logging.getLogger(__name__)

        commandutils.print_args(self._log, args)

    def run(self):
        return asyncio.run(asynccommandutils.process_files(self._log, self._args, self._audit))


