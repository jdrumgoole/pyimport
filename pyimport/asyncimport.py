import _csv
import logging
import os
import pprint
import sys
import time
import asyncio

from datetime import datetime, timezone

from pyimport import asynccommandutils, commandutils


class AsyncImportCommand:

    def __init__(self, audit=None, args=None):

        self._args = args
        self._audit = audit
        self._log = logging.getLogger(__name__)
        self._total_written = 0
        self._total_elapsed = 0

        commandutils.print_args(self._log, args)

    @staticmethod
    def time_stamp(d):
        d["timestamp"] = datetime.now(timezone.utc)
        return d

    @property
    def delimiter(self):
        return self._delimiter

    def batch_time_stamp(self, d):
        d["timestamp"] = self._batch_timestamp
        return d

    def run(self):
        self._total_written, self._total_elapsed = asyncio.run(asynccommandutils.process_files(self._log, self._args, self._audit))
        return self._total_written, self._total_elapsed


    def total_written(self):
        return self._total_written

    @property
    def field_info(self):
        return self._field_file

