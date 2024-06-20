import _csv
import logging
import os
import sys
from datetime import datetime, timezone


from requests import exceptions

from pyimport.commandutils import prep_import, process_file, prep_field_file
from pyimport.fieldfile import FieldFile, FieldFileException



class ImportCommand:

    def __init__(self, audit=None, args=None):

        self._audit = audit
        self._args = args
        self._log = logging.getLogger(__name__)
        self._total_written: int = 0
        self._field_filename = args.fieldfile
        self._field_file: FieldFile = None

        self._log.info(f"Using collection :'{args.collection}'")
        self._log.info(f"Write concern    : {args.writeconcern}")
        self._log.info(f"journal          : {args.journal}")
        self._log.info(f"fsync            : {args.fsync}")
        self._log.info(f"has header       : {args.hasheader}")

    @staticmethod
    def time_stamp(d):
        d["timestamp"] = datetime.now(timezone.utc)
        return d

    def batch_time_stamp(self, d):
        d["timestamp"] = self._batch_timestamp
        return d

    def run(self):
        field_file = prep_field_file(self._args.filenames, self._args.fieldfile)
        filenames: list[str] = self._args.filenames
        for i in filenames:
            self._log.info(f"Processing:'{i}'")
            try:
                collection, parser = prep_import(self._args, i, field_file)
                total_written_this_file, elapsed_time = process_file(self._args, self._log, collection, parser, field_file, i)
                self._total_written = self._total_written + total_written_this_file
                if self._audit:
                    audit_doc = { "command": "import",
                                  "filename": i,
                                  "elapsed_time": elapsed_time,
                                  "total_written": total_written_this_file}
                    self._audit.add_batch_info(self._audit.current_batch_id, audit_doc)
            except OSError as e:
                self._log.error(f"{e}")
            except exceptions.HTTPError as e:
                self._log.error(f"{e}")
            except FieldFileException as e:
                self._log.error(f"{e}")
            except _csv.Error as e:
                self._log.error(f"{e}")
            except ValueError as e:
                self._log.error(f"{e}")
            except KeyboardInterrupt:
                self._log.error(f"Keyboard interrupt... exiting")
                sys.exit(1)

        return self._total_written

    def total_written(self):
        return self._total_written

    @property
    def field_info(self):
        return self._field_file
