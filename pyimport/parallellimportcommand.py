import asyncio
import os

from pyimport.asyncimport import AsyncMDBImportCommand
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.importresult import ImportResult, ImportResults


class ParallelMDBImportCommand(MDBImportCommand):

    def __init__(self, args):
        super().__init__(args)

    @staticmethod
    def async_processor(args, log, filename: str):
        if not os.path.isfile(filename):
            log.warning(f"No such file: '{filename}' ignoring")
            return ImportResult.import_error(filename, "No such file")
        else:
            return asyncio.run(AsyncMDBImportCommand.process_one_file(args, log, filename))

    @staticmethod
    def sync_processor(args, log, filename: str):
        if not os.path.isfile(filename):
            log.warning(f"No such file: '{filename}' ignoring")
            return ImportResult.error(filename, "No such file")
        else:
            return MDBImportCommand.process_one_file(args, log, filename)

