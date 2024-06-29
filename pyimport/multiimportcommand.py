import _csv
import asyncio
import logging
import multiprocessing
import os
import sys

from pyimport.command import seconds_to_duration
from pyimport.importresult import ImportResults
from pyimport.parallellimportcommand import ParallelImportCommand


class MultiImportCommand(ParallelImportCommand):

    def __init__(self, args):
        super().__init__(args)
        self._log.info(f"Pool size        : {args.poolsize}")
        self._log.info(f"Fork using       : {args.forkmethod}")

    def process_files(self) -> ImportResults:

        with multiprocessing.Pool(self._args.poolsize) as pool:
            try:
                if self._args.asyncpro:
                    results = pool.starmap(ParallelImportCommand.async_processor, [(self._args, self._log, filename) for filename in self._args.filenames])
                else:
                    results = pool.starmap(ParallelImportCommand.sync_processor, [(self._args, self._log, filename) for filename in self._args.filenames])
            except KeyboardInterrupt:
                self._log.import_error(f"Keyboard interrupt... exiting")
                pool.terminate()
                pool.join()
                sys.exit(1)
        pool.join()
        import_results = ImportResults(results)
        self.report_process_files(self._args, import_results)
        return import_results

    def run(self) -> ImportResults:
        return self.process_files()




