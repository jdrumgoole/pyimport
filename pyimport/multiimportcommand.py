import _csv
import asyncio
import logging
import multiprocessing
import os
import subprocess
import sys

from pyimport.timer import seconds_to_duration
from pyimport.importresult import ImportResults
from pyimport.parallellimportcommand import ParallelMDBImportCommand


class MultiImportCommand(ParallelMDBImportCommand):

    def __init__(self, args):
        super().__init__(args)
        self._log.info(f"Pool size        : {args.poolsize}")
        self._log.info(f"Fork using       : {args.forkmethod}")

    def process_files(self) -> ImportResults:

        self.print_args(self._args)
        self._log.info("Using multiprocessing")
        self._log.info(f"Pool size        : {self._args.poolsize}")
        with multiprocessing.Pool(self._args.poolsize) as pool:
            try:
                if self._args.asyncpro:
                    results = pool.starmap(ParallelMDBImportCommand.async_processor,
                                           [(self._args, self._log, filename) for filename in self._args.filenames])
                else:
                    results = pool.starmap(ParallelMDBImportCommand.sync_processor,
                                           [(self._args, self._log, filename) for filename in self._args.filenames])

            except subprocess.CalledProcessError as e:
                print(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}")
                print(f"Output: {e.output.decode()}")
                print(f"Error: {e.stderr.decode()}")
            except KeyboardInterrupt:
                self._log.import_error(f"Keyboard interrupt... exiting subprocesses")
                pool.terminate()
                pool.join()
                sys.exit(1)

        pool.join()
        import_results = ImportResults(results)
        self.report_process_files(self._args, import_results)
        return import_results





