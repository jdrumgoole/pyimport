import asyncio
import logging
import os
import queue
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed

from pyimport.importresult import ImportResults
from pyimport.parallellimportcommand import ParallelImportCommand


class ThreadImportCommand(ParallelImportCommand):

    def __init__(self, args):
        super().__init__(args)
        self._log.info(f"Thread count     : {args.poolsize}")

    def process_files(self) -> ImportResults:

        results = []
        tasks = [(self._args, self._log, filename) for filename in self._args.filenames]

        try:
            with ThreadPoolExecutor(max_workers=self._args.poolsize) as executor:
                # Submit the tasks to the thread pool
                if self._args.asyncpro:
                    futures = [executor.submit(ParallelImportCommand.async_processor, *task) for task in tasks]
                else:
                    futures = [executor.submit(ParallelImportCommand.sync_processor, *task) for task in tasks]

                # Collect the results as they complete
                for future in as_completed(futures):
                    results.append(future.result())
        except KeyboardInterrupt:
            self._log.error(f"Keyboard interrupt... cleaning up thread pool and exiting")
            for future in futures:
                future.cancel()
            sys.exit(1)
        finally:
            executor.shutdown(wait=True)

        import_results = ImportResults(results)
        self.report_process_files(self._args, import_results)
        return import_results

    def run(self) -> ImportResults:
        return self.process_files()

