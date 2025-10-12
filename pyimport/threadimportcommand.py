import asyncio
import logging
import os
import queue
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed

from pyimport.importresult import ImportResults
from pyimport.parallellimportcommand import ParallelMDBImportCommand


class ThreadImportCommand(ParallelMDBImportCommand):

    def __init__(self, args):
        super().__init__(args)

    def process_files(self) -> ImportResults:

        results = []

        # Handle restart mode or create new batch for audit
        batch_id = None
        files_to_process = self._args.filenames

        if self._audit and not self._args.restart:
            # Create a new batch ID for tracking
            from pyimport.batchid import generate_suffix_only_batch_id
            batch_id = generate_suffix_only_batch_id()
            # Record batch start
            self._audit.start_batch(batch_id)

        if self._args.restart:
            if not self._audit:
                self._log.error("--restart requires --audit to be enabled")
                raise ValueError("Restart mode requires audit tracking")

            # Get batch_id for restart
            if self._args.batch_id:
                batch_id = self._args.batch_id
            else:
                # Auto-detect last incomplete batch
                incomplete_batch = self._audit.get_last_incomplete_batch()
                if incomplete_batch:
                    batch_id = incomplete_batch['batchID']
                else:
                    self._log.error("No incomplete batch found to restart")
                    raise ValueError("No incomplete batch found. Use --batch-id or start a new import.")

            # Get completed files and skip them
            completed_files = self._audit.get_completed_files(batch_id)
            files_to_process = [f for f in self._args.filenames if f not in completed_files]

        # Print args with batch ID now that we have it
        self.print_args(self._args, batch_id=batch_id)
        self._log.info("Using Threading")
        self._log.info(f"Pool size        : {self._args.poolsize}")
        self._log.info(f"Thread count     : {self._args.poolsize}")

        if self._args.restart:
            self._log.info(f"RESTARTING batch {batch_id}")
            self._log.info(f"Skipping {len(completed_files)} completed files: {completed_files}")
            self._log.info(f"Resuming with {len(files_to_process)} remaining files")

        # Prepare audit parameters for workers (avoid pickling Audit object with locks)
        audit_params = None
        if self._audit:
            audit_params = {
                'host': self._args.audithost,
                'database': self._args.auditdatabase,
                'collection': self._args.auditcollection
            }

        tasks = [(self._args, self._log, filename, audit_params, batch_id) for filename in files_to_process]
        try:
            with ThreadPoolExecutor(max_workers=self._args.poolsize) as executor:
                # Submit the tasks to the thread pool
                if self._args.asyncpro:
                    futures = [executor.submit(ParallelMDBImportCommand.async_processor, *task) for task in tasks]
                else:
                    futures = [executor.submit(ParallelMDBImportCommand.sync_processor, *task) for task in tasks]

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

        # Mark each file as completed in the main process (workers can't do this due to pickling)
        if self._audit and batch_id:
            for result in results:
                if result.total_written > 0:  # Successfully imported
                    self._audit.mark_file_completed(batch_id, result.filename, result.total_written)
                    self._log.info(f"Marked '{result.filename}' as completed in audit")

        # Mark batch as complete if audit is enabled and all files completed successfully
        # Only end batch on restart when we've processed remaining files
        if self._audit and batch_id and self._args.restart:
            # On restart, check if there are any remaining incomplete files
            remaining = [f for f in self._args.filenames if f not in self._audit.get_completed_files(batch_id)]
            if len(remaining) == 0:
                self._audit.end_batch(batch_id)
                self._log.info(f"Marked batch {batch_id} as completed (all files processed)")

        return import_results


