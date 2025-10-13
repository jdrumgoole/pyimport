import _csv
import argparse
import logging
import asyncio
import os
import sys
import time
from asyncio import TaskGroup

import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient
from requests import exceptions
from asyncstdlib import enumerate as aenumerate

from pyimport import timer
from pyimport.db.syncmdbwriter import AsyncMDBWriter
from pyimport.importcmd import ImportCommand
from pyimport.importresult import ImportResults
from pyimport.csvreader import AsyncCSVReader
from pyimport.enricher import Enricher
from pyimport.fieldfile import FieldFileException, FieldFile
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.importresult import ImportResult
from pyimport.linereader import is_url, RemoteLineReader


class AsyncMDBImportCommand(MDBImportCommand):

    def __init__(self, args=None):

        super().__init__(args)
        self._log = logging.getLogger(__name__)
        self._q = asyncio.Queue()

        # Override parent's sync Audit with AsyncAudit for async operations
        if args.audit:
            from motor.motor_asyncio import AsyncIOMotorClient
            from pyimport.asyncaudit import AsyncAudit
            client = AsyncIOMotorClient(args.audithost)
            db = client[args.auditdatabase]
            self._audit = AsyncAudit(database=db, collection_name=args.auditcollection)
        else:
            self._audit = None

    async def report_process_one_file(self, args, result):
        """Override to make async-compatible"""
        from pyimport.version import __VERSION__
        audit_doc = None
        if self._audit:
            audit_doc = {"command": "process one file",
                         "version": __VERSION__,
                         "filename": result.filename,
                         "elapsed_time": result.elapsed_time,
                         "total_written": result.total_written,
                         "mode": self.process_mode(args),
                         "avg_records_per_sec": result.avg_records_per_sec,
                         "cmd_line": " ".join(sys.argv)}
            await self._audit.add_batch_info(audit_doc)
        return audit_doc

    async def report_process_files(self, args, results):
        """Override to make async-compatible"""
        audit_doc = None
        if self._audit:
            audit_doc = {"command": "process files",
                         "filenames": results.filenames,
                         "elapsed_time": results.elapsed_time,
                         "total_written": results.total_written,
                         "avg_records_per_sec": results.avg_records_per_sec,
                         "mode": self.process_mode(args),
                         "cmd_line": " ".join(sys.argv)}
            await self._audit.add_batch_info(audit_doc)
        return audit_doc

    @staticmethod
    def async_prep_collection(args):
        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            client = AsyncIOMotorClient(args.mdburi, w=args.writeconcern)
        else:
            client = AsyncIOMotorClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)

        database = client[args.database]
        collection = database[args.collection]

        return collection

    @staticmethod
    async def async_prep_import(args: argparse.Namespace, filename: str, field_info: FieldFile):
        parser = ImportCommand.prep_parser(args, field_info, filename)

        if is_url(filename):
            csv_file = RemoteLineReader(url=filename)
        else:
            csv_file = await aiofiles.open(filename, "r")

        reader = AsyncCSVReader(file=csv_file,
                                limit=args.limit,
                                field_file=field_info,
                                has_header=args.hasheader,
                                cut_fields=args.cut,
                                delimiter=args.delimiter)

        return reader, parser

    @staticmethod
    async def get_csv_doc(args, q, p: Enricher, async_reader: AsyncCSVReader):

        new_field = ImportCommand.parse_new_field(args.addfield)
        async for i, doc in aenumerate(async_reader, 1):
            if args.noenrich:
                d = doc
            else:
                d = p.enrich_doc(doc, new_field, args.cut,  i)
            await q.put(d)
        await q.put(None)
        return i

    @staticmethod
    async def put_db_doc(args, q, log, writer: AsyncMDBWriter, filename: str) -> ImportResult:
        total_written = 0

        time_start = time.time()
        loop_timer = timer.QuantumTimer(start_now=True, quantum=1.0)
        while True:
            doc = await q.get()
            if doc is None:
                q.task_done()
                break
            else:
                total_written = await writer.write(doc)
                q.task_done()
                elapsed, docs_per_second = loop_timer.elapsed_quantum(total_written)
                if elapsed:
                    log.info(f"Input:'{filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")

        await writer.close()
        time_finish = time.time()
        elapsed_time = time_finish - time_start

        return ImportResult(total_written, elapsed_time, filename)

    @staticmethod
    async def process_one_file(args, log, filename, audit=None, batch_id=None) -> ImportResult:

        field_file = ImportCommand.prep_field_file(args, filename)
        q: asyncio.Queue = asyncio.Queue()
        writer = await AsyncMDBWriter.create(args, audit=audit, batch_id=batch_id, filename=filename)
        async_reader, parser = await AsyncMDBImportCommand.async_prep_import(args, filename, field_file)
        try:
            async with TaskGroup() as tg:
                t1 = tg.create_task(AsyncMDBImportCommand.get_csv_doc(args, q, parser, async_reader))
                t2 = tg.create_task(AsyncMDBImportCommand.put_db_doc(args, q, log, writer, filename))

            total_documents_processed = t1.result()
            result = t2.result()
            await q.join()

            if total_documents_processed != result.total_written:
                log.error(
                    f"Total documents processed: {total_documents_processed} is not equal to  Total written: {t2.total_written}")
                raise ValueError(
                    f"Total documents processed: {total_documents_processed} is not equal to  Total written: {t2.total_written}")

            # Mark file as completed if audit is enabled
            if audit and batch_id:
                await audit.mark_file_completed(batch_id, filename, result.total_written)
                log.info(f"Marked '{filename}' as completed in audit")
        finally:
            await writer.close()
            if not is_url(filename):
                await async_reader.file.close()
        return result

    async def process_files(self) -> ImportResults:
        tasks = []
        results : list = []

        # Handle restart mode or create new batch for audit
        batch_id = None
        files_to_process = self._args.filenames

        if self._audit and not self._args.restart:
            # Create a new batch ID for tracking
            from pyimport.batchid import generate_suffix_only_batch_id
            batch_id = generate_suffix_only_batch_id()
            # Record batch start (async audit uses different signature, skip for now)
            # TODO: Align async audit with sync audit batch tracking

        if self._args.restart:
            if not self._audit:
                self._log.error("--restart requires --audit to be enabled")
                raise ValueError("Restart mode requires audit tracking")

            # Get batch_id for restart
            if self._args.batch_id:
                batch_id = self._args.batch_id
            else:
                # Auto-detect last incomplete batch
                incomplete_batch = await self._audit.get_last_incomplete_batch()
                if incomplete_batch:
                    batch_id = incomplete_batch['batchID']
                else:
                    self._log.error("No incomplete batch found to restart")
                    raise ValueError("No incomplete batch found. Use --batch-id or start a new import.")

            # Get completed files and skip them
            completed_files = await self._audit.get_completed_files(batch_id)
            files_to_process = [f for f in self._args.filenames if f not in completed_files]

        # Print args with batch ID now that we have it
        self.print_args(self._args, batch_id=batch_id)
        self._log.info("Using asyncpro")

        if self._args.restart:
            self._log.info(f"RESTARTING batch {batch_id}")
            self._log.info(f"Skipping {len(completed_files)} completed files: {completed_files}")
            self._log.info(f"Resuming with {len(files_to_process)} remaining files")

        try:
            async with TaskGroup() as tg:
                for filename in files_to_process:
                    if not os.path.isfile(filename):
                        self._log.warning(f"No such file: '{filename}' ignoring")
                        continue
                    task = tg.create_task(AsyncMDBImportCommand.process_one_file(self._args, self._log, filename,
                                                                                  audit=self._audit, batch_id=batch_id))
                    tasks.append(task)

            for task in tasks:
                result = task.result()
                await self.report_process_one_file(self._args, result)
                self._log.info(f"imported file: '{result.filename}' ({result.total_written} rows)")
                self._log.info(f"Total elapsed time to upload '{result.filename}' : {result.elapsed_duration}")
                self._log.info(f"Average upload rate per second: {round(result.avg_records_per_sec)}")
                results.append(result)
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
        results = ImportResults(results)
        await self.report_process_files(self._args, results)
        return results

    def run(self) -> ImportResults:
        return asyncio.run(self.process_files())


