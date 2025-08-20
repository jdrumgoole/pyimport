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
    async def process_one_file(args, log, filename) -> ImportResult:

        field_file = ImportCommand.prep_field_file(args, filename)
        q: asyncio.Queue = asyncio.Queue()
        writer = await AsyncMDBWriter.create(args)
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
        finally:
            await writer.close()
            if not is_url(filename):
                await async_reader.file.close()
        return result

    async def process_files(self) -> ImportResults:
        tasks = []
        results : list = []
        self.print_args(self._args)
        self._log.info("Using asyncpro")
        try:
            async with TaskGroup() as tg:
                for filename in self._args.filenames:
                    if not os.path.isfile(filename):
                        self._log.warning(f"No such file: '{i}' ignoring")
                        continue
                    task = tg.create_task(AsyncMDBImportCommand.process_one_file(self._args, self._log, filename))
                    tasks.append(task)

            for task in tasks:
                result = task.result()
                self.report_process_one_file(self._args, result)
                self._log.info(f"imported file: '{filename}' ({result.total_written} rows)")
                self._log.info(f"Total elapsed time to upload '{filename}' : {result.elapsed_duration}")
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
        self.report_process_files(self._args, results)
        return results

    def run(self) -> ImportResults:
        return asyncio.run(self.process_files())


