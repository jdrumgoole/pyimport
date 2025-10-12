import _csv
import argparse
import os
import sys
import time
from datetime import datetime, timezone

import pymongo
from requests import exceptions
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError

from pyimport import timer
from pyimport.audit import Audit
from pyimport.csvreader import CSVReader
from pyimport.db.rdbmaker import RDBMaker
from pyimport.db.rdbmanager import RDBManager
from pyimport.db.syncmdbwriter import SyncMDBWriter
from pyimport.doctimestamp import DocTimeStamp
from pyimport.enricher import Enricher
from pyimport.fieldfile import FieldFileException, FieldFile
from pyimport.filereader import FileReader
from pyimport.importcmd import ImportCommand
from pyimport.importresult import ImportResult, ImportResults
from pyimport.linereader import is_url, RemoteLineReader
from pyimport.logger import Log, eh


class MDBImportCommand(ImportCommand):

    def __init__(self, args):
        super().__init__(args)

    def print_args(self, args, batch_id=None):
        self._log.info(f"Using host       :'{args.mdburi}'")
        if self._audit:
            self._log.info(f"Using audit host :'{args.audithost}'")
        if batch_id:
            self._log.info(f"Batch ID         : {batch_id}")
        self._log.info(f"Using database   :'{args.database}'")
        self._log.info(f"Using collection :'{args.collection}'")
        self._log.info(f"Write concern    : {args.writeconcern}")
        self._log.info(f"journal          : {args.journal}")
        self._log.info(f"fsync            : {args.fsync}")
        self._log.info(f"has header       : {args.hasheader}")

    # @staticmethod
    # def prep_mdb_database(args) -> pymongo.database.Database:
    #     if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
    #         client = pymongo.MongoClient(args.mdburi, w=args.writeconcern)
    #     else:
    #         client = pymongo.MongoClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)
    #     database = client[args.database]
    #     return database
    #
    # @staticmethod
    # def prep_collection(args) -> pymongo.collection.Collection:
    #     database = MDBImportCommand.prep_mdb_database(args)
    #     collection = database[args.collection]
    #     return collection

    @staticmethod
    def prep_import(args: argparse.Namespace, filename: str, field_info: FieldFile):
        parser = ImportCommand.prep_parser(args, field_info, filename)

        reader = ImportCommand.prep_csv_reader(args, filename, field_info)

        return reader, parser

    @staticmethod
    def process_one_file(args, log, filename, audit=None, batch_id=None) -> ImportResult:
        time_period = 1.0
        field_file = ImportCommand.prep_field_file(args, filename)
        reader, parser = MDBImportCommand.prep_import(args, filename, field_file)
        time_start = time.time()
        writer = SyncMDBWriter(args, audit=audit, batch_id=batch_id, filename=filename)
        try:
            new_field = MDBImportCommand.parse_new_field(args.addfield)
            loop_timer = timer.QuantumTimer(start_now=True, quantum=time_period)
            for i, doc in enumerate(reader, 1):
                if args.noenrich:
                    d = doc
                else:
                    d = parser.enrich_doc(doc, new_field, args.cut, i)

                writer.write(d)
                elapsed, docs_per_second = loop_timer.elapsed_quantum(writer.total_written)
                if elapsed:
                    log.info(f"Input:'{filename}': docs per sec:{docs_per_second:7.0f}, total docs:{writer.total_written:>10}")
        finally:
            writer.close()
            if not is_url(filename):
                reader.file.close()

        time_finish = time.time()
        elapsed_time = time_finish - time_start
        import_result = ImportResult(writer.total_written, elapsed_time, filename)

        return import_result

    def process_files(self) -> ImportResults:

        results: list = []

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

        if self._args.restart:
            self._log.info(f"RESTARTING batch {batch_id}")
            self._log.info(f"Skipping {len(completed_files)} completed files: {completed_files}")
            self._log.info(f"Resuming with {len(files_to_process)} remaining files")

        for filename in files_to_process:
            self._log.info(f"Processing:'{filename}'")
            try:
                result = MDBImportCommand.process_one_file(self._args, self._log, filename,
                                                          audit=self._audit, batch_id=batch_id)
                self._log.info(f"imported file: '{filename}' ({result.total_written} rows)")
                self._log.info(f"Total elapsed time to upload '{filename}' : {result.elapsed_duration}")
                self._log.info(f"Average upload rate per second: {round(result.avg_records_per_sec)}")

                # Mark file as completed if audit is enabled
                if self._audit and batch_id:
                    self._audit.mark_file_completed(batch_id, filename, result.total_written)
                    self._log.info(f"Marked '{filename}' as completed in audit")

            except OSError as e:
                self._log.error(f"{e}")
                result = ImportResult.import_error(filename, e)
                results.append(result)
            except exceptions.HTTPError as e:
                self._log.error(f"{e}")
                result = ImportResult.import_error(filename, e)
                results.append(result)
            except FieldFileException as e:
                self._log.error(f"{e}")
                result = ImportResult.import_error(filename, e)
                results.append(result)
            except _csv.Error as e:
                self._log.error(f"{e}")
                result = ImportResult.import_error(filename, e)
                results.append(result)
            except ValueError as e:
                self._log.error(f"{e}")
                result = ImportResult.import_error(filename, e)
                results.append(result)
            else:
                results.append(result)
                self.report_process_one_file(self._args, result)

        import_results = ImportResults(results)
        self.report_process_files(self._args, import_results)

        # Mark batch as complete if audit is enabled and all files completed successfully
        # Only end batch on restart when we've processed remaining files
        if self._audit and batch_id and self._args.restart:
            # On restart, check if there are any remaining incomplete files
            remaining = [f for f in self._args.filenames if f not in self._audit.get_completed_files(batch_id)]
            if len(remaining) == 0:
                self._audit.end_batch(batch_id)
                self._log.info(f"Marked batch {batch_id} as completed (all files processed)")

        return import_results

    def run(self) -> ImportResults:
        try:
            return self.process_files()
        except KeyboardInterrupt:
            self._log.error(f"Keyboard interrupt... exiting")
            sys.exit(1)


