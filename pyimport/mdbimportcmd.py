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

    def print_args(self, args):
        self._log.info(f"Using host       :'{args.mdburi}'")
        if self._audit:
            self._log.info(f"Using audit host :'{args.audithost}'")
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
    def process_one_file(args, log, filename) -> ImportResult:
        time_period = 1.0
        field_file = ImportCommand.prep_field_file(args, filename)
        reader, parser = MDBImportCommand.prep_import(args, filename, field_file)
        time_start = time.time()
        writer = SyncMDBWriter(args)
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
        self.print_args(self._args)
        for filename in self._args.filenames:
            self._log.info(f"Processing:'{filename}'")
            try:
                result = MDBImportCommand.process_one_file(self._args, self._log, filename)
                self._log.info(f"imported file: '{filename}' ({result.total_written} rows)")
                self._log.info(f"Total elapsed time to upload '{filename}' : {result.elapsed_duration}")
                self._log.info(f"Average upload rate per second: {round(result.avg_records_per_sec)}")
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
        return import_results

    def run(self) -> ImportResults:
        try:
            return self.process_files()
        except KeyboardInterrupt:
            self._log.error(f"Keyboard interrupt... exiting")
            sys.exit(1)


