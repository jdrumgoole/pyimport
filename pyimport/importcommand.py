import _csv
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pymongo
from pymongo import errors
from requests import exceptions

from pyimport import timer
from pyimport.audit import Audit
from pyimport.command import seconds_to_duration
from pyimport.csvreader import CSVReader
from pyimport.mdbwriter import MDBWriter
from pyimport.doctimestamp import DocTimeStamp
from pyimport.enricher import Enricher
from pyimport.fieldfile import FieldFileException, FieldFile
from pyimport.filereader import FileReader
from pyimport.importresult import ImportResult, ImportResults
from pyimport.linereader import is_url, RemoteLineReader
from pyimport.logger import Log, eh
from pyimport.version import __VERSION__


class ImportCommand:

    def __init__(self, args):

        self._args = args
        self._log = Log().log
        if args.audit:
            self._audit = Audit(args.audithost, database_name=args.auditdatabase, collection_name=args.auditcollection)
        else:
            self._audit = None

    @staticmethod
    def parse_new_field(new_field):
        if new_field:
            k, v = new_field.split("=")
            return k.strip(), v.strip()
        else:
            return None

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

    @staticmethod
    def prep_field_file(args, csv_filename=None) -> FieldFile:
        """
        Prepares the field file for the import process. If a field file is specified it is loaded. If not, a field file
        is autogenerated from the csv file. If the CSV file has not header line, a FieldFileException is raised.
        Args:
            args: list of command line args
            csv_filename: the input CSV file.

        Returns:
            FieldFile object
        """

        if args.fieldfile:
            if os.path.isfile(args.fieldfile):
                field_file = FieldFile.load(args.fieldfile)
            else:
                raise FieldFileException(f"No such field file:'{args.fieldfile}'")
        else:
            field_filename = FieldFile.make_default_tff_name(args.filenames[0])
            if os.path.isfile(field_filename):
                field_file = FieldFile.load(field_filename)
            elif FileReader.sniff_header(csv_filename):
                field_file = FieldFile.generate_field_file(csv_filename=csv_filename,
                                                           ff_filename=field_filename,
                                                           delimiter=args.delimiter,
                                                           has_header=args.hasheader)
                eh.info(f"Auto generated field file:'{field_filename}'")
            else:
                raise FieldFileException(f"'{csv_filename}' has no header line, so we cannot generate a field file")

        return field_file

    @staticmethod
    def prep_parser(args, field_info, filename) -> Enricher:
        if args.addtimestamp == DocTimeStamp.DOC_TIMESTAMP:
            ts_func = args.addtimestamp
        elif args.addtimestamp == DocTimeStamp.BATCH_TIMESTAMP:
            ts_func = datetime.now(timezone.utc)
        else:
            ts_func = None

        parser = Enricher(field_info, locator=args.locator, timestamp_func=ts_func, onerror=args.onerror,
                          filename=filename)
        return parser

    @staticmethod
    def prep_database(args) -> pymongo.database.Database:
        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            client = pymongo.MongoClient(args.mdburi, w=args.writeconcern)
        else:
            client = pymongo.MongoClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)
        database = client[args.database]
        return database

    @staticmethod
    def prep_collection(args) -> pymongo.collection.Collection:
        database = ImportCommand.prep_database(args)
        collection = database[args.collection]
        return collection

    @staticmethod
    def prep_csv_reader(args, filename, field_info) -> CSVReader:
        if is_url(filename):
            csv_file = RemoteLineReader(url=filename)
        else:
            csv_file = open(filename, "r")

        return CSVReader(file=csv_file, limit=args.limit, field_file=field_info,
                         has_header=args.hasheader,
                         cut_fields=args.cut,
                         delimiter=args.delimiter)

    @staticmethod
    def prep_import(args: argparse.Namespace, filename: str, field_info: FieldFile):
        collection = ImportCommand.prep_collection(args)
        parser = ImportCommand.prep_parser(args, field_info, filename)

        reader = ImportCommand.prep_csv_reader(args, filename, field_info)

        return collection, reader, parser

    @staticmethod
    def process_mode(args):
        mode = {}
        if args.multi or args.asyncpro or args.threads:
            if args.multi:
                mode["multi"] = args.poolsize
            if args.asyncpro:
                mode["asyncpro"] = True
            if args.threads:
                mode["threads"] = args.threads
        else:
            mode["single"] = True
        return mode

    def report_process_one_file(self, args, result):
        audit_doc = None
        if self._audit:
            audit_doc = {"command": "process one file",
                         "version": __VERSION__,
                         "filename": result.filename,
                         "elapsed_time": result.elapsed_time,
                         "total_written": result.total_written,
                         "mode": ImportCommand.process_mode(args),
                         "avg_records_per_sec": result.avg_records_per_sec,
                         "cmd_line": " ".join(sys.argv)}

            self._audit.add_batch_info(audit_doc)
        return audit_doc

    def report_process_files(self, args, results: ImportResults):

        audit_doc = None
        if self._audit:
            audit_doc = {"command": "process files",
                         "filenames": results.filenames,
                         "elapsed_time": results.elapsed_time,
                         "total_written": results.total_written,
                         "avg_records_per_sec": results.avg_records_per_sec,
                         "mode": ImportCommand.process_mode(args),
                         "cmd_line": " ".join(sys.argv)}
            self._audit.add_batch_info(audit_doc)
        return audit_doc

    @staticmethod
    def process_one_file(args, log, filename) -> ImportResult:
        time_period = 1.0
        field_file = ImportCommand.prep_field_file(args, filename)
        collection, reader, parser = ImportCommand.prep_import(args, filename, field_file)
        time_start = time.time()
        writer = MDBWriter(args)
        try:
            new_field = ImportCommand.parse_new_field(args.addfield)
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
                result = ImportCommand.process_one_file(self._args, self._log, filename)
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


