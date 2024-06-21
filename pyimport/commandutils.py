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
from pyimport.command import seconds_to_duration
from pyimport.csvreader import CSVReader
from pyimport.doctimestamp import DocTimeStamp
from pyimport.enrichtypes import EnrichTypes
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.linereader import RemoteLineReader, is_url


def time_stamp(d):
    d["timestamp"] = datetime.now(timezone.utc)
    return d


def batch_time_stamp(self, d):
    d["timestamp"] = self._batch_timestamp
    return d


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


class ImportResults:
    def __init__(self, total_written, elapsed_time, filename, error=None):
        self._filename = filename
        self._total_written = total_written
        self._elapsed_time = elapsed_time
        self._error = error

    @classmethod
    def error(cls, filename, error):
        return cls(None, None, filename, error)

    def __str__(self):
        return f"Total written:{self.total_written}, Elapsed time:{seconds_to_duration(self.elapsed_time)}"

    @property
    def total_written(self):
        if self._error:
            return None
        else:
            return self._total_written

    @property
    def elapsed_time(self):
        if self._error:
            return None
        else:
            return self._elapsed_time

    @property
    def filename(self):
        if self._error:
            return None
        else:
            return self._filename


def print_args(log, args):
    log.info(f"Using host       :'{args.host}'")
    log.info(f"Using database   :'{args.database}'")
    log.info(f"Using collection :'{args.collection}'")
    log.info(f"Write concern    : {args.writeconcern}")
    log.info(f"journal          : {args.journal}")
    log.info(f"fsync            : {args.fsync}")
    log.info(f"has header       : {args.hasheader}")


def prep_field_file(args) -> FieldFile:
    log = logging.getLogger(__name__)
    if args.fieldfile is None:
        field_filename = FieldFile.make_default_tff_name(args.filenames[0])
    else:
        field_filename = args.fieldfile

    if not os.path.isfile(field_filename):
        raise OSError(f"No such field file:'{field_filename}'")

    field_file = FieldFile.load(field_filename)
    log.info(f"Using field file:'{field_filename}'")
    return field_file


def prep_parser(args, field_info, filename) -> EnrichTypes:
    if args.addtimestamp == DocTimeStamp.DOC_TIMESTAMP:
        ts_func = args.addtimestamp
    elif args.addtimestamp == DocTimeStamp.BATCH_TIMESTAMP:
        ts_func = datetime.now(timezone.utc)
    else:
        ts_func = None

    parser = EnrichTypes(field_info, locator=args.locator, timestamp_func=ts_func, onerror=args.onerror,
                         filename=filename)
    return parser


def prep_database(args) -> pymongo.database.Database:
    if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
        client = pymongo.MongoClient(args.host, w=args.writeconcern)
    else:
        client = pymongo.MongoClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

    database = client[args.database]
    return database


def prep_collection(args) -> pymongo.collection.Collection:
    database = prep_database(args)
    collection = database[args.collection]
    return collection


def prep_import(log, args: argparse.Namespace, filename: str, field_info: FieldFile):
    collection = prep_collection(args)
    parser = prep_parser(args, field_info, filename)

    if is_url(filename):
        log.info(f"Reading from URL:'{filename}'")
        csv_file = RemoteLineReader(url=filename)
    else:
        log.info(f"Reading from file:'{filename}'")
        csv_file = open(filename, "r")

    reader = CSVReader(file=csv_file, limit=args.limit, field_file=field_info, has_header=args.hasheader,
                       delimiter=args.delimiter)

    return collection, reader, parser


def process_file(log, args, filename):
    # time_start = time.time()
    time_period = 1.0
    buffer = []
    inserted_this_quantum = 0
    total_written = 0

    field_file = prep_field_file(args)
    collection, reader, parser = prep_import(log, args, filename, field_file)
    time_start = time.time()
    try:
        loop_timer = timer.Timer(start_now=True)
        for i, doc in enumerate(reader, 1):
            d = parser.enrich_doc(doc, i)
            buffer.append(d)
            if len(buffer) >= args.batchsize:
                collection.insert_many(buffer)
                inserted_this_quantum = inserted_this_quantum + len(buffer)
                total_written = total_written + len(buffer)
                buffer = []
                elapsed = loop_timer.elapsed()
                if elapsed >= time_period:
                    docs_per_second = inserted_this_quantum / elapsed
                    loop_timer.reset()
                    inserted_this_quantum = 0
                    log.info(f"Input:'{filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")
    finally:
        if not is_url(filename):
            reader.file.close()
    if len(buffer) > 0:
        try:
            collection.insert_many(buffer)
            total_written = total_written + len(buffer)
            log.info("Read: '%s' : Inserted %i records", filename, total_written)
        except errors.BulkWriteError as e:
            log.error(f"pymongo.errors.BulkWriteError: {e.details}")
            log.error(f"Aborting due to database write errors...")
            sys.exit(1)
    time_finish = time.time()
    elapsed_time = time_finish - time_start
    log.info(f"imported file: '{filename}' ({total_written} rows)")
    log.info(f"Total elapsed time to upload '{filename}' : {seconds_to_duration(elapsed_time)}")
    log.info(f"Average upload rate per second: {round(total_written / elapsed_time)}")

    return total_written, elapsed_time


def process_files(log, args, audit):
    total_written = 0
    elapsed_time = 0
    for filename in args.filenames:
        log.info(f"Processing:'{filename}'")
        try:
            total_written_this_file, elapsed_time = process_file(log, args, filename)
            total_written = total_written + total_written_this_file
            if audit:
                audit_doc = {"command": "import", "filename": filename, "elapsed_time": elapsed_time,
                             "total_written": total_written_this_file}
                audit.add_batch_info(audit.current_batch_id, audit_doc)
        except OSError as e:
            log.error(f"{e}")
        except exceptions.HTTPError as e:
            log.error(f"{e}")
        except FieldFileException as e:
            log.error(f"{e}")
        except _csv.Error as e:
            log.error(f"{e}")
        except ValueError as e:
            log.error(f"{e}")
        except KeyboardInterrupt:
            log.error(f"Keyboard interrupt... exiting")
            sys.exit(1)

    return total_written, elapsed_time
