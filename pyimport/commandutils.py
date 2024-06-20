import argparse
import logging
import os
import sys
from datetime import datetime, timezone
import time

import pymongo
from pymongo import errors

from pyimport import timer
from pyimport.command import seconds_to_duration
from pyimport.csvreader import CSVReader
from pyimport.enrichtypes import EnrichTypes
from pyimport.fieldfile import FieldFile
from pyimport.linereader import RemoteLineReader, is_url
from pyimport.logger import Logger
from pyimport.doctimestamp import DocTimeStamp


def prep_field_file(csv_filenames, field_filename: str) -> FieldFile:
    log = logging.getLogger(__name__)
    if field_filename is None:
        field_filename = FieldFile.make_default_tff_name(csv_filenames[0])

    if not os.path.isfile(field_filename):
        raise OSError(f"No such field file:'{field_filename}'")

    field_file = FieldFile.load(field_filename)
    log.info(f"Using field file:'{field_filename}'")
    return field_file


def prep_import(args: argparse.Namespace, filename: str, fieldinfo: FieldFile):

    if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
        client = pymongo.MongoClient(args.host, w=args.writeconcern)
    else:
        client = pymongo.MongoClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

    database = client[args.database]
    collection = database[args.collection]

    if args.addtimestamp == DocTimeStamp.DOC_TIMESTAMP:
        ts_func = args.addtimestamp
    elif args.addtimestamp == DocTimeStamp.BATCH_TIMESTAMP:
        ts_func = datetime.now(timezone.utc)
    else:
        ts_func = None

    parser = EnrichTypes(fieldinfo,
                         locator=args.locator,
                         timestamp_func=ts_func,
                         onerror=args.onerror,
                         filename=filename)

    return collection, parser


def process_file(args, log,  collection, parser, field_info:FieldFile, filename):

    time_start = time.time()
    time_period = 1.0
    insert_list = []
    inserted_this_quantum = 0
    total_written = 0

    if is_url(filename):
        log.info(f"Reading from URL:'{filename}'")
        csv_file = RemoteLineReader(url=filename)
    else:
        log.info(f"Reading from file:'{filename}'")
        csv_file = open(filename, "r")

    try:
        reader = CSVReader(file=csv_file,
                           limit=args.limit,
                           field_file=field_info,
                           has_header=args.hasheader,
                           delimiter=args.delimiter)

        loop_timer = timer.Timer(start_now=True)
        for i, doc in enumerate(reader, 1):
            d = parser.enrich_doc(doc, i)
            insert_list.append(d)
            if len(insert_list) >= args.batchsize:
                collection.insert_many(insert_list)
                inserted_this_quantum = inserted_this_quantum + len(insert_list)
                total_written = total_written + len(insert_list)
                insert_list = []
                elapsed = loop_timer.elapsed()
                if elapsed >= time_period:
                    docs_per_second = inserted_this_quantum / elapsed
                    loop_timer.reset()
                    inserted_this_quantum = 0
                    log.info(f"Input:'{filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")
    finally:
        if not is_url(filename):
            csv_file.close()
    if len(insert_list) > 0:
        try:
            collection.insert_many(insert_list)
            total_written = total_written + len(insert_list)
            log.info("Input: '%s' : Inserted %i records", filename, total_written)
        except errors.BulkWriteError as e:
            log.error(f"pymongo.errors.BulkWriteError: {e.details}")
            log.error(f"Aborting due to database write errors...")
            sys.exit(1)

    time_finish = time.time()
    elapsed_time = time_finish - time_start
    log.info(f"imported file: '{i}' ({total_written} rows)")
    log.info(f"Total elapsed time to upload '{i}' : {seconds_to_duration(elapsed_time)}")
    log.info(f"Average upload rate per second: {round(total_written / elapsed_time)}")
    return total_written, elapsed_time

