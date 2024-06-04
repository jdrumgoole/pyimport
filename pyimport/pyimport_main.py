#!/usr/bin/env python3

"""
Created on 19 Feb 2016

@author: jdrumgoole
"""

import argparse
import os
import sys
from multiprocessing import Process
import logging

import pymongo
from requests import exceptions

from pyimport.argparser import add_standard_args
from pyimport.audit import Audit
from pyimport.command import GenerateFieldfileCommand
from pyimport.dropcollectioncommand import DropCollectionCommand
from pyimport.importcommand import ImportCommand
from pyimport.logger import Logger
from pyimport.fieldfile import FieldFile


# class Importer(object):
#
#     def __init__(self, audit, batch_ID, args):
#
#         self._audit = audit
#         self._batch_ID = batch_ID
#         self._log = logging.getLogger(__name__)
#         self._host = args.host
#         self._write_concern = args.writeconcern
#         self._fsync = args.fsync
#         self._journal = args.journal
#         self._audit = args.audit
#         self._database_name = args.database
#         self._collection_name = args.collection
#         self._collection = None
#         self._field_filename = args.fieldfile
#         self._has_header = args.hasheader
#         self._delimiter = args.delimiter
#         self._onerror = args.onerror
#         self._limit = args.limit
#         self._locator = args.locator
#         self._timestamp = args.addtimestamp
#         self._locator = args.locator
#         self._args = args
#         self._batch_size = args.batchsize
#
#     def setup_log_handlers(self):
#         self._log = Logger(self._args.logname, self._args.loglevel).log()
#
#         Logger.add_file_handler(self._args.logname)
#
#         if not self._args.silent:
#             Logger.add_stream_handler(self._args.logname)
#
#     def run(self, filename):
#         if not self._log:
#             self._log = Logger(self._args.logname, self._args.loglevel).log()
#
#         self._log.info("Started pyimport")
#
#         if self._field_filename is None:
#             self._field_filename = FieldFile.make_default_tff_name(filename)
#
#         if self._write_concern == 0:  # pymongo won't allow other args with w=0 even if they are false
#             client = pymongo.MongoClient(self._host, w=self._write_concern)
#         else:
#             client = pymongo.MongoClient(self._host, w=self._write_concern, fsync=self._fsync, j=self._journal)
#
#         database = client[self._database_name]
#         self._collection = database[self._collection_name]
#
#         self._log.info(f"Write concern : {self._write_concern}")
#         self._log.info(f"journal       : {self._journal}")
#         self._log.info(f"fsync         : {self._fsync}")
#         self._log.info(f"has header    : {self._has_header}")
#
#         cmd = ImportCommand(collection=self._collection,
#                             field_filename=self._field_filename,
#                             delimiter=self._delimiter,
#                             has_header=self._has_header,
#                             onerror=self._onerror,
#                             limit=self._limit,
#                             audit=self._audit,
#                             locator=self._locator,
#                             timestamp=self._timestamp,
#                             id=self._batch_ID,
#                             batch_size=self._batch_size)
#
#         cmd.run(filename)
#
#         return 1

    # def process_batch(self, pool_size, files):
    #
    #     procs = []
    #     for f in files[:pool_size]:
    #         self._log.info("Processing:'%s'", f)
    #         proc = Process(target=self.run, args=(f,), name=f)
    #         proc.start()
    #         procs.append(proc)
    #
    #     for p in procs:
    #         p.join()
    #
    #     return files[pool_size:]


def pyimport_main(input_args=None):
    """
    Expect to recieve an array of args
    
    1.3 : Added lots of support for the NHS Public Data sets project. --addfilename and --addtimestamp.
    Also we now fail back to string when type conversions fail.
    
    >>> pyimport_main( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    """

    usage_message = """
    
    pyimport is a python program that will import data into a mongodb
    database (default 'test' ) and a mongodb collection (default 'test' ).
    
    Each file in the input list must correspond to a fieldfile format that is
    common across all the files. The fieldfile is specified by the 
    --fieldfile parameter.
    
    An example run:
    
    python pyimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
    """

    # if input_args:
    #     print("args: {}".format( " ".join(input_args)))

    parser = argparse.ArgumentParser(usage=usage_message)
    parser = add_standard_args(parser)
    # print( "Argv: %s" % argv )
    # print(argv)

    if input_args:
        cmd = input_args
        args = parser.parse_args(cmd)
    else:
        cmd = tuple(sys.argv[1:])
        args = parser.parse_args(cmd)
        cmd_args = " ".join(cmd)
    # print("args: %s" % args)

    log = Logger(args.logname, args.loglevel).log()

    # Logger.add_file_handler(args.logname)

    if not args.silent:
        Logger.add_stream_handler(args.logname)

    #print(args.filenames)

    if args.filelist:
        try:
            with open(args.filelist) as input_file:
                for line in input_file.readlines():
                    args.filenames.append(line)
        except OSError as e:
            log.error(f"{e}")

    if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
        client = pymongo.MongoClient(args.host, w=args.writeconcern)
    else:
        client = pymongo.MongoClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

    if args.genfieldfile:
        args.has_header = True
        log.info('Forcing has_header true for --genfieldfile')
        GenerateFieldfileCommand(args.audit).run(args)

    if args.audit:
        audit = Audit(client=client)
        batch_id = audit.start_batch({"command": input_args})
    else:
        audit = None
        batch_id = None

    if args.database:
        database_name = args.database
    else:
        database_name = "PYIM"

    if args.collection:
        collection_name = args.collection
    else:
        collection_name = "ported"

    database = client[database_name]
    collection = database[collection_name]

    if args.drop:
        if args.restart:
            log.info("Warning --restart overrides --drop ignoring drop commmand")
        else:
            cmd = DropCollectionCommand(audit=audit, id=batch_id).run(database, args)

    if args.fieldinfo:
        cfg = FieldFile(args.fieldinfo)

        for i,field in enumerate(cfg.fields(), 1 ):
            print(f"{i:3}. {field:25}:{cfg.type_value(field)}")
        print(f"Total fields: {len(cfg.fields())}")

    if not args.genfieldfile:
        if args.filenames:
            if args.audit:
                audit = Audit(client=client)
                batch_id = audit.start_batch({"command": sys.argv})
            else:
                audit = None
                batch_id = None

            ImportCommand(audit, batch_id, args).run(args)

            if args.audit:
                audit.end_batch(batch_id)
        else:
            log.info("No input files: Nothing to do")

    return 1


if __name__ == '__main__':
    pyimport_main()
