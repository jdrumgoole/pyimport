#!/usr/bin/env python3

"""
Created on 19 Feb 2016

@author: jdrumgoole
"""

import argparse
import logging
import os
import sys
import time

from pyimport.argparser import add_standard_args
from pyimport.asyncimport import AsyncImportCommand
from pyimport.audit import Audit
from pyimport.command import seconds_to_duration
from pyimport.filesplitter import split_files
from pyimport.generatefieldfilecommand import GenerateFieldfileCommand
from pyimport.dropcollectioncommand import DropCollectionCommand
from pyimport.importcommand import ImportCommand
from pyimport.logger import Logger
from pyimport.fieldfile import FieldFile
from pyimport.multiimportcommand import MultiImportCommand
from pyimport.threadimportcommand import ThreadImportCommand


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

    audithost = os.getenv("AUDITHOST", "mongodb://localhost:27017")
    mdbhost = os.getenv("MDB_HOST", "mongodb://localhost:27017")
    parser = argparse.ArgumentParser(usage=usage_message)
    parser = add_standard_args(parser, mdbhost=mdbhost,  audithost=audithost)
    splits = []

    log = logging.getLogger(__name__)
    if input_args:
        cmd = input_args
        args = parser.parse_args(cmd)
    else:
        cmd = tuple(sys.argv[1:])
        args = parser.parse_args(cmd)

    log = Logger(args.logname, args.loglevel).log()
    #log.addHandler(logging.StreamHandler(sys.stdout))

    if not args.silent:
        Logger.add_stream_handler(args.logname)

    if args.filelist:
        try:
            with open(args.filelist) as input_file:
                for line in input_file.readlines():
                    args.filenames.append(line)
        except OSError as e:
            log.import_error(f"{e}")

    try:
        if args.filenames is None:
            log.info("No input files: Nothing to do")
            return 0

        if args.drop:
            if args.restart:
                log.info("Warning --restart overrides --drop ignoring drop commmand")
            else:
                DropCollectionCommand(args=args).run()

        if args.fieldinfo:
            cfg = FieldFile(args.fieldinfo)
            for i,field in enumerate(cfg.fields(), 1 ):
                print(f"{i:3}. {field:25}:{cfg.type_value(field)}")
            print(f"Total fields: {len(cfg.fields())}")

        if args.splitfile: # we replaces the filenames if we are autosplitting
            splits = split_files(args)
            split_files_list = [split[0] for split in splits]

        if args.genfieldfile:
            args.has_header = True
            log.info('Forcing has_header true for --genfieldfile')
            GenerateFieldfileCommand(args=args).run()

        if not args.genfieldfile:
            if args.filenames:
                start_time = time.time()
                if args.splitfile:
                    args.filenames = split_files_list  # use the split files for processing
                    args.hasheader = False
                if args.multi:
                    results = MultiImportCommand(args).run()
                elif args.threads:
                    results = ThreadImportCommand(args).run()
                elif args.asyncpro:
                    results = AsyncImportCommand(args).run()
                else:
                    results = ImportCommand(args).run()
                end_time = time.time()
                elapsed = end_time - start_time
                log.info(f"Total elapsed time to upload all files : {seconds_to_duration(elapsed)} seconds")
                log.info(f"Average upload rate per second: {round(results.total_written / elapsed)}")
                log.info(f"Total records written: {results.total_written}")
            else:
                log.warning("No input files: Nothing to do")
    except KeyboardInterrupt:
        log.import_error("Keyboard interrupt... exiting")
    finally:
        if len(splits) > 0 and args.keepsplits is False:
            for filename, _ in splits:
                os.unlink(filename)
            log.info(f"Deleted split files: {[filename for filename, _ in splits]}")

    return 1


if __name__ == '__main__':
    pyimport_main()
