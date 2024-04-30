"""
@author: jdrumgoole
"""
import argparse
import multiprocessing
import os
import sys
import time
from collections import OrderedDict
from multiprocessing import Process

import pymongo

from pyimport.argparser import add_standard_args
from pyimport.audit import Audit
from pyimport.logger import Logger
from pyimport.pyimport_main import Importer


def strip_arg(arg_list, remove_arg, has_trailing=False):
    """
    Remove arg and arg argument from a list of args. If has_trailing is true then
    remove --arg value else just remove --arg.

    Args:

    arg_list (list) : List of args that we want to remove items from
    remove_arg (str) : Name of arg to remove. Must match element in `arg_list`.
    has_trailing (boolean) : If the arg in `remove_arg` has an arg. Then make sure
    to remove that arg as well
    """
    try:
        location = arg_list.index(remove_arg)
        if has_trailing:
            del arg_list[location + 1]
        del arg_list[location]

    except ValueError:
        pass

    return arg_list


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def multi_import(*argv):
    """
.. function:: multi_import ( *argv )

   Import CSV files using multiprocessing

   :param argv: list of command lines

   """

    usage_message = '''
    
    A master script to manage uploading of a single data file as multiple input files. Multi-import
    will optionally split a single file (specified by the --single argument) or optionally upload an
    already split list of files passed in on the command line.
    Each file is uplaoded by a separate pyimport subprocess. 
    '''

    parser = argparse.ArgumentParser(usage=usage_message)
    parser = add_standard_args(parser)
    parser.add_argument("--poolsize", type=int, default=multiprocessing.cpu_count(),
                        help="The number of parallel processes to run")
    parser.add_argument("--forkmethod", choices=["spawn", "fork", "forkserver"], default="spawn",
                        help="The model used to define how we create subprocesses (default:'spawn')")

    args = parser.parse_args(*argv)

    multiprocessing.set_start_method(args.forkmethod)

    log = Logger("multi_import").log()

    Logger.add_file_handler("multi_import")
    Logger.add_stream_handler("multi_import")

    child_args = sys.argv[1:]
    children = OrderedDict()

    log.info("filenames:%s", args.filenames)
    if len(args.filenames) == 0:
        log.info("no input files")
        sys.exit(0)

    if args.poolsize:
        poolsize = args.poolsize
        child_args = strip_arg(child_args, "--poolsize", True)

    if args.restart:
        log.info("Ignoring --drop overridden by --restart")
    elif args.drop:
        client = pymongo.MongoClient(args.host)
        log.info("Dropping database : %s", args.database)
        client.drop_database(args.database)
        child_args = strip_arg(child_args, args.drop)

    if args.audit:
        audit = Audit(client)
        batch_ID = audit.start_batch({"command": sys.argv})
    else:
        audit = None
        batch_ID = None

    start = time.time()

    process_count = 0
    log.info("Poolsize:{}".format(poolsize))

    log.info("Fork using:'%s'", args.forkmethod)
    subprocess = Importer(audit=audit, batch_ID=batch_ID, args=args)
    subprocess.setup_log_handlers()

    try:

        #
        # Should use a Pool here but Pools need top level functions which is
        # ugly.
        #
        proc_list = []

        for arg_list in chunker(args.filenames, poolsize):  # blocks of poolsize
            proc_list = []
            for i in arg_list:
                if os.path.isfile(i):
                    log.info(f"Processing:'{i}'")
                    proc = Process(target=subprocess.run, args=(i,), name=i)
                    proc.start()
                    proc_list.append(proc)
                else:
                    log.warning(f"No such file: '{i}' ignoring")

            for proc in proc_list:
                proc.join()

    except KeyboardInterrupt:
        log.info("Keyboard interrupt...")
        for i in proc_list:
            log.info("terminating process: '%s'", proc_list[i].name)
            proc_list[i].terminate()

    finish = time.time()

    log.info("Total elapsed time:%f" % (finish - start))


if __name__ == '__main__':
    multi_import(sys.argv[1:])
