"""
Created on 12 Aug 2017

@author: jdrumgoole
"""

import multiprocessing
import os
import sys

import configargparse

from pyimport.doctimestamp import DocTimeStamp
from pyimport.logger import ErrorResponse
from pyimport.version import __VERSION__

usage_message = """

pyimport is a python program that will import data into a mongodb collection. 
It can import a single file or a list of files. Large files can be split into smaller files 
for uploading in parallel. 

Each file in the input list must have a fieldfile format that is either specified by the
--fieldfile parameter or generated by the --genfieldfile parameter. The fieldfile is a TOML file.
if a fieldfile is not specified the program will look for a file of the same name a the source file but with
the extension  .tff. In no fieldfile is found the program will attempt to generate one from the data file and 
parse it using that file. So provide a field file if you have one, otherwise let the program generate one for you.

The program can read CSV files from a URL or a local file.

You can specify a different delimiter with --delimiter. The default is ','.

An example run:

pyimport --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
"""


class ParserError(Exception):
    pass


class CustomArgumentParser(configargparse.ArgumentParser):
    def error(self, message):
        # Override the error method to suppress the preamble
        # self.print_usage()
        raise ParserError(f"Error: {message}\n")


def default_config_files():
    home_dir = os.getenv("HOME")
    return [f"{home_dir}/.pyimport.conf", "pyimport.conf"]


def make_parser():
    return CustomArgumentParser(usage=usage_message, default_config_files=default_config_files())



def parse_args_and_cfg_files(cfgparser, input_args=None) -> configargparse.ArgumentParser:
    """
    Construct cfgparser for pyimport return it as a list suitable for passing to the parents
    argument of the next cfgparser
    """

    audit_host = "mongodb://localhost:27017"
    mdb_uri = "mongodb://localhost:27017"
    pg_host = "postgresql://localhost:5432/postgres"

    cfgparser.add_argument('-v", ''--version', action='version', version='%(prog)s ' + __VERSION__)

    cfgparser.add_argument('--locator', default=False, action="store_true",
                           help="add a locator field consisting of "
                                "filename and input record line to each doc [default: %(default)s]")
    cfgparser.add_argument('--batchsize', type=int, default=1000,
                           help='set mongodb batch size for bulk inserts [default: %(default)s]')
    cfgparser.add_argument('--restart', default=False, action="store_true",
                           help="use record thread_id insert to restart at last write"
                                "also enable restart logfile [default: %(default)s]")
    cfgparser.add_argument('--drop', default=False, action="store_true",
                           help="drop collection before loading [default: %(default)s]")
    # cfgparser.add_argument('--ordered', default=False, action="store_true", help="forced ordered inserts")
    cfgparser.add_argument("--fieldfile", default=None, type=str, help="Field and type mappings")
    cfgparser.add_argument("--delimiter", default=",", type=str,
                           help="The delimiter string used to split fields [default: %(default)s]")
    cfgparser.add_argument("filenames", nargs="*", help='list of files')
    cfgparser.add_argument("--filelist", default=None, help="Read files from an input file one per line")
    cfgparser.add_argument('--addfilename', default=False, action="store_true",
                           help="Add file filename field to every entry")
    cfgparser.add_argument('--addtimestamp', default=DocTimeStamp.NO_TIMESTAMP, type=DocTimeStamp,
                           choices=list(DocTimeStamp),
                           help="Add a timestamp to each doc, either generate per doc('doc'),"
                                " or per batch {'batch') [default: %(default)s]")
    cfgparser.add_argument('--hasheader', default=False, action="store_true",
                           help="Use header line for column names [default: %(default)s]")
    cfgparser.add_argument('--genfieldfile', default=False, action="store_true",
                           help="Generate a fieldfile from the data file, we set has_header to true [default: %(default)s]")
    cfgparser.add_argument('--onerror', type=ErrorResponse, default=ErrorResponse.Warn, choices=list(ErrorResponse),
                           help="What to do when we hit an error parsing a csv file [default: %(default)s]")
    # cfgparser.add_argument('--logname', default=Log.LOGGER_NAME,
    #                     help="Logfile to write output to [default: %(default)s]")
    cfgparser.add_argument('--loglevel', default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
                           help='Logging level [default: %(default)s]')
    cfgparser.add_argument('--silent', default=False, action="store_true",
                           help="Suspend output except for log file [default: %(default)s]")
    cfgparser.add_argument('--audit', action="store_true", default=False, help="Capture audit records for an upload")
    cfgparser.add_argument('--audithost', default=audit_host, help="Host for audit records [default: %(default)s]",
                           env_var="AUDIT_HOST")
    cfgparser.add_argument('--auditcollection', default="audit",
                           help="Collection for audit records [default: %(default)s]")
    cfgparser.add_argument('--auditdatabase', default="PYIMPORT_AUDIT",
                           help="Database for audit records [default: %(default)s]")
    cfgparser.add_argument('--info', default="", help="Info string to be added to audit record")
    # cfgparser.add_argument('--tag', default=False, action="store_true", help="Tag each record with filename:<record number>")
    cfgparser.add_argument('--noenrich', default=False, action="store_true",
                           help="Don't enrich records with type info from field file")
    cfgparser.add_argument("--fieldinfo", default=None, type=str,
                           help="Report field info from a named field file e.g. --fieldinfo <filename>.tff")
    cfgparser.add_argument("--limit", default=0, type=int,
                           help="Limit the number of records we read in (0 means read all records) [default: %(default)s]")
    #
    # MongoDB options
    #
    cfgparser.add_argument('--database', default="PYIM", help='specify the database filename [default: %(default)s]')
    cfgparser.add_argument('--collection', default="imported",
                           help='specify the collection filename [default: %(default)s]')
    cfgparser.add_argument('--mdburi', default=mdb_uri, help='mongodb URI. [default: %(default)s]', env_var="MDB_URI")
    cfgparser.add_argument('--writeconcern', default=0, type=int,
                           help="specify write concern for a write operation [default: %(default)s]")
    cfgparser.add_argument('--journal', default=False, action="store_true",
                           help="Turn on journaling [default: %(default)s]")
    cfgparser.add_argument('--fsync', default=False, action="store_true",
                           help="Sync all nodes to disk [default: %(default)s]")

    #
    # Postgres options
    #
    cfgparser.add_argument('--table', default="imported", help='specify the table name [default: %(default)s]')
    cfgparser.add_argument('--pguser', default="postgres", help='specify the postgres user [default: %(default)s]')
    cfgparser.add_argument('--pguri', default=pg_host, help='specify the postgres host [default: %(default)s]',
                           env_var="PG_URI")
    cfgparser.add_argument('--pgport', default=5432, type=int, help='specify the postgres port [default: %(default)s]')
    cfgparser.add_argument('--pgdatabase', default="postgres",
                           help='specify the postgres database [default: %(default)s]')
    cfgparser.add_argument('--pgpassword', help='specify the postgres password')

    #
    # Async and multiprocessing
    #
    cfgparser.add_argument("--asyncpro", default=False, action="store_true", help="Use async IO for processing files")
    cfgparser.add_argument("--multi", default=False, action="store_true",
                           help="Use multiprocessing for processing files")
    cfgparser.add_argument("--poolsize", type=int, default=multiprocessing.cpu_count(),
                           help="The number of parallel processes to run")
    cfgparser.add_argument("--forkmethod", choices=["spawn", "fork", "forkserver"], default="fork",
                           help="The model used to define how we create subprocesses (default:'spawn')")

    #
    # Splitfile
    #

    cfgparser.add_argument("--splitfile", default=False, action="store_true", help="Split file into chunks")
    cfgparser.add_argument("--autosplit", default=2, type=int,
                           help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    cfgparser.add_argument("--splitsize", default=1024 * 10, type=int,
                           help="Split file into chunks of this size [default: %(default)s (10k)]")
    cfgparser.add_argument('--verbose', default=False, action="store_true", help="Print out what is happening")
    cfgparser.add_argument('--input', default=False, action="store_true",
                           help="Generate output for another program (list of args)")
    cfgparser.add_argument("--threads", default=False, action="store_true",
                           help="Use threads to process the data --poolsize sets the no of threads")
    cfgparser.add_argument("--keepsplits", default=False, action="store_true",
                           help="Keep the split files after processing")

    #
    # Also try ISO-8859-1
    #
    # cfgparser.add_argument( '--encoding', default="utf-8", help="Unicode encoding for input file [default: %(default)s]")
    splits = []

    if input_args:
        cmd = input_args
        args = cfgparser.parse_args(cmd)
    else:
        cmd = tuple(sys.argv[1:])
        args = cfgparser.parse_args(cmd)

    return args


class ArgMgr:

    def __init__(self, ns: configargparse.Namespace):
        self._args = ns

    def merge_namespace(self, ns: configargparse.Namespace) -> configargparse.Namespace:
        merged = configargparse.Namespace()
        merged.__dict__.update(vars(self._args))
        merged.__dict__.update(vars(ns))
        self._args = merged
        return merged

    def merge(self, am: "ArgMgr") -> "ArgMgr":
        return ArgMgr(self.merge_namespace(am._args))

    def __len__(self):
        return len(vars(self._args))

    @property
    def d(self) -> dict:
        return vars(self._args)

    @property
    def ns(self) -> configargparse.Namespace:
        return self._args

    @classmethod
    def default_args(cls) -> "ArgMgr":
        p = make_parser()
        args = parse_args_and_cfg_files(p)
        return ArgMgr(args)

    @classmethod
    def args(cls, **kwargs) -> "ArgMgr":
        ns = configargparse.Namespace(**kwargs)
        return ArgMgr(ns)

    @staticmethod
    def default_args_dict() -> dict:
        return ArgMgr.ns_to_dict(ArgMgr.default_args())

    def add_arguments(self, **kwargs) -> "ArgMgr":
        new_ns = configargparse.Namespace(**kwargs)
        self.merge_namespace(new_ns)
        return self

    @staticmethod
    def dict_to_ns(d: dict) -> configargparse.Namespace:
        """
        Convert a dictionary to an configargparse.Namespace object.

        :param d: Dictionary to convert
        :return: Namespace object with attributes corresponding to dictionary keys and values
        """
        return configargparse.Namespace(**d)

    @staticmethod
    def ns_to_dict(namespace: configargparse.Namespace) -> dict:
        """
        Convert an configargparse.Namespace object to a dictionary.

        :param namespace: Namespace object to convert
        :return: Dictionary with keys and values corresponding to Namespace attributes
        """
        return vars(namespace)

    def __getitem__(self, key):
        return self._args.__dict__[key]

    def __setitem__(self, key, value):
        self._args.__dict__[key] = value


if __name__ == "__main__":
    home = os.getenv("HOME")
    parser = ArgumentParser(default_config_files=["pyimport.conf", "~/pyimport.conf", "~/.config/pyimport.conf"])
    parser = parse_args_and_cfg_files(parser)
    args = parser.parse_args()
    print(args)
