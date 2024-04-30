"""
Created on 12 Aug 2017

@author: jdrumgoole
"""
from pyimport.logger import Logger
from pyimport.version import __VERSION__
from pyimport.csvlinetodictparser import ErrorResponse
from pyimport.doctimestamp import DocTimeStamp
from configargparse import ArgumentParser


def add_standard_args(parser):
    """
    Construct parser for pyimport return it as a list suitable for passing to the parents
    argument of the next parser
    """

    parser.add_argument('-v", ''--version', action='version', version='%(prog)s ' + __VERSION__)
    parser.add_argument('--database', default="PYIM", help='specify the database filename [default: %(default)s]')
    parser.add_argument('--collection', default="imported", help='specify the collection filename [default: %(default)s]')
    parser.add_argument('--host', default="mongodb://localhost:27017/test",
                        help='mongodb URI. [default: %(default)s]')
    parser.add_argument('--locator', default=False, action="store_true",
                        help="add a locator field consisting of filename and \
                        input record line to each doc [default: %(default)s]")
    parser.add_argument('--batchsize', type=int, default=1000,
                        help='set mongodb batch size for bulk inserts [default: %(default)s]')
    parser.add_argument('--restart', default=False, action="store_true",
                        help="use record thread_id insert to restart at last write also enable restart logfile [default: %(default)s]")
    parser.add_argument('--drop', default=False, action="store_true",
                        help="drop collection before loading [default: %(default)s]")
    #parser.add_argument('--ordered', default=False, action="store_true", help="forced ordered inserts")
    parser.add_argument("--fieldfile", default=None, type=str, help="Field and type mappings")
    parser.add_argument("--delimiter", default=",", type=str,
                        help="The delimiter string used to split fields [default: %(default)s]")
    parser.add_argument("filenames", nargs="*", help='list of files')
    parser.add_argument("--filelist", default=None, help="Read files from an input file one per line")
    parser.add_argument('--addfilename', default=False, action="store_true", help="Add file filename field to every entry")
    parser.add_argument('--addtimestamp', default=DocTimeStamp.NO_TIMESTAMP, type=DocTimeStamp, choices=list(DocTimeStamp),
                        help="Add a timestamp to each doc, either generate per doc('doc'), or per batch {'batch') [default: %(default)s]")
    parser.add_argument('--hasheader', default=False, action="store_true",
                        help="Use header line for column names [default: %(default)s]")
    parser.add_argument('--genfieldfile', default=False, action="store_true",
                        help="Generate a fieldfile from the data file, we set has_header to true [default: %(default)s]")
    parser.add_argument('--onerror', type=ErrorResponse,  default=ErrorResponse.Warn, choices=list(ErrorResponse),
                        help="What to do when we hit an error parsing a csv file [default: %(default)s]")
    parser.add_argument('--logname', default=Logger.LOGGER_NAME,
                        help="Logfile to write output to [default: %(default)s]")
    parser.add_argument('--loglevel', default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
                        help='Logging level [default: %(default)s]')
    parser.add_argument('--silent', default=False, action="store_true",
                        help="Suspend output except for log file [default: %(default)s]")
    parser.add_argument('--writeconcern', default=0, type=int,
                        help="specify write concern for a write operation [default: %(default)s]")
    parser.add_argument('--journal', default=False, action="store_true",
                        help="Turn on journaling [default: %(default)s]")
    parser.add_argument('--fsync', default=False, action="store_true",
                        help="Sync all nodes to disk [default: %(default)s]")
    parser.add_argument('--audit', action="store_true", default=False, help="Capture audit records for an upload")
    parser.add_argument('--info', default="", help="Info string to be added to audit record")
    # parser.add_argument('--tag', default=False, action="store_true", help="Tag each record with filename:<record number>")

    parser.add_argument("--fieldinfo", default=None, type=str,
                        help="Report field info from a named field file e.g. --fieldinfo <filename>.tff")
    parser.add_argument("--limit", default=0, type=int,
                        help="Limit the number of records we read in (0 means read all records) [default: %(default)s]")
    #
    # Also try ISO-8859-1
    #
    # parser.add_argument( '--encoding', default="utf-8", help="Unicode encoding for input file [default: %(default)s]")
    return parser


if __name__ == "__main__":

    parser = ArgumentParser()
    parser = add_standard_args(parser)
    args = parser.parse_args()
    print(args)
