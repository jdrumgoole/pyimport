"""
==================================================
splitfile : Split a file into separate pieces
==================================================
Files can be split on
preset line boundaries using **--splitsize** or split automatically
into a preset number of pieces using **--autosplit**.

We include the **--has_header** for use with csv files as we don't want
to include the header line in any of the input files.

**--autosplit** *<number of splits*
    Split a file into several chunks by looking at the first ten lines
    and using that to work out the average line os_size. We then use that os_size
    to determine how many lines each chunk needs to have to return *<number of splits>*
    splits.

**--has_header**
    lets the program know this file has a header line. We do not include
    header lines in any of the split file outputs.

**--splitsize** *<no of lines>*
    Split a file into a specific number of chunks of os_size *<no of lines>*.

**filename**
    Name of file to split

Created on 11 Aug 2017

@author: jdrumgoole

"""
import argparse
import os
import sys

from pyimport.filesplitter import FileSplitter, split_files
from pyimport.version import __VERSION__


def split_file_main(*argv):
    usage_message = '''
    
Split a text file into seperate pieces. if you specify 
autosplit then the program will use the first ten lines 
to calcuate an average line os_size and use that to 
determine the rough number of splits.

if you use **--splitsize** then the file will be split 
using **--splitsize** chunks until it is consumed.
'''

    parser = argparse.ArgumentParser(usage=usage_message)

    parser.add_argument('-v", ''--version', action='version', version='%(prog)s ' + __VERSION__)
    parser.add_argument("--autosplit", default=2, type=int,
                        help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    parser.add_argument('--hasheader', default=False, action="store_true",
                        help="Ignore header when calculating splits, don't include header in output")
    parser.add_argument('--delimiter', default=",", help="Delimiter for fields[default : %(default)s] ")
    parser.add_argument("--splitsize", default=0, type=int, help="Split file into chunks of this size")
    parser.add_argument('--verbose', default=False, action="store_true",
                        help="Print out what is happening")
    parser.add_argument('--input', default=False, action="store_true",
                        help="Generate output for another program (list of args)")
    parser.add_argument("filenames", nargs="*", help='list of files')
    args = parser.parse_args(*argv)

    if len(args.filenames) == 0:
        print("No input file specified to split")
        sys.exit(0)

    return split_files(args)


if __name__ == '__main__':
    split_file_main(sys.argv[1:])
