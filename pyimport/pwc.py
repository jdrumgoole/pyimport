"""
=======================================
pwc - python word count
=======================================
Created on 27 Aug 2017

A program to count lines as opposed to \n characters. The *wc* program will often miss
the last line of programs that do not terminate their last line with a \n.

This uses the Python readline() function to count lines correctly and opens files
in universal mode.

@author: jdrumgoole
"""

import argparse
import os
import sys

from pyimport.filesplitter import LineCounter


def pwc(*argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help='list of files')
    args = parser.parse_args(*argv)

    total_count = 0
    total_size = 0
    if args.filenames:
        print("lines\tbytes\tfilename")
    for filename in args.filenames:
        lines_this_file = LineCounter.count_now(filename)
        size_this_file = os.path.getsize(filename)
        total_count = total_count + lines_this_file
        total_size = total_size + size_this_file

        print("%i\t%i\t%s" % (lines_this_file, size_this_file, filename))
    if len(args.filenames) > 1:
        print("%i\t%i\ttotal" % (total_count, total_size))


if __name__ == "__main__":
    pwc(sys.argv[1:])
