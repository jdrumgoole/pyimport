"""
Created on 27 Aug 2017

@author: jdrumgoole
"""

import os
import sys
import unittest
from contextlib import contextmanager
from io import StringIO

from pyimport.filesplitter import File_Splitter
from pyimport.splitfile import split_file_main


@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class TestSplitFile(unittest.TestCase):

    def setUp(self):
        self._dir = os.path.dirname(os.path.realpath(__file__))

    def tearDown(self):
        pass

    def _compare_input_output(self, input_filename, output_filenames, has_header=False):
        original_count = 0
        file_piece_count = 0
        with open(input_filename, "r") as original_file:
            if has_header:
                _ = original_file.readline()
            for filename in File_Splitter.shim_names(output_filenames):
                with open(filename, "r") as file_piece:
                    for line in file_piece:
                        left = original_file.readline()
                        original_count = original_count + 1
                        right = line
                        file_piece_count = file_piece_count + 1
                        self.assertEqual(left, right)
                os.unlink(filename)

    def test_auto_split(self):
        input_filename = "mot_test_set_small.csv"
        filenames = split_file_main(["--autosplit", "2", input_filename])
        self._compare_input_output(input_filename, filenames)

    def test_split_size(self):
        input_filename = "mot_test_set_small.csv"
        filenames = split_file_main(["--splitsize", "50", input_filename])
        self._compare_input_output(input_filename, filenames)
        filenames = split_file_main(["--splitsize", "1", input_filename])
        self._compare_input_output(input_filename, filenames)
        filenames = split_file_main(["--splitsize", "23", input_filename])
        self._compare_input_output(input_filename, filenames)

    def test_split_dos_file(self):
        filenames = split_file_main(["--autosplit", "4", "--hasheader", "yellow_tripdata_2015-01-06-200k.csv"])

        self._compare_input_output("yellow_tripdata_2015-01-06-200k.csv", filenames, has_header=True)


if __name__ == "__main__":
    unittest.main()
