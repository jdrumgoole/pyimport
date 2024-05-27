"""
Created on 13 Aug 2017

@author: jdrumgoole
"""
import os
import unittest

from pyimport.filesplitter import LineCounter, FileSplitter, FileType

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def setUp(self):
        self._dir = os.path.dirname(os.path.realpath(__file__))

    def f(self, path):
        return os.path.join(self._dir, path)

    def test_count_lines(self):
        self.assertEqual(3, FileSplitter("threelines.txt").line_count)
        self.assertEqual(0, FileSplitter("emptyfile.txt").line_count)
        self.assertEqual(4, FileSplitter("fourlines.txt").line_count)
        self.assertEqual(5, FileSplitter("inventory.csv").line_count)

    def _split_helper(self, filename, split_size, has_header=False, dos_adjust=False):

        splitter = FileSplitter(filename, has_header)

        count = 0
        part_total_size = 0
        part_total_count = 0

        for (part_name, line_count) in splitter.splitfile(split_size):
            splitter_part = FileSplitter(part_name)
            part_count = LineCounter(part_name).line_count
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            os.unlink(part_name)

        lc = LineCounter(filename)

        if has_header:
            self.assertEqual(part_total_count, lc.line_count - 1)
        else:
            self.assertEqual(part_total_count, lc.line_count)


    def test_split_file(self):
        self._split_helper("fourlines.txt", 3)
        self._split_helper("AandE_Data_2011-04-10.csv", 47, has_header=True, dos_adjust=True)
        self._split_helper("10k.txt", 2300, has_header=False)

    def _auto_split_helper(self, filename, lines, split_count, has_header=False, dos_adjust=False):

        splitter = FileSplitter(filename, has_header=has_header)
        part_total_count = 0
        total_line_count = splitter.line_count
        self.assertEqual(total_line_count, lines)
        for (part_name, line_count) in splitter.autosplit(split_count):
            part_count = LineCounter(part_name).line_count
            self.assertGreater(part_count, 0)
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            os.unlink(part_name)

        if has_header:
            self.assertEqual(part_total_count, lines - 1)
        else:
            self.assertEqual(part_total_count, lines)

    def test_copy_file(self):
        splitter = FileSplitter("AandE_Data_2011-04-10.csv", has_header=True)
        self.assertEqual(splitter.file_type(), FileType.DOS)
        (_, total_lines) = splitter.copy_file("AandE_Data_2011-04-10.csv" + ".1", ignore_header=True)


    def test_autosplit_file(self):
        self.assertEqual(FileSplitter("AandE_Data_2011-04-10.csv").file_type(), FileType.DOS)
        self._auto_split_helper("fourlines.txt", 4, 2, has_header=False)
        self._auto_split_helper("ninelines.txt", 9, 3, has_header=True)
        self._auto_split_helper("inventory.csv", 5, 2, has_header=True)
        self._auto_split_helper("AandE_Data_2011-04-10.csv", 301, 3, has_header=True, dos_adjust=True)
        self._auto_split_helper("AandE_Data_2011-04-10.csv", 301, 2, has_header=True, dos_adjust=True)
        self._auto_split_helper("AandE_Data_2011-04-10.csv", 301, 1, has_header=True, dos_adjust=True)
        self._auto_split_helper("AandE_Data_2011-04-10.csv", 301, 0, has_header=True, dos_adjust=True)
        self._auto_split_helper("10k.txt", 10000, 5, has_header=True)
        self._auto_split_helper("yellow_tripdata_2015-01-06-1999.csv", 1999, 4, has_header=False)

    def test_get_average_line_size(self):
        self.assertEqual(10, FileSplitter("tenlines.txt").get_average_line_size())


if __name__ == "__main__":
    unittest.main()
