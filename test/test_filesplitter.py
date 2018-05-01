'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import unittest
import os

from pymongo_import.filesplitter import Line_Counter,File_Splitter, File_Type


class Test(unittest.TestCase):

    def test_count_lines(self):
        self.assertEqual(3, File_Splitter("data/threelines.txt").line_count())
        self.assertEqual(0, File_Splitter("data/emptyfile.txt").line_count())
        self.assertEqual(4, File_Splitter("data/fourlines.txt").line_count())
        self.assertEqual(5, File_Splitter("data/inventory.csv").line_count())

    def _split_helper(self, filename, split_size, has_header=False, dos_adjust=False):

        splitter = File_Splitter(filename, has_header)

        count = 0
        part_total_size = 0
        part_total_count = 0

        for (part_name, line_count) in splitter.split_file(split_size):
            splitter_part = File_Splitter( part_name)
            part_count = Line_Counter(part_name).count()
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            part_total_size = part_total_size + splitter_part.size()
            os.unlink(part_name)

        lc = Line_Counter(filename)

        if has_header:
            self.assertEqual(part_total_count, lc.count() - 1)
        else:
            self.assertEqual(part_total_count, lc.count())

        if dos_adjust:
            self.assertEqual(part_total_size, splitter.size() - lc.count() - len( splitter.header_line()) +1)
        else:
            self.assertEqual(part_total_size, splitter.size() - len( splitter.header_line()))

    def test_split_file(self):
        self._split_helper("data/fourlines.txt", 3)
        self._split_helper("data/AandE_Data_2011-04-10.csv", 47, has_header=True, dos_adjust=True)
        self._split_helper("data/10k.txt", 2300, has_header=False)


    def _auto_split_helper(self, filename, lines, split_count, has_header=False, dos_adjust=False):

        splitter = File_Splitter(filename, has_header=has_header)
        count = 0
        part_total_size = 0
        part_total_count = 0
        total_line_count = Line_Counter(filename).count()
        self.assertEqual( total_line_count, lines)
        for (part_name, line_count) in splitter.autosplit(split_count):
            splitter_part = File_Splitter(part_name)
            part_count = Line_Counter( part_name).count()
            self.assertGreater(part_count, 0)
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            part_total_size = part_total_size + splitter_part.size()
            os.unlink( part_name )


        lc = Line_Counter(filename)

        if has_header:
            self.assertEqual(part_total_count, lines - 1)
            if splitter.file_type() is File_Type.DOS:
                self.assertEqual(part_total_size, splitter.size() - lc.count() - len( splitter.header_line()) +1)
            else:
                self.assertEqual(part_total_size, splitter.size() - len( splitter.header_line()))
        else:
            self.assertEqual(part_total_count, lines)
            if splitter.file_type() is File_Type.DOS:
                self.assertEqual(part_total_size, splitter.size() - lc.count())
            else:
                self.assertEqual(part_total_size, splitter.size())




    def test_copy_file(self):
        splitter = File_Splitter("data/AandE_Data_2011-04-10.csv", has_header=True)
        self.assertEqual( splitter.file_type(), File_Type.DOS )
        (_, total_lines)=splitter.copy_file("data/AandE_Data_2011-04-10.csv" + ".1", ignore_header=True)

        #
        # we subtract 1 char for each line to account for dos \r\n.
        # We subtract the header line
        # because we have already subtracted all the lines in the original including the header line
        # (which has a \r\n) we add one back to account for this extra char.
        #
        self.assertEqual(os.path.getsize("data/AandE_Data_2011-04-10.csv.1"), splitter.size() - splitter.line_count() - len( splitter.header_line()) +1)

    def test_autosplit_file(self):
        self.assertEqual( File_Splitter("data/AandE_Data_2011-04-10.csv").file_type(), File_Type.DOS)
        self._auto_split_helper("data/fourlines.txt", 4, 2, has_header=False)
        self._auto_split_helper("data/ninelines.txt", 9, 3, has_header=True)
        self._auto_split_helper("data/inventory.csv", 5, 2, has_header=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 301, 3, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 301, 2, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 301, 1, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 301, 0, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/10k.txt", 10000, 5, has_header=True)
        self._auto_split_helper("data/yellow_tripdata_2015-01-06-1999.csv", 1999, 4, has_header=False)

    def test_get_average_line_size(self):
        self.assertEqual(10, File_Splitter( "data/tenlines.txt").get_average_line_size())


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
