import os
import unittest

from pyimport.filesplitter import LineCounter
from pyimport.liner import make_line_file

class MyTestCase(unittest.TestCase):

	def _test_file(self, count, doseol=False,filename="liner.txt", unlink=True):
        	f = make_line_file(count=count, doseol=doseol, filename=filename)
        	self.assertEqual(count, LineCounter(f).line_count)
        	if unlink:
            		os.unlink(f)
	
	def test_Line_Counter(self):
        	self._test_file(5, filename="5.txt")

if __name__ == '__main__':
	unittest.main()

