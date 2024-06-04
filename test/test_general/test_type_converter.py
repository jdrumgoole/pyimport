import datetime
import unittest
from datetime import timezone

from pyimport.type_converter import Converter


class Test(unittest.TestCase):

    def test_converter(self):

        self.assertEqual(10, Converter.convert("int", "10"))
        self.assertEqual(40.724250793457000, Converter.convert("float", "40.724250793457000"))
        self.assertEqual(10.0, Converter.convert("int", "10.0"))
        self.assertEqual(10.0, Converter.convert("float", "10.0"))
        self.assertEqual(datetime.datetime(2018, 5, 7, 3, 1, 54),
                         Converter.convert("timestamp", "1525658514"))

        self.assertEqual(datetime.datetime(2018, 5, 25, 11, 30),
                         Converter.convert("datetime", "11:30am 25-May-2018"))

        # see datetime.datetime.strptime for formatting
        # https://docs.python.org/3.6/library/datetime.html#datetime.datetime.strptime
        # 25-May-2018 : %d-%b-%Y
        # 11:30am : %I:%M%p
        self.assertEqual(datetime.datetime(2018, 5, 25, 11, 30),
                         Converter.convert("datetime", "11:30am 25-May-2018", "%I:%M%p %d-%b-%Y"))

        assert Converter.convert("datetime", "NULL") is None
        assert Converter.convert("datetime", "") is None
        assert Converter.guess_type("10") == "int"
        assert Converter.guess_type("10.0") == "float"
        assert Converter.guess_type("1525658514") == "int"
        assert Converter.guess_type("11:30am 25-May-2018") == "datetime"
        assert Converter.guess_type("2018-05-25T11:30:00") == "datetime"
        assert Converter.guess_type("2018-05-25") == "date"


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
