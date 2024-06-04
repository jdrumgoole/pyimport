from datetime import datetime
import unittest
from csv import DictReader
from datetime import timezone

from pyimport.type_converter import Converter, generate_format


class Test(unittest.TestCase):

    def test_converter(self):

        self.assertEqual(10, Converter.convert("int", "10"))
        self.assertEqual(40.724250793457000, Converter.convert("float", "40.724250793457000"))
        self.assertEqual(10.0, Converter.convert("int", "10.0"))
        self.assertEqual(10.0, Converter.convert("float", "10.0"))
        self.assertEqual(datetime(2018, 5, 7, 3, 1, 54),
                         Converter.convert("timestamp", "1525658514"))

        self.assertEqual(datetime(2018, 5, 25, 11, 30),
                         Converter.convert("datetime", "11:30am 25-May-2018"))

        # see datetime.datetime.strptime for formatting
        # https://docs.python.org/3.6/library/datetime.html#datetime.datetime.strptime
        # 25-May-2018 : %d-%b-%Y
        # 11:30am : %I:%M%p
        self.assertEqual(datetime(2018, 5, 25, 11, 30),
                         Converter.convert("datetime", "11:30am 25-May-2018", "%I:%M%p %d-%b-%Y"))

        assert Converter.convert("datetime", "NULL") is None
        assert Converter.convert("datetime", "") is None
        assert Converter.guess_type("10") == ("int", "")
        assert Converter.guess_type("10.0") == ("float", "")
        assert Converter.guess_type("1525658514") == ("int", "")

    def test_guess_type_date(self):
        with open("datetime_formats.csv", "r") as f:
            f.readline()
            for i,d in enumerate(DictReader(f, delimiter=",", fieldnames=["datetime", "Type", "format"]), 1):
                guess, fmt = Converter.guess_type(d["datetime"])
                assert guess == d["Type"], f"Expected {d['Type']} but got {guess} for {d['datetime']} line {i}"

    def test_guess_type_int(self):
        assert Converter.guess_type("10") == ("int", "")
        assert Converter.guess_type("10.0") == ("float", "")
        assert Converter.guess_type("1525658514") == ("int", "")
        assert Converter.guess_type("11:30am 25-May-2018") == ("datetime", "")
        assert Converter.guess_type("2018-05-25T11:30:00") == ("datetime", "%Y-%m-%dT%H:%M:%S")
        assert Converter.guess_type("2018-05-25") == ("date", "%Y-%m-%d")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
