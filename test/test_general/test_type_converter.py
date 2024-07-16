from datetime import datetime
import unittest
from csv import DictReader
from datetime import timezone

import pytest

from pyimport.type_converter import convert_it, guess_type, generate_format
# List of test date strings

test_dates = [
    "1-Jan-2016", "01-01-2016", "31/12/2020", "31.12.2020",
    "Monday, 1 January 2018", "Mon, 1 Jan 18", "2024-07-01",
    "01/07/2024", "1. January 2024", "15-Feb-2021", "15/Feb/2021",
    "July 4, 2020", "Jul 4, 20", "Tuesday, July 4, 2020",
    "Tue, July 4, 2020", "July 4, 2020,", "Jul 4, 20,", "04/22/2017 04:46:37 AM",
]


def test_auto_date_format():
    for i in test_dates:
        assert generate_format(i) is not None, f"Failed to parse: {i}"


class Test(unittest.TestCase):

    def test_converter(self):

        self.assertEqual(10, convert_it("int", "10"))
        self.assertEqual(40.724250793457000, convert_it("float", "40.724250793457000"))
        self.assertEqual(10.0, convert_it("int", "10.0"))
        self.assertEqual(10.0, convert_it("float", "10.0"))
        self.assertEqual(datetime(2018, 5, 7, 3, 1, 54),
                         convert_it("timestamp", "1525658514"))

        self.assertEqual(datetime(2018, 5, 25, 11, 30),
                         convert_it("datetime", "11:30am 25-May-2018"))

        # see datetime.datetime.strptime for formatting
        # https://docs.python.org/3.6/library/datetime.html#datetime.datetime.strptime
        # 25-May-2018 : %d-%b-%Y
        # 11:30am : %I:%M%p
        self.assertEqual(datetime(2018, 5, 25, 11, 30),
                         convert_it("datetime", "11:30am 25-May-2018", "%I:%M%p %d-%b-%Y"))

        assert convert_it("datetime", "NULL") is None
        assert convert_it("datetime", "") is None
        assert guess_type("10") == ("int", "")
        assert guess_type("10.0") == ("float", "")
        assert guess_type("1525658514") == ("int", "")

    def test_guess_type_date(self):
        with open("datetime_formats.csv", "r") as f:
            f.readline()
            for i,d in enumerate(DictReader(f, delimiter=",", fieldnames=["datetime", "Type", "format"]), 1):
                guess, fmt = guess_type(d["datetime"])
                assert guess == d["Type"], f"Expected {d['Type']} but got {guess} for {d['datetime']} line {i}"

    def test_guess_type_int(self):
        assert guess_type("10") == ("int", "")
        assert guess_type("10.0") == ("float", "")
        assert guess_type("1525658514") == ("int", "")
        assert guess_type("11:30am 25-May-2018") == ("datetime", "%I:%M%p %d-%b-%Y")
        assert guess_type("2018-05-25T11:30:00") == ("datetime", "%Y-%m-%dT%I:%M:%S")
        assert guess_type("2018-05-25") == ("date", "%Y-%m-%d")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
