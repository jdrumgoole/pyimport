import datetime
from datetime import timezone, datetime
from enum import Enum

from dateutil.parser import parse as date_parse
from dateutil.parser import ParserError

EU_Date_formats = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%Y.%m.%d",
    "%d.%m.%Y",
]

US_Date_formats = [
    "%m-%d-%Y",
    "%m/%d/%Y",
    "%m.%d.%Y"

]

EU_datetime_formats = [
    "%Y-%m-%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y.%m.%d %H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d-%m-%Y %H:%M",
    "%Y/%m/%d %H:%M",
    "%d/%m/%Y %H:%M",
    "%Y.%m.%d %H:%M",
    "%d.%m.%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]

US_datetime_formats = [
    "%m-%d-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%m.%d.%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    "%m/%d/%Y %H:%M",
    "%m.%d.%Y %H:%M",
    "%m-%d-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%m.%d.%Y %H:%M:%S",
    "%m-%d-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%m.%d.%Y %H:%M:%S",
    "%m-%d-%YT%H:%M:%S",
    "%m-%d-%YT%H:%M:%S.%f",
    "%m-%d-%YT%H:%M:%SZ",
    "%m-%d-%YT%H:%M:%S.%fZ",
]


class DateType(Enum):
    EU = 1
    US = 2


def generate_format(date_str: str, format_list=None, dt: DateType = DateType.EU) -> str:
    # Common date
    if format_list is None:
        if dt == DateType.EU:
            format_list = EU_Date_formats + EU_datetime_formats
        else:
            format_list = US_Date_formats + US_datetime_formats

    for fmt in format_list:
        try:
            datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            continue
    return ""


# # Example usage:
# date_str1 = "2023-05-22"
# date_str2 = "22-05-2023 14:30:00"
# date_str3 = "2023/05/22"
# date_str4 = "2023-05-22T14:30:00"
#
# print(generate_format_string(date_str1))  # Output: "%Y-%m-%d"
# print(generate_format_string(date_str2))  # Output: "%d-%m-%Y %H:%M:%S"
# print(generate_format_string(date_str3))  # Output: "%Y/%m/%d"
# print(generate_format_string(date_str4))  # Output: "%Y-%m-%dT%H:%M:%S"

class Converter(object):
    type_fields = ["int", "float", "str", "datetime", "date", "timestamp"]

    def __init__(self, log=None, utctime=False):

        self._log = log
        self._utctime = utctime

        self._converter = {
            "int": Converter.to_int,
            "float": Converter.to_float,
            "str": Converter.to_str,
            "datetime": self.to_datetime,
            "date": self.to_datetime,
            "isodate": self.iso_to_datetime,
            "timestamp": Converter.to_timestamp
        }

        if self._utctime:
            self._converter["timestamp"] = Converter.to_timestamp_utc

    @staticmethod
    def to_int(v:str) -> int:
        try:
            v = int(v)
        except ValueError:
            v = float(v)
        return v

    @staticmethod
    def to_float(v:str) -> float:
        return float(v)

    @staticmethod
    def to_str(v)->str:
        return str(v)

    @staticmethod
    def iso_to_datetime(v) -> datetime:
        return datetime.fromisoformat(v)

    @staticmethod
    def to_datetime(v, fmt=None) -> datetime:
        if v == "NULL":
            return None
        if v == "":
            return None
        if fmt is None:
            return date_parse(v)  # much slower than strptime, avoid for large jobs
        else:
            return datetime.strptime(v, fmt)

    @staticmethod
    def to_timestamp(v) -> datetime:
        return datetime.fromtimestamp(int(v))

    @staticmethod
    def to_timestamp_utc(v) -> datetime:
        return datetime.fromtimestamp(int(v), tz=timezone.utc)

    @staticmethod
    def convert(t, v, fmt=None) -> str | int | float | datetime:
        """
        Use type entry for the field in the fieldConfig file (.ff) to determine what type
        conversion to use.
        """

        try:
            if t == "datetime":
                return Converter.to_datetime(v, fmt)
            elif t == "timestamp":
                return Converter.to_timestamp(v)
            elif t == "date":
                return Converter.to_datetime(v, fmt)
            elif t == "isodate":
                return Converter.iso_to_datetime(v)
            elif t == "float":
                return Converter.to_float(v)
            elif t == "int":
                return Converter.to_int(v)
            elif t == "str":
                return Converter.to_str(v)

        except ValueError:
            return v

    @staticmethod
    def guess_type(s: str) -> [str, str]:
        """
        Try and convert a string s to an object. Start with float, then try int
        and if that doesn't work return the string.

        Returns a tuple:
           The value itself
           The type of the value as a string
        """

        if type(s) is not str:
            raise ValueError(f"guess_type expects a string parameter value: type({s}) is '{type(s)}'")

        try:
            _ = int(s)
            return "int", ""
        except ValueError:
            pass

        try:
            _ = float(s)
            return "float", ""
        except ValueError:
            pass

        try:
            d = date_parse(s)
            if d.hour == 0 and d.minute == 0 and d.second == 0 and d.microsecond == 0:
                fmt = generate_format(s)
                return "date", fmt
            else:
                fmt = generate_format(s)
                return "datetime", fmt

        except ParserError:
            pass

        return "str", ""
