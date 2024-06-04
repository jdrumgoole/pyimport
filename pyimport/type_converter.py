import datetime
from datetime import timezone

from dateutil.parser import parse as date_parse
from dateutil.parser import ParserError


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
    def iso_to_datetime(v) -> datetime.datetime:
        return datetime.datetime.fromisoformat(v)

    @staticmethod
    def to_datetime(v, format=None) -> datetime.datetime:
        if v == "NULL":
            return None
        if v == "":
            return None
        if format is None:
            return date_parse(v)  # much slower than strptime, avoid for large jobs
        else:
            return datetime.datetime.strptime(v, format)

    @staticmethod
    def to_timestamp(v) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(v))

    @staticmethod
    def to_timestamp_utc(v) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(v), tz=timezone.utc)

    @staticmethod
    def convert(t, v, fmt=None) -> str | int | float | datetime.datetime:
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
    def guess_type(s: str) -> str:
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
            return "int"
        except ValueError:
            pass

        try:
            _ = float(s)
            return "float"
        except ValueError:
            pass

        try:
            d = date_parse(s)
            if d.hour == 0 and d.minute == 0 and d.second == 0 and d.microsecond == 0:
                return "date"
            else:
                return "datetime"

        except ParserError:
            pass

        return "str"
