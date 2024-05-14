import datetime
from datetime import timezone

from dateutil.parser import parse as date_parse


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
    def to_int(v):
        try:
            # print( "converting : '%s' to int" % v )
            v = int(v)
        except ValueError:
            v = float(v)
        return v

    @staticmethod
    def to_float(v):
        return float(v)

    @staticmethod
    def to_str(v):
        return str(v)

    def iso_to_datetime(self, v, format=None, line_number=0, line=""):
        #print("isodate")
        if v == "NULL":
            return None
        if v == "":
            return None
        try:
            return datetime.datetime.fromisoformat(v)
        except ValueError:
            if self._log:
                self._log.warning(f"Using isoformat() for value '{v}' has failed at line: {line_number}. '{line}'")
            return date_parse(v)

    def to_datetime(self, v, format=None, line_number=0, line=""):
        if v == "NULL":
            return None
        if v == "":
            return None
        if format is None:
            return date_parse(v)  # much slower than strptime, avoid for large jobs
        else:
            try:
                # print(f"v={v}")
                # print(f"format={format}")
                return datetime.datetime.strptime(v, format)
            except ValueError:
                if self._log:
                    self._log.warning(f"Using the slower date parse: for value '{v}' as format '{format}' has failed at line: {line_number}. '{line}'")
                return date_parse(v)

    @staticmethod
    def to_timestamp(v, line_number, line):
        return datetime.datetime.fromtimestamp(int(v))

    @staticmethod
    def to_timestamp_utc(v):
        return datetime.datetime.fromtimestamp(int(v), tz=timezone.utc)

    def convert_time(self, t, v, f=None, line_number=0, line=""):
        return self._converter[t](v, f, line_number, line)

    def convert(self, t, v):
        """
        Use type entry for the field in the fieldConfig file (.ff) to determine what type
        conversion to use.
        """
        try:
            return self._converter[t](v)
        except ValueError:
            v = str(v)

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

        if type(s) != str:
            raise ValueError(f"guess_type expects a string parameter value: type({s}) is '{type(s)}'")

        v = None
        try:
            v = int(s)
            return "int"
        except ValueError:
            pass

        try:
            v = float(s)
            return "float"
        except ValueError:
            pass

        try:
            v = date_parse(s)  # dateutil.parse.parser
            return "datetime"
        except ValueError:
            pass

        v = str(s)
        return "str"
