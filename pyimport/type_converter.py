import datetime
from datetime import timezone, datetime
from enum import Enum

from dateutil.parser import parse as date_parse
from dateutil.parser import ParserError

from pyimport.compounddateformats import EU_Formats, US_Formats, All_Formats
from pyimport.logger import Log
import pydateinfer


class DateType(Enum):
    EU = 1
    US = 2


def infer_format(date_strs: list[str], dt: DateType = None) -> str:
    if dt == DateType.EU:
        return pydateinfer.infer(date_strs, date_order="DMY")
    elif dt == DateType.US:
        return pydateinfer.infer(date_strs, date_order="MDY")
    else:
        return pydateinfer.infer(date_strs)


def generate_format(date_str: str, format_list=None, dt: DateType = None) -> str:
    # Common date
    if format_list is None:
        if dt == DateType.EU:
            format_list = EU_Formats
        elif dt == DateType.US:
            format_list = US_Formats
        else:
            format_list = All_Formats

    for fmt in format_list:
        try:
            datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            continue
    Log().log.warning(f"Could not generate format for date string: {date_str} using any of the formats in {format_list}")
    return None


# # Example usage:
# date_str1 = "2023-05-22"
# date_str2 = "22-05-2023 14:30:00"
# date_str3 = "2023/05/22"
# date_str4 = "2023-05-22T14:30:00"
#
# print(generate_format_string(date_str1))  # Output: "%Y-%m-%d"
# print(generate_format_string(date_str2))  # Output: "%d-%m-%Y %H:%M:%S"
# print(generate_format_string(date_str3))  # Output: "%Y/%m/%d"
# print(generate_format_string(date_str4))  # Output: "%Y-%m-%dT%H:%M:%S


def to_int(v:str, fmt=None) -> int:
    try:
        v = int(v)
    except ValueError:
        v = float(v)
    return v


def to_float(v:str, fmt=None) -> float:
    return float(v)


def to_str(v, fmt=None)->str:
    return str(v)


def iso_to_datetime(v, fmt=None) -> datetime:
    return datetime.fromisoformat(v)


def to_datetime(v, fmt=None) -> datetime:
    if v == "NULL":
        return None
    if v == "":
        return None
    if fmt is None:
        return date_parse(v)  # much slower than strptime, avoid for large jobs
    else:
        return datetime.strptime(v, fmt)


def to_timestamp(v, fmt=None) -> datetime:
    return datetime.fromtimestamp(int(v))


def to_timestamp_utc(v, fmt=None) -> datetime:
    return datetime.fromtimestamp(int(v), tz=timezone.utc)


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
            fmt = infer_format([s])
            return "date", fmt
        else:
            fmt = infer_format([s])
            return "datetime", fmt

    except ParserError:
        pass

    return "str", ""


converter = {
    "int": to_int,
    "float": to_float,
    "str": to_str,
    "datetime": to_datetime,
    "date": to_datetime,
    "isodate": iso_to_datetime,
    "timestamp": to_timestamp
}


def convert_it(t, v, fmt=None, utc_time=False) -> str | int | float | datetime:
    """
    Use type entry for the field in the fieldConfig file (.ff) to determine what type
    conversion to use.
    """

    if utc_time:
        converter["timestamp"] = to_timestamp_utc
    try:
        return converter[t](v, fmt)
    except ValueError:
        return v
