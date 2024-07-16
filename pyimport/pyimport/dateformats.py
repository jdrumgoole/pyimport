import itertools
import pprint
import calendar
from datetime import datetime
from itertools import combinations
from typing import Callable, Generator

format_dict = {
    "%a": "Weekday as locale’s abbreviated name.",
    "%A": "Weekday as locale’s full name.",
    "%w": "Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.",
    "%d": "Day of the month as a zero-padded decimal number.",
    "%b": "Month as locale’s abbreviated name.",
    "%B": "Month as locale’s full name.",
    "%m": "Month as a zero-padded decimal number.",
    "%y": "Year without century as a zero-padded decimal number.",
    "%Y": "Year with century as a decimal number.",
    "%H": "Hour (24-hour clock) as a zero-padded decimal number.",
    "%I": "Hour (12-hour clock) as a zero-padded decimal number.",
    "%p": "Locale’s equivalent of either AM or PM.",
    "%M": "Minute as a zero-padded decimal number.",
    "%S": "Second as a zero-padded decimal number.",
    "%f": "Microsecond as a decimal number, zero-padded to 6 digits.",
    "%z": "UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).",
    "%Z": "Time zone name (empty string if the object is naive).",
    "%j": "Day of the year as a zero-padded decimal number.",
    "%U": "Week number of the year (Sunday as the first day of the week) as a zero-padded decimal number.",
    "%W": "Week number of the year (Monday as the first day of the week) as a zero-padded decimal number.",
    "%c": "Locale’s appropriate date and time representation.",
    "%x": "Locale’s appropriate date representation.",
    "%X": "Locale’s appropriate time representation.",
    "%%": "A literal '%' character."
}


def time_range(start, finish, zfill=None) -> Generator[str, None, None]:
    if zfill:
        return (str(x).zfill(zfill) for x in range(start, finish + 1))
    else:
        return (str(x) for x in range(start, finish+1))


#
# Leave out all locale specific formats for this iteration
#

second_formats = {
    "%S": time_range(0, 59, zfill=True),
    "%f": time_range(0, 999999, zfill=6)  # "Microsecond as a decimal number, zero-padded to 6 digits."
}

minute_formats = {
    "%M": time_range(0, 59, zfill=2),   # Minute as a zero-padded decimal number.
}

hour_formats = {
    "%H":  time_range(0, 59, zfill=2),    # "hour, using a 24-hour clock (00 to 23)",
    "%I":  time_range(1, 12, zfill=2),    # "hour, using a 12-hour clock (01 to 12)",
    "%p": ["AM", "PM"]   # "either am or pm according to the given time value",
}

day_formats = {
    "%a": "Weekday as locale’s abbreviated name.",
    "%A": "Weekday as locale’s full name.",
    "%w": time_range(0, 6, zfill=0),    # "Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.",
    "%d": time_range(1, 31, zfill=0),   # "day of the month (01 to 31)",
    "%e": time_range(1, 31, zfill=0),   # "day of the month (1 to 31)",
    "%j": time_range(0, 366, zfill=3),  # "Day of the year as a zero-padded decimal number."
    "%u": time_range(1, 7, zfill=0)     # ISO 8601 weekday as a decimal number where 1 is Monday.
}

week_formats = {
    "%U": time_range(0, 53, zfill=True),   #  week number of the current year, starting with the first Sunday as the first day of the first week
    "%W": time_range(0, 53, zfill=True),   #  week number of the current year, starting with the first Monday as the first day of the first week
    "%V": time_range(1, 53, zfill=2)       #  ISO 8601 week as a decimal number with Monday as the first day of the week. Week 01 is the week containing Jan 4
}

month_formats = {
    "%b": [calendar.month_abbr[i] for i in range(1, 13)],  #"abbreviated month name",
    "%B": [calendar.month_name[i] for i in range(1, 13)],  # "full month name",
    "%m": time_range(1, 12, zfill=True),  # "month (01 to 12)",
}

year_formats = {
    "%y": time_range(0, 99, zfill=2),  #"Year without century as a zero-padded decimal number.",
    "%Y": time_range(0, 9999, zfill=4), # "Year with century as a decimal number.",
    "%G": time_range(0, 9999, zfill=4),  #"ISO 8601 year with century representing the year that contains the greater part of the ISO week (%V).",
}


separators_dict = {
    "": "no separator",
    "-": "hyphen",
    "/": "forward slash",
    ".": "period",
    ",": "comma",
    " ": "space",
    ":": "colon",
}

format_charts_dict = {
    "%%": "a literal % character",
    "%t": "tab character",
    "%n": "newline character",
}



format_results_d1 = {}
format_results_d2 = {}


def str_join(i):
    return ("".join(x) for x in i)


def permute(d: dict, n: int, predicate: Callable | None = None):
    if predicate is None:
        def predicate(x): return x and False
    return str_join(itertools.filterfalse(predicate, combinations(d.keys(), n)))


def get_percent_fmts(perms):
    return (x for x in perms if "%" in x)


# for x in combinations(date_format_dict.keys(), 2):
#     print(x)

# for i in permute(format_dict, 2):
#     print(i)
#
# hrs_mins_formats = itertools.product(hour_formats, minute_formats)
# for h,m in hrs_mins_formats:
#     for sep in separators_dict:
#         print(f"{h}{sep}{m}")
#
#
# hrs_mins_secs_formats = itertools.product(hour_formats, minute_formats, second_formats)
# for h,m, s in hrs_mins_secs_formats:
#     for sep in separators_dict:
#         print(f"{h}{sep}{m}{sep}{s}")
# def try_strptime(s:str, fmt:str) -> datetime:
#     try:
#         return datetime.strptime(s, fmt)
#     except ValueError:
#         return None
#
#
# for i in range(60):
#     for j in range(60):
#         for k, v in format_dict.items():
#             d1 = try_strptime(str(i), k)
#             d2 = try_strptime(f"{i}:{j}", k)
#             if d1:
#                 format_results_d1[k] = f" {i} :  {v}"
#             if d2:
#                 format_results_d2[k] = f" {i}:{j} :  {v}"
#
# pprint.pprint(format_results_d1)
# print("\n")
# pprint.pprint(format_results_d2)
