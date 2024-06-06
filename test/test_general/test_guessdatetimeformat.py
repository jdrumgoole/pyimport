import pytest
from pyimport.guessdatetimeformat import guess_datetime_format
# Pytest script


@pytest.mark.parametrize("date_str, expected_format", [
    ("2024-06-04", "%Y-%m-%d"),
    ("04-06-2024", "%d-%m-%Y"),
    ("06/04/2024", "%d/%m/%Y"),
    ("June 4, 2024", "%B %d, %Y"),
    ("2024 June 4", "%Y %B %d"),
    ("4th June 2024", "%dth %B %Y"),
    ("2024-06-04 15:30", "%Y-%m-%d %H:%M"),
    ("04-06-2024 03:30 PM", "%d-%m-%Y %I:%M %p"),
    ("06/04/2024 15:30:45", "%d/%m/%Y %H:%M:%S"),
    ("June 4, 2024 3:30 PM", "%B %d, %Y %I:%M %p"),
    ("2024-06-04T15:30:45", "%Y-%m-%dT%H:%M:%S"),
    ("2024-06-04 15:30:45", "%Y-%m-%d %H:%M:%S"),
    ("04-06-2024 15:30:45", "%d-%m-%Y %H:%M:%S"),
    ("20240604 153045", "%Y%m%d %H%M%S"),
    ("15:30 2024-06-04", "%H:%M %Y-%m-%d"),
    ("03:30 PM 04-06-2024", "%I:%M %p %d-%m-%Y"),
    ("15:30:45 06/04/2024", "%H:%M:%S %d/%m/%Y"),
    ("3:30 PM June 4, 2024", "%I:%M %p %B %d, %Y"),
    ("15:30:45 2024-06-04", "%H:%M:%S %Y-%m-%d"),
    ("153045 20240604", "%H%M%S %Y%m%d"),
    ("3:30 PM 2024 June 4", "%I:%M %p %Y %B %d"),
    ("2024-06-04 15:30:00.000000", "%Y-%m-%d %H:%M:%S.%f"),
    ("15:30:00.000000 2024-06-04", "%H:%M:%S.%f %Y-%m-%d"),
    ("June 4, 2024 at 3:30 PM", "%B %d, %Y at %I:%M %p"),
    ("3:30 PM, June 4, 2024", "%I:%M %p, %B %d, %Y"),
    ("03:30:00 PM 04-06-2024", "%I:%M:%S %p %d-%m-%Y"),
    ("15:30:45.123456 06/04/2024", "%H:%M:%S.%f %d/%m/%Y"),
    ("2024-06-04 3:30:45 PM", "%Y-%m-%d %I:%M:%S %p"),
    ("2024-June-04 15:30", "%Y-%B-%d %H:%M"),
    ("2024/06/04 15:30", "%Y/%m/%d %H:%M"),
    ("2024年06月04日 15時30分", "%Y年%m月%d日 %H時%M分"),
    ("4 June 2024", "%d %B %Y"),
    ("04/June/2024", "%d/%B/%Y")
])
def test_guess_datetime_format(date_str, expected_format):
    assert guess_datetime_format(date_str) == expected_format
