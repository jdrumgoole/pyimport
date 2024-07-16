import pyimport.dateformats

EU_Date_formats = [
    # standard formats
    "%D",        # 1
    # Day-Month-Year formats
    "%d-%m-%Y",        # 1
    "%-d-%-m-%Y",      # 2
    "%d/%m/%Y",        # 3
    "%-d/%-m/%Y",      # 4
    "%d.%m.%Y",        # 5
    "%-d.%-m.%Y",      # 6
    "%d %B %Y",        # 7
    "%-d %B %Y",       # 8
    "%d %b %Y",        # 9
    "%-d %b %Y",       # 10
    "%a, %d %b %y",
    "%d/%b/%Y",

    # Day-Month-Year with weekday formats
    "%A, %d %B %Y",    # 11
    "%A, %-d %B %Y",   # 12
    "%a, %d %b %Y",    # 13
    "%a, %-d %b %Y",   # 14

    # Day-Month-Year short year formats
    "%d-%m-%y",        # 15
    "%-d-%-m-%y",      # 16
    "%d/%m/%y",        # 17
    "%-d/%-m/%y",      # 18
    "%d.%m.%y",        # 19
    "%-d.%-m.%y",      # 20
    "%d %B %y",        # 21
    "%-d %B %y",       # 22
    "%d %b %y",        # 23
    "%-d %b %y",       # 24

    # Year-Month-Day formats
    "%Y-%m-%d",        # 25
    "%Y/%m/%d",        # 26

    # Day-Month-Year with comma formats
    "%d-%m-%Y,",       # 27
    "%-d-%-m-%Y,",     # 28
    "%d/%m/%Y,",       # 29
    "%-d/%-m/%Y,",     # 30
    "%d.%m.%Y,",       # 31
    "%-d.%-m.%Y,",     # 32
    "%d-%m-%y,",       # 33
    "%-d-%-m-%y,",     # 34
    "%d/%m/%y,",       # 35
    "%-d/%-m/%y,",     # 36
    "%d.%m.%y,",       # 37
    "%-d.%-m.%y,",     # 38

    # Additional variations
    "%d. %B %Y",       # 39
    "%d. %b %Y",       # 40
    "%d. %B %y",       # 41
    "%d. %b %y",       # 42
    "%d %B, %Y",       # 43
    "%d %b, %Y",       # 44
    "%d %B, %y",       # 45
    "%d %b, %y",       # 46
    "%d-%b-%Y",

    # Month-Day-Year formats
    "%m-%d-%Y",        # 47
    "%m/%d/%Y",        # 48
    "%m.%d.%Y",        # 49
    "%m-%d-%y",        # 50
    "%m/%d/%y",        # 51
    "%m.%d.%y",        # 52

    # Variations with month name
    "%B %d, %Y",       # 53
    "%b %d, %Y",       # 54
    "%B %d, %y",       # 55
    "%b %d, %y",       # 56
]

US_Date_formats = [
    # Month-Day-Year formats
    "%m-%d-%Y",        # 1
    "%m/%d/%Y",        # 2
    "%m.%d.%Y",        # 3
    "%-m-%-d-%Y",      # 4
    "%-m/%-d/%Y",      # 5
    "%-m.%-d.%Y",      # 6
    "%m-%d-%y",        # 7
    "%m/%d/%y",        # 8
    "%m.%d.%y",        # 9
    "%-m-%-d-%y",      # 10
    "%-m/%-d/%y",      # 11
    "%-m.%-d.%y",      # 12

    # Month Day, Year formats
    "%B %d, %Y",       # 13
    "%b %d, %Y",       # 14
    "%B %-d, %Y",      # 15
    "%b %-d, %Y",      # 16
    "%B %d, %y",       # 17
    "%b %d, %y",       # 18
    "%B %-d, %y",      # 19
    "%b %-d, %y",      # 20

    # Weekday, Month Day, Year formats
    "%A, %B %d, %Y",   # 21
    "%A, %b %d, %Y",   # 22
    "%A, %B %-d, %Y",  # 23
    "%A, %b %-d, %Y",  # 24
    "%A, %B %d, %y",   # 25
    "%A, %b %d, %y",   # 26
    "%A, %B %-d, %y",  # 27
    "%A, %b %-d, %y",  # 28
    "%a, %B %d, %Y",   # 29
    "%a, %b %d, %Y",   # 30
    "%a, %B %-d, %Y",  # 31
    "%a, %b %-d, %Y",  # 32
    "%a, %B %d, %y",   # 33
    "%a, %b %d, %y",   # 34
    "%a, %B %-d, %y",  # 35
    "%a, %b %-d, %y",  # 36

    # Year-Month-Day formats
    "%Y-%m-%d",        # 37
    "%Y/%m/%d",        # 38

    # Month-Day-Year with comma formats
    "%m-%d-%Y,",       # 39
    "%m/%d/%Y,",       # 40
    "%m.%d.%Y,",       # 41
    "%-m-%-d-%Y,",     # 42
    "%-m/%-d/%Y,",     # 43
    "%-m.%-d.%Y,",     # 44
    "%m-%d-%y,",       # 45
    "%m/%d/%y,",       # 46
    "%m.%d.%y,",       # 47
    "%-m-%-d-%y,",     # 48
    "%-m/%-d/%y,",     # 49
    "%-m.%-d.%y,",     # 50

    # Month Day, Year with comma formats
    "%B %d, %Y,",      # 51
    "%b %d, %Y,",      # 52
    "%B %-d, %Y,",     # 53
    "%b %-d, %Y,",     # 54
    "%B %d, %y,",      # 55
    "%b %d, %y,",      # 56
    "%B %-d, %y,",     # 57
    "%b %-d, %y,",     # 58

    # Weekday, Month Day, Year with comma formats
    "%A, %B %d, %Y,",  # 59
    "%A, %b %d, %Y,",  # 60
    "%A, %B %-d, %Y,", # 61
    "%A, %b %-d, %Y,", # 62
    "%A, %B %d, %y,",  # 63
    "%A, %b %d, %y,",  # 64
    "%A, %B %-d, %y,", # 65
    "%A, %b %-d, %y,", # 66
    "%a, %B %d, %Y,",  # 67
    "%a, %b %d, %Y,",  # 68
    "%a, %B %-d, %Y,", # 69
    "%a, %b %-d, %Y,", # 70
    "%a, %B %d, %y,",  # 71
    "%a, %b %d, %y,",  # 72
    "%a, %B %-d, %y,", # 73
    "%a, %b %-d, %y,", # 74
]

EU_datetime_formats = [
    "%d-%m-%Y %H:%M",
    "%-d-%-m-%Y %H:%M",
    "%d/%m/%Y %H:%M",
    "%-d/%-m/%Y %H:%M",
    "%d.%m.%Y %H:%M",
    "%-d.%-m.%Y %H:%M",
    "%d %B %Y %H:%M",
    "%-d %B %Y %H:%M",
    "%d %b %Y %H:%M",
    "%-d %b %Y %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%-d-%-m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%-d/%-m/%Y %H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%-d.%-m.%Y %H:%M:%S",
    "%d %B %Y %H:%M:%S",
    "%-d %B %Y %H:%M:%S",
    "%d %b %Y %H:%M:%S",
    "%-d %b %Y %H:%M:%S",
    "%A, %d %B %Y %H:%M:%S",
    "%A, %-d %B %Y %H:%M:%S",
    "%a, %d %b %Y %H:%M:%S",
    "%a, %-d %b %Y %H:%M:%S",
    "%A, %d %B %Y %H:%M",
    "%A, %-d %B %Y %H:%M",
    "%a, %d %b %Y %H:%M",
    "%a, %-d %b %Y %H:%M",
    "%d-%m-%y %H:%M",       # 29
    "%-d-%-m-%y %H:%M",     # 30
    "%d/%m/%y %H:%M",       # 31
    "%-d/%-m/%y %H:%M",     # 32
    "%d.%m.%y %H:%M",       # 33
    "%-d.%-m.%y %H:%M",     # 34
    "%d %B %y %H:%M",       # 35
    "%-d %B %y %H:%M",      # 36
    "%d %b %y %H:%M",       # 37
    "%-d %b %y %H:%M",      # 38
    "%d-%m-%y %H:%M:%S",    # 39
    "%-d-%-m-%y %H:%M:%S",  # 40
    "%d/%m/%y %H:%M:%S",    # 41
    "%-d/%-m/%y %H:%M:%S",  # 42
    "%d.%m.%y %H:%M:%S",    # 43
    "%-d.%-m.%y %H:%M:%S",  # 44
    "%d %B %y %H:%M:%S",    # 45
    "%-d %B %y %H:%M:%S",   # 46
    "%d %b %y %H:%M:%S",    # 47
    "%-d %b %y %H:%M:%S",   # 48
    "%Y-%m-%d %H:%M:%S",    # 49
    "%Y-%m-%d %H:%M",       # 50
    "%Y/%m/%d %H:%M:%S",    # 51
    "%Y/%m/%d %H:%M",       # 52
    "%d-%m-%Y, %H:%M:%S",   # 53
    "%-d-%-m-%Y, %H:%M:%S", # 54
    "%d/%m/%Y, %H:%M:%S",   # 55
    "%-d/%-m/%Y, %H:%M:%S", # 56
    "%d.%m.%Y, %H:%M:%S",   # 57
    "%-d.%-m.%Y, %H:%M:%S", # 58
    "%d-%m-%y, %H:%M:%S",   # 59
    "%-d-%-m-%y, %H:%M:%S", # 60
    "%d/%m/%y, %H:%M:%S",   # 61
    "%-d/%-m/%y, %H:%M:%S", # 62
    "%d.%m.%y, %H:%M:%S",   # 63
    "%-d.%-m.%y, %H:%M:%S", # 64
    "%d %B %Y %I:%M %p",    # 65
    "%-d %B %Y %I:%M %p",   # 66
    "%d %b %Y %I:%M %p",    # 67
    "%-d %b %Y %I:%M %p",   # 68
    "%d %B %y %I:%M %p",    # 69
    "%-d %B %y %I:%M %p",   # 70
    "%d %b %y %I:%M %p",    # 71
    "%-d %b %y %I:%M %p",   # 72
    "%d %B %Y %I:%M:%S %p", # 73
    "%-d %B %Y %I:%M:%S %p",# 74
    "%d %b %Y %I:%M:%S %p", # 75
    "%-d %b %Y %I:%M:%S %p",# 76
    "%d %B %y %I:%M:%S %p", # 77
    "%-d %B %y %I:%M:%S %p",# 78
    "%d %b %y %I:%M:%S %p", # 79
    "%-d %b %y %I:%M:%S %p",# 80
    "%A, %d %B %Y %H:%M %p",# 81
    "%A, %-d %B %Y %H:%M %p", # 82
    "%a, %d %b %Y %H:%M %p",  # 83
    "%a, %-d %b %Y %H:%M %p", # 84
    "%A, %d %B %y %H:%M %p",  # 85
    "%A, %-d %B %y %H:%M %p", # 86
    "%a, %d %b %y %H:%M %p",  # 87
    "%a, %-d %b %y %H:%M %p"  # 88
    "%I:%M%p %d-%b-%Y",
]

US_datetime_formats = [
    "%m/%d/%Y %I:%M %p",
    "%-m/%-d/%Y %I:%M %p",
    "%m-%d-%Y %I:%M %p",
    "%-m-%-d-%Y %I:%M %p",
    "%B %d, %Y %I:%M %p",
    "%b %d, %Y %I:%M %p",
    "%B %-d, %Y %I:%M %p",
    "%b %-d, %Y %I:%M %p",
    "%m/%d/%Y %H:%M:%S",
    "%-m/%-d/%Y %H:%M:%S",
    "%m-%d-%Y %H:%M:%S",
    "%-m-%-d-%Y %H:%M:%S",
    "%B %d, %Y %H:%M:%S",
    "%b %d, %Y %H:%M:%S",
    "%B %-d, %Y %H:%M:%S",
    "%b %-d, %Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%-m/%-d/%Y %H:%M",
    "%m-%d-%Y %H:%M",
    "%-m-%-d-%Y %H:%M",
    "%A, %B %d, %Y %H:%M:%S",
    "%A, %B %-d, %Y %H:%M:%S",
    "%a, %b %d, %Y %H:%M:%S",
    "%a, %b %-d, %Y %H:%M:%S",
    "%A, %B %d, %Y %I:%M %p",
    "%A, %B %-d, %Y %I:%M %p",
    "%a, %b %d, %Y %I:%M %p",
    "%a, %b %-d, %Y %I:%M %p",
    "%m/%d/%y %I:%M %p",  # 29
    "%-m/%-d/%y %I:%M %p",  # 30
    "%m-%d-%y %I:%M %p",  # 31
    "%-m-%-d-%y %I:%M %p",  # 32
    "%m/%d/%y %H:%M:%S",  # 33
    "%-m/%-d/%y %H:%M:%S",  # 34
    "%m-%d-%y %H:%M:%S",  # 35
    "%-m-%-d-%y %H:%M:%S",  # 36
    "%m/%d/%y %H:%M",  # 37
    "%-m/%-d/%y %H:%M",  # 38
    "%m-%d-%y %H:%M",  # 39
    "%-m-%-d-%y %H:%M",  # 40
    "%A, %b %d, %Y %H:%M",  # 41
    "%A, %b %-d, %Y %H:%M",  # 42
    "%a, %b %d, %Y %H:%M",  # 43
    "%a, %b %-d, %Y %H:%M",  # 44
    "%A, %B %d, %Y %H:%M",  # 45
    "%A, %B %-d, %Y %H:%M",  # 46
    "%a, %B %d, %Y %H:%M",  # 47
    "%a, %B %-d, %Y %H:%M",  # 48
    "%Y-%m-%d %H:%M:%S",  # 49
    "%Y-%m-%d %I:%M %p",  # 50
    "%Y/%m/%d %H:%M:%S",  # 51
    "%Y/%m/%d %I:%M %p",  # 52
    "%m/%d/%Y, %H:%M:%S",  # 53
    "%-m/%-d/%Y, %H:%M:%S",  # 54
    "%m-%d-%Y, %H:%M:%S",  # 55
    "%-m-%-d-%Y, %H:%M:%S",  # 56
    "%m/%d/%y, %H:%M:%S",  # 57
    "%-m/%-d/%y, %H:%M:%S",  # 58
    "%m-%d-%y, %H:%M:%S",  # 59
    "%-m-%-d-%y, %H:%M:%S",  # 60
    "%m/%d/%Y %I:%M:%S %p",

]

EU_Formats = EU_Date_formats + EU_datetime_formats

US_Formats = US_Date_formats + US_datetime_formats

All_Formats = EU_Formats + US_Formats