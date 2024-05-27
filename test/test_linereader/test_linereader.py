import pytest

import pytest
from pyimport.linereader import LineReader


def test_read_local_file():
    expected_lines = [
        "Inventory Item, Amount, Last Order",
        "Screws,         300,    1-Jan-2016",
        "Bolts,          150,    3- Feb-2017",
        "Nails,          25,     31-Dec-2017",
        "Nuts,           75,     2 -Feb-2016",
    ]

    with LineReader('inventory.csv') as file_iter:
        for i, line in enumerate(file_iter):
            assert line.strip() == expected_lines.pop(i)
            break


def test_read_remote_file():
    expected_lines = [
        "VendorID;tpep_pickup_datetime;tpep_dropoff_datetime;passenger_count;trip_distance;RatecodeID;store_and_fwd_flag;PULocationID;DOLocationID;payment_type;fare_amount;extra;mta_tax;tip_amount;tolls_amount;improvement_surcharge;total_amount",
        "2;04/22/2017 04:30:39 AM;04/22/2017 04:46:37 AM;1;4.46;1;N;48;7;1;16;0.5;0.5;3.46;0;0.3;20.76",
        "2;04/22/2017 04:30:43 AM;04/22/2017 04:41:53 AM;1;2.54;1;N;80;198;2;10.5;0.5;0.5;0;0;0.3;11.8",
        "1;04/22/2017 04:30:44 AM;04/22/2017 04:53:44 AM;1;13.1;1;N;181;130;3;36.5;0.5;0.5;0;0;0.3;37.8",
        "2;04/22/2017 04:30:44 AM;04/22/2017 04:35:09 AM;5;0.94;1;N;70;70;1;5.5;0.5;0.5;1.36;0;0.3;8.16"
    ]
    remote_url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"

    with LineReader(remote_url) as file_iter:
        for i, line in enumerate(file_iter):
            assert line == expected_lines[i]
            if i == 4:
                break
