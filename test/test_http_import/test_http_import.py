import pytest
import os

import pymongo
import requests

from pyimport.argmgr import ArgMgr
from pyimport.csvreader import CSVReader
from pyimport.fieldfile import FieldFile
from pyimport.enricher import Enricher
from pyimport.fieldfile import FieldFile
from pyimport.mdbimportcmd import MDBImportCommand
from test.mdbtest import MDBTestDB

path_dir = os.path.dirname(os.path.realpath(__file__))

def check_internet():
    url='http://www.google.com/'
    timeout=2
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return True
    except requests.ConnectionError:
        pass
    return False


def test_limit():
    #
    # TODO:need to test limit with a noheader file
    #

    with open("2018_Yellow_Taxi_Trip_Data_1000.csv", "r") as f:
        ff = FieldFile.load("2018_Yellow_Taxi_Trip_Data_1000.tff")
        reader = CSVReader(file=f,
                           delimiter=";",
                           limit=10,
                           field_file=ff,
                           has_header=True)

        for i, doc in enumerate(reader, 1):
            pass

        assert i == 10


def test_http_generate_fieldfile():
    if check_internet():
        # Demographic_Statistics_By_Zip_Code.csv
        url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"

        ff_file = FieldFile.generate_field_file(url,
                                                delimiter=";",
                                                ff_filename="yellow-trip-data.tff")

        assert "VendorID" in ff_file.fields()
        assert len(ff_file.fields()) == 17
        assert "fare_amount" in ff_file.fields()

        os.unlink("yellow-trip-data.tff")

    else:
        print("Warning:No internet: Skipping test for generating field files from URLs")


def test_http_import():
    if check_internet():
        with MDBTestDB() as tr:
            url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"

            FieldFile.generate_field_file(url,
                                          delimiter=";",
                                          ff_filename="yellow-trip-data.tff")
            args = tr.args.add_arguments(fieldfile="yellow-trip-data.tff", filenames=[url], delimiter=";", hasheader=True)
            before_doc_count = tr.test_col.count_documents({})
            result = MDBImportCommand(args=args.ns).run()
            after_doc_count = tr.test_col.count_documents({})
            assert 999 == (after_doc_count - before_doc_count)
            assert 999 == result.total_written
            os.unlink("yellow-trip-data.tff")
    else:
        print("Warning:No internet: test_http_import() skipped")



