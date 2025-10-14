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

        tff_path = os.path.join(path_dir, "yellow-trip-data.tff")
        ff_file = FieldFile.generate_field_file(url,
                                                delimiter=";",
                                                ff_filename=tff_path)

        assert "VendorID" in ff_file.fields()
        assert len(ff_file.fields()) == 17
        assert "fare_amount" in ff_file.fields()

        if os.path.exists(tff_path):
            os.unlink(tff_path)

    else:
        print("Warning:No internet: Skipping test for generating field files from URLs")


def test_http_import():
    if check_internet():
        with MDBTestDB() as tr:
            url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"

            # Generate field file in current directory
            tff_path = os.path.join(path_dir, "yellow-trip-data.tff")
            FieldFile.generate_field_file(url,
                                          delimiter=";",
                                          ff_filename=tff_path)

            # Verify field file exists
            if not os.path.exists(tff_path):
                raise FileNotFoundError(f"Field file not generated at {tff_path}")

            args = tr.args.add_arguments(fieldfile=tff_path, filenames=[url], delimiter=";", hasheader=True)
            before_doc_count = tr.test_col.count_documents({})
            result = MDBImportCommand(args=args.ns).run()
            after_doc_count = tr.test_col.count_documents({})
            assert 999 == (after_doc_count - before_doc_count)
            assert 999 == result.total_written

            # Clean up
            if os.path.exists(tff_path):
                os.unlink(tff_path)
    else:
        print("Warning:No internet: test_http_import() skipped")



