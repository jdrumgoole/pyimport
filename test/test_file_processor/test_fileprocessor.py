"""
Created on 7 Aug 2017

@author: jdrumgoole
"""

import os
import unittest

import pymongo
import pymongo.errors

from pyimport.argmgr import ArgMgr
from pyimport.csvreader import CSVReader
from pyimport.filesplitter import LineCounter
from pyimport.mdbimportcmd import MDBImportCommand
import pytest

from test.mdbtest import MDBTestDB


def test_a_and_e_data():
    with MDBTestDB() as tr:
        start_count = tr.test_col.count_documents({})
        args = tr.args.add_arguments(filenames=["AandE_Data_2011-04-10-300.csv"],
                                     fieldfile="AandE_Data_2011-04-10.tff",
                                     delimiter=",", hasheader=True)
        results = MDBImportCommand(args=args.ns).run()
        assert results.total_errors == 0
        assert results.total_results == 1
        lines = LineCounter.count_now("AandE_Data_2011-04-10-300.csv") - 1
        assert lines == (tr.test_col.count_documents({}) - start_count)
        assert tr.test_col.find_one({"Code": "RA4"})
        assert results.total_written == lines


def test_sniff():
    assert CSVReader.sniff_header("uk_property_prices.csv") is False
    assert CSVReader.sniff_header("10k.txt") is False
    assert CSVReader.sniff_header("AandE_Data_2011-04-10-300.csv") is True
    assert CSVReader.sniff_header("gdelt.tsv") is False


def test_property_prices():

    with MDBTestDB() as tr:
        start_count = tr.test_col.count_documents({})
        args = tr.args.add_arguments(filenames=["uk_property_prices.csv"], delimiter=",", hasheader=True)
        results = MDBImportCommand(args=args.ns).run()
        lines = LineCounter.count_now("uk_property_prices.csv") - 1
        assert lines == tr.test_col.count_documents({}) - start_count
        assert tr.test_col.find_one({"Postcode": "NG10 5NN"})


def test_mot_data():

    with MDBTestDB() as tr:
        start_count = tr.test_col.count_documents({})
        args = tr.args.add_arguments(filenames=["10k.txt"], delimiter="|", hasheader=True)
        result = MDBImportCommand(args=args.ns).run()
        lines = LineCounter.count_now("10k.txt") - 1
        assert lines == tr.test_col.count_documents({}) - start_count
        assert lines == result.total_written
        assert tr.test_col.find_one({"test_id": 114624})


def test_date_format():

    with MDBTestDB() as tr:
        start_count = tr.test_col.count_documents({})
        args = tr.args.add_arguments(filenames=["mot_time_format_test.txt"], delimiter="|", hasheader=True)
        result=MDBImportCommand(args=args.ns).run()
        lines = LineCounter.count_now("mot_time_format_test.txt") - 1
        assert lines == tr.test_col.count_documents({}) - start_count
        assert tr.test_col.find_one({"test_id": 1077})
        assert lines == result.total_written


def test_gdelt_data():
    with MDBTestDB() as tr:
        start_count = tr.test_col.count_documents({})
        args = tr.args.add_arguments(filenames=["gdelt.tsv"], fieldfile="GDELT_columns.tff", delimiter="tab", hasheader=False)
        result = MDBImportCommand(args=args.ns).run()
        lines = LineCounter.count_now("gdelt.tsv")
        assert lines == tr.test_col.count_documents({}) - start_count
        assert result.total_written == lines
        assert tr.test_col.find_one({"SOURCEURL": "https://www.standardspeaker.com/news/dream-factory-director-retiring-1.2467094"})

