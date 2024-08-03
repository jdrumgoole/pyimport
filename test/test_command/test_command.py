
import os
import shutil
import random

import pymongo
import pytest

from pyimport import type_converter
from pyimport.argparser import ArgMgr
from pyimport.generatefieldfilecommand import GenerateFieldfileCommand
from pyimport.dropcollectioncommand import DropCollectionCommand
from pyimport.asyncimport import AsyncImportCommand
from pyimport.enricher import Enricher
from pyimport.importcommand import ImportCommand
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.filesplitter import LineCounter
from test.mdbtest import MDBTestDB, AsyncMDBTestDB


def get_random_line(file_path: str) -> str:
    # we ignore the header line
    selected_line = None
    with open(file_path, 'r') as file:
        for i, line in enumerate(file):
            if i == 0:
                continue
            if i == 1:
                selected_line = line
            else:
                # Randomly replace the selected line with a probability of 1/(i+1)
                if random.randint(1, i) == 1:
                    selected_line = line
    return selected_line.strip()


def get_random_field(csvfile: str, ff: FieldFile, delimiter: str) -> [str, str]:
    line = get_random_line(csvfile)
    vals = line.split(delimiter)
    doc = dict(zip(ff.fields(), vals))
    doc = Enricher(field_file=ff, locator=False).enrich_doc(doc)
    k = random.choice(list(doc.keys()))
    return k, doc[k]


@pytest.mark.asyncio
async def test_async_one_file():
    async with AsyncMDBTestDB() as tr:
        size_120 = LineCounter.count_now("120lines.txt")
        files = ["120lines.txt"]
        args = tr.args.add_arguments(fieldfile="10k.tff", filenames=files, delimiter="|")
        start_size = await tr.test_col.count_documents({})
        cmd = AsyncImportCommand(args.ns)
        result = await cmd.process_one_file(args.ns, tr.log, filename=files[0])
        end_size = await tr.test_col.count_documents({})
        assert result.total_written == size_120
        assert size_120 == (end_size - start_size)

        field_info = ImportCommand.prep_field_file(args.ns)
        test_values = { "test_id": 346,
                        "make": "TOYOTA",
                        "model": "COROLLA",
                        "test_id": 2212}
        for i in range(20):
            k, v = get_random_field("120lines.txt", field_info, "|")
            v = type_converter.convert_it(field_info.type_value(k), v)
            assert await tr.test_col.find_one(
                {k: v}) is not None, f"Field '{k}' with value '{v}' not found in collection i={i}"


@pytest.mark.asyncio
async def test_async_import_command():
    async with AsyncMDBTestDB() as tr:
        size_10k = LineCounter.count_now("10k.txt")
        size_120 = LineCounter.count_now("120lines.txt")
        files = ["10k.txt", "120lines.txt"]
        args = tr.args.add_arguments(fieldfile="10k.tff", filenames=files, delimiter="|")
        start_size = await tr.test_col.count_documents({})
        results = await AsyncImportCommand(args.ns).process_files()
        end_size = await tr.test_col.count_documents({})
        assert results.total_written == (size_10k + size_120)
        assert (size_10k + size_120) == (end_size - start_size)

        field_info = ImportCommand.prep_field_file(args.ns)
        for i in range(20):
            k, v = get_random_field("10k.txt", field_info, "|")
            v = type_converter.convert_it(field_info.type_value(k), v)
            assert await tr.test_col.find_one({k: v}) is not None, f"Field '{k}' with value '{v}' not found in collection i={i}"


def test_import_command_small():
    with MDBTestDB() as tr:
        start_size = tr.test_col.count_documents({})
        size_test = LineCounter.count_now("test_date_data.csv") - 1
        args = tr.args.add_arguments(fieldfile="10k.tff",
                                     filenames=["test_date_data.csv"],
                                     hasheader=True)
        results = ImportCommand(args=args.ns).run()
        result = results.filename_results("test_date_data.csv")
        assert size_test == result.total_written

        new_size = tr.test_col.count_documents({})
        assert size_test == (new_size - start_size)


def test_import_command_add_field():
    with MDBTestDB() as tr:
        args = tr.args.add_arguments(fieldfile="10k.tff",
                                     filenames=["test_date_data.csv"],
                                     addfield="test_field=20",
                                     hasheader=True)
        results = ImportCommand(args=args.ns).run()

        assert len(list(tr.test_col.find({"test_field": 20}))) == results.total_written

        args = tr.args.add_arguments(fieldfile="10k.tff",
                                     filenames=["test_date_data.csv"],
                                     addfield="test_field=hello",
                                     hasheader=True)
        results = ImportCommand(args=args.ns).run()

        assert len(list(tr.test_col.find({"test_field": "hello"}))) == results.total_written

        args = tr.args.add_arguments(fieldfile="10k.tff",
                                     filenames=["test_date_data.csv"],
                                     addfield="test_field=3.71",
                                     hasheader=True)
        results = ImportCommand(args=args.ns).run()

        assert len(list(tr.test_col.find({"test_field": 3.71}))) == results.total_written



def test_import_command_cut():
    with MDBTestDB() as tr:
        start_size = tr.test_col.count_documents({})
        size_test = LineCounter.count_now("test_date_data.csv") - 1
        args = tr.args.add_arguments(fieldfile="10k.tff",
                                     filenames=["test_date_data.csv"],
                                     hasheader=True)
        args = args.add_arguments(cut="test_mileage,colour")
        results = ImportCommand(args=args.ns).run()
        result = results.filename_results("test_date_data.csv")
        assert size_test == result.total_written

        new_size = tr.test_col.count_documents({})
        assert size_test == (new_size - start_size)
def test_drop_command():

    c = pymongo.MongoClient()
    if "TEST_DROP_CMD" in c.list_database_names():
        c.drop_database("TEST_DROP_CMD")
    db = c["TEST_DROP_CMD"]
    assert "testx" not in db.list_collection_names()
    col = db["testx"]

    col.insert_one({"hello": "world"})
    args = ArgMgr.default_args().add_arguments(database="TEST_DROP_CMD", collection="testx")
    assert col.find_one({"hello": "world"}, projection={"_id":0}) == {"hello": "world"}
    DropCollectionCommand(args=args.ns).run()
    import time
    time.sleep(0.01) # some times the delete doesn't complete as its a server side action
    assert "testx" not in db.list_collection_names()


def test_generate_fieldfile_command():
    shutil.copy("yellow_tripdata_2015-01-06-200k.csv", "test_generate_ff.csv")
    args = ArgMgr.default_args().add_arguments(delimiter=",", fieldfile=None, filenames=["test_generate_ff.csv"])
    GenerateFieldfileCommand(args=args.ns).run()
    assert os.path.isfile("test_generate_ff.tff")
    ff = FieldFile.load("test_generate_ff.tff")
    assert ff.field_dict["tpep_pickup_datetime"]["type"] == "datetime"
    assert ff.field_dict["tpep_dropoff_datetime"]["type"] == "datetime"
    assert ff.field_dict["pickup_latitude"]["type"] == "float"
    os.unlink("test_generate_ff.tff")
    os.unlink("test_generate_ff.csv")


# def test_generate_nyc_200k():
#     with MDBTestDB() as tr:
#         args = tr.args.add_arguments(delimiter=",", fieldfile="yellow_trip.tff", filenames=["yellow_tripdata_2015-01-06-200k.csv"])
#         results = ImportCommand(args=args.ns).run()
#         assert results.total_errors == 0


def test_import_command_nyc():
    with MDBTestDB() as tr:

        start_size = tr.test_col.count_documents({})
        size_test = LineCounter.count_now("yellow_trip_data_10.csv") - 1
        args = tr.args.add_arguments(fieldfile="yellow_trip_data_10.tff",
                                     filenames=["yellow_trip_data_10.csv"], hasheader=True)
        ImportCommand(args=args.ns).run()
        new_size = tr.test_col.count_documents({})
        assert size_test == (new_size - start_size)


def test_import_command_nyc_no_field_file():
    with MDBTestDB() as tr:
        with pytest.raises(FieldFileException):
            args = tr.args.add_arguments(fieldfile="yellow_trip_data_10xxx.tff",
                                         filenames=["yellow_trip_data_10.csv"], hasheader=True)
            results = ImportCommand(args=args.ns).process_one_file(args.ns, tr.log, filename="yellow_trip_data_10.csv")

    with MDBTestDB() as tr:
        args = tr.args.add_arguments(fieldfile="yellow_trip_data_10xxx.tff",
                                     filenames=["yellow_trip_data_10.csv"], hasheader=True)
        results = ImportCommand(args=args.ns).run()
        assert results.total_errors == 1
        assert results.total_results == 0



@pytest.mark.asyncio
async def test_import_command_nyc_async():
    async with AsyncMDBTestDB() as tr:
        start_size = await tr.test_col.count_documents({})
        size_test = LineCounter.count_now("yellow_trip_data_10.csv") - 1
        args = tr.args.add_arguments(fieldfile="yellow_trip_data_10.tff",
                                     asyncpro=True,
                                     filenames=["yellow_trip_data_10.csv"], hasheader=True)
        result = await AsyncImportCommand(args=args.ns).process_one_file(args.ns, tr.log,
                                                                         filename="yellow_trip_data_10.csv")
        new_size = await tr.test_col.count_documents({})
        assert result.total_written == (new_size - start_size)
        assert size_test == result.total_written

    async with AsyncMDBTestDB() as tr:
        start_size = await tr.test_col.count_documents({})
        size_test = LineCounter.count_now("yellow_trip_data_10.csv") - 1
        args = tr.args.add_arguments(fieldfile="yellow_trip_data_10.tff",
                                     asyncpro=True,
                                     filenames=["yellow_trip_data_10.csv", "yellow_trip_data_20.csv" ], hasheader=True)
        result = await AsyncImportCommand(args=args.ns).process_one_file(args.ns, tr.log,
                                                                         filename="yellow_trip_data_10.csv")
        new_size = await tr.test_col.count_documents({})
        assert result.total_written == (new_size - start_size)
        assert size_test == result.total_written


