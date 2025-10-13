"""
Test coverage for threadimportcommand module
Currently at 33% coverage - adding tests to improve
"""
import pytest
import os
from pyimport.threadimportcommand import ThreadImportCommand
from pyimport.fieldfile import FieldFile
from test.mdbtest import MDBTestDB


def test_thread_import_basic():
    """Test basic threaded import functionality"""
    with MDBTestDB() as tr:
        test_file = "test_thread_basic.csv"
        test_ff = "test_thread_basic.tff"

        try:
            # Create test CSV
            with open(test_file, 'w') as f:
                f.write("name,age,city\n")
                f.write("Alice,30,NYC\n")
                f.write("Bob,25,LA\n")
                f.write("Charlie,35,Chicago\n")
                f.write("David,28,Boston\n")
                f.write("Eve,32,Seattle\n")

            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                threads=True
            )

            results = ThreadImportCommand(args=args.ns).run()

            assert results.total_results == 1
            assert results.total_written == 5
            assert results.total_errors == 0

            # Verify data
            count = tr.test_col.count_documents({})
            assert count == 5

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


def test_thread_import_multiple_files():
    """Test threaded import with multiple files"""
    with MDBTestDB() as tr:
        test_files = ["test_thread_1.csv", "test_thread_2.csv"]
        test_ffs = ["test_thread_1.tff", "test_thread_2.tff"]

        try:
            # Create first test file
            with open(test_files[0], 'w') as f:
                f.write("name,value\n")
                f.write("Item1,100\n")
                f.write("Item2,200\n")

            # Create second test file
            with open(test_files[1], 'w') as f:
                f.write("name,value\n")
                f.write("Item3,300\n")
                f.write("Item4,400\n")

            # Generate field files
            for f in test_files:
                FieldFile.generate_field_file(f)

            args = tr.args.add_arguments(
                filenames=test_files,
                hasheader=True,
                threads=True
            )

            results = ThreadImportCommand(args=args.ns).run()

            assert results.total_results == 2
            assert results.total_written == 4
            assert tr.test_col.count_documents({}) == 4

        finally:
            for f in test_files + test_ffs:
                if os.path.exists(f):
                    os.unlink(f)


def test_thread_import_with_poolsize():
    """Test threaded import with custom pool size"""
    with MDBTestDB() as tr:
        # Create multiple test files
        test_files = [f"test_thread_pool_{i}.csv" for i in range(4)]
        test_ffs = [f"test_thread_pool_{i}.tff" for i in range(4)]

        try:
            for i, test_file in enumerate(test_files):
                with open(test_file, 'w') as f:
                    f.write("id,value\n")
                    f.write(f"{i},value_{i}\n")

                FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=test_files,
                hasheader=True,
                threads=True,
                poolsize=2  # Use pool of 2 threads
            )

            results = ThreadImportCommand(args=args.ns).run()

            # Wait for MongoDB writes to complete (write concern 0)
            import time
            time.sleep(0.2)

            assert results.total_results == 4
            assert results.total_written == 4
            assert tr.test_col.count_documents({}) == 4

        finally:
            for f in test_files + test_ffs:
                if os.path.exists(f):
                    os.unlink(f)


def test_thread_import_with_delimiter():
    """Test threaded import with custom delimiter"""
    with MDBTestDB() as tr:
        test_file = "test_thread_delim.csv"
        test_ff = "test_thread_delim.tff"

        try:
            with open(test_file, 'w') as f:
                f.write("product|price|stock\n")
                f.write("Widget|19.99|100\n")
                f.write("Gadget|29.99|50\n")

            FieldFile.generate_field_file(test_file, delimiter="|")

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                delimiter="|",
                threads=True
            )

            results = ThreadImportCommand(args=args.ns).run()

            assert results.total_written == 2
            assert tr.test_col.count_documents({}) == 2

            widget = tr.test_col.find_one({"product": "Widget"})
            assert widget["price"] == 19.99

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


def test_thread_import_with_batchsize():
    """Test threaded import with custom batch size"""
    with MDBTestDB() as tr:
        test_file = "test_thread_batch.csv"
        test_ff = "test_thread_batch.tff"

        try:
            # Create larger file
            with open(test_file, 'w') as f:
                f.write("id,data\n")
                for i in range(50):
                    f.write(f"{i},data_{i}\n")

            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                batchsize=10,
                threads=True
            )

            results = ThreadImportCommand(args=args.ns).run()

            assert results.total_written == 50
            assert tr.test_col.count_documents({}) == 50

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


def test_thread_import_with_addfilename():
    """Test threaded import with addfilename option (tests import success)"""
    with MDBTestDB() as tr:
        test_file = "test_thread_fname.csv"
        test_ff = "test_thread_fname.tff"

        try:
            with open(test_file, 'w') as f:
                f.write("name,score\n")
                f.write("Test1,100\n")
                f.write("Test2,200\n")

            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                addfilename=True,
                threads=True
            )

            results = ThreadImportCommand(args=args.ns).run()

            # Verify basic import succeeded
            assert results.total_written == 2
            assert tr.test_col.count_documents({}) == 2

            # Verify documents were imported
            doc = tr.test_col.find_one({"name": "Test1"})
            assert doc is not None
            assert doc["score"] == 100

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


def test_thread_import_combined_with_async():
    """Test threaded import combined with async processing"""
    with MDBTestDB() as tr:
        test_file = "test_thread_async.csv"
        test_ff = "test_thread_async.tff"

        try:
            with open(test_file, 'w') as f:
                f.write("id,value\n")
                for i in range(20):
                    f.write(f"{i},value_{i}\n")

            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                threads=True,
                asyncpro=True  # Combine threading with async
            )

            results = ThreadImportCommand(args=args.ns).run()

            assert results.total_written == 20
            assert tr.test_col.count_documents({}) == 20

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


def test_thread_import_with_addtimestamp():
    """Test threaded import with timestamp option"""
    with MDBTestDB() as tr:
        test_file = "test_thread_timestamp.csv"
        test_ff = "test_thread_timestamp.tff"

        try:
            # Create CSV
            with open(test_file, 'w') as f:
                f.write("name,value\n")
                f.write("Item1,100\n")
                f.write("Item2,200\n")

            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                addtimestamp="now",
                threads=True
            )

            results = ThreadImportCommand(args=args.ns).run()

            # Verify basic import succeeded
            assert results.total_written == 2
            assert tr.test_col.count_documents({}) == 2

            # Verify documents were imported
            doc = tr.test_col.find_one({"name": "Item1"})
            assert doc is not None
            assert doc["value"] == 100

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)
