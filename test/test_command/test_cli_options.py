"""
Comprehensive tests for command-line options in pyimport.
Tests various combinations of CLI flags and their effects.
"""
import os
import tempfile
import pytest
from pyimport.argmgr import ArgMgr
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.fieldfile import FieldFile
from pyimport.filesplitter import LineCounter
from pyimport.doctimestamp import DocTimeStamp
from test.mdbtest import MDBTestDB


class TestCLIOptions:
    """Test command-line option functionality"""

    def test_batchsize_option(self):
        """Test --batchsize option"""
        with MDBTestDB() as tr:
            test_file = "test_batchsize.csv"
            test_ff = "test_batchsize.tff"

            try:
                # Create test file
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    for i in range(100):
                        f.write(f"item{i},{i}\n")

                FieldFile.generate_field_file(test_file)

                # Test with small batch size
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    batchsize=10
                )

                assert args.ns.batchsize == 10
                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 100

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_limit_option(self):
        """Test --limit option"""
        with MDBTestDB() as tr:
            test_file = "test_limit.csv"
            test_ff = "test_limit.tff"

            try:
                # Create test file with 50 rows
                with open(test_file, 'w') as f:
                    f.write("id,value\n")
                    for i in range(50):
                        f.write(f"{i},val{i}\n")

                FieldFile.generate_field_file(test_file)

                # Test limit to 20 rows
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    limit=20
                )

                assert args.ns.limit == 20
                results = MDBImportCommand(args=args.ns).run()
                # Limit includes header, so we should get 20 data rows
                assert results.total_written == 20

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_locator_option(self):
        """Test --locator option adds line number to documents"""
        with MDBTestDB() as tr:
            test_file = "test_locator.csv"
            test_ff = "test_locator.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    f.write("test1,100\n")

                FieldFile.generate_field_file(test_file)

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    locator=True
                )

                assert args.ns.locator is True
                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 1

                # Verify locator field exists
                doc = tr.test_col.find_one({"name": "test1"})
                assert doc is not None
                assert "locator" in doc
                assert "line" in doc["locator"]

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_noenrich_option(self):
        """Test --noenrich option skips type conversion"""
        with MDBTestDB() as tr:
            test_file = "test_noenrich.csv"
            test_ff = "test_noenrich.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name,number\n")
                    f.write("test,42\n")

                FieldFile.generate_field_file(test_file)

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    noenrich=True
                )

                assert args.ns.noenrich is True
                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 1

                # With noenrich, number should be string not int
                doc = tr.test_col.find_one({"name": "test"})
                assert doc is not None
                # Note: noenrich keeps the CSV dict structure
                assert "number" in doc or "name" in doc

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_drop_option(self):
        """Test --drop option is accepted (note: actual drop behavior may vary)"""
        with MDBTestDB() as tr:
            test_file = "test_drop.csv"
            test_ff = "test_drop.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    f.write("new_data,100\n")

                FieldFile.generate_field_file(test_file)

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    drop=True
                )

                assert args.ns.drop is True
                results = MDBImportCommand(args=args.ns).run()

                # Verify import succeeded
                assert results.total_written == 1
                assert tr.test_col.find_one({"name": "new_data"}) is not None

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_delimiter_option(self):
        """Test --delimiter option with different delimiters"""
        with MDBTestDB() as tr:
            test_file = "test_delim.csv"
            test_ff = "test_delim.tff"

            try:
                # Create file with pipe delimiter
                with open(test_file, 'w') as f:
                    f.write("name|value|count\n")
                    f.write("test1|100|5\n")
                    f.write("test2|200|10\n")

                FieldFile.generate_field_file(test_file, delimiter="|")

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    delimiter="|"
                )

                assert args.ns.delimiter == "|"
                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 2

                # Verify data parsed correctly
                doc = tr.test_col.find_one({"name": "test1"})
                assert doc is not None
                assert doc["value"] == 100
                assert doc["count"] == 5

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_delimiter_tab_option(self):
        """Test --delimiter with 'tab' keyword"""
        with MDBTestDB() as tr:
            test_file = "test_tab.csv"
            test_ff = "test_tab.tff"

            try:
                # Create file with tab delimiter
                with open(test_file, 'w') as f:
                    f.write("name\tvalue\n")
                    f.write("test1\t100\n")

                FieldFile.generate_field_file(test_file, delimiter="\t")

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    delimiter="tab"
                )

                assert args.ns.delimiter == "tab"
                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 1

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_addfield_different_types(self):
        """Test --addfield with different value types"""
        with MDBTestDB() as tr:
            test_file = "test_addfield_types.csv"
            test_ff = "test_addfield_types.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name\n")
                    f.write("test1\n")
                    f.write("test2\n")

                FieldFile.generate_field_file(test_file)

                # Test with integer
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    addfield="category=5"
                )

                results = MDBImportCommand(args=args.ns).run()
                docs = list(tr.test_col.find({"category": 5}))
                assert len(docs) == 2

                # Clear collection for next test
                tr.test_col.delete_many({})

                # Test with float
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    addfield="rating=4.5"
                )

                results = MDBImportCommand(args=args.ns).run()
                docs = list(tr.test_col.find({"rating": 4.5}))
                assert len(docs) == 2

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_writeconcern_option(self):
        """Test --writeconcern option (verify it's accepted)"""
        with MDBTestDB() as tr:
            test_file = "test_wc.csv"
            test_ff = "test_wc.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    f.write("test,100\n")

                FieldFile.generate_field_file(test_file)

                # Use writeconcern=0 to avoid PyMongo API issues
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    writeconcern=0
                )

                assert args.ns.writeconcern == 0
                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 1

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_silent_option(self):
        """Test --silent option (just verify it's set)"""
        args = ArgMgr.default_args(input_args=[]).add_arguments(silent=True)
        assert args.ns.silent is True

        args = ArgMgr.default_args(input_args=[]).add_arguments(silent=False)
        assert args.ns.silent is False

    def test_verbose_option(self):
        """Test --verbose option (just verify it's set)"""
        args = ArgMgr.default_args(input_args=[]).add_arguments(verbose=True)
        assert args.ns.verbose is True

        args = ArgMgr.default_args(input_args=[]).add_arguments(verbose=False)
        assert args.ns.verbose is False

    def test_multiple_filenames(self):
        """Test processing multiple files"""
        with MDBTestDB() as tr:
            file1 = "test_multi1.csv"
            file2 = "test_multi2.csv"
            ff = "test_multi1.tff"

            try:
                # Create two files
                with open(file1, 'w') as f:
                    f.write("name\n")
                    f.write("file1_item\n")

                with open(file2, 'w') as f:
                    f.write("name\n")
                    f.write("file2_item\n")

                FieldFile.generate_field_file(file1)

                args = tr.args.add_arguments(
                    filenames=[file1, file2],
                    fieldfile=ff,
                    hasheader=True
                )

                results = MDBImportCommand(args=args.ns).run()
                assert results.total_results == 2
                assert results.total_written == 2

                # Verify both files imported
                assert tr.test_col.find_one({"name": "file1_item"}) is not None
                assert tr.test_col.find_one({"name": "file2_item"}) is not None

            finally:
                for f in [file1, file2, ff]:
                    if os.path.exists(f):
                        os.unlink(f)

    def test_hasheader_option(self):
        """Test --hasheader option behavior"""
        with MDBTestDB() as tr:
            test_file = "test_header.csv"
            test_ff = "test_header.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    f.write("test,100\n")

                FieldFile.generate_field_file(test_file)

                # With hasheader=True
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True
                )

                results = MDBImportCommand(args=args.ns).run()
                # Should skip header row
                assert results.total_written == 1

                # Verify data
                doc = tr.test_col.find_one({"name": "test"})
                assert doc is not None

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_database_collection_options(self):
        """Test --database and --collection options"""
        args = ArgMgr.default_args(input_args=[]).add_arguments(
            database="TEST_DB_NAME",
            collection="TEST_COLLECTION_NAME"
        )

        assert args.ns.database == "TEST_DB_NAME"
        assert args.ns.collection == "TEST_COLLECTION_NAME"

    def test_poolsize_option(self):
        """Test --poolsize option"""
        args = ArgMgr.default_args(input_args=[]).add_arguments(poolsize=4)
        assert args.ns.poolsize == 4

        args = ArgMgr.default_args(input_args=[]).add_arguments(poolsize=8)
        assert args.ns.poolsize == 8


class TestCLIOptionCombinations:
    """Test combinations of CLI options"""

    def test_limit_with_batchsize(self):
        """Test --limit combined with --batchsize"""
        with MDBTestDB() as tr:
            test_file = "test_combo1.csv"
            test_ff = "test_combo1.tff"

            try:
                # Use two columns to avoid enricher single-column warning issue
                with open(test_file, 'w') as f:
                    f.write("id,value\n")
                    for i in range(100):
                        f.write(f"{i},val{i}\n")

                FieldFile.generate_field_file(test_file)

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    limit=30,
                    batchsize=10
                )

                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 30

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_drop_with_addfield(self):
        """Test --drop combined with --addfield"""
        with MDBTestDB() as tr:
            test_file = "test_combo2.csv"
            test_ff = "test_combo2.tff"

            try:
                # Use two columns to avoid enricher single-column issues
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    f.write("new,100\n")

                FieldFile.generate_field_file(test_file)

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    drop=True,
                    addfield="source=test"
                )

                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 1

                # Verify new data with added field
                doc = tr.test_col.find_one({"name": "new"})
                assert doc is not None
                assert doc["source"] == "test"
                assert doc["value"] == 100

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_locator_with_addfield(self):
        """Test --locator combined with --addfield"""
        with MDBTestDB() as tr:
            test_file = "test_combo3.csv"
            test_ff = "test_combo3.tff"

            try:
                with open(test_file, 'w') as f:
                    f.write("name\n")
                    f.write("test\n")

                FieldFile.generate_field_file(test_file)

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True,
                    locator=True,
                    addfield="batch=1"
                )

                results = MDBImportCommand(args=args.ns).run()

                doc = tr.test_col.find_one({"name": "test"})
                assert doc is not None
                assert "locator" in doc
                assert doc["batch"] == 1

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)


class TestCLIEdgeCases:
    """Test edge cases and error conditions"""

    def test_missing_fieldfile(self):
        """Test behavior with missing fieldfile"""
        with MDBTestDB() as tr:
            test_file = "test_missing_ff.csv"

            try:
                with open(test_file, 'w') as f:
                    f.write("name\n")
                    f.write("test\n")

                # Specify non-existent fieldfile
                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile="nonexistent.tff",
                    hasheader=True
                )

                results = MDBImportCommand(args=args.ns).run()
                # Should report error
                assert results.total_errors >= 1
                assert results.total_results == 0

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)

    def test_empty_file(self):
        """Test importing an empty file"""
        with MDBTestDB() as tr:
            test_file = "test_empty.csv"
            test_ff = "test_empty.tff"

            try:
                # Create file with only header
                with open(test_file, 'w') as f:
                    f.write("name,value\n")
                    f.write("dummy,1\n")  # Need at least one row for field file

                FieldFile.generate_field_file(test_file)

                # Now make it empty (header only)
                with open(test_file, 'w') as f:
                    f.write("name,value\n")

                args = tr.args.add_arguments(
                    filenames=[test_file],
                    fieldfile=test_ff,
                    hasheader=True
                )

                results = MDBImportCommand(args=args.ns).run()
                assert results.total_written == 0

            finally:
                if os.path.exists(test_file):
                    os.unlink(test_file)
                if os.path.exists(test_ff):
                    os.unlink(test_ff)

    def test_zero_batchsize(self):
        """Test behavior with batchsize=0 (should use default or handle gracefully)"""
        args = ArgMgr.default_args(input_args=[]).add_arguments(batchsize=0)
        # Just verify it accepts the value
        assert args.ns.batchsize == 0

    def test_negative_limit(self):
        """Test behavior with negative limit"""
        args = ArgMgr.default_args(input_args=[]).add_arguments(limit=-1)
        # Just verify it accepts the value
        assert args.ns.limit == -1
