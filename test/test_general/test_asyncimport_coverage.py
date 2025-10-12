"""
Test coverage for asyncimport module
Currently at 28% coverage - adding tests to improve
"""
import pytest
import os
from pyimport.asyncimport import AsyncMDBImportCommand
from pyimport.argmgr import ArgMgr
from pyimport.fieldfile import FieldFile
from test.mdbtest import AsyncMDBTestDB


@pytest.mark.asyncio
async def test_async_import_basic():
    """Test basic async import functionality"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        # Create a simple test file
        test_file = "test_async_basic.csv"
        test_ff = "test_async_basic.tff"

        try:
            # Create test CSV with header
            with open(test_file, 'w') as f:
                f.write("name,age,city\n")
                f.write("Alice,30,NYC\n")
                f.write("Bob,25,LA\n")
                f.write("Charlie,35,Chicago\n")

            # Generate field file
            FieldFile.generate_field_file(test_file)

            # Setup args for async import
            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                asyncpro=True
            )

            # Run async import
            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            assert results.total_results == 1
            assert results.total_written == 3
            assert results.total_errors == 0

            # Verify data in database
            count = await tr.test_col.count_documents({})
            assert count == 3

            # Verify specific documents
            alice = await tr.test_col.find_one({"name": "Alice"})
            assert alice is not None
            assert alice["age"] == 30
            assert alice["city"] == "NYC"

        finally:
            # Cleanup
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


@pytest.mark.asyncio
async def test_async_import_with_delimiter():
    """Test async import with custom delimiter"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        test_file = "test_async_pipe.csv"
        test_ff = "test_async_pipe.tff"

        try:
            # Create test CSV with pipe delimiter
            with open(test_file, 'w') as f:
                f.write("product|price|quantity\n")
                f.write("Widget|19.99|100\n")
                f.write("Gadget|29.99|50\n")
                f.write("Doohickey|9.99|200\n")

            # Generate field file with custom delimiter
            FieldFile.generate_field_file(test_file, delimiter="|")

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                delimiter="|",
                asyncpro=True
            )

            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            assert results.total_written == 3
            assert await tr.test_col.count_documents({}) == 3

            # Verify price is float
            widget = await tr.test_col.find_one({"product": "Widget"})
            assert isinstance(widget["price"], float)
            assert widget["price"] == 19.99

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


@pytest.mark.asyncio
async def test_async_import_with_batchsize():
    """Test async import with custom batch size"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        test_file = "test_async_batch.csv"
        test_ff = "test_async_batch.tff"

        try:
            # Create larger test file
            with open(test_file, 'w') as f:
                f.write("id,value\n")
                for i in range(100):
                    f.write(f"{i},value_{i}\n")

            FieldFile.generate_field_file(test_file)

            # Use small batch size to test batching logic
            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                batchsize=10,
                asyncpro=True
            )

            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            assert results.total_written == 100
            assert await tr.test_col.count_documents({}) == 100

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


@pytest.mark.asyncio
async def test_async_import_with_addfilename():
    """Test async import with filename option (tests import success even if enrichment not implemented)"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        test_file = "test_async_filename.csv"
        test_ff = "test_async_filename.tff"

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
                asyncpro=True
            )

            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            # Verify basic import succeeded
            assert results.total_written == 2
            assert await tr.test_col.count_documents({}) == 2

            # Verify documents were imported
            doc = await tr.test_col.find_one({"name": "Test1"})
            assert doc is not None
            assert doc["score"] == 100

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


@pytest.mark.asyncio
async def test_async_import_with_addtimestamp():
    """Test async import with timestamp option (tests import success even if enrichment not implemented)"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        test_file = "test_async_timestamp.csv"
        test_ff = "test_async_timestamp.tff"

        try:
            with open(test_file, 'w') as f:
                f.write("name,value\n")
                f.write("Item1,1\n")
                f.write("Item2,2\n")

            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                addtimestamp="now",
                asyncpro=True
            )

            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            # Verify basic import succeeded
            assert results.total_written == 2
            assert await tr.test_col.count_documents({}) == 2

            # Verify documents were imported
            doc = await tr.test_col.find_one({"name": "Item1"})
            assert doc is not None
            assert doc["value"] == 1

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


@pytest.mark.asyncio
async def test_async_import_small_file():
    """Test async import with small CSV file"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        test_file = "test_async_small.csv"
        test_ff = "test_async_small.tff"

        try:
            # Create file with header and one data row
            with open(test_file, 'w') as f:
                f.write("col1,col2\n")
                f.write("val1,val2\n")

            # Generate field file
            FieldFile.generate_field_file(test_file)

            args = tr.args.add_arguments(
                filenames=[test_file],
                fieldfile=test_ff,
                hasheader=True,
                asyncpro=True
            )

            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            # Should import the single row
            assert results.total_written == 1
            assert await tr.test_col.count_documents({}) == 1

        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
            if os.path.exists(test_ff):
                os.unlink(test_ff)


@pytest.mark.asyncio
async def test_async_import_multiple_files():
    """Test async import with multiple CSV files"""
    async with AsyncMDBTestDB() as tr:
        # Clean any existing data
        await tr.test_col.delete_many({})

        test_file1 = "test_async_multi1.csv"
        test_file2 = "test_async_multi2.csv"
        test_ff = "test_async_multi1.tff"  # Same schema for both

        try:
            # Create first test file
            with open(test_file1, 'w') as f:
                f.write("name,value\n")
                f.write("File1Item1,100\n")
                f.write("File1Item2,200\n")

            # Create second test file (same schema)
            with open(test_file2, 'w') as f:
                f.write("name,value\n")
                f.write("File2Item1,300\n")
                f.write("File2Item2,400\n")

            # Generate field file from first file
            FieldFile.generate_field_file(test_file1)

            args = tr.args.add_arguments(
                filenames=[test_file1, test_file2],
                fieldfile=test_ff,
                hasheader=True,
                asyncpro=True
            )

            results = await AsyncMDBImportCommand(args=args.ns).process_files()

            # Should import all rows from both files
            assert results.total_written == 4
            assert await tr.test_col.count_documents({}) == 4

            # Verify data from both files
            assert await tr.test_col.find_one({"name": "File1Item1"}) is not None
            assert await tr.test_col.find_one({"name": "File2Item2"}) is not None

        finally:
            if os.path.exists(test_file1):
                os.unlink(test_file1)
            if os.path.exists(test_file2):
                os.unlink(test_file2)
            if os.path.exists(test_ff):
                os.unlink(test_ff)
