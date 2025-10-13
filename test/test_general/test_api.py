"""
Test the PyImport Python API

Tests the programmatic API interface for PyImport.
"""

import unittest
import tempfile
import os
from pathlib import Path

import pymongo

from pyimport.api import PyImportAPI, PyImportBuilder
from pyimport.fieldfile import FieldFile
from pyimport.importresult import ImportResults


class TestPyImportAPI(unittest.TestCase):
    """Test PyImportAPI class."""

    def setUp(self):
        """Set up test database and collection."""
        self.client = pymongo.MongoClient()
        self.database = "TEST_API_DB"
        self.collection = "test_api"
        self.db = self.client[self.database]
        self.col = self.db[self.collection]

        # Clean up before tests - ensure fully dropped
        if self.collection in self.db.list_collection_names():
            self.db.drop_collection(self.collection)

        # Wait for drop to complete
        import time
        time.sleep(0.1)

        # Create test CSV
        self.test_csv = tempfile.NamedTemporaryFile(
            mode='w', delete=False, suffix='.csv'
        )
        self.test_csv.write("name,age,city\n")
        self.test_csv.write("Alice,30,NYC\n")
        self.test_csv.write("Bob,25,LA\n")
        self.test_csv.write("Charlie,35,SF\n")
        self.test_csv.close()

    def tearDown(self):
        """Clean up test data."""
        # Drop test collection
        if self.collection in self.db.list_collection_names():
            self.db.drop_collection(self.collection)

        # Remove test CSV
        if os.path.exists(self.test_csv.name):
            os.unlink(self.test_csv.name)

        # Remove generated field file
        tff_file = Path(self.test_csv.name).with_suffix('.tff')
        if tff_file.exists():
            tff_file.unlink()

    def test_api_init(self):
        """Test API initialization."""
        api = PyImportAPI(
            mongodb_uri="mongodb://localhost:27017",
            database="test_db",
            collection="test_col"
        )

        self.assertEqual(api.mongodb_uri, "mongodb://localhost:27017")
        self.assertEqual(api.database, "test_db")
        self.assertEqual(api.collection, "test_col")

    def test_simple_import(self):
        """Test basic CSV import."""
        api = PyImportAPI(
            database=self.database,
            collection=self.collection,
            log_level="WARNING"
        )

        result = api.import_csv(
            self.test_csv.name,
            has_header=True
        )

        # Verify result
        self.assertIsInstance(result, ImportResults)
        self.assertEqual(result.total_written, 3)
        self.assertGreater(result.avg_records_per_sec, 0)

        # Verify data in MongoDB
        count = self.col.count_documents({})
        self.assertEqual(count, 3)

        # Check a document
        doc = self.col.find_one({"name": "Alice"})
        self.assertIsNotNone(doc)
        self.assertEqual(doc["name"], "Alice")
        # Age is auto-converted to int by field file generation
        self.assertEqual(doc["age"], 30)
        self.assertEqual(doc["city"], "NYC")

    def test_import_with_field_file_generation(self):
        """Test import with automatic field file generation."""
        api = PyImportAPI(
            database=self.database,
            collection=self.collection,
            log_level="WARNING"
        )

        # Generate field file
        field_file = api.generate_field_file(self.test_csv.name)
        self.assertIsInstance(field_file, FieldFile)
        self.assertEqual(len(field_file.fields()), 3)

        # Import with field file
        result = api.import_csv(
            self.test_csv.name,
            field_file=Path(self.test_csv.name).with_suffix('.tff'),
            has_header=True
        )

        self.assertEqual(result.total_written, 3)

        # Verify type conversion worked (age should be int)
        doc = self.col.find_one({"name": "Alice"})
        self.assertIsInstance(doc["age"], int)
        self.assertEqual(doc["age"], 30)

    def test_import_with_enrichment(self):
        """Test import with document enrichment."""
        api = PyImportAPI(
            database=self.database,
            collection=self.collection,
            log_level="WARNING"
        )

        result = api.import_csv(
            self.test_csv.name,
            has_header=True,
            add_timestamp=True,
            add_filename=True,
            add_field=["source=test"]  # API currently only supports first field
        )

        self.assertEqual(result.total_written, 3)

        # Check enriched document
        doc = self.col.find_one({"name": "Alice"})
        self.assertIn("timestamp", doc)
        self.assertEqual(doc["source"], "test")
        self.assertIn("filename", doc)
        # Verify filename is actually the CSV file path
        self.assertEqual(doc["filename"], self.test_csv.name)

    def test_drop_collection(self):
        """Test drop collection functionality."""
        api = PyImportAPI(
            database=self.database,
            collection=self.collection,
            log_level="WARNING"
        )

        # Insert some data
        api.import_csv(self.test_csv.name, has_header=True)
        self.assertEqual(self.col.count_documents({}), 3)

        # Drop collection
        api.drop_collection(
            database=self.database,
            collection=self.collection
        )

        # Wait for drop to propagate and refresh collection reference
        import time
        time.sleep(0.1)
        # Check if collection still exists
        if self.collection in self.db.list_collection_names():
            # Collection exists - count should be 0
            self.assertEqual(self.col.count_documents({}), 0)
        else:
            # Collection doesn't exist - that's also valid (can't count docs in non-existent collection)
            pass

    def test_drop_before_import(self):
        """Test drop_collection parameter in import."""
        api = PyImportAPI(
            database=self.database,
            collection=self.collection,
            log_level="WARNING"
        )

        # Ensure clean start
        if self.collection in self.db.list_collection_names():
            self.db.drop_collection(self.collection)
        import time
        time.sleep(0.1)

        # Import data
        api.import_csv(self.test_csv.name, has_header=True)
        count_after_first = self.col.count_documents({})
        self.assertEqual(count_after_first, 3, f"Expected 3 docs after first import, got {count_after_first}")

        # Import again with drop
        result = api.import_csv(
            self.test_csv.name,
            has_header=True,
            drop_collection=True
        )

        # Should still have 3 docs (dropped then re-imported)
        count_after_second = self.col.count_documents({})
        self.assertEqual(count_after_second, 3, f"Expected 3 docs after second import with drop, got {count_after_second}")
        self.assertEqual(result.total_written, 3)

    def test_load_field_file(self):
        """Test loading existing field file."""
        api = PyImportAPI(log_level="WARNING")

        # Generate field file first
        api.generate_field_file(self.test_csv.name)
        tff_path = Path(self.test_csv.name).with_suffix('.tff')

        # Load it
        field_file = api.load_field_file(str(tff_path))
        self.assertIsInstance(field_file, FieldFile)
        self.assertGreater(len(field_file.fields()), 0)

    def test_batch_size(self):
        """Test custom batch size."""
        api = PyImportAPI(
            database=self.database,
            collection=self.collection,
            log_level="WARNING"
        )

        result = api.import_csv(
            self.test_csv.name,
            has_header=True,
            batch_size=1  # Force small batches
        )

        self.assertEqual(result.total_written, 3)


class TestPyImportBuilder(unittest.TestCase):
    """Test PyImportBuilder fluent interface."""

    def setUp(self):
        """Set up test database and collection."""
        self.client = pymongo.MongoClient()
        self.database = "TEST_BUILDER_DB"
        self.collection = "test_builder"
        self.db = self.client[self.database]
        self.col = self.db[self.collection]

        # Clean up - ensure fully dropped
        if self.collection in self.db.list_collection_names():
            self.db.drop_collection(self.collection)
        import time
        time.sleep(0.1)

        # Create test CSV
        self.test_csv = tempfile.NamedTemporaryFile(
            mode='w', delete=False, suffix='.csv'
        )
        self.test_csv.write("product,price\n")
        self.test_csv.write("Widget,10.99\n")
        self.test_csv.write("Gadget,25.50\n")
        self.test_csv.close()

    def tearDown(self):
        """Clean up test data."""
        if self.collection in self.db.list_collection_names():
            self.db.drop_collection(self.collection)
        if os.path.exists(self.test_csv.name):
            os.unlink(self.test_csv.name)
        tff_file = Path(self.test_csv.name).with_suffix('.tff')
        if tff_file.exists():
            tff_file.unlink()

    def test_builder_basic(self):
        """Test basic builder pattern."""
        result = (PyImportBuilder()
            .connect("mongodb://localhost:27017")
            .database(self.database)
            .collection(self.collection)
            .csv_file(self.test_csv.name)
            .has_header(True)
            .log_level("WARNING")
            .import_data())

        self.assertIsInstance(result, ImportResults)
        self.assertEqual(result.total_written, 2)
        self.assertEqual(self.col.count_documents({}), 2)

    def test_builder_with_enrichment(self):
        """Test builder with enrichment options."""
        result = (PyImportBuilder()
            .connect("mongodb://localhost:27017")
            .database(self.database)
            .collection(self.collection)
            .csv_file(self.test_csv.name)
            .has_header(True)
            .add_timestamp()
            .add_filename()
            .add_field("category", "electronics")
            .log_level("WARNING")
            .import_data())

        self.assertEqual(result.total_written, 2)

        doc = self.col.find_one({"product": "Widget"})
        self.assertIn("timestamp", doc)
        self.assertEqual(doc["category"], "electronics")
        self.assertIn("filename", doc)
        self.assertEqual(doc["filename"], self.test_csv.name)

    def test_builder_chaining(self):
        """Test method chaining works correctly."""
        builder = (PyImportBuilder()
            .connect("mongodb://localhost:27017")
            .database(self.database)
            .collection(self.collection)
            .csv_file(self.test_csv.name)
            .delimiter(",")
            .has_header(True)
            .batch_size(100)
            .log_level("WARNING"))

        # Verify builder returns itself for chaining
        self.assertIsInstance(builder, PyImportBuilder)

        result = builder.import_data()
        self.assertEqual(result.total_written, 2)

    def test_builder_drop_first(self):
        """Test drop_first option."""
        # Import data
        (PyImportBuilder()
            .database(self.database)
            .collection(self.collection)
            .csv_file(self.test_csv.name)
            .has_header(True)
            .log_level("WARNING")
            .import_data())

        self.assertEqual(self.col.count_documents({}), 2)

        # Import again with drop
        result = (PyImportBuilder()
            .database(self.database)
            .collection(self.collection)
            .csv_file(self.test_csv.name)
            .has_header(True)
            .drop_first()
            .log_level("WARNING")
            .import_data())

        # Should still have 2 (dropped then imported)
        self.assertEqual(self.col.count_documents({}), 2)

    def test_builder_no_files_error(self):
        """Test error when no files specified."""
        with self.assertRaises(ValueError) as context:
            (PyImportBuilder()
                .database(self.database)
                .collection(self.collection)
                .has_header(True)
                .import_data())

        self.assertIn("No CSV files specified", str(context.exception))


if __name__ == '__main__':
    unittest.main()
