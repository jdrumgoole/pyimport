"""
Integration tests for TFF v2.0 nested document format.
Tests end-to-end CSV import with nested field mappings.
"""
import os
import pytest
from pyimport.fieldfile import FieldFile
from pyimport.csvreader import CSVReader
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.argmgr import ArgMgr
from test.mdbtest import MDBTestDB


class TestV2NestedIntegration:
    """Integration tests for v2.0 nested format."""

    def setup_method(self):
        """Set up test resources."""
        self.test_dir = os.path.dirname(__file__)
        self.csv_file = os.path.join(self.test_dir, "test_v2_nested.csv")
        self.tff_file = os.path.join(self.test_dir, "test_v2_nested.tff")

    def test_load_v2_field_file(self):
        """Test that v2.0 field file loads correctly."""
        ff = FieldFile.load(self.tff_file)

        # Check v2.0 detection
        assert ff.is_v2_format() is True

        # Check field paths
        paths = ff.get_field_paths()
        assert paths["first_name"] == "personal.name.first"
        assert paths["city"] == "address.city"
        assert paths["age"] == "personal.age"

    def test_csvreader_produces_nested_docs(self):
        """Test that CSVReader produces nested documents with v2.0 TFF."""
        ff = FieldFile.load(self.tff_file)

        with open(self.csv_file, 'r') as csv_file:
            reader = CSVReader(csv_file, ff, delimiter=',', has_header=True)
            docs = list(reader)

        # Should have 3 documents
        assert len(docs) == 3

        # Check first document structure
        doc = docs[0]
        assert "personal" in doc
        assert "address" in doc
        assert "contact" in doc

        # Check nested structure
        assert doc["personal"]["name"]["first"] == "John"
        assert doc["personal"]["name"]["last"] == "Doe"
        assert doc["personal"]["age"] == 30

        assert doc["address"]["street"] == "123 Main St"
        assert doc["address"]["city"] == "Boston"
        assert doc["address"]["state"] == "MA"
        assert doc["address"]["postal_code"] == "02101"

        assert doc["contact"]["email"] == "john@example.com"

    def test_import_v2_nested_to_mongodb(self):
        """Test full import of v2.0 nested format to MongoDB."""
        with MDBTestDB() as tr:
            # Build arguments for import
            args = tr.args.add_arguments(
                filenames=[self.csv_file],
                fieldfile=self.tff_file,
                hasheader=True,
                writeconcern=1  # Wait for writes to complete
            )

            # Run import
            result = MDBImportCommand(args=args.ns).run()

            # Wait for MongoDB writes to complete
            import time
            time.sleep(0.2)

            # Verify import
            assert result.total_written == 3
            assert tr.test_col.count_documents({}) == 3

            # Verify document structure in MongoDB
            doc = tr.test_col.find_one({"personal.name.first": "John"})
            assert doc is not None
            assert doc["personal"]["name"]["last"] == "Doe"
            assert doc["personal"]["age"] == 30
            assert doc["address"]["city"] == "Boston"
            assert doc["contact"]["email"] == "john@example.com"

            # Verify we can query nested fields
            jane_doc = tr.test_col.find_one({"personal.name.first": "Jane"})
            assert jane_doc is not None
            assert jane_doc["address"]["city"] == "Cambridge"

    def test_v1_compatibility_no_paths(self):
        """Test that v1.0 format still works (backward compatibility)."""
        # Create a simple v1.0 TFF without paths
        v1_tff = os.path.join(self.test_dir, "test_v1_compat.tff")
        v1_csv = os.path.join(self.test_dir, "test_v1_compat.csv")

        # Create simple v1.0 CSV
        with open(v1_csv, 'w') as f:
            f.write("name,age\n")
            f.write("Alice,25\n")
            f.write("Bob,30\n")

        # Create simple v1.0 TFF (no paths)
        with open(v1_tff, 'w') as f:
            f.write("[name]\n")
            f.write('type = "str"\n')
            f.write('name = "name"\n')
            f.write('format = ""\n\n')
            f.write("[age]\n")
            f.write('type = "int"\n')
            f.write('name = "age"\n')
            f.write('format = ""\n\n')
            f.write("[DEFAULTS_SECTION]\n")
            f.write('delimiter = ","\n')
            f.write('has_header = true\n')
            f.write('"CSV File" = "test_v1_compat.csv"\n')

        # Load and test
        ff = FieldFile.load(v1_tff)
        assert ff.is_v2_format() is False

        with open(v1_csv, 'r') as csv_file:
            reader = CSVReader(csv_file, ff, delimiter=',', has_header=True)
            docs = list(reader)

        # Should produce flat documents (v1.0 behavior)
        assert len(docs) == 2
        assert docs[0] == {"name": "Alice", "age": 25}
        assert docs[1] == {"name": "Bob", "age": 30}

        # Cleanup
        os.remove(v1_tff)
        os.remove(v1_csv)

    def test_mixed_v1_v2_fields(self):
        """Test mixing v1.0 (no path) and v2.0 (with path) fields in same TFF."""
        # Create TFF with mixed fields
        mixed_tff = os.path.join(self.test_dir, "test_mixed.tff")
        mixed_csv = os.path.join(self.test_dir, "test_mixed.csv")

        # Create CSV
        with open(mixed_csv, 'w') as f:
            f.write("first_name,id,city\n")
            f.write("Alice,123,Boston\n")

        # Create TFF with some fields having paths, others not
        with open(mixed_tff, 'w') as f:
            f.write("[first_name]\n")
            f.write('type = "str"\n')
            f.write('name = "first_name"\n')
            f.write('path = "name.first"\n')
            f.write('format = ""\n\n')
            f.write("[id]\n")
            f.write('type = "int"\n')
            f.write('name = "id"\n')
            # No path - stays at top level
            f.write('format = ""\n\n')
            f.write("[city]\n")
            f.write('type = "str"\n')
            f.write('name = "city"\n')
            f.write('path = "address.city"\n')
            f.write('format = ""\n\n')
            f.write("[DEFAULTS_SECTION]\n")
            f.write('delimiter = ","\n')
            f.write('has_header = true\n')
            f.write('"CSV File" = "test_mixed.csv"\n')

        # Load and test
        ff = FieldFile.load(mixed_tff)
        assert ff.is_v2_format() is True  # Has at least one path

        with open(mixed_csv, 'r') as csv_file:
            reader = CSVReader(csv_file, ff, delimiter=',', has_header=True)
            docs = list(reader)

        # Should produce mixed structure: nested for mapped, flat for unmapped
        assert len(docs) == 1
        doc = docs[0]
        assert doc["name"]["first"] == "Alice"
        assert doc["address"]["city"] == "Boston"
        assert doc["id"] == 123  # Stays at top level (no path)

        # Cleanup
        os.remove(mixed_tff)
        os.remove(mixed_csv)
