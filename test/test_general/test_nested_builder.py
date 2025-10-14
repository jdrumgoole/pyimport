"""
Unit tests for nested document builder (TFF v2.0 format).
"""
import pytest
from pyimport.nested_builder import NestedDocumentBuilder, FieldPathMapper
from pyimport.fieldfile import FieldFile


class TestNestedDocumentBuilder:
    """Test the NestedDocumentBuilder class."""

    def test_set_nested_value_simple(self):
        """Test setting a simple nested value."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "address.city", "Boston")
        assert doc == {"address": {"city": "Boston"}}

    def test_set_nested_value_deep(self):
        """Test setting a deeply nested value."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "person.name.first", "John")
        assert doc == {"person": {"name": {"first": "John"}}}

    def test_set_nested_value_multiple(self):
        """Test setting multiple nested values."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "address.city", "Boston")
        NestedDocumentBuilder.set_nested_value(doc, "address.state", "MA")
        NestedDocumentBuilder.set_nested_value(doc, "name", "John")
        assert doc == {
            "address": {
                "city": "Boston",
                "state": "MA"
            },
            "name": "John"
        }

    def test_set_nested_value_empty_path(self):
        """Test that empty path raises error."""
        doc = {}
        with pytest.raises(ValueError, match="Path cannot be empty"):
            NestedDocumentBuilder.set_nested_value(doc, "", "value")

    def test_set_nested_value_conflict_scalar_to_dict(self):
        """Test that setting a scalar value where a dict exists raises error."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "address.city", "Boston")
        with pytest.raises(ValueError, match="Path conflict"):
            # Try to set address to a scalar when address.city already exists
            NestedDocumentBuilder.set_nested_value(doc, "address", "New Value")

    def test_set_nested_value_conflict_dict_to_scalar(self):
        """Test that setting a dict path where a scalar exists raises error."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "address", "123 Main St")
        with pytest.raises(ValueError, match="Path conflict"):
            # Try to create address.city when address is already a scalar
            NestedDocumentBuilder.set_nested_value(doc, "address.city", "Boston")

    def test_build_nested_doc_simple(self):
        """Test building a simple nested document."""
        flat_doc = {
            "first_name": "John",
            "city": "Boston"
        }
        field_paths = {
            "first_name": "name.first",
            "city": "address.city"
        }
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)
        assert result == {
            "name": {"first": "John"},
            "address": {"city": "Boston"}
        }

    def test_build_nested_doc_mixed_mapped_unmapped(self):
        """Test building document with both mapped and unmapped fields."""
        flat_doc = {
            "first_name": "John",
            "age": 30,
            "city": "Boston"
        }
        field_paths = {
            "first_name": "name.first",
            "city": "address.city"
            # age has no mapping, should stay at top level
        }
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)
        assert result == {
            "name": {"first": "John"},
            "address": {"city": "Boston"},
            "age": 30  # Top-level field (v1.0 compatibility)
        }

    def test_build_nested_doc_same_parent(self):
        """Test building document with multiple fields under same parent."""
        flat_doc = {
            "first_name": "John",
            "last_name": "Doe",
            "street": "123 Main St",
            "city": "Boston",
            "state": "MA"
        }
        field_paths = {
            "first_name": "personal.name.first",
            "last_name": "personal.name.last",
            "street": "address.street",
            "city": "address.city",
            "state": "address.state"
        }
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)
        assert result == {
            "personal": {
                "name": {
                    "first": "John",
                    "last": "Doe"
                }
            },
            "address": {
                "street": "123 Main St",
                "city": "Boston",
                "state": "MA"
            }
        }

    def test_validate_paths_valid(self):
        """Test that valid paths pass validation."""
        field_paths = {
            "field1": "address.city",
            "field2": "address.state",
            "field3": "name.first"
        }
        # Should not raise
        NestedDocumentBuilder.validate_paths(field_paths)

    def test_validate_paths_prefix_conflict(self):
        """Test that prefix conflicts are detected."""
        field_paths = {
            "field1": "address",
            "field2": "address.city"
        }
        with pytest.raises(ValueError, match="Path conflict.*incompatible"):
            NestedDocumentBuilder.validate_paths(field_paths)

    def test_validate_paths_duplicate(self):
        """Test that duplicate paths are detected."""
        field_paths = {
            "field1": "address.city",
            "field2": "address.city"
        }
        with pytest.raises(ValueError, match="Duplicate path.*multiple fields"):
            NestedDocumentBuilder.validate_paths(field_paths)


class TestFieldPathMapper:
    """Test the FieldPathMapper class."""

    def test_v1_format_detection(self):
        """Test that v1.0 format is correctly detected."""
        field_dict = {
            "name": {"type": "str", "name": "name"},
            "age": {"type": "int", "name": "age"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)
        assert mapper.is_v2_format is False
        assert len(mapper.field_paths) == 0

    def test_v2_format_detection(self):
        """Test that v2.0 format is correctly detected."""
        field_dict = {
            "first_name": {"type": "str", "name": "first_name", "path": "name.first"},
            "age": {"type": "int", "name": "age"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)
        assert mapper.is_v2_format is True
        assert mapper.field_paths == {"first_name": "name.first"}

    def test_build_document_v1_passthrough(self):
        """Test that v1.0 format documents pass through unchanged."""
        field_dict = {
            "name": {"type": "str", "name": "name"},
            "age": {"type": "int", "name": "age"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        flat_doc = {"name": "John", "age": 30}
        result = mapper.build_document(flat_doc)
        assert result == flat_doc  # Should be unchanged

    def test_build_document_v2_nested(self):
        """Test that v2.0 format documents are nested."""
        field_dict = {
            "first_name": {"type": "str", "name": "first_name", "path": "name.first"},
            "city": {"type": "str", "name": "city", "path": "address.city"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        flat_doc = {"first_name": "John", "city": "Boston"}
        result = mapper.build_document(flat_doc)
        assert result == {
            "name": {"first": "John"},
            "address": {"city": "Boston"}
        }

    def test_path_conflict_validation_on_init(self):
        """Test that path conflicts are caught during mapper initialization."""
        field_dict = {
            "field1": {"type": "str", "name": "field1", "path": "address"},
            "field2": {"type": "str", "name": "field2", "path": "address.city"}
        }
        field_file = FieldFile(field_dict)

        with pytest.raises(ValueError, match="Path conflict"):
            FieldPathMapper(field_file)


class TestFieldFileV2Extensions:
    """Test the v2.0 extensions to FieldFile class."""

    def test_path_value_exists(self):
        """Test getting path value when it exists."""
        field_dict = {
            "first_name": {"type": "str", "name": "first_name", "path": "name.first"}
        }
        field_file = FieldFile(field_dict)
        assert field_file.path_value("first_name") == "name.first"

    def test_path_value_not_exists(self):
        """Test getting path value when it doesn't exist."""
        field_dict = {
            "name": {"type": "str", "name": "name"}
        }
        field_file = FieldFile(field_dict)
        assert field_file.path_value("name") is None

    def test_is_v2_format_true(self):
        """Test is_v2_format returns True when paths exist."""
        field_dict = {
            "first_name": {"type": "str", "name": "first_name", "path": "name.first"},
            "age": {"type": "int", "name": "age"}
        }
        field_file = FieldFile(field_dict)
        assert field_file.is_v2_format() is True

    def test_is_v2_format_false(self):
        """Test is_v2_format returns False when no paths exist."""
        field_dict = {
            "name": {"type": "str", "name": "name"},
            "age": {"type": "int", "name": "age"}
        }
        field_file = FieldFile(field_dict)
        assert field_file.is_v2_format() is False

    def test_get_field_paths(self):
        """Test getting all field paths."""
        field_dict = {
            "first_name": {"type": "str", "name": "first_name", "path": "name.first"},
            "city": {"type": "str", "name": "city", "path": "address.city"},
            "age": {"type": "int", "name": "age"}
        }
        field_file = FieldFile(field_dict)
        paths = field_file.get_field_paths()
        assert paths == {
            "first_name": "name.first",
            "city": "address.city"
        }
