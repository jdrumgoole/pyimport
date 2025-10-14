"""
Comprehensive test suite for nested document builder (TFF v2.0 format).
Tests edge cases, error conditions, stress scenarios, and real-world use cases.
"""
import pytest
from pyimport.nested_builder import NestedDocumentBuilder, FieldPathMapper
from pyimport.fieldfile import FieldFile


class TestNestedDocumentBuilderEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_level_path(self):
        """Test path with no nesting (single level)."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "name", "John")
        assert doc == {"name": "John"}

    def test_very_deep_nesting(self):
        """Test deeply nested paths (10+ levels)."""
        doc = {}
        path = "level1.level2.level3.level4.level5.level6.level7.level8.level9.level10.value"
        NestedDocumentBuilder.set_nested_value(doc, path, "deep")

        # Navigate to verify
        current = doc
        for level in ["level1", "level2", "level3", "level4", "level5",
                      "level6", "level7", "level8", "level9", "level10"]:
            assert level in current
            current = current[level]
        assert current["value"] == "deep"

    def test_numeric_values(self):
        """Test with numeric values of various types."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.int", 42)
        NestedDocumentBuilder.set_nested_value(doc, "data.float", 3.14159)
        NestedDocumentBuilder.set_nested_value(doc, "data.negative", -100)
        NestedDocumentBuilder.set_nested_value(doc, "data.zero", 0)

        assert doc == {
            "data": {
                "int": 42,
                "float": 3.14159,
                "negative": -100,
                "zero": 0
            }
        }

    def test_none_value(self):
        """Test setting None as a value."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.null_field", None)
        assert doc == {"data": {"null_field": None}}

    def test_empty_string_value(self):
        """Test setting empty string as a value."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.empty", "")
        assert doc == {"data": {"empty": ""}}

    def test_special_characters_in_values(self):
        """Test values with special characters."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.special", "value with spaces & symbols!@#$%")
        NestedDocumentBuilder.set_nested_value(doc, "data.unicode", "日本語 🎉")
        NestedDocumentBuilder.set_nested_value(doc, "data.newline", "line1\nline2")

        assert doc["data"]["special"] == "value with spaces & symbols!@#$%"
        assert doc["data"]["unicode"] == "日本語 🎉"
        assert doc["data"]["newline"] == "line1\nline2"

    def test_boolean_values(self):
        """Test with boolean values."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "flags.enabled", True)
        NestedDocumentBuilder.set_nested_value(doc, "flags.disabled", False)

        assert doc == {
            "flags": {
                "enabled": True,
                "disabled": False
            }
        }

    def test_list_value(self):
        """Test setting a list as a value."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.items", [1, 2, 3, 4, 5])
        assert doc == {"data": {"items": [1, 2, 3, 4, 5]}}

    def test_dict_value(self):
        """Test setting a dict as a value (not a path)."""
        doc = {}
        value_dict = {"nested": "value", "count": 42}
        NestedDocumentBuilder.set_nested_value(doc, "data.object", value_dict)
        assert doc == {"data": {"object": value_dict}}

    def test_overwrite_scalar_with_scalar(self):
        """Test overwriting an existing scalar value."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.field", "original")
        NestedDocumentBuilder.set_nested_value(doc, "data.field", "updated")
        assert doc["data"]["field"] == "updated"

    def test_many_siblings(self):
        """Test many fields at the same nesting level."""
        doc = {}
        for i in range(100):
            NestedDocumentBuilder.set_nested_value(doc, f"data.field{i}", i)

        assert len(doc["data"]) == 100
        assert doc["data"]["field0"] == 0
        assert doc["data"]["field99"] == 99

    def test_mixed_depth_siblings(self):
        """Test siblings with different nesting depths."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "a", "top")
        NestedDocumentBuilder.set_nested_value(doc, "b.c", "mid")
        NestedDocumentBuilder.set_nested_value(doc, "d.e.f", "deep")

        assert doc == {
            "a": "top",
            "b": {"c": "mid"},
            "d": {"e": {"f": "deep"}}
        }

    def test_underscore_in_path(self):
        """Test paths with underscores."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "user_data.first_name", "John")
        NestedDocumentBuilder.set_nested_value(doc, "user_data.last_name", "Doe")

        assert doc == {
            "user_data": {
                "first_name": "John",
                "last_name": "Doe"
            }
        }

    def test_hyphen_in_path(self):
        """Test paths with hyphens."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "user-data.first-name", "John")
        assert doc == {"user-data": {"first-name": "John"}}

    def test_numeric_string_keys(self):
        """Test path components that look like numbers."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.123.value", "test")
        assert doc == {"data": {"123": {"value": "test"}}}


class TestNestedDocumentBuilderErrorHandling:
    """Test error conditions and validation."""

    def test_empty_path_raises_error(self):
        """Test that empty path raises ValueError."""
        doc = {}
        with pytest.raises(ValueError, match="Path cannot be empty"):
            NestedDocumentBuilder.set_nested_value(doc, "", "value")

    def test_conflict_parent_is_scalar(self):
        """Test error when parent in path is already a scalar."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data", "scalar_value")

        with pytest.raises(ValueError, match="Path conflict"):
            NestedDocumentBuilder.set_nested_value(doc, "data.field", "value")

    def test_conflict_intermediate_is_scalar(self):
        """Test error when intermediate path component is a scalar."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "a.b", "scalar")

        with pytest.raises(ValueError, match="Path conflict"):
            NestedDocumentBuilder.set_nested_value(doc, "a.b.c", "value")

    def test_conflict_target_is_dict(self):
        """Test error when trying to set scalar to existing dict."""
        doc = {}
        NestedDocumentBuilder.set_nested_value(doc, "data.field.nested", "value")

        with pytest.raises(ValueError, match="Path conflict"):
            NestedDocumentBuilder.set_nested_value(doc, "data.field", "scalar")

    def test_validate_prefix_conflict_forward(self):
        """Test validation catches prefix conflict (parent before child)."""
        field_paths = {
            "field1": "address",
            "field2": "address.city"
        }
        with pytest.raises(ValueError, match="Path conflict.*incompatible"):
            NestedDocumentBuilder.validate_paths(field_paths)

    def test_validate_prefix_conflict_reverse(self):
        """Test validation catches prefix conflict (child before parent)."""
        field_paths = {
            "field1": "address.city.name",
            "field2": "address.city"
        }
        with pytest.raises(ValueError, match="Path conflict.*incompatible"):
            NestedDocumentBuilder.validate_paths(field_paths)

    def test_validate_duplicate_paths(self):
        """Test validation catches duplicate paths."""
        field_paths = {
            "field1": "data.value",
            "field2": "data.value",
            "field3": "other"
        }
        with pytest.raises(ValueError, match="Duplicate path.*multiple fields"):
            NestedDocumentBuilder.validate_paths(field_paths)

    def test_validate_multiple_duplicates(self):
        """Test validation with multiple fields mapping to same path."""
        field_paths = {
            "a": "x",
            "b": "x",
            "c": "x"
        }
        with pytest.raises(ValueError, match="Duplicate path"):
            NestedDocumentBuilder.validate_paths(field_paths)


class TestBuildNestedDocComprehensive:
    """Comprehensive tests for build_nested_doc."""

    def test_empty_flat_doc(self):
        """Test with empty input document."""
        result = NestedDocumentBuilder.build_nested_doc({}, {})
        assert result == {}

    def test_no_mappings(self):
        """Test with no path mappings (all fields stay top-level)."""
        flat_doc = {"a": 1, "b": 2, "c": 3}
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, {})
        assert result == {"a": 1, "b": 2, "c": 3}

    def test_all_fields_mapped(self):
        """Test when all fields have path mappings."""
        flat_doc = {"a": 1, "b": 2, "c": 3}
        field_paths = {
            "a": "group1.a",
            "b": "group1.b",
            "c": "group2.c"
        }
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)
        assert result == {
            "group1": {"a": 1, "b": 2},
            "group2": {"c": 3}
        }

    def test_partial_mappings(self):
        """Test with some fields mapped, some not."""
        flat_doc = {"a": 1, "b": 2, "c": 3, "d": 4}
        field_paths = {
            "a": "nested.a",
            "c": "nested.c"
        }
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)
        assert result == {
            "nested": {"a": 1, "c": 3},
            "b": 2,  # Unmapped, stays top-level
            "d": 4   # Unmapped, stays top-level
        }

    def test_complex_hierarchy(self):
        """Test complex multi-level hierarchy."""
        flat_doc = {
            "org_name": "ACME Corp",
            "dept_type1": 100,
            "dept_type2": 50,
            "perf_metric1": 85.5,
            "perf_metric2": 92.3,
            "meta_created": "2025-01-01"
        }
        field_paths = {
            "org_name": "organization.name",
            "dept_type1": "departments.type1.count",
            "dept_type2": "departments.type2.count",
            "perf_metric1": "performance.metrics.score1",
            "perf_metric2": "performance.metrics.score2",
            "meta_created": "metadata.timestamps.created"
        }
        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)

        assert result == {
            "organization": {"name": "ACME Corp"},
            "departments": {
                "type1": {"count": 100},
                "type2": {"count": 50}
            },
            "performance": {
                "metrics": {
                    "score1": 85.5,
                    "score2": 92.3
                }
            },
            "metadata": {
                "timestamps": {
                    "created": "2025-01-01"
                }
            }
        }


class TestFieldPathMapperComprehensive:
    """Comprehensive tests for FieldPathMapper."""

    def test_empty_field_file(self):
        """Test with field file containing no fields."""
        field_dict = {}
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        assert mapper.is_v2_format is False
        assert mapper.field_paths == {}

    def test_single_field_v1(self):
        """Test with single v1.0 field."""
        field_dict = {
            "name": {"type": "str", "name": "name"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        assert mapper.is_v2_format is False
        assert mapper.build_document({"name": "John"}) == {"name": "John"}

    def test_single_field_v2(self):
        """Test with single v2.0 field."""
        field_dict = {
            "name": {"type": "str", "name": "name", "path": "user.name"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        assert mapper.is_v2_format is True
        assert mapper.field_paths == {"name": "user.name"}
        assert mapper.build_document({"name": "John"}) == {"user": {"name": "John"}}

    def test_many_fields_v1(self):
        """Test with many v1.0 fields."""
        field_dict = {f"field{i}": {"type": "int", "name": f"field{i}"}
                      for i in range(50)}
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        assert mapper.is_v2_format is False

    def test_many_fields_v2(self):
        """Test with many v2.0 fields."""
        field_dict = {
            f"field{i}": {
                "type": "int",
                "name": f"field{i}",
                "path": f"group{i % 5}.field{i}"
            }
            for i in range(50)
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        assert mapper.is_v2_format is True
        assert len(mapper.field_paths) == 50

    def test_mostly_v1_one_v2(self):
        """Test with mostly v1.0 fields but one v2.0 field."""
        field_dict = {
            "field1": {"type": "str", "name": "field1"},
            "field2": {"type": "str", "name": "field2"},
            "field3": {"type": "str", "name": "field3"},
            "field4": {"type": "str", "name": "field4", "path": "nested.field4"}  # Only v2.0
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        assert mapper.is_v2_format is True  # Even one path makes it v2.0
        assert mapper.field_paths == {"field4": "nested.field4"}

    def test_path_conflict_detected_on_init(self):
        """Test that path conflicts are detected during initialization."""
        field_dict = {
            "field1": {"type": "str", "name": "field1", "path": "data"},
            "field2": {"type": "str", "name": "field2", "path": "data.field"}
        }
        field_file = FieldFile(field_dict)

        with pytest.raises(ValueError, match="Path conflict"):
            FieldPathMapper(field_file)

    def test_duplicate_path_detected_on_init(self):
        """Test that duplicate paths are detected during initialization."""
        field_dict = {
            "field1": {"type": "str", "name": "field1", "path": "data.value"},
            "field2": {"type": "str", "name": "field2", "path": "data.value"}
        }
        field_file = FieldFile(field_dict)

        with pytest.raises(ValueError, match="Duplicate path"):
            FieldPathMapper(field_file)


class TestRealWorldScenarios:
    """Test real-world use cases and scenarios."""

    def test_healthcare_ae_data(self):
        """Test with healthcare A&E (Accident & Emergency) data structure."""
        field_dict = {
            "SHA": {"type": "str", "name": "SHA", "path": "organization.sha_code"},
            "Code": {"type": "str", "name": "Code", "path": "organization.code"},
            "Name": {"type": "str", "name": "Name", "path": "organization.name"},
            "Type1_Attendances": {"type": "int", "name": "Type1_Attendances",
                                  "path": "departments.type1.attendances"},
            "Type1_Over4Hours": {"type": "int", "name": "Type1_Over4Hours",
                                 "path": "departments.type1.over_4_hours"},
            "Percentage_4Hours": {"type": "float", "name": "Percentage_4Hours",
                                  "path": "performance.within_4_hours_pct"},
            "Admissions_Type1": {"type": "int", "name": "Admissions_Type1",
                                "path": "admissions.type1"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        flat_doc = {
            "SHA": "Q44",
            "Code": "REM",
            "Name": "BURTON HOSPITALS NHS TRUST",
            "Type1_Attendances": 7523,
            "Type1_Over4Hours": 1234,
            "Percentage_4Hours": 83.6,
            "Admissions_Type1": 1456
        }

        result = mapper.build_document(flat_doc)

        assert result["organization"]["sha_code"] == "Q44"
        assert result["organization"]["name"] == "BURTON HOSPITALS NHS TRUST"
        assert result["departments"]["type1"]["attendances"] == 7523
        assert result["departments"]["type1"]["over_4_hours"] == 1234
        assert result["performance"]["within_4_hours_pct"] == 83.6
        assert result["admissions"]["type1"] == 1456

    def test_ecommerce_order(self):
        """Test with e-commerce order data."""
        field_dict = {
            "order_id": {"type": "int", "name": "order_id", "path": "order.id"},
            "customer_name": {"type": "str", "name": "customer_name",
                             "path": "customer.name"},
            "customer_email": {"type": "str", "name": "customer_email",
                              "path": "customer.contact.email"},
            "ship_street": {"type": "str", "name": "ship_street",
                           "path": "shipping.address.street"},
            "ship_city": {"type": "str", "name": "ship_city",
                         "path": "shipping.address.city"},
            "ship_zip": {"type": "str", "name": "ship_zip",
                        "path": "shipping.address.postal_code"},
            "product_name": {"type": "str", "name": "product_name",
                           "path": "items.product.name"},
            "product_price": {"type": "float", "name": "product_price",
                            "path": "items.product.price"},
            "quantity": {"type": "int", "name": "quantity",
                        "path": "items.quantity"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        flat_doc = {
            "order_id": 12345,
            "customer_name": "Jane Smith",
            "customer_email": "jane@example.com",
            "ship_street": "456 Oak Ave",
            "ship_city": "Seattle",
            "ship_zip": "98101",
            "product_name": "Laptop",
            "product_price": 999.99,
            "quantity": 2
        }

        result = mapper.build_document(flat_doc)

        assert result["order"]["id"] == 12345
        assert result["customer"]["name"] == "Jane Smith"
        assert result["customer"]["contact"]["email"] == "jane@example.com"
        assert result["shipping"]["address"]["city"] == "Seattle"
        assert result["items"]["product"]["name"] == "Laptop"
        assert result["items"]["quantity"] == 2

    def test_iot_sensor_data(self):
        """Test with IoT sensor data."""
        field_dict = {
            "device_id": {"type": "str", "name": "device_id", "path": "device.id"},
            "device_type": {"type": "str", "name": "device_type", "path": "device.type"},
            "location_lat": {"type": "float", "name": "location_lat",
                           "path": "location.coordinates.lat"},
            "location_lon": {"type": "float", "name": "location_lon",
                           "path": "location.coordinates.lon"},
            "temp_celsius": {"type": "float", "name": "temp_celsius",
                           "path": "readings.temperature.celsius"},
            "humidity_pct": {"type": "float", "name": "humidity_pct",
                           "path": "readings.humidity.percentage"},
            "battery_level": {"type": "int", "name": "battery_level",
                            "path": "status.battery.level"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        flat_doc = {
            "device_id": "SENSOR_001",
            "device_type": "weather_station",
            "location_lat": 42.3601,
            "location_lon": -71.0589,
            "temp_celsius": 22.5,
            "humidity_pct": 65.0,
            "battery_level": 87
        }

        result = mapper.build_document(flat_doc)

        assert result["device"]["id"] == "SENSOR_001"
        assert result["location"]["coordinates"]["lat"] == 42.3601
        assert result["readings"]["temperature"]["celsius"] == 22.5
        assert result["status"]["battery"]["level"] == 87

    def test_financial_transaction(self):
        """Test with financial transaction data."""
        field_dict = {
            "txn_id": {"type": "str", "name": "txn_id", "path": "transaction.id"},
            "from_account": {"type": "str", "name": "from_account",
                           "path": "parties.sender.account_number"},
            "from_bank": {"type": "str", "name": "from_bank",
                        "path": "parties.sender.bank_code"},
            "to_account": {"type": "str", "name": "to_account",
                         "path": "parties.receiver.account_number"},
            "to_bank": {"type": "str", "name": "to_bank",
                      "path": "parties.receiver.bank_code"},
            "amount": {"type": "float", "name": "amount", "path": "payment.amount"},
            "currency": {"type": "str", "name": "currency", "path": "payment.currency"},
            "status": {"type": "str", "name": "status", "path": "transaction.status"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        flat_doc = {
            "txn_id": "TXN-2025-001",
            "from_account": "123456789",
            "from_bank": "BANK001",
            "to_account": "987654321",
            "to_bank": "BANK002",
            "amount": 1500.00,
            "currency": "USD",
            "status": "completed"
        }

        result = mapper.build_document(flat_doc)

        assert result["transaction"]["id"] == "TXN-2025-001"
        assert result["parties"]["sender"]["account_number"] == "123456789"
        assert result["parties"]["receiver"]["bank_code"] == "BANK002"
        assert result["payment"]["amount"] == 1500.00
        assert result["transaction"]["status"] == "completed"


class TestPerformanceAndStress:
    """Performance and stress tests."""

    def test_large_flat_document(self):
        """Test with large document (1000 fields)."""
        flat_doc = {f"field{i}": i for i in range(1000)}
        field_paths = {f"field{i}": f"group{i % 10}.field{i}" for i in range(1000)}

        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)

        # Verify structure
        assert len(result) == 10  # 10 groups
        assert all(f"group{i}" in result for i in range(10))
        assert result["group0"]["field0"] == 0
        assert result["group9"]["field999"] == 999

    def test_many_nested_levels(self):
        """Test with many nested levels (stress test)."""
        flat_doc = {}
        field_paths = {}

        for i in range(100):
            # Create paths with varying depths
            depth = (i % 10) + 1
            path_parts = [f"level{j}" for j in range(depth)]
            path = ".".join(path_parts + [f"field{i}"])

            field_name = f"field{i}"
            flat_doc[field_name] = i
            field_paths[field_name] = path

        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)

        # Verify some values
        assert result["level0"]["field0"] == 0
        assert result["level0"]["level1"]["field1"] == 1

    def test_repeated_builds(self):
        """Test repeated document building (performance check)."""
        field_dict = {
            "a": {"type": "int", "name": "a", "path": "data.a"},
            "b": {"type": "int", "name": "b", "path": "data.b"},
            "c": {"type": "int", "name": "c", "path": "data.c"}
        }
        field_file = FieldFile(field_dict)
        mapper = FieldPathMapper(field_file)

        # Build 1000 documents
        for i in range(1000):
            flat_doc = {"a": i, "b": i * 2, "c": i * 3}
            result = mapper.build_document(flat_doc)
            assert result["data"]["a"] == i

    def test_wide_document(self):
        """Test document with many fields at same level."""
        flat_doc = {f"field{i}": i for i in range(500)}
        field_paths = {f"field{i}": f"data.field{i}" for i in range(500)}

        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)

        assert len(result["data"]) == 500
        assert result["data"]["field0"] == 0
        assert result["data"]["field499"] == 499

    def test_mixed_complexity(self):
        """Test document with mixed simple and complex paths."""
        flat_doc = {}
        field_paths = {}

        # Simple paths
        for i in range(50):
            flat_doc[f"simple{i}"] = i
            field_paths[f"simple{i}"] = f"simple.field{i}"

        # Complex nested paths
        for i in range(50):
            flat_doc[f"complex{i}"] = i + 100
            field_paths[f"complex{i}"] = f"l1.l2.l3.l4.field{i}"

        result = NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)

        assert len(result["simple"]) == 50
        assert result["l1"]["l2"]["l3"]["l4"]["field0"] == 100
