"""
GUI tests for field file generation and management

Tests field file generation, editing, validation, and type conversion.
"""

import pytest
from playwright.sync_api import Page, expect
import tempfile
import csv
from pathlib import Path


@pytest.mark.gui
class TestFieldFile:
    """Test suite for field file functionality"""

    def test_field_file_generation_creates_correct_fields(self, authenticated_page: Page, test_csv_file):
        """Test that field file generation identifies all CSV columns"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Verify all fields are present
        field_list = authenticated_page.locator('#field-list')

        # Check each expected field
        expected_fields = ['id', 'name', 'email', 'age', 'signup_date']
        for field_name in expected_fields:
            field_input = field_list.locator(f'input[value="{field_name}"]')
            expect(field_input).to_be_visible()

    def test_field_name_inputs_are_readonly(self, authenticated_page: Page, test_csv_file):
        """Test that field names cannot be edited"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Verify field name inputs are readonly
        field_name_inputs = authenticated_page.locator('input[data-field="name"]')
        first_field_name = field_name_inputs.first

        # Check readonly attribute
        expect(first_field_name).to_have_attribute('readonly', '')

    def test_all_field_types_available(self, authenticated_page: Page, test_csv_file):
        """Test that all supported field types are available in dropdowns"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Get first type selector
        type_select = authenticated_page.locator('select[data-field="type"]').first

        # Verify all expected types are available
        expected_types = ['str', 'int', 'float', 'bool', 'date', 'datetime', 'isodate', 'timestamp']

        for field_type in expected_types:
            option = type_select.locator(f'option[value="{field_type}"]')
            expect(option).to_be_attached()

    def test_change_all_fields_to_different_types(self, authenticated_page: Page, test_csv_file):
        """Test changing all fields to different types"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Change each field to a specific type
        types_to_set = ['int', 'str', 'str', 'float', 'date']

        for index, field_type in enumerate(types_to_set):
            type_select = authenticated_page.locator(f'select[data-index="{index}"][data-field="type"]')
            type_select.select_option(field_type)
            expect(type_select).to_have_value(field_type)

    def test_format_field_for_date_types(self, authenticated_page: Page, test_csv_file):
        """Test adding format strings for date/datetime fields"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Set signup_date to date and add format
        date_field_index = 4  # signup_date is 5th field (index 4)
        date_type_select = authenticated_page.locator(f'select[data-index="{date_field_index}"][data-field="type"]')
        date_type_select.select_option('date')

        date_format_input = authenticated_page.locator(f'input[data-index="{date_field_index}"][data-field="format"]')
        date_format_input.fill('%Y-%m-%d')

        expect(date_format_input).to_have_value('%Y-%m-%d')

    def test_format_field_optional_for_non_date_types(self, authenticated_page: Page, test_csv_file):
        """Test that format field can be left empty for non-date types"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Set id to int, leave format empty
        int_type_select = authenticated_page.locator('select[data-index="0"][data-field="type"]')
        int_type_select.select_option('int')

        format_input = authenticated_page.locator('input[data-index="0"][data-field="format"]')
        expect(format_input).to_have_value('')

    def test_datetime_format_examples(self, authenticated_page: Page, test_csv_file):
        """Test various datetime format strings"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Test different datetime formats
        date_field_index = 4
        format_input = authenticated_page.locator(f'input[data-index="{date_field_index}"][data-field="format"]')

        test_formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m-%d-%Y',
            '%Y-%m-%d %H:%M:%S',
            '%d-%b-%Y'
        ]

        for fmt in test_formats:
            format_input.fill(fmt)
            expect(format_input).to_have_value(fmt)

    def test_field_file_with_csv_with_many_columns(self, authenticated_page: Page):
        """Test field file generation with CSV containing many columns"""
        # Create CSV with 20 columns
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            headers = [f'field_{i}' for i in range(20)]
            writer.writerow(headers)
            writer.writerow([str(i) for i in range(20)])
            temp_path = Path(f.name)

        try:
            # Generate field file
            authenticated_page.set_input_files('#file-upload', str(temp_path))
            authenticated_page.fill('#import-database', 'testdb')
            authenticated_page.fill('#import-collection', 'testcol')
            authenticated_page.click('#generate-btn')
            authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

            # Verify all 20 fields are present
            field_rows = authenticated_page.locator('#field-list .field-row')
            expect(field_rows).to_have_count(20)

        finally:
            # Cleanup
            if temp_path.exists():
                temp_path.unlink()

    def test_field_file_with_special_characters_in_names(self, authenticated_page: Page):
        """Test field file generation with special characters in column names"""
        # Create CSV with special characters
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            headers = ['field-with-dash', 'field_with_underscore', 'field.with.dot', 'field with space']
            writer.writerow(headers)
            writer.writerow(['1', '2', '3', '4'])
            temp_path = Path(f.name)

        try:
            # Generate field file
            authenticated_page.set_input_files('#file-upload', str(temp_path))
            authenticated_page.fill('#import-database', 'testdb')
            authenticated_page.fill('#import-collection', 'testcol')
            authenticated_page.click('#generate-btn')
            authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

            # Verify all fields with special characters are present
            # Note: Backend may sanitize field names (e.g., dots and spaces become underscores)
            # Check that we have the right number of field rows
            field_rows = authenticated_page.locator('#field-list .field-row')
            expect(field_rows).to_have_count(len(headers))

            # Get all field name inputs - backend only sanitizes dots to underscores
            # Dashes, underscores, and spaces are kept as-is
            expected_sanitized = ['field-with-dash', 'field_with_underscore', 'field_with_dot', 'field with space']
            field_name_inputs = authenticated_page.locator('#field-list input[data-field="name"]')
            for i, expected in enumerate(expected_sanitized):
                field_value = field_name_inputs.nth(i).input_value()
                assert field_value == expected, f"Expected field {i} to be '{expected}', got '{field_value}'"

        finally:
            # Cleanup
            if temp_path.exists():
                temp_path.unlink()

    def test_field_file_boolean_type(self, authenticated_page: Page):
        """Test setting boolean field type"""
        # Create CSV with boolean-like values
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'is_active', 'is_verified'])
            writer.writerow(['1', 'true', 'false'])
            writer.writerow(['2', 'True', 'False'])
            writer.writerow(['3', '1', '0'])
            temp_path = Path(f.name)

        try:
            # Generate field file
            authenticated_page.set_input_files('#file-upload', str(temp_path))
            authenticated_page.fill('#import-database', 'testdb')
            authenticated_page.fill('#import-collection', 'testcol')
            authenticated_page.click('#generate-btn')
            authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

            # Set is_active to bool
            bool_select = authenticated_page.locator('select[data-index="1"][data-field="type"]')
            bool_select.select_option('bool')
            expect(bool_select).to_have_value('bool')

            # Set is_verified to bool
            bool_select2 = authenticated_page.locator('select[data-index="2"][data-field="type"]')
            bool_select2.select_option('bool')
            expect(bool_select2).to_have_value('bool')

        finally:
            # Cleanup
            if temp_path.exists():
                temp_path.unlink()

    def test_field_file_timestamp_type(self, authenticated_page: Page):
        """Test setting timestamp field type"""
        # Create CSV with timestamp values
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'created_at'])
            writer.writerow(['1', '1609459200'])
            writer.writerow(['2', '1609545600'])
            temp_path = Path(f.name)

        try:
            # Generate field file
            authenticated_page.set_input_files('#file-upload', str(temp_path))
            authenticated_page.fill('#import-database', 'testdb')
            authenticated_page.fill('#import-collection', 'testcol')
            authenticated_page.click('#generate-btn')
            authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

            # Set created_at to timestamp
            timestamp_select = authenticated_page.locator('select[data-index="1"][data-field="type"]')
            timestamp_select.select_option('timestamp')
            expect(timestamp_select).to_have_value('timestamp')

        finally:
            # Cleanup
            if temp_path.exists():
                temp_path.unlink()

    def test_field_file_persistence_across_cancel(self, authenticated_page: Page, test_csv_file):
        """Test that field file changes are lost when canceling"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Make changes
        type_select = authenticated_page.locator('select[data-index="0"][data-field="type"]')
        type_select.select_option('int')

        # Cancel
        authenticated_page.click('button:has-text("Cancel")')
        authenticated_page.wait_for_timeout(500)

        # Generate again
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Previous changes should not be preserved (should be back to default 'str')
        type_select = authenticated_page.locator('select[data-index="0"][data-field="type"]')
        expect(type_select).to_have_value('str')

    def test_field_file_header_row(self, authenticated_page: Page, test_csv_file):
        """Test that field file correctly identifies header row"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Verify header row is present
        field_header = authenticated_page.locator('.field-header')
        expect(field_header).to_contain_text('Field Name')
        expect(field_header).to_contain_text('Type')
        expect(field_header).to_contain_text('Format')

    @pytest.mark.slow
    def test_import_with_mixed_types(self, authenticated_page: Page):
        """Test import with various mixed field types"""
        # Create CSV with various data types
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'score', 'is_active', 'created', 'price'])
            writer.writerow(['1', 'Alice', '95.5', 'true', '2024-01-15', '19.99'])
            writer.writerow(['2', 'Bob', '87.3', 'false', '2024-02-20', '29.99'])
            temp_path = Path(f.name)

        try:
            # Generate field file and configure types
            authenticated_page.set_input_files('#file-upload', str(temp_path))
            authenticated_page.fill('#import-database', 'gui_test_db')
            authenticated_page.fill('#import-collection', 'mixed_types_test')
            authenticated_page.click('#generate-btn')
            authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

            # Configure types
            authenticated_page.locator('select[data-index="0"][data-field="type"]').select_option('int')
            authenticated_page.locator('select[data-index="2"][data-field="type"]').select_option('float')
            authenticated_page.locator('select[data-index="3"][data-field="type"]').select_option('bool')
            authenticated_page.locator('select[data-index="4"][data-field="type"]').select_option('isodate')
            authenticated_page.locator('select[data-index="5"][data-field="type"]').select_option('float')

            # Import
            authenticated_page.click('button:has-text("Import with Field File")')

            # Wait for final status (success or error), not just the initial "info" status
            # The status changes from "info" to "success" or "error" when import completes
            # Note: Status auto-hides after 5 seconds, so we check for state not visibility
            try:
                authenticated_page.wait_for_selector('#import-status.status.success', state='attached', timeout=30000)
                status_is_success = True
            except:
                # If success didn't appear, check for error
                authenticated_page.wait_for_selector('#import-status.status.error', state='attached', timeout=5000)
                status_is_success = False

            # Get the final status
            status_element = authenticated_page.locator('#import-status')
            status_html = status_element.inner_html()

            # If we got an error, fail with the full error message
            if not status_is_success:
                raise AssertionError(f"Import failed with error: {status_html}")

            # Verify success
            expect(status_element).to_have_class('status success')
            expect(status_element).to_contain_text('Import successful')

        finally:
            # Cleanup
            if temp_path.exists():
                temp_path.unlink()
