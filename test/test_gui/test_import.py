"""
GUI tests for CSV import functionality

Tests file upload, field file generation, and import operations.
"""

import pytest
from playwright.sync_api import Page, expect
import time


@pytest.mark.gui
class TestImport:
    """Test suite for CSV import workflows"""

    def test_file_selection(self, authenticated_page: Page, test_csv_file):
        """Test CSV file selection through the file picker"""
        # Should be on import view
        expect(authenticated_page.locator('#import-view')).to_be_visible()

        # Select the file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))

        # Verify the file is selected and displayed
        # The selected-filename field shows "filename (size)", so check it contains the filename
        selected_filename = authenticated_page.locator('#selected-filename')
        filename_value = selected_filename.input_value()
        assert test_csv_file.name in filename_value, f"Expected filename '{test_csv_file.name}' to be in '{filename_value}'"

        # Verify the upload button text changed
        upload_text = authenticated_page.locator('#file-upload-text')
        expect(upload_text).to_contain_text(test_csv_file.name)

    def test_database_and_collection_input(self, authenticated_page: Page):
        """Test filling in database and collection names"""
        # Fill in database name
        authenticated_page.fill('#import-database', 'test_database')
        expect(authenticated_page.locator('#import-database')).to_have_value('test_database')

        # Fill in collection name
        authenticated_page.fill('#import-collection', 'test_collection')
        expect(authenticated_page.locator('#import-collection')).to_have_value('test_collection')

    def test_generate_field_file_without_file(self, authenticated_page: Page):
        """Test that field file generation fails without selecting a file"""
        # Fill in database and collection
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')

        # Try to generate field file without selecting file
        authenticated_page.click('#generate-btn')

        # Should get error
        error_status = authenticated_page.locator('#import-status.status.error')
        expect(error_status).to_be_visible(timeout=5000)
        expect(error_status).to_contain_text('Please select a CSV file first')

    def test_generate_field_file_without_database(self, authenticated_page: Page, test_csv_file):
        """Test that field file generation fails without database/collection"""
        # Select file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))

        # Try to generate field file without database/collection
        authenticated_page.click('#generate-btn')

        # Should get error
        error_status = authenticated_page.locator('#import-status.status.error')
        expect(error_status).to_be_visible(timeout=5000)
        expect(error_status).to_contain_text('Please fill in database and collection')

    def test_generate_field_file(self, authenticated_page: Page, test_csv_file):
        """Test successful field file generation"""
        # Select file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))

        # Fill in database and collection
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')

        # Generate field file
        authenticated_page.click('#generate-btn')

        # Wait for field file section to appear
        field_file_section = authenticated_page.locator('#field-file-section')
        expect(field_file_section).to_be_visible(timeout=10000)

        # Verify generate button is hidden
        expect(authenticated_page.locator('#generate-btn')).not_to_be_visible()

        # Verify field list contains fields from CSV
        field_list = authenticated_page.locator('#field-list')
        expect(field_list).to_be_visible()

        # Check for expected fields (based on test CSV structure)
        # Fields are in input elements with data-field="name", so check for those
        expect(authenticated_page.locator('#field-list input[data-field="name"][value="id"]')).to_be_attached()
        expect(authenticated_page.locator('#field-list input[data-field="name"][value="name"]')).to_be_attached()
        expect(authenticated_page.locator('#field-list input[data-field="name"][value="email"]')).to_be_attached()
        expect(authenticated_page.locator('#field-list input[data-field="name"][value="age"]')).to_be_attached()
        expect(authenticated_page.locator('#field-list input[data-field="name"][value="signup_date"]')).to_be_attached()

    def test_field_file_editor_displays_all_fields(self, authenticated_page: Page, test_csv_file):
        """Test that field file editor displays all CSV columns"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')

        # Wait for field file section
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Count field rows
        field_rows = authenticated_page.locator('#field-list .field-row')
        expect(field_rows).to_have_count(5)  # 5 fields in test CSV

    def test_field_type_selection(self, authenticated_page: Page, test_csv_file):
        """Test changing field types in the field file editor"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Change type of first field
        first_type_select = authenticated_page.locator('select[data-field="type"]').first
        first_type_select.select_option('int')

        # Verify it changed
        expect(first_type_select).to_have_value('int')

        # Change type of age field to int
        age_type_select = authenticated_page.locator('select[data-index="3"][data-field="type"]')
        age_type_select.select_option('int')
        expect(age_type_select).to_have_value('int')

    def test_field_format_input(self, authenticated_page: Page, test_csv_file):
        """Test adding format string for date fields"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Set signup_date field to date type and add format
        date_type_select = authenticated_page.locator('select[data-index="4"][data-field="type"]')
        date_type_select.select_option('date')

        date_format_input = authenticated_page.locator('input[data-index="4"][data-field="format"]')
        date_format_input.fill('%Y-%m-%d')

        expect(date_format_input).to_have_value('%Y-%m-%d')

    def test_cancel_import(self, authenticated_page: Page, test_csv_file):
        """Test canceling import returns to initial state"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Click cancel
        authenticated_page.click('button:has-text("Cancel")')

        # Verify field file section is hidden
        expect(authenticated_page.locator('#field-file-section')).not_to_be_visible()

        # Verify generate button is visible again
        expect(authenticated_page.locator('#generate-btn')).to_be_visible()

    @pytest.mark.slow
    def test_import_with_field_file(self, authenticated_page: Page, test_csv_file):
        """Test complete import workflow with field file"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'gui_test_db')
        authenticated_page.fill('#import-collection', 'gui_test_collection')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Configure field types
        # id -> int
        authenticated_page.locator('select[data-index="0"][data-field="type"]').select_option('int')
        # age -> int
        authenticated_page.locator('select[data-index="3"][data-field="type"]').select_option('int')
        # signup_date -> date
        authenticated_page.locator('select[data-index="4"][data-field="type"]').select_option('date')
        authenticated_page.locator('input[data-index="4"][data-field="format"]').fill('%Y-%m-%d')

        # Proceed with import
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
        # Get inner HTML to see full message before it auto-hides
        status_html = status_element.inner_html()

        # If we got an error, fail with the full error message
        if not status_is_success:
            raise AssertionError(f"Import failed with error: {status_html}")

        # Otherwise verify it's a success
        expect(status_element).to_have_class('status success')
        expect(status_element).to_contain_text('Import successful')
        expect(status_element).to_contain_text('5 records')  # 5 rows in test CSV

        # Verify form reset
        expect(authenticated_page.locator('#selected-filename')).to_have_value('')
        expect(authenticated_page.locator('#field-file-section')).not_to_be_visible()
        expect(authenticated_page.locator('#generate-btn')).to_be_visible()

    @pytest.mark.slow
    def test_import_string_fields(self, authenticated_page: Page, test_csv_file):
        """Test import with all fields as strings"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'gui_test_db')
        authenticated_page.fill('#import-collection', 'string_test')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Keep all fields as strings (default)
        # Just proceed with import
        authenticated_page.click('button:has-text("Import with Field File")')

        # Wait for success
        success_status = authenticated_page.locator('#import-status.status.success')
        expect(success_status).to_be_visible(timeout=30000)
        expect(success_status).to_contain_text('Import successful')

    @pytest.mark.slow
    def test_import_different_types(self, authenticated_page: Page, test_csv_file):
        """Test import with various field types"""
        # Generate field file
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'gui_test_db')
        authenticated_page.fill('#import-collection', 'types_test')
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

        # Set different types
        authenticated_page.locator('select[data-index="0"][data-field="type"]').select_option('int')
        authenticated_page.locator('select[data-index="1"][data-field="type"]').select_option('str')
        authenticated_page.locator('select[data-index="2"][data-field="type"]').select_option('str')
        authenticated_page.locator('select[data-index="3"][data-field="type"]').select_option('float')
        authenticated_page.locator('select[data-index="4"][data-field="type"]').select_option('isodate')

        # Import
        authenticated_page.click('button:has-text("Import with Field File")')

        # Wait for success
        success_status = authenticated_page.locator('#import-status.status.success')
        expect(success_status).to_be_visible(timeout=30000)
        expect(success_status).to_contain_text('Import successful')

    @pytest.mark.slow
    def test_multiple_imports_same_collection(self, authenticated_page: Page, test_csv_file):
        """Test importing multiple times to the same collection"""
        database = 'gui_test_db'
        collection = 'multi_import_test'

        # First import
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', database)
        authenticated_page.fill('#import-collection', collection)
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)
        authenticated_page.click('button:has-text("Import with Field File")')
        authenticated_page.wait_for_selector('#import-status.status.success', timeout=30000)

        # Wait a moment for form to reset
        authenticated_page.wait_for_timeout(1000)

        # Second import to same collection
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', database)
        authenticated_page.fill('#import-collection', collection)
        authenticated_page.click('#generate-btn')
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)
        authenticated_page.click('button:has-text("Import with Field File")')

        # Should succeed
        success_status = authenticated_page.locator('#import-status.status.success')
        expect(success_status).to_be_visible(timeout=30000)
        expect(success_status).to_contain_text('Import successful')

    def test_ui_feedback_during_upload(self, authenticated_page: Page, test_csv_file):
        """Test that UI provides feedback during file upload"""
        # Start field file generation
        authenticated_page.set_input_files('#file-upload', str(test_csv_file))
        authenticated_page.fill('#import-database', 'testdb')
        authenticated_page.fill('#import-collection', 'testcol')
        authenticated_page.click('#generate-btn')

        # Should show info status during processing
        info_status = authenticated_page.locator('#import-status.status.info')
        expect(info_status).to_be_visible(timeout=2000)
        expect(info_status).to_contain_text('Uploading file')

        # Eventually should show success or field file
        authenticated_page.wait_for_selector('#field-file-section', timeout=10000)

    @pytest.mark.slow
    def test_import_error_handling(self, authenticated_page: Page):
        """Test error handling for import failures"""
        # Try to import without proper setup (this should trigger an error)
        # Note: This test depends on MongoDB being unavailable or other error conditions
        # For now, we'll test the UI's ability to display errors

        # Skip this test for now as it requires specific error conditions
        pytest.skip("Requires specific error conditions to test error handling")
