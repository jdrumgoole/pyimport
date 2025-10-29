# PyImport GUI Test Suite

Comprehensive end-to-end tests for the PyImport web interface using Playwright.

## Overview

This test suite provides browser-based testing of all major GUI functionality including:

- **Authentication**: User registration, login, logout, password management
- **File Upload**: CSV file selection and upload workflows
- **Field File Management**: Field type detection, editing, and configuration
- **Import Operations**: Complete CSV-to-MongoDB import workflows
- **Progress Monitoring**: Status messages and operation feedback
- **Error Handling**: Validation and error recovery

## Prerequisites

### 1. Install Dependencies

```bash
# Install Python dependencies including Playwright
poetry install --with dev

# Install Playwright browsers
poetry run playwright install

# Or install just Chromium (faster)
poetry run playwright install chromium
```

### 2. Start MongoDB

Ensure MongoDB is running and accessible:

```bash
# Default: mongodb://localhost:27017
mongod
```

### 3. Configure Environment (Optional)

Create a `.env` file in the project root if you need custom MongoDB settings:

```bash
# Optional: Custom MongoDB URI
MONGODB_URI=mongodb://localhost:27017

# Optional: JWT secret for authentication
SECRET_KEY=your-secret-key-for-testing
```

## Running Tests

### Quick Start

Run all GUI tests:

```bash
invoke test-gui
```

### Test Categories

Run specific test categories:

```bash
# Authentication tests (registration, login, logout, password change)
invoke test-gui-auth

# Import workflow tests (file upload, field file, import)
invoke test-gui-import

# Field file tests (generation, editing, type configuration)
invoke test-gui-fieldfile

# Progress monitoring tests (status messages, feedback)
invoke test-gui-progress
```

### Run with Visible Browser

By default, tests run in headless mode. To see the browser:

```bash
invoke test-gui-headed
```

Or with pytest directly:

```bash
poetry run pytest test/test_gui -v --headed
```

### Direct pytest Commands

```bash
# Run all GUI tests
poetry run pytest test/test_gui -v -m gui

# Run specific test file
poetry run pytest test/test_gui/test_authentication.py -v

# Run specific test
poetry run pytest test/test_gui/test_authentication.py::TestAuthentication::test_user_login -v

# Run with different browser
poetry run pytest test/test_gui --browser chromium
poetry run pytest test/test_gui --browser firefox
poetry run pytest test/test_gui --browser webkit

# Run with multiple browsers
poetry run pytest test/test_gui --browser chromium --browser firefox

# Run in headed mode with slow motion (for debugging)
poetry run pytest test/test_gui -v --headed --slowmo 500

# Run with verbose output and show print statements
poetry run pytest test/test_gui -v -s
```

### Skip Slow Tests

Some tests are marked as slow (e.g., full import operations):

```bash
# Skip slow tests
poetry run pytest test/test_gui -v -m "gui and not slow"
```

## Test Structure

```
test/test_gui/
├── README.md                    # This file
├── conftest.py                  # Shared fixtures and configuration
├── test_authentication.py       # Authentication and user management tests
├── test_import.py               # CSV import workflow tests
├── test_field_file.py           # Field file generation and editing tests
└── test_progress.py             # Progress monitoring and status tests
```

## Key Fixtures

### `pyimport_server`

Starts the PyImport REST API server for the test session. The server is automatically started before tests and stopped after all tests complete.

### `authenticated_page`

Provides a Playwright page that is already authenticated. Creates a test user, logs in, and yields the page ready for testing authenticated functionality.

### `test_csv_file`

Creates a temporary CSV file with sample data (5 rows). The file is automatically cleaned up after the test.

### `large_test_csv_file`

Creates a larger temporary CSV file (1000 rows) for testing progress tracking and performance.

## Test Coverage

### Authentication Tests (`test_authentication.py`)

- ✓ User registration with validation
- ✓ Duplicate username prevention
- ✓ User login with credentials
- ✓ Invalid credential handling
- ✓ Logout functionality
- ✓ Admin user login
- ✓ Token persistence across page reloads
- ✓ Navigation between Import and Profile views
- ✓ Load user profile information
- ✓ Change password
- ✓ Password validation (length, mismatch)
- ✓ Current password verification

### Import Tests (`test_import.py`)

- ✓ File selection through file picker
- ✓ Database and collection input validation
- ✓ Field file generation workflow
- ✓ Error handling for missing inputs
- ✓ Field file editor display
- ✓ Field type selection
- ✓ Field format string input
- ✓ Cancel import operation
- ✓ Complete import with field file configuration
- ✓ Multiple imports to same collection
- ✓ UI feedback during operations

### Field File Tests (`test_field_file.py`)

- ✓ Field file generation from CSV headers
- ✓ All CSV columns identified
- ✓ Field names are readonly
- ✓ All field types available (str, int, float, bool, date, datetime, isodate, timestamp)
- ✓ Changing field types
- ✓ Format strings for date/datetime fields
- ✓ CSVs with many columns (20+)
- ✓ Special characters in column names
- ✓ Boolean field type configuration
- ✓ Timestamp field type configuration
- ✓ Mixed type imports
- ✓ Field changes lost on cancel

### Progress Tests (`test_progress.py`)

- ✓ Status messages display (info, success, error)
- ✓ Status messages auto-dismiss
- ✓ Success message with import details
- ✓ UI responsiveness during import
- ✓ Multiple sequential operations
- ✓ Error recovery
- ✓ Form validation feedback
- ⏳ Async import with progress tracking (future)
- ⏳ Real-time progress bar (future)
- ⏳ Upload rate display (future)
- ⏳ ETA display (future)

## Writing New Tests

### Basic Test Template

```python
import pytest
from playwright.sync_api import Page, expect


@pytest.mark.gui
def test_my_feature(authenticated_page: Page):
    """Test my awesome feature"""
    # Your test code here
    authenticated_page.click('#my-button')
    expect(authenticated_page.locator('#result')).to_contain_text('Success')
```

### Mark Tests Appropriately

```python
@pytest.mark.gui              # Required for all GUI tests
@pytest.mark.slow             # For tests that take >5 seconds
```

### Use Proper Selectors

Prefer stable selectors:

```python
# Good: Use IDs
page.click('#submit-button')

# Good: Use data attributes
page.click('[data-testid="submit"]')

# Good: Use text content for unique elements
page.click('button:has-text("Submit")')

# Avoid: CSS classes (can change)
page.click('.btn-primary')  # Fragile
```

### Wait for Elements

Always wait for elements to be ready:

```python
# Wait for element to be visible
page.wait_for_selector('#result', timeout=5000)

# Use expect for assertions (includes auto-wait)
expect(page.locator('#result')).to_be_visible()
expect(page.locator('#result')).to_contain_text('Success')
```

## Debugging Tests

### Run with Headed Mode

```bash
poetry run pytest test/test_gui/test_authentication.py::test_user_login --headed
```

### Slow Motion

Add delays between actions:

```bash
poetry run pytest test/test_gui --headed --slowmo 1000
```

### Playwright Inspector

Use Playwright's built-in debugger:

```bash
PWDEBUG=1 poetry run pytest test/test_gui/test_authentication.py::test_user_login
```

### Screenshots on Failure

Screenshots are automatically captured on test failure and saved to `test-results/`.

### Trace Viewer

Enable tracing for detailed debugging:

```python
# In conftest.py, modify browser_context_args fixture:
@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {
        **browser_context_args,
        "record_video_dir": "test-results/videos",
        "record_trace": "on-first-retry",
    }
```

Then view traces:

```bash
poetry run playwright show-trace test-results/trace.zip
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: GUI Tests

on: [push, pull_request]

jobs:
  test-gui:
    runs-on: ubuntu-latest

    services:
      mongodb:
        image: mongo:7
        ports:
          - 27017:27017

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install Poetry
        run: pip install poetry

      - name: Install dependencies
        run: poetry install --with dev

      - name: Install Playwright browsers
        run: poetry run playwright install --with-deps chromium

      - name: Run GUI tests
        run: poetry run pytest test/test_gui -v -m gui

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: test-results/
```

## Troubleshooting

### Server Won't Start

**Problem**: Tests fail with "Server failed to start within timeout"

**Solutions**:
- Ensure port 8000 is not already in use: `lsof -i :8000`
- Check MongoDB is running: `mongosh`
- Verify dependencies installed: `poetry install --extras rest-api`

### Browser Not Installed

**Problem**: "Executable doesn't exist" error

**Solution**:
```bash
poetry run playwright install chromium
```

### Tests Are Flaky

**Problem**: Tests pass sometimes, fail other times

**Solutions**:
- Increase timeouts in `conftest.py`
- Add explicit waits: `page.wait_for_selector('#element', timeout=10000)`
- Use `expect()` assertions which have built-in auto-waiting
- Check for race conditions in async operations

### MongoDB Connection Issues

**Problem**: Tests fail with MongoDB connection errors

**Solutions**:
- Ensure MongoDB is running: `brew services start mongodb-community` (macOS)
- Check connection string in `.env`
- Verify network access: `mongosh mongodb://localhost:27017`

### Authentication Fails

**Problem**: Login or registration doesn't work

**Solutions**:
- Check server logs for errors
- Verify JWT secret is set (uses default if not)
- Clear browser storage: Tests use fresh context per test
- Check for CORS issues in browser console

## Performance Tips

### Run Tests in Parallel

Playwright tests can run in parallel:

```bash
poetry run pytest test/test_gui -v -n auto
```

### Use Only Chromium

For faster CI runs, use only Chromium:

```bash
poetry run pytest test/test_gui --browser chromium
```

### Skip Slow Tests in Development

```bash
poetry run pytest test/test_gui -m "gui and not slow"
```

## Best Practices

1. **Keep tests independent**: Each test should work standalone
2. **Use fixtures for setup**: Leverage `authenticated_page`, `test_csv_file`
3. **Clean up resources**: Fixtures automatically clean up temp files
4. **Meaningful test names**: Use descriptive names like `test_user_can_login_with_valid_credentials`
5. **One assertion per test**: Focus each test on one behavior
6. **Use markers**: Mark slow tests with `@pytest.mark.slow`
7. **Wait properly**: Use `expect()` with auto-waiting, not `time.sleep()`
8. **Stable selectors**: Prefer IDs and data attributes over classes

## Contributing

When adding new GUI features:

1. Write tests first (TDD approach recommended)
2. Add tests to appropriate test file
3. Update this README if adding new test categories
4. Run full test suite before committing: `invoke test-gui`
5. Ensure tests pass in both headless and headed modes

## Resources

- [Playwright Python Docs](https://playwright.dev/python/)
- [pytest-playwright Plugin](https://github.com/microsoft/playwright-pytest)
- [PyImport REST API Docs](../../REST_API_README.md)
- [PyImport Main Docs](../../README.md)
