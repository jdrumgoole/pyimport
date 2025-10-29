# GUI Tests Quick Start Guide

Get up and running with PyImport GUI tests in 5 minutes.

## Step 1: Install Dependencies

```bash
# Install Python dependencies
poetry install --with dev

# Install Playwright browsers
invoke test-gui-install-chromium
```

## Step 2: Start MongoDB

```bash
# Make sure MongoDB is running
mongod

# Or with Homebrew on macOS
brew services start mongodb-community
```

## Step 3: Run Tests

```bash
# Run all GUI tests
invoke test-gui

# Or run specific categories
invoke test-gui-auth       # Authentication tests
invoke test-gui-import     # Import workflow tests
invoke test-gui-fieldfile  # Field file tests
invoke test-gui-progress   # Progress monitoring tests
```

## See Tests Running (Optional)

```bash
# Run with visible browser
invoke test-gui-headed
```

## Common Commands

```bash
# Run specific test
poetry run pytest test/test_gui/test_authentication.py::TestAuthentication::test_user_login -v

# Run without slow tests
poetry run pytest test/test_gui -v -m "gui and not slow"

# Run with debugging
PWDEBUG=1 poetry run pytest test/test_gui/test_authentication.py::test_user_login
```

## Troubleshooting

### "Executable doesn't exist"

```bash
poetry run playwright install chromium
```

### "Server failed to start"

```bash
# Check if port 8000 is in use
lsof -i :8000

# Kill any process using it
kill -9 <PID>
```

### "MongoDB connection failed"

```bash
# Check MongoDB is running
mongosh

# Start it if not running
mongod
```

## Next Steps

- Read the [full README](README.md) for detailed documentation
- Explore test files to understand test patterns
- Add your own tests following the examples

## Test File Overview

- `test_authentication.py` - 15 tests for registration, login, logout, password management
- `test_import.py` - 17 tests for file upload and import workflows
- `test_field_file.py` - 16 tests for field type configuration
- `test_progress.py` - 11 tests for status messages and feedback

**Total: 59 comprehensive GUI tests**
