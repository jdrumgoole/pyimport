# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pyimport` is a CSV-to-MongoDB import tool written in Python. It provides automatic field type detection, parallel processing, and restart capabilities for importing CSV data into MongoDB. Unlike `mongoimport`, it focuses on handling "dirty" data gracefully and offers multiple import strategies (sync, async, multi-process, threaded).

## Build and Development Commands

### Essential Commands

```bash
# Install dependencies
poetry install

# Run the main program
poetry run pyimport <options> <csv-files>

# Build the package
invoke build

# Publish to PyPI
invoke publish
```

### Task Management with Invoke

This project uses **Invoke** for task automation (replacing Make). All development tasks are defined in `tasks.py`.

```bash
# List all available tasks with descriptions
invoke --list

# Quick development cycle (essential tests only - fastest)
invoke quick-dev

# Run all tests (pytest + integration + tox)
invoke test-all

# Run specific test categories
invoke run-pytest              # All pytest suites
invoke run-pytest-parallel     # Parallel pytest (faster)
invoke quick-pytest            # Essential tests only
invoke test-all-scripts        # Integration tests only

# Quick smoke tests (various import modes)
invoke quick-test              # All quick tests
invoke std-quicktest           # Standard sync import
invoke async-quicktest         # Async import
invoke thread-quicktest        # Thread-based import
invoke multi-quicktest         # Multi-process import

# Build and publish workflow
invoke build                   # Full test suite + tox + build
invoke publish                 # Build, test, tag, publish to PyPI

# Cross-version testing
invoke tox-run                 # Test on Python 3.9-3.13
invoke tox-run --env py312     # Test specific Python version
invoke check-python-versions   # Check available Python versions

# Documentation
invoke docs-build              # Build Sphinx docs
invoke docs-serve              # Build and serve docs on localhost:8000

# Environment info
invoke testenv                 # Show environment variables
invoke pguri                   # Show PostgreSQL URI
invoke root                    # Show project root path
```

### Direct Testing Commands

```bash
# Run specific test suites directly
cd test/test_command && poetry run pytest
cd test/test_e2e && poetry run pytest
cd test/test_db && PGURI=${PGURI} poetry run pytest

# Parallel testing with pytest-xdist
cd test/test_general && poetry run pytest -n auto
```

### Utility Commands

```bash
# Generate a field file from a CSV
poetry run pyimport --genfieldfile <csv-file>

# Database operations (using dbop.py)
poetry run python mdbutils/dbop.py --drop DATABASE.COLLECTION
poetry run python mdbutils/dbop.py --count DATABASE.COLLECTION
```

### Migration from Make

**Note**: The project has migrated from Make to Invoke. If you see references to `make` commands:
- `make test_all` → `invoke test-all`
- `make quick_test` → `invoke quick-test`
- `make build` → `invoke build`
- `make publish` → `invoke publish`

See `MAKEFILE_TO_INVOKE_MIGRATION.md` for complete command mapping.

## Architecture

### Core Concepts

**Field Files (.tff)**: TOML-formatted files that define column types and formats for CSV imports. The system can auto-generate these with `--genfieldfile`. Field files support:
- Type inference (str, int, float, date, datetime, isodate, bool, timestamp)
- Custom date format strings using `strptime` syntax
- Delimiter and header configuration in a `DEFAULTS_SECTION`

**Import Strategies**: The application supports four execution modes:
- **Standard (sync)**: Single-threaded synchronous imports using `MDBImportCommand`
- **Async**: Event-loop based async imports using `AsyncMDBImportCommand` (Motor driver)
- **Multi-process**: Parallel import using multiple processes via `MultiImportCommand`
- **Threaded**: Thread-based parallel import via `ThreadImportCommand`

**File Splitting**: Large CSV files can be automatically split into smaller chunks for parallel processing (`--splitfile --autosplit N`). Split files are cleaned up automatically unless `--keepsplits` is specified.

### Key Components

**Entry Point** (`pyimport/pyimport_main.py`):
- Parses command-line arguments via `argparser`
- Handles field file generation with `GenerateFieldfileCommand`
- Routes to appropriate import command based on flags (`--multi`, `--threads`, `--asyncpro`)
- Manages file splitting and cleanup

**Field File System** (`pyimport/fieldfile.py`):
- `FieldFile` class: Loads/parses TOML field files, validates field definitions
- `generate_field_file()`: Auto-generates field files by sampling CSV data
- Type guessing via `type_converter.guess_type()`
- Handles both local files and remote URLs

**Database Writers** (`pyimport/db/`):
- `syncmdbwriter.py`: Synchronous MongoDB writer (PyMongo)
- `asyncdbwriter.py`: Async MongoDB writer (Motor)
- `syncrdbwriter.py` / `asyncrdbwriter.py`: PostgreSQL writers (experimental)
- `dbwriter.py`: Abstract base class for all writers

**Import Commands**:
- `mdbimportcmd.py`: Synchronous single-file import
- `asyncimport.py`: Async single-file import
- `multiimportcommand.py`: Multi-process parallel import
- `threadimportcommand.py`: Thread-based parallel import
- All commands return `ImportResult` objects

**CSV Processing**:
- `csvreader.py`: CSV file reader with type conversion
- `filereader.py`: Handles both local and remote CSV files
- `type_converter.py`: Converts string values to target types with fallback to string on error
- `enricher.py`: Adds metadata fields (filename, timestamp) to documents

**Utilities**:
- `filesplitter.py`: Splits large CSV files for parallel processing
- `audit.py` / `asyncaudit.py`: Track import progress for restart capability
- `timer.py`: Performance timing utilities
- `logger.py`: Centralized logging configuration

### Import Flow

1. **Argument Parsing**: `argparser.py` processes CLI arguments and config files
2. **Field File Resolution**:
   - If `--genfieldfile`, generate field file from CSV header/first row
   - Otherwise load existing `.tff` file (auto-discovered or specified)
3. **File Splitting** (optional): If `--splitfile`, divide CSV into chunks
4. **Import Execution**:
   - Route to appropriate command based on flags
   - Each command creates a database writer (MongoDB or PostgreSQL)
   - CSV data is read, type-converted per field file, optionally enriched
   - Batch inserts to database (default batch size: 500)
5. **Cleanup**: Remove split files if `--keepsplits` not set

### Type Conversion Strategy

Type conversion is "forgiving" - if a field value cannot be converted to its specified type, the system falls back to storing it as a string rather than failing (unless `--onerror fail` is set). This handles "dirty" data gracefully.

Date handling has three modes:
- `isodate`: Fast parsing for ISO 8601 dates (YYYY-MM-DD)
- `date`/`datetime` with format string: Use `strptime` for predictable formats
- `date`/`datetime` without format: Use `dateutil.parser` (slow but flexible)

## Project Structure

```
pyimport/
├── pyimport_main.py          # Main entry point
├── argparser.py              # CLI argument handling
├── fieldfile.py              # Field file parsing/generation
├── type_converter.py         # Type conversion logic
├── csvreader.py              # CSV reading with type conversion
├── enricher.py               # Document enrichment (timestamps, filenames)
├── filesplitter.py           # File splitting for parallel processing
├── db/                       # Database writer implementations
│   ├── dbwriter.py           # Abstract base
│   ├── syncmdbwriter.py      # Sync MongoDB
│   ├── asyncdbwriter.py      # Async MongoDB
│   └── syncrdbwriter.py      # Sync PostgreSQL
├── mdbimportcmd.py           # Standard import command
├── asyncimport.py            # Async import command
├── multiimportcommand.py     # Multi-process import
├── threadimportcommand.py    # Thread-based import
├── audit.py / asyncaudit.py  # Progress tracking
└── logger.py                 # Logging utilities

test/                         # Organized by feature
├── test_command/             # Command-line tests
├── test_e2e/                 # End-to-end integration tests
├── test_fieldfile/           # Field file tests
├── test_db/                  # Database writer tests
└── test_general/             # Unit tests

tasks.py                      # Invoke task definitions (replaces Makefile)
mdbutils/dbop.py              # Database operations (drop, count)
```

### Task Organization (tasks.py)

The project uses Invoke for task automation with the following categories:

**Testing Tasks**:
- `test-all`, `run-pytest`, `run-pytest-parallel`: Full test suites
- `quick-test`, `quick-dev`, `quick-pytest`: Fast development cycles
- `std-quicktest`, `async-quicktest`, `thread-quicktest`, `multi-quicktest`: Smoke tests
- `test-audit`, `test-scripts`, `test-data`: Specific test categories

**Build & Publish Tasks**:
- `build`: Full build with cross-version testing (pytest + tox + poetry build)
- `publish`: Complete release workflow (build, tag, publish, trigger docs)
- `poetry-build`, `poetry-publish`: Direct poetry commands
- `clean`: Remove build artifacts

**Cross-Version Testing**:
- `tox-run`: Test across Python 3.9-3.13
- `check-python-versions`: Verify Python installations

**Documentation Tasks**:
- `docs-build`: Build Sphinx documentation
- `docs-serve`: Serve docs locally on port 8000
- `docs-clean`: Clean documentation artifacts
- `trigger-rtd-build`: Trigger Read the Docs webhook

**Utility Tasks**:
- `testenv`, `path`, `pguri`, `root`: Environment inspection
- `genfieldfile`: Generate field files
- Integration test tasks: `test-yellowtrip`, `test-multi`, `test-threads`, etc.

```

## Environment Variables

- `AUDITHOST`: MongoDB connection string for audit collection
- `PGURI`: PostgreSQL connection string (for PostgreSQL import mode)

Create a `.env` file in the project root for local development.

## Common Patterns

### Adding a New Import Mode

1. Create a new command class in `pyimport/` inheriting from base patterns
2. Implement the `run()` method returning `ImportResult`
3. Register the command in `pyimport_main.py` with a new CLI flag
4. Add corresponding test in `test/test_command/`
5. Update `argparser.py` if new arguments are needed

### Adding a New Field Type

1. Add type conversion logic in `type_converter.py`
2. Update `guess_type()` to recognize the new type
3. Handle the type in `FieldFile` validation
4. Add tests in `test/test_general/test_type_converter.py`

### Database Backend Support

The system abstracts database operations through `dbwriter.py`. To add support for a new database:

1. Create a new writer class in `pyimport/db/` implementing the `DbWriter` interface
2. Implement `write_batch()` and `close()` methods
3. Update import commands to instantiate the new writer based on connection URI scheme
4. Add tests in `test/test_db/`
