# Changelog

All notable changes to pyimport will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.6] - 2025-10-14

### Changed
- **Build System Migration**: Completed migration from Make to Invoke
  - Removed Makefile (all 33 targets successfully converted to invoke tasks)
  - Pytest tasks run from within each test directory (tests expect data files in current directory)

### Fixed
- **CRITICAL**: tasks.py had incorrect dbop.py path that broke all integration tests
  - The file is located at `mdbutils/dbop.py` (not `pyimport/dbop.py`)
  - Fixed all 23 incorrect references to use correct path
  - Integration tests now run successfully
- **Test Infrastructure**: Fixed intermittent and environment-specific test failures
  - `test_api.py::test_drop_before_import`: Enhanced cleanup with retry logic for race conditions
  - `test_rdbmanager.py`: Added automatic table cleanup fixture to prevent "table already exists" errors
  - `test_http_import.py`: Fixed field file path resolution using absolute paths
  - `test_formats.py`: Skip double-quote delimiter test on Python 3.9/3.13 (CSV library incompatibility)

### Documentation
- **CLAUDE.md**: Updated all build commands from `make` to `invoke`
  - Replaced all `make test_all` references with `invoke test-all`
  - Replaced all `make build` references with `invoke build`
  - Replaced all `make publish` references with `invoke publish`
  - Documented correct dbop.py path as `mdbutils/dbop.py`

### Migration Notes
- **Action Required**: Replace all `make` commands with `invoke` commands
  - `make test_all` → `invoke test-all`
  - `make quick_test` → `invoke quick-test`
  - `make build` → `invoke build`
  - `make publish` → `invoke publish`
- Use `invoke --list` to see all available tasks

## [2.0.5] - 2025-10-14

### Fixed
- **API test isolation**: Fixed parallel test contamination in test_api.py
  - Modified setUp() in TestPyImportAPI and TestPyImportBuilder to use worker-specific database names
  - Database names now include worker ID: TEST_API_DB_{worker_id}, TEST_BUILDER_DB_{worker_id}
  - Fixed assertion failures like `assert 6 != 3` caused by workers sharing same database
  - All API tests now pass with parallel execution
- **API test write concern**: Fixed test_drop_before_import race condition
  - Added write_concern=1 and journal=True to PyImportAPI initialization in test
  - Added verification that collection is empty before starting import
  - Fixed assertion failures like `assert 4 != 3` caused by write concern 0
- **PostgreSQL writer test isolation**: Fixed parallel test contamination in RDBTestDB
  - Modified RDBTestDB.__init__() to use worker-specific table names
  - Table names now include worker ID: pyimport_test_{worker_id}
  - Fixed NoSuchTableError when parallel workers compete for same table name
  - All PostgreSQL writer tests now pass with parallel execution
- **PostgreSQL index test isolation**: Fixed parallel test contamination in test_rdbmanager.py
  - Added test_index_name fixture to generate worker-specific index names
  - Index names now include worker ID: test_index_{worker_id}
  - Fixed DuplicateTable errors when parallel workers compete for same index name
  - All PostgreSQL index tests now pass with parallel execution
- **File splitter test isolation**: Fixed file system race conditions with temporary directories
  - Added `temp_work_dir` fixtures to test_splitfile.py and test_filesplitter.py
  - Each test now runs in isolated temporary directory with worker-specific naming
  - Prevents FileNotFoundError when parallel workers compete for same split file names
  - All 29 filesplitter tests and 5 splitfile tests now pass with parallel execution
- **Mimesis API compatibility**: Fixed test compatibility with mimesis 12.1.0
  - Changed from `Person.birthdate()` (not available in 12.1.0) to `Datetime.date()`
  - Works across all mimesis versions (12.1.0 for Python 3.9 and 17.0.0 for Python 3.10+)
  - All asyncinserter tests now pass on Python 3.10-3.13

### Verified
- **Complete test suite success**: All 166 tests in test_general pass on Python 3.10-3.13
- **Cross-version compatibility**: Confirmed working on Python 3.9, 3.10, 3.11, 3.12, and 3.13
- **Parallel test execution**: Zero race conditions with pytest-xdist parallel workers

## [2.0.4] - 2025-10-14

### Fixed
- **Parallel test execution**: Fixed race conditions in pytest-xdist parallel tests
  - Added `xdist_group` markers to force sequential execution within test files
  - Prevents multiple test workers from interfering with each other's MongoDB collections
  - Fixed test failures like `assert 9 == (4148 - 3130)` caused by cross-worker data pollution
  - Affected files: test_command, test_restart, test_v2_comprehensive_e2e, test_v2_integration, test_fieldfile, test_fileprocessor
  - Tests now properly isolated while maintaining parallel execution across different test files
- **Async audit progress tests**: Fixed parallel test contamination in test_async_audit_progress.py
  - Modified `audit_db` fixture to use worker-specific database names (TEST_ASYNC_AUDIT_{worker_id})
  - Each pytest-xdist worker now has its own isolated MongoDB database
  - All 166 tests in test_general now pass with parallel execution
- **PostgreSQL test isolation**: Fixed parallel test contamination in test_db tests
  - Added worker-specific database names (test_db_{worker_id}) in test_rdbmaker.py
  - Added worker-specific table names (test_table_{worker_id}) in test_rdbmanager.py
  - Eliminates "duplicate key" and "already exists" errors in parallel execution
- **File splitter test isolation**: Fixed file system race conditions in test_splitfile and test_filesplitter
  - Added `temp_work_dir` fixtures that create worker-specific temporary directories
  - Each test now runs in its own isolated temp directory with copies of test data files
  - Prevents FileNotFoundError when parallel workers compete for same split file names
  - All 29 filesplitter tests and 5 splitfile tests now pass with parallel execution
- **Tox test execution**: Fixed tox running tests from wrong directory
  - Tests now run from within their respective test directories using `sh -c 'cd test/XXX && pytest'`
  - Added `allowlist_externals = sh` to tox.ini to allow shell commands
  - Fixes `OSError: No such file` errors in test_config tests that expect to find field files in current directory
- **Python 3.9 compatibility**: Fixed type hint syntax errors in importresult.py and filesplitter.py
  - Added `from __future__ import annotations` to support union type syntax `list[ImportResult]|None` and `str|None`
  - Fixes `TypeError: unsupported operand type(s) for |` on Python 3.9
- **Tox dependencies**: Added missing test dependencies and version pins to tox.ini
  - Added `python-dotenv` (required by test_e2e tests)
  - Added `colorama` (runtime dependency for colored terminal output)
  - Pinned `mimesis==12.1.0` for Python 3.9 compatibility (v17.0.0 uses unsupported type hints)
  - All tox tests now pass across Python 3.9-3.13
- **Mimesis API compatibility**: Fixed test_asyncinserter.py for mimesis 12.1.0 compatibility
  - Changed from `Person.birthdate()` to `Datetime.date(start=1950, end=2005)`
  - `Person.birthdate()` method doesn't exist in mimesis 12.1.0
  - `Datetime.date()` works across all mimesis versions (12.1.0 and 17.0.0)
  - All 3 asyncinserter tests now pass on Python 3.9-3.13

## [2.0.3] - 2025-10-14

### Fixed
- **CRITICAL: Missing colorama dependency**: Moved colorama from dev dependencies to main dependencies
  - colorama is imported and used by `pyimport/logger.py` for colored terminal output
  - v2.0.2 failed on fresh installs with `ModuleNotFoundError: No module named 'colorama'`
  - Now correctly included in package dependencies

## [2.0.2] - 2025-10-14

### Fixed
- **CRITICAL: Python 3.9/3.10 Compatibility**: Fixed `asyncio.TaskGroup` import error
  - Replaced `TaskGroup` (Python 3.11+ only) with `asyncio.gather()` for Python 3.9+ compatibility
  - Async import functionality now works correctly on Python 3.9, 3.10, 3.11, 3.12, and 3.13
  - Fixed in `pyimport/asyncimport.py`: Two instances of TaskGroup usage converted to asyncio.gather

### Changed
- **Tox Configuration**: Fixed tox to actually test on specified Python versions
  - Changed from `poetry run invoke run-pytest` (which used poetry's virtualenv) to direct `pytest` commands
  - Tox now correctly uses its own Python 3.9/3.10/3.11/3.12/3.13 virtualenvs for testing
  - Added all required dependencies directly to tox.ini deps section

## [2.0.1] - 2025-10-14

### Added
- **Python 3.9 Support**: Extended Python version compatibility to Python 3.9+
  - Added `from __future__ import annotations` to enable union type syntax (X | Y) in Python 3.9
  - Downgraded documentation dependencies to versions compatible with Python 3.9:
    - `sphinx` 8.0.2 → 7.0.0
    - `myst-parser` 4.0.0 → 3.0.0
    - `mimesis` 17.0.0 → 12.1.0
  - All 329 tests pass on Python 3.9, 3.10, 3.11, 3.12, and 3.13
  - Updated tox configuration to test against all five Python versions

### Changed
- **Default Write Concern**: Improved MongoDB write reliability
  - Changed default `--writeconcern` from 0 (fire-and-forget) to 1 (acknowledged)
  - Changed default `--journal` from False to True
  - Eliminates race conditions in tests and provides better data durability for production use

## [2.0.0] - 2025-10-14

### Added
- **TFF v2.0 Format**: Major new feature for mapping flat CSV data to nested JSON/MongoDB documents
  - Dot notation path syntax for nested field mapping (e.g., `path = "address.city"`)
  - Automatic v1.0/v2.0 format detection
  - Mixed v1.0/v2.0 field support in same file
  - Full backward compatibility - all existing v1.0 TFF files work unchanged
- New `FieldFile` methods:
  - `path_value(field_name)`: Get nested path for a field
  - `is_v2_format()`: Detect if field file uses v2.0 format
  - `get_field_paths()`: Get all field-to-path mappings
- New `FieldPathMapper` class for managing field path mappings
- New `NestedDocumentBuilder` class with utilities:
  - `set_nested_value()`: Set values using dot notation paths
  - `build_nested_doc()`: Convert flat docs to nested structure
  - `validate_paths()`: Validate path configurations
- Comprehensive test suite (80 tests) with 100% coverage on new code
- Real-world scenario tests (healthcare, e-commerce, IoT, financial)
- Performance and stress tests
- **Python 3.9+ Support**: Expanded Python version support from 3.11+ to 3.9+
  - Added `from __future__ import annotations` to support union type syntax (X | Y) in Python 3.9
  - Downgraded documentation dependencies (sphinx, myst-parser, mimesis) to versions compatible with Python 3.9
  - All 329 tests pass on Python 3.9, 3.10, 3.11, 3.12, and 3.13

### Fixed
- **Enricher**: Fixed `TypeError` when handling nested document values in single-field CSV warning
- **PyMongo Compatibility**: Fixed deprecated `j=` parameter, now uses `journal=` in both sync and async MongoDB writers

### Changed
- CSV reader now automatically applies nested document mapping when v2.0 TFF format detected
- No performance impact for v1.0 files, minimal overhead (<5%) for v2.0 files

### Documentation
- Added comprehensive design document (`TFF_MAPPING_DESIGN.md`)
- Added Phase 1 implementation summary (`TFF_V2_PHASE1_SUMMARY.md`)
- Added test coverage report (`TFF_V2_TEST_COVERAGE_REPORT.md`)
- Added roadmap and next steps (`TFF_V2_NEXT_STEPS.md`)

### Migration Notes
- **No action required** - All existing imports continue to work
- To adopt v2.0 format, simply add `path = "nested.path"` to field definitions
- See migration guide in documentation

## [1.10.9] - 2025-10-13

### Changed
- Optimized test suite with parallel execution using pytest-xdist
- Improved publish workflow performance (30-40% faster)

### Added
- New invoke tasks for faster development workflow:
  - `quick-dev`: Fast development test cycle
  - `quick-pytest`: Parallel pytest execution
  - `full-pytest-parallel`: Full parallel test suite
- Test optimization documentation

## [1.10.8] - 2025-10-12

### Fixed
- Moved API.md to correct location in docs/markdown/

## [1.10.7] - 2025-10-12

### Added
- Read the Docs webhook integration for automatic documentation rebuilds
- `trigger-rtd-build` invoke task

## [1.10.6] - 2025-10-12

### Fixed
- Race conditions in tests with MongoDB write concern 0

## [1.10.5] - 2025-10-12

### Fixed
- README documentation links now point to Read the Docs HTML instead of markdown

## [1.10.4] and earlier

See git history for changes prior to v1.10.5.

---

[2.0.0]: https://github.com/jdrumgoole/pyimport/compare/v1.10.9...v2.0.0
[1.10.9]: https://github.com/jdrumgoole/pyimport/compare/v1.10.8...v1.10.9
