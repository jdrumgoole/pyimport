# Changelog

All notable changes to pyimport will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.4] - 2025-10-14

### Fixed
- **Parallel test execution**: Fixed race conditions in pytest-xdist parallel tests
  - Added `xdist_group` markers to force sequential execution within test files
  - Prevents multiple test workers from interfering with each other's MongoDB collections
  - Fixed test failures like `assert 9 == (4148 - 3130)` caused by cross-worker data pollution
  - Affected files: test_command, test_restart, test_v2_comprehensive_e2e, test_v2_integration, test_fieldfile, test_fileprocessor
  - Tests now properly isolated while maintaining parallel execution across different test files
- **Tox test execution**: Fixed tox running tests from wrong directory
  - Tests now run from within their respective test directories using `sh -c 'cd test/XXX && pytest'`
  - Added `allowlist_externals = sh` to tox.ini to allow shell commands
  - Fixes `OSError: No such file` errors in test_config tests that expect to find field files in current directory
- **Python 3.9 compatibility**: Fixed type hint syntax error in importresult.py
  - Added `from __future__ import annotations` to support union type syntax `list[ImportResult]|None`
  - Fixes `TypeError: unsupported operand type(s) for |` on Python 3.9
- **Tox dependencies**: Added missing test dependencies to tox.ini
  - Added `python-dotenv` (required by test_e2e tests)
  - Added `colorama` (runtime dependency for colored terminal output)
  - All tox tests now pass across Python 3.9-3.13

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
