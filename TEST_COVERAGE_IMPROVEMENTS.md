# Test Coverage Improvements

## Current Status

**Overall Coverage: 64%** (1498 of 4149 lines missing)

## Summary of Improvements

This document outlines the test coverage improvements made to the pyimport project. New test files have been created to significantly increase coverage for low-coverage modules.

### New Test Files Created

#### 1. `test/test_general/test_asyncimport_coverage.py`
**Target Module:** `pyimport/asyncimport.py` (was 28% coverage)

**New Tests Added:**
- `test_async_import_basic()` - Basic async import functionality
- `test_async_import_with_delimiter()` - Custom delimiter handling
- `test_async_import_with_batchsize()` - Batch size configuration
- `test_async_import_with_addfilename()` - Filename field addition
- `test_async_import_with_addtimestamp()` - Timestamp field addition
- `test_async_import_empty_file()` - Empty file handling
- `test_async_import_with_drop()` - Collection drop functionality

**Coverage Areas:**
- Async MongoDB import using Motor driver
- Error handling in async context
- Document enrichment (timestamps, filenames)
- Batch processing logic
- Collection management (drop/create)

#### 2. `test/test_general/test_threadimport_coverage.py`
**Target Module:** `pyimport/threadimportcommand.py` (was 33% coverage)

**New Tests Added:**
- `test_thread_import_basic()` - Basic threaded import
- `test_thread_import_multiple_files()` - Multiple file handling
- `test_thread_import_with_poolsize()` - Thread pool configuration
- `test_thread_import_with_delimiter()` - Custom delimiter support
- `test_thread_import_with_batchsize()` - Batch size handling
- `test_thread_import_with_addfilename()` - Filename enrichment
- `test_thread_import_combined_with_async()` - Thread + async combination
- `test_thread_import_error_handling()` - Error scenarios

**Coverage Areas:**
- Thread-based parallel import
- Thread pool management
- Multiple file concurrent processing
- Error handling in threaded context
- Integration with async processing

#### 3. `test/test_filesplitter/test_filesplitter_coverage.py`
**Target Module:** `pyimport/filesplitter.py` (was 35% coverage, 173 lines missing)

**New Test Classes:**

**TestLineCounter:**
- `test_count_lines_simple()` - Basic line counting
- `test_count_lines_empty_file()` - Empty file handling
- `test_count_lines_no_trailing_newline()` - Files without trailing newline
- `test_count_lines_large_file()` - Large file handling (1000+ lines)

**TestFileSplitter:**
- `test_split_file_basic()` - Basic file splitting
- `test_split_file_no_header()` - Splitting without headers
- `test_split_file_exact_division()` - Even split scenarios
- `test_split_file_single_split()` - Files smaller than split size
- `test_split_with_autosplit()` - Automatic split count calculation
- `test_split_with_header_preservation()` - Header preservation in splits

**TestSplitHelperFunctions:**
- `test_get_split_name()` - Split filename generation
- `test_copy_lines_generator()` - Line copying utility
- `test_copy_lines_generator_more_than_available()` - Edge case handling

**TestSplitFilesFunction:**
- `test_split_files_with_args()` - High-level split function
- `test_split_files_multiple_input_files()` - Multiple file splitting

**TestEdgeCases:**
- `test_split_empty_file()` - Empty file splitting
- `test_split_file_with_long_lines()` - Very long line handling
- `test_split_file_different_delimiters()` - Custom delimiter support
- `test_split_maintains_line_count()` - Data integrity verification

**Coverage Areas:**
- File splitting for parallel processing
- Line counting utilities
- Header preservation logic
- Split filename generation
- Edge cases (empty files, long lines, various delimiters)
- Data integrity verification

## Modules Still Needing Attention

### High Priority (Low Coverage)

1. **asyncaudit.py (35% coverage, 69 lines missing)**
   - Audit collection management in async context
   - Progress tracking for restartable imports
   - Needs: Integration tests with actual MongoDB audit collection

2. **filereader.py (34% coverage, 74 lines missing)**
   - Remote file reading (HTTP/HTTPS)
   - Local file reading with various encodings
   - Needs: Tests for URL-based imports, encoding edge cases

3. **monotonicid.py (41% coverage, 24 lines missing)**
   - Custom ID generation
   - Needs: Tests for ID generation strategies

4. **parallellimportcommand.py (60% coverage, 8 lines missing)**
   - Parallel import command coordination
   - Needs: Integration tests for parallel execution

### Medium Priority (Moderate Coverage)

5. **syncmdbwriter.py (58% coverage, 52 lines missing)**
   - Synchronous MongoDB writer
   - Batch insert logic
   - Needs: More edge case testing

6. **csvreader.py (65% coverage, 31 lines missing)**
   - CSV reading with type conversion
   - Needs: More delimiter and encoding tests

7. **linereader.py (67% coverage, 37 lines missing)**
   - Line-by-line reading utilities
   - Needs: Remote file reading tests

8. **logger.py (67% coverage, 30 lines missing)**
   - Logging configuration
   - Needs: Log level and handler tests

### Test Infrastructure Issues

Several existing tests are failing due to:

1. **Working Directory Issues**: Tests expect to run from specific directories
   - Tests look for data files in current directory
   - Solution: Update tests to use absolute paths or proper test fixtures

2. **Missing PostgreSQL Configuration**: PostgreSQL tests require `PGHOST` environment variable
   - 14 errors in test_db/test_rdbmaker.py and test_db/test_rdbmanager.py
   - Solution: Set PostgreSQL environment variables (PGHOST, PGPORT, PGDATABASE, PGUSER) in .env file and configure credentials in ~/.pgpass
   - Note: Tests now skip gracefully when PGHOST is not set

3. **Test Data File Locations**: Some tests expect files in wrong locations
   - FileNotFoundError in multiple test suites
   - Solution: Copy test data to correct locations or fix path references

## Running New Tests

### Run All New Coverage Tests
```bash
# Run async import tests
cd test/test_general && poetry run pytest test_asyncimport_coverage.py -v

# Run thread import tests
cd test/test_general && poetry run pytest test_threadimport_coverage.py -v

# Run filesplitter tests
cd test/test_filesplitter && poetry run pytest test_filesplitter_coverage.py -v
```

### Run with Coverage Report
```bash
poetry run coverage run -m pytest test/test_general/test_asyncimport_coverage.py
poetry run coverage run -a -m pytest test/test_general/test_threadimport_coverage.py
poetry run coverage run -a -m pytest test/test_filesplitter/test_filesplitter_coverage.py
poetry run coverage report --include="pyimport/*"
```

### Expected Coverage Improvements

After these new tests are successfully integrated:

| Module | Old Coverage | Expected New Coverage | Improvement |
|--------|--------------|----------------------|-------------|
| asyncimport.py | 28% | ~70-80% | +42-52% |
| threadimportcommand.py | 33% | ~75-85% | +42-52% |
| filesplitter.py | 35% | ~70-80% | +35-45% |
| **Overall Project** | **64%** | **~72-75%** | **+8-11%** |

## Next Steps

### Immediate Actions

1. **Fix Existing Test Failures**
   - Update test working directory handling
   - Add pytest fixtures for test data files
   - Configure PostgreSQL test environment (or add mocks)

2. **Run New Tests and Verify**
   ```bash
   poetry run pytest test/test_general/test_asyncimport_coverage.py -v
   poetry run pytest test/test_general/test_threadimport_coverage.py -v
   poetry run pytest test/test_filesplitter/test_filesplitter_coverage.py -v
   ```

3. **Generate Updated Coverage Report**
   ```bash
   poetry run coverage run -m pytest test/
   poetry run coverage report > coverage_report_new.txt
   poetry run coverage html
   ```

### Future Improvements

1. **Add PostgreSQL Integration Tests**
   - Set up test PostgreSQL database
   - Test RDB import functionality
   - Test table creation and management

2. **Add Remote File Tests**
   - Mock HTTP server for testing URL-based imports
   - Test various response codes and edge cases
   - Test timeout and retry logic

3. **Add Audit System Tests**
   - Test restart capability
   - Test progress tracking
   - Test audit collection management

4. **Performance Tests**
   - Add benchmarks for different import strategies
   - Test with large datasets (100k+ rows)
   - Compare sync vs async vs multi vs thread performance

5. **Error Scenario Tests**
   - Network failures
   - Database connection issues
   - Malformed CSV data
   - Type conversion edge cases

## Testing Best Practices Applied

1. **Isolation**: Each test creates its own temporary files and cleans up
2. **Fixtures**: Using MDBTestDB context manager for database setup/teardown
3. **Parameterization**: Testing multiple scenarios with similar logic
4. **Descriptive Names**: Clear test names indicating what is being tested
5. **Documentation**: Docstrings explaining test purpose
6. **Edge Cases**: Comprehensive edge case coverage (empty files, long lines, etc.)

## Maintenance

This document should be updated when:
- New tests are added
- Coverage metrics change significantly
- Test infrastructure improvements are made
- New modules are added to the project

Last Updated: 2025-10-10
