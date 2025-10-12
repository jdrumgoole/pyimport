# Test Args Fix - Missing Tests Now Included

## Issue Found

During audit of `invoke test-all`, discovered that **4 ArgMgr tests were NOT being run** by the test suite.

## Root Cause

The test file `test/test_args.py` was located in the **root of the test directory** rather than in a subdirectory. The `run_pytest()` function in `tasks.py` only runs tests in **subdirectories** under `test/`, so this file was being skipped.

## Tests That Were Missing

These 4 tests in `test_args.py` were not being run:
1. `test_args()` - Tests default args creation
2. `test_add()` - Tests adding arguments
3. `test_length()` - Tests argument count tracking
4. `test_merge_namespaces()` - Tests merging argument managers

## Solution Implemented

### 1. Moved Test File ✅
- Created new directory: `test/test_args/`
- Moved `test/test_args.py` → `test/test_args/test_argmgr.py`

### 2. Updated tasks.py ✅
Added `'test/test_args'` to the `test_dirs` list in `run_pytest()` function (line 251):

```python
test_dirs = [
    'test/test_args',        # NEW - Added this line
    'test/test_command',
    'test/test_config',
    # ... rest of directories
]
```

### 3. Fixed Test Failures ✅
The tests were failing with `SystemExit: 0` because `ArgMgr.default_args()` calls the argument parser which was reading pytest's command-line arguments (like `-v`).

**Fix:** Added a pytest fixture to mock `sys.argv`:

```python
@pytest.fixture(autouse=True)
def mock_sys_argv(monkeypatch):
    """Mock sys.argv to avoid pytest arguments interfering with argparser"""
    monkeypatch.setattr(sys, 'argv', ['pyimport'])
```

This fixture automatically runs for all tests in the file and ensures `sys.argv` only contains the program name, preventing interference from pytest's flags.

## Results

### Before Fix
- **Tests reported by `invoke test-all`**: 199 tests
- **Tests actually run**: 199 tests
- **Tests missing**: 4 tests (test_args.py not included)

### After Fix
- **Tests reported by `invoke run-pytest`**: 203 tests ✅
- **Tests actually run**: 203 tests ✅
- **Tests missing**: 0 ✅
- **All tests passing**: YES ✅

## Files Modified

1. **Created**: `test/test_args/` directory
2. **Moved**: `test/test_args.py` → `test/test_args/test_argmgr.py`
3. **Modified**: `tasks.py` - Added test_args to test directories list
4. **Modified**: `test/test_args/test_argmgr.py` - Added sys.argv mocking fixture

## Verification

```bash
invoke run-pytest
# Result: 203 passed, 0 failed
```

All ArgMgr tests now run successfully and are included in the CI/CD test suite.
