# Test Suite Optimization Strategy

## Current State

### Test Runtime Analysis

**Pytest Suite** (~15-20 seconds total):
- test_general: 5.17s (99 tests)
- test_e2e: 3.18s (15 tests)
- test_command: 2.13s (32 tests)
- test_db: 1.50s (29 tests)
- Other test dirs: < 1s each

**Integration Scripts** (~90-120 seconds):
- test_yellowtrip: Large dataset processing
- test_multi: Multi-process with file splitting
- test_small_multi: File splitting and import
- test_audit: Multiple import modes
- test_scripts: Basic functionality verification

**Full Suite** (invoke test-all): ~2-3 minutes
**Tox (all Python versions)**: ~2 minutes per version

### Slowest Individual Tests

1. `test_timer.py::test_timer` - 1.00s (intentional sleep)
2. `test_restart.py::test_multiprocess_restart` - 0.90s
3. `test_e2e.py::test_multi_split` - 0.72s
4. `test_command.py::test_async_import_command` - 0.47s
5. `test_api.py::test_drop_before_import` - 0.27s

## Optimization Strategies

### Quick Wins (Low Effort, High Impact)

#### 1. Parallel Pytest Execution
Run pytest in parallel using pytest-xdist:

```bash
# Install
poetry add --group dev pytest-xdist

# Run with 4 workers
pytest -n 4

# Run with auto-detection
pytest -n auto
```

**Expected Impact**: 30-50% reduction in pytest runtime

#### 2. Selective Script Testing
Split test_all_scripts into fast and slow categories:

**Fast tests** (quick-test-scripts):
- test_scripts: Basic functionality (~5s)
- test_data: Single test case (~3s)

**Slow tests** (full-test-scripts):
- test_yellowtrip: Large dataset processing (~30s)
- test_multi: Multi-process tests (~20s)
- test_small_multi: File splitting (~10s)
- test_audit: Multiple modes (~25s)

**Expected Impact**: Daily dev workflow 80% faster

#### 3. Test Fixtures Optimization
Use pytest fixtures with appropriate scopes to avoid repeated setup:

```python
# Session-scoped fixtures for expensive operations
@pytest.fixture(scope="session")
def mongodb_connection():
    # Reuse connection across all tests
    pass

# Module-scoped fixtures for test data
@pytest.fixture(scope="module")
def sample_csv_data():
    # Load once per module
    pass
```

**Expected Impact**: 10-20% reduction in setup/teardown time

#### 4. Pytest Configuration
Add pytest optimization flags to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
minversion = "8.0"
testpaths = ["test"]
asyncio_mode = "strict"
# Performance optimizations
addopts = [
    "--strict-markers",
    "--tb=short",
    "--disable-warnings",
    "-ra",  # Show summary of all non-passing tests
]
```

**Expected Impact**: 5-10% faster test execution

### Medium Effort Optimizations

#### 5. Reduce Timer Test Sleep
The `test_timer` test has an intentional 1-second sleep. Consider:
- Reducing to 0.1s with adjusted tolerance
- Using mock time for instant testing

```python
# Current
def test_timer(self):
    timer.start()
    time.sleep(1)  # <-- 1 second wait
    timer.stop()

# Optimized
def test_timer(self, monkeypatch):
    # Mock time.time() for instant testing
    pass
```

**Expected Impact**: 0.9s saved per test run

#### 6. Database Connection Pooling
Use connection pooling for MongoDB operations:

```python
# Single shared connection for tests
@pytest.fixture(scope="session")
def db_client():
    client = pymongo.MongoClient(host, maxPoolSize=50)
    yield client
    client.close()
```

**Expected Impact**: 15-25% faster database tests

#### 7. Smaller Test Datasets
Use smaller datasets for integration tests:
- 10k rows instead of 100k for basic functionality tests
- 200k rows only for performance tests

**Expected Impact**: 30-40% faster integration tests

### High Effort, Long-Term Optimizations

#### 8. Test Categorization with Markers
Add pytest markers for different test categories:

```python
# In test files
@pytest.mark.fast
def test_quick_operation():
    pass

@pytest.mark.slow
def test_large_dataset():
    pass

@pytest.mark.integration
def test_end_to_end():
    pass
```

```bash
# Run only fast tests
pytest -m fast

# Skip slow tests
pytest -m "not slow"
```

**Expected Impact**: Flexible test execution for different scenarios

#### 9. Mock Heavy Operations
Mock expensive operations for unit tests:
- MongoDB operations
- File I/O for large files
- Network requests

**Expected Impact**: 40-60% faster unit test suite

#### 10. Parallel Script Testing
Run integration scripts in parallel using pytest-xdist or concurrent invoke tasks.

**Expected Impact**: 50-70% reduction in script testing time

## Implementation Plan

### Phase 1: Immediate (< 1 hour)
1. ✅ Add pytest-xdist for parallel execution
2. ✅ Create fast/slow test categories
3. ✅ Add pytest configuration optimizations
4. ✅ Create new invoke tasks for selective testing

### Phase 2: Short-term (< 1 day)
1. Add pytest markers for test categorization
2. Optimize timer test
3. Implement connection pooling fixtures
4. Reduce dataset sizes for non-performance tests

### Phase 3: Long-term (ongoing)
1. Convert unit tests to use mocks where appropriate
2. Optimize database setup/teardown
3. Implement parallel integration testing
4. Add performance regression testing

## Proposed New Invoke Tasks

```python
@task
def quick_pytest(c):
    """Run pytest with parallel execution (fast)"""
    with c.cd(ROOT):
        c.run('poetry run pytest -n auto -m "not slow"')

@task
def quick_test_scripts(c):
    """Run only fast integration scripts"""
    test_scripts(c)
    test_data(c)

@task
def quick_dev(c):
    """Quick development test cycle"""
    quick_pytest(c)
    quick_test_scripts(c)

@task
def full_test(c):
    """Full test suite (all pytest + all scripts)"""
    c.run('poetry run pytest -n auto')
    test_all_scripts(c)
```

## Expected Overall Impact

| Scenario | Current | Optimized | Improvement |
|----------|---------|-----------|-------------|
| Daily dev (quick tests) | 120s | 25s | 79% faster |
| Full pytest suite | 20s | 10s | 50% faster |
| Full test suite | 150s | 60s | 60% faster |
| Tox (per version) | 60s | 35s | 42% faster |

## Maintaining Coverage

All optimizations maintain 100% test coverage by:
- Not removing any tests
- Running all tests in CI/CD
- Using parallel execution (not skipping tests)
- Reducing dataset sizes while maintaining code paths
- Mocking only implementation details, not business logic

## Monitoring

Track test performance over time:
```bash
# Generate timing report
pytest --durations=20 > test_timings.txt

# Compare with previous runs
# Add to CI/CD metrics
```
