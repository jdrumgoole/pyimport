# Version Centralization - Single Source of Truth

## Summary

Centralized version management to use **single source of truth** in `pyimport/version.py`. All other files now reference this single definition.

## Version 1.9.0

Current version: **1.9.0**

## Single Source of Truth

**Primary location:** `pyimport/version.py`

```python
__VERSION__: str = "1.9.0"
```

This is the ONLY place where the version number is defined.

## How Other Files Use It

### 1. pyproject.toml (Poetry)

Poetry requires version to be in pyproject.toml, so we keep it there with a comment:

```toml
[tool.poetry]
name = "pyimport"
version = "1.9.0"  # Also defined in pyimport/version.py - keep in sync
description = "A CSV importer for MongoDB"
```

**Note:** Poetry doesn't support dynamic version import, so manual sync is required when updating version.

**Process to update version:**
1. Update `pyimport/version.py`
2. Update `pyproject.toml` version field
3. Run `poetry install` to update lock file

### 2. docs/conf.py (Sphinx Documentation)

**Dynamically imports** version from `pyimport/version.py`:

```python
import sys
import os

# Add parent directory to path to import version
sys.path.insert(0, os.path.abspath('..'))

from pyimport.version import __VERSION__

# -- Project information
project = 'Pyimport'
copyright = '2024, Joe Drumgoole'
author = 'Joe Drumgoole'
release = __VERSION__  # Dynamically loaded
```

**Benefit:** No manual sync needed! Documentation always shows correct version.

### 3. pyimport/argparser.py (CLI)

Already imports from `pyimport/version.py`:

```python
from pyimport.version import __VERSION__

def parse_args_and_cfg_files(cfgparser, input_args=None):
    cfgparser.add_argument('-v', '--version', action='version',
                          version='%(prog)s ' + __VERSION__)
```

**Benefit:** `--version` flag automatically uses correct version.

### 4. Documentation Examples

Documentation files reference version in example outputs:

- `docs/markdown/installation.md` - Example: `# Output: pyimport 1.9.0`
- `docs/markdown/cli_reference.md` - Example: `# Output: pyimport 1.9.0`

**Note:** These are static strings in documentation and need manual update.

## Verification

### Test Version Import

```bash
# Test Python import
poetry run python -c "from pyimport.version import __VERSION__; print(__VERSION__)"
# Output: 1.9.0

# Test CLI flag
poetry run python -m pyimport.pyimport_main --version
# Output: pyimport_main.py 1.9.0

# Test documentation build
cd docs && poetry run sphinx-build -b html . _build/html
# Check: HTML shows "Pyimport 1.9.0 documentation"
```

### Check All References

```bash
# Single source of truth
cat pyimport/version.py
# Output: __VERSION__: str = "1.9.0"

# Poetry config (manual sync required)
grep "^version" pyproject.toml
# Output: version = "1.9.0"  # Also defined in pyimport/version.py - keep in sync

# Sphinx docs (auto-imported)
grep "release = " docs/conf.py
# Output: release = __VERSION__

# CLI (auto-imported)
grep "__VERSION__" pyimport/argparser.py
# Output: from pyimport.version import __VERSION__
```

## Process to Update Version

### 1. Update Single Source

Edit `pyimport/version.py`:
```python
__VERSION__: str = "1.10.0"  # New version
```

### 2. Update Poetry Config

Edit `pyproject.toml`:
```toml
version = "1.10.0"  # Also defined in pyimport/version.py - keep in sync
```

### 3. Update Documentation Examples (if needed)

Search and replace in docs:
```bash
# Update example outputs
cd docs/markdown
sed -i '' 's/1.9.0/1.10.0/g' *.md
```

### 4. Verify Everything

```bash
# Test import
poetry run python -c "from pyimport.version import __VERSION__; print(__VERSION__)"

# Rebuild docs
cd docs && poetry run sphinx-build -b html . _build/html

# Run tests
poetry run pytest
```

## Benefits of This Approach

### ✅ Automatic Version Propagation

- **CLI (`--version`)**: Automatically correct
- **Sphinx docs**: Automatically correct
- **Python imports**: Automatically correct

### ✅ Reduced Manual Work

Only need to update 2 files:
1. `pyimport/version.py` (primary)
2. `pyproject.toml` (Poetry requirement)

### ✅ Reduced Errors

- No risk of version mismatch in code
- Documentation always shows current version
- CLI always shows current version

### ⚠️ Manual Sync Still Required

- `pyproject.toml` (Poetry limitation)
- Documentation example outputs (static strings)

## Why Not Full Automation?

### Poetry Limitation

Poetry requires `version` field in `pyproject.toml`. It doesn't support:
- Reading from Python files
- Dynamic version resolution
- Import statements in TOML

**Workaround:** Use comment to remind maintainers to keep in sync.

### Documentation Examples

Example outputs in documentation are static strings:
```markdown
```bash
pyimport --version
# Output: pyimport 1.9.0
```
```

Could automate with:
- Pre-processing scripts
- Template variables
- Build-time substitution

**Current approach:** Manual update is simple and rare (version changes are infrequent).

## Alternative Approaches Considered

### 1. setuptools_scm (Git Tags)

**Pros:**
- Truly single source (git tags)
- No manual version updates

**Cons:**
- Requires git repository
- Version tied to git history
- More complex setup

### 2. Dynamic Version Plugin

**Pros:**
- Single source in Python file

**Cons:**
- Requires Poetry plugin
- Additional dependency
- May break with Poetry updates

### 3. Version in pyproject.toml as Primary

**Pros:**
- Poetry-native approach
- Standard location

**Cons:**
- Python code must parse TOML to read version
- Additional dependency (toml parser)
- TOML parsing in every import

**Current approach is best balance of:**
- Simplicity
- Maintainability
- Automation where possible
- Manual sync only where necessary

## Bug Fix: argparser.py

Fixed syntax error in version argument:

**Before:**
```python
cfgparser.add_argument('-v", ''--version', ...)  # Mismatched quotes
```

**After:**
```python
cfgparser.add_argument('-v', '--version', ...)  # Fixed
```

This bug prevented `--version` flag from working.

## Summary

**Single source of truth:** `pyimport/version.py`

**Auto-imported by:**
- ✅ `pyimport/argparser.py` (CLI)
- ✅ `docs/conf.py` (Sphinx)
- ✅ Any Python code that needs version

**Manual sync required:**
- ⚠️ `pyproject.toml` (Poetry limitation)
- ⚠️ Documentation example outputs (rare, simple)

**Version update process:**
1. Edit `pyimport/version.py`
2. Edit `pyproject.toml`
3. Optionally update doc examples
4. Build and test

**Current version:** 1.9.0
