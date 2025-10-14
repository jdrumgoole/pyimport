# Setting Up pyenv, Poetry, Invoke, and Tox Together

This guide explains how to set up your development environment to test `pyimport` across multiple Python versions using pyenv, poetry, invoke, and tox.

## Overview

- **pyenv**: Manages multiple Python versions on your system
- **poetry**: Manages project dependencies and virtual environments
- **invoke**: Task automation (replacement for Make)
- **tox**: Automated testing across multiple Python versions

## Prerequisites

### 1. Install pyenv

If you haven't already installed pyenv:

```bash
# macOS (using Homebrew)
brew install pyenv

# Add to your shell profile (~/.zshrc or ~/.bash_profile)
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
```

Restart your shell after adding these lines.

### 2. Install Poetry

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

## Setup Steps

### Step 1: Install Required Python Versions

The project requires Python 3.11+ and tox is configured to test against 3.11, 3.12, and 3.13.

```bash
# List available Python versions
pyenv install --list | grep "^\s*3\.\(11\|12\|13\)\."

# Install the required versions
pyenv install 3.11.9    # or latest 3.11.x
pyenv install 3.12.11   # or latest 3.12.x
pyenv install 3.13.8    # or latest 3.13.x

# Verify installations
pyenv versions
```

### Step 2: Set Python Versions for the Project

You need to make all three Python versions available to tox. Use `pyenv local` to set multiple versions:

```bash
# Navigate to the project root
cd /path/to/pyimport

# Set all three versions (first one is the default)
pyenv local 3.12.11 3.11.9 3.13.8

# This creates a .python-version file
# Verify all versions are available
pyenv versions
```

The first version in the list (3.12.11) becomes your default Python for the project.

### Step 3: Install Dependencies with Poetry

```bash
# Use the default Python version (3.12) for poetry
poetry env use $(pyenv which python)

# Install all dependencies
poetry install

# Verify the environment
poetry env info
poetry run python --version
```

### Step 4: Verify Tox Can Find All Python Versions

```bash
# List tox environments
poetry run invoke tox-list

# Should show:
# py311
# py312
# py313

# Verify each Python version is accessible
python3.11 --version
python3.12 --version
python3.13 --version
```

### Step 5: Run Tests Across All Python Versions

```bash
# Run tests on all Python versions
poetry run invoke tox-run

# Run tests on a specific Python version
poetry run invoke tox-run --env=py312

# Direct tox usage (alternative)
poetry run tox
poetry run tox -e py311
```

## Common Workflows

### Adding a New Python Version

When a new Python version is released:

```bash
# Install the new version
pyenv install 3.14.0

# Update .python-version to include the new version
pyenv local 3.12.11 3.11.9 3.13.8 3.14.0

# Update tox.ini to include the new environment
# Edit tox.ini and add py314 to envlist

# Run tests on the new version
poetry run tox -e py314
```

### Troubleshooting

#### Problem: "could not find python interpreter with spec(s): py3XX"

**Solution**: Ensure the Python version is installed and available:

```bash
# Check if version is installed
pyenv versions

# Install if missing
pyenv install 3.11.9

# Update .python-version
pyenv local 3.12.11 3.11.9 3.13.8

# Verify tox can find it
which python3.11
python3.11 --version
```

#### Problem: Poetry using wrong Python version

**Solution**: Reset the poetry environment:

```bash
# Remove existing environment
poetry env remove python

# Recreate with correct Python
poetry env use $(pyenv which python)
poetry install
```

#### Problem: Tox tests failing due to missing dependencies

**Solution**: Rebuild tox environments:

```bash
# Remove all tox environments
rm -rf .tox

# Recreate and run tests
poetry run tox
```

## Project Structure

```
pyimport/
├── .python-version          # pyenv configuration (multiple versions)
├── pyproject.toml          # poetry configuration
├── tox.ini                 # tox configuration
├── tasks.py                # invoke tasks
└── .env                    # environment variables (optional)
```

## Quick Reference

### pyenv Commands

```bash
pyenv versions              # List installed versions
pyenv install 3.12.11      # Install a specific version
pyenv local 3.12.11        # Set version for current directory
pyenv which python         # Show path to active Python
pyenv rehash              # Rebuild shim binaries (after pip installs)
```

### Poetry Commands

```bash
poetry install             # Install dependencies
poetry env info           # Show environment info
poetry env use python     # Set Python version for env
poetry run <command>      # Run command in poetry environment
poetry shell              # Activate poetry shell
```

### Invoke Commands

```bash
invoke --list             # List all available tasks
invoke tox-list          # List tox environments
invoke tox-run           # Run tox on all versions
invoke tox-run --env=py312  # Run tox on specific version
invoke test-all          # Run all tests (current Python)
invoke docs-build        # Build documentation
```

### Tox Commands

```bash
poetry run tox -l         # List environments
poetry run tox            # Run all environments
poetry run tox -e py312   # Run specific environment
poetry run tox -r         # Recreate environments
poetry run tox -p         # Run in parallel
```

## CI/CD Integration

For GitHub Actions or other CI/CD systems, you can use a matrix strategy:

```yaml
# Example GitHub Actions workflow
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.11', '3.12', '3.13']
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      - name: Install dependencies
        run: |
          pip install poetry
          poetry install
      - name: Run tests
        run: poetry run invoke run-pytest
```

## Recommended Workflow

1. **Initial Setup** (once):
   ```bash
   pyenv install 3.11.9 3.12.11 3.13.8
   cd /path/to/pyimport
   pyenv local 3.12.11 3.11.9 3.13.8
   poetry install
   ```

2. **Daily Development** (using default Python 3.12):
   ```bash
   poetry run invoke test-all    # Run tests
   poetry run invoke quick-test  # Quick smoke tests
   ```

3. **Before Committing** (test all versions):
   ```bash
   poetry run invoke tox-run
   ```

4. **Before Release** (full test suite + docs):
   ```bash
   poetry run invoke tox-run
   poetry run invoke docs-build
   poetry run invoke publish
   ```

## Additional Resources

- [pyenv documentation](https://github.com/pyenv/pyenv)
- [Poetry documentation](https://python-poetry.org/docs/)
- [Tox documentation](https://tox.wiki/)
- [Invoke documentation](https://www.pyinvoke.org/)
