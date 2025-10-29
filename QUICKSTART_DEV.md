# Quick Start for Developers

This is a quick reference for getting started with `pyimport` development.

## Initial Setup (First Time)

```bash
# 1. Clone the repository
git clone https://github.com/jdrumgoole/pyimport.git
cd pyimport

# 2. Install required Python versions (if not already installed)
pyenv install 3.11.9 3.12.11 3.13.8

# 3. Set Python versions for the project
pyenv local 3.12.11 3.11.9

# 4. Verify Python versions are available
poetry run invoke check-python-versions

# 5. Install dependencies
poetry install

# 6. Verify setup
poetry run invoke tox-list
```

## Daily Development Workflow

```bash
# Run quick tests on current Python version
poetry run invoke quick-test

# Run full test suite on current Python version
poetry run invoke test-all

# Run tests across all Python versions (before committing)
poetry run invoke tox-run

# Check Python version availability
poetry run invoke check-python-versions
```

## Common Commands

### Testing

```bash
# Quick smoke tests
poetry run invoke quick-test

# Run all pytest tests (current Python)
poetry run invoke run-pytest

# Run all tests including scripts (current Python)
poetry run invoke test-all

# Test on all Python versions
poetry run invoke tox-run

# Test on specific Python version
poetry run invoke tox-run --env=py312
```

### Documentation

```bash
# Build documentation
poetry run invoke docs-build

# Build and serve documentation locally
poetry run invoke docs-serve

# Clean documentation build artifacts
poetry run invoke docs-clean
```

### Publishing

```bash
# Full build (tests + package build)
poetry run invoke build

# Publish to PyPI (runs tests, builds, publishes, triggers RTD)
poetry run invoke publish

# Just build without tests
poetry run invoke poetry-build

# Trigger Read the Docs rebuild
poetry run invoke trigger-rtd-build
```

### Utilities

```bash
# List all available tasks
poetry run invoke --list

# List tox environments
poetry run invoke tox-list

# Check Python versions
poetry run invoke check-python-versions

# Clean build artifacts
poetry run invoke clean
```

## Project Structure

```
pyimport/
├── pyimport/              # Main package source
│   ├── pyimport_main.py  # CLI entry point
│   ├── fieldfile.py      # Field file handling
│   ├── csvreader.py      # CSV reading with type conversion
│   └── db/               # Database writers
├── test/                  # Test suite
│   ├── test_command/     # CLI tests
│   ├── test_e2e/         # End-to-end tests
│   ├── test_general/     # Unit tests
│   └── test_db/          # Database tests
├── docs/                  # Sphinx documentation
├── tasks.py              # Invoke task definitions
├── tox.ini               # Tox configuration
├── pyproject.toml        # Poetry/project configuration
└── .python-version       # pyenv Python versions
```

## Environment Setup

### MongoDB (Required for tests)

```bash
# macOS
brew tap mongodb/brew
brew install mongodb-community
brew services start mongodb-community

# Verify MongoDB is running
mongosh --eval "db.version()"
```

### PostgreSQL (Optional, for PostgreSQL import tests)

```bash
# Use standard PostgreSQL environment variables in .env file
echo 'PGHOST=localhost' >> .env
echo 'PGPORT=5432' >> .env
echo 'PGDATABASE=postgres' >> .env
echo 'PGUSER=jdrumgoole' >> .env

# Store credentials in ~/.pgpass for security
echo 'localhost:5432:postgres:jdrumgoole:yourpassword' >> ~/.pgpass
chmod 600 ~/.pgpass
```

### Read the Docs (Optional, for documentation triggers)

```bash
# Add RTD webhook token to .env
echo 'RTD_WEBHOOK_TOKEN=your-token-here' >> .env
```

## Troubleshooting

### "could not find python interpreter"

```bash
# Check installed versions
pyenv versions

# Install missing version
pyenv install 3.11.9

# Update .python-version
pyenv local 3.12.11 3.11.9

# Verify
poetry run invoke check-python-versions
```

### Tests failing due to MongoDB

```bash
# Check MongoDB is running
brew services list | grep mongodb

# Start MongoDB
brew services start mongodb-community

# Test connection
mongosh --eval "db.version()"
```

### Poetry environment issues

```bash
# Remove and recreate environment
poetry env remove python
poetry env use $(pyenv which python)
poetry install
```

### Tox environment issues

```bash
# Remove tox environments
rm -rf .tox

# Recreate
poetry run tox
```

## Git Workflow

```bash
# 1. Create feature branch
git checkout -b feature/my-feature

# 2. Make changes and test
poetry run invoke test-all

# 3. Test across all Python versions
poetry run invoke tox-run

# 4. Commit changes
git add .
git commit -m "Add my feature"

# 5. Push and create PR
git push origin feature/my-feature
```

## Release Workflow

```bash
# 1. Update version in pyimport/version.py and pyproject.toml
# 2. Run full test suite
poetry run invoke tox-run

# 3. Build and publish
poetry run invoke publish

# 4. Create git tag
git tag v1.x.x
git push origin v1.x.x
```

## Getting Help

```bash
# List all invoke tasks
poetry run invoke --list

# Task help
poetry run invoke --help <task-name>

# View documentation
poetry run invoke docs-serve
```

## Additional Resources

- [Full pyenv setup guide](./PYENV_SETUP.md)
- [Project documentation](https://pyimport.readthedocs.io/)
- [GitHub repository](https://github.com/jdrumgoole/pyimport)
- [PyPI package](https://pypi.org/project/pyimport/)
