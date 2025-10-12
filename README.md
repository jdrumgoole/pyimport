# PyImport - A Powerful CSV Importer for MongoDB

[![Documentation Status](https://readthedocs.org/projects/pyimport/badge/?version=latest)](https://pyimport.readthedocs.io/en/latest/?badge=latest)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**PyImport** is a Python command-line tool for importing CSV data into MongoDB with automatic type detection, parallel processing, and graceful handling of "dirty" data.

Unlike MongoDB's native `mongoimport`, PyImport focuses on handling real-world messy data, automatic type inference, and high-performance parallel imports.

**Version**: 1.9.0
**Author**: Joe Drumgoole ([joe@joedrumgoole.com](mailto:joe@joedrumgoole.com) | [BlueSky](https://bsky.app/profile/joedrumgoole.com))
**License**: Apache 2.0
**Source**: [github.com/jdrumgoole/pyimport](https://github.com/jdrumgoole/pyimport)
**Documentation**: [pyimport.readthedocs.io](https://pyimport.readthedocs.io/)

## Key Features

- **Automatic Type Detection** - Generate field files with inferred types using `--genfieldfile`
- **Graceful Error Handling** - Falls back to strings on type conversion errors instead of failing
- **Multiple Import Strategies** - Sync, async, multi-process, and threaded imports
- **Parallel Processing** - Split large files and import in parallel for maximum throughput
- **Flexible Date Parsing** - Multiple date formats with fast ISO date parsing (100x faster)
- **Performance Optimized** - Recent improvements provide 20-35% faster imports
- **URL Support** - Import directly from URLs or local files
- **Audit Tracking** - Optional audit records for import tracking and monitoring

## Performance

- **Sync**: ~24,000-32,000 docs/sec
- **Async**: ~30,000-40,000 docs/sec
- **Multi-process**: ~50,000+ docs/sec

## Requirements

- **Python**: 3.11 or higher
- **MongoDB**: 4.0 or higher

## Installation

### From PyPI (Recommended)

```bash
pip install pyimport
```

### From Source

```bash
git clone https://github.com/jdrumgoole/pyimport.git
cd pyimport
poetry install
```

### Verify Installation

```bash
pyimport --version
# Output: pyimport 1.9.0
```

## Quick Start

### Step 1: Create a Simple CSV File

```bash
# Create a test CSV file
echo "name,age,city" > test.csv
echo "Alice,30,NYC" >> test.csv
echo "Bob,25,LA" >> test.csv
```

### Step 2: Generate Field File (Type Definitions)

```bash
pyimport --genfieldfile test.csv
# Output: Created field filename 'test.tff' from 'test.csv'
```

This creates a `test.tff` file that defines the type of each column (string, int, date, etc.).

### Step 3: Import to MongoDB

```bash
pyimport --database mydb --collection people test.csv
# Imports data using the auto-generated test.tff field file
```

### Step 4: Verify Import

```bash
mongosh mydb --eval "db.people.find().pretty()"
```

## Advanced Usage

### Fast Parallel Import for Large Files

```bash
pyimport --multi --splitfile --autosplit 8 --poolsize 4 \
         --database mydb --collection mycol largefile.csv
```

This splits the file into 8 chunks and processes them with 4 parallel workers.

### Async Import (High Performance)

```bash
pyimport --asyncpro --database mydb --collection mycol data.csv
```

### Import from URL

```bash
pyimport --database mydb --collection taxi \
         https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv
```

### Track Imports with Audit

```bash
# Import with audit tracking enabled
pyimport --audit --audithost mongodb://localhost:27017 \
         --database mydb --collection mycol largefile.csv
```

Audit records capture metadata about each import including filename, record count, elapsed time, and command-line arguments for monitoring and debugging.

## Why PyImport?

MongoDB's native [mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) is excellent, but PyImport offers several additional capabilities:

### PyImport Advantages

| Feature | PyImport | mongoimport |
|---------|----------|-------------|
| **Type inference** | Automatic with `--genfieldfile` | Manual with `--columnsHaveTypes` |
| **Dirty data handling** | Graceful fallback to string | Strict, may fail |
| **Date formats** | Multiple formats, automatic detection | Limited |
| **Parallel processing** | Built-in `--multi`, `--asyncpro`, `--threads` | Requires external scripting |
| **Audit tracking** | Built-in `--audit` for import monitoring | Not built-in |
| **URL imports** | Direct URL support | Requires pre-download |
| **File splitting** | Automatic with `--splitfile` | Manual |
| **Performance optimization** | Pre-compiled converters, fast ISO dates | Standard |

### mongoimport Advantages

- Richer security options (Kerberos, LDAP, x.509)
- MongoDB Enterprise Advanced features
- JSON file imports (in addition to CSV)
- Official MongoDB support

### When to Use PyImport

Choose PyImport when you need to:
- Handle messy, inconsistent, or "dirty" CSV data
- Automatically infer types from CSV columns
- Import large files quickly with parallel processing
- Import data directly from URLs
- Add metadata (timestamps, filenames, line numbers) to documents
- Track import operations with audit records

## Field Files (`.tff`)

Field files are TOML-formatted files that define column types and formats for CSV imports. They enable automatic type conversion during import.

### Automatic Generation

The easiest way to create a field file is to generate it automatically:

```bash
pyimport --genfieldfile data.csv
# Creates data.tff with inferred types
```

### Supported Types

- **str** - String (text)
- **int** - Integer
- **float** - Floating point number
- **date** - Date without time
- **datetime** - Date with time
- **isodate** - ISO format date (YYYY-MM-DD) - fastest parsing
- **bool** - Boolean (true/false)
- **timestamp** - Unix timestamp

### Field File Naming

PyImport automatically looks for field files with the `.tff` extension:
- For `data.csv`, it looks for `data.tff`
- You can specify a custom field file with `--fieldfile`

### Example Field File

For a CSV file with inventory data:

| Inventory Item | Amount | Last Order |
|---------------|--------|------------|
| Screws | 300 | 1-Jan-2016 |
| Bolts | 150 | 3-Feb-2017 |
| Nails | 25 | 31-Dec-2017 |

Running `pyimport --genfieldfile inventory.csv` generates:

```toml
# Created 'inventory.tff'
# at UTC: 2025-10-12 by pyimport.fieldfile

["Inventory Item"]
type = "str"
name = "Inventory Item"

["Amount"]
type = "int"
name = "Amount"

["Last Order"]
type = "date"
name = "Last Order"
format = "%d-%b-%Y"  # Date format string

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
```

### Type Inference

PyImport analyzes the first data row after the header to infer types:
1. Tries to parse as **int**
2. If that fails, tries **float**
3. If that fails, tries **date**
4. Falls back to **str**

You can manually edit `.tff` files to correct types if inference is incorrect.

### Graceful Error Handling

If type conversion fails during import, PyImport falls back to storing the value as a string instead of failing the entire import (unless `--onerror fail` is specified).

### Date Format Strings

Date and datetime fields support [strptime format strings](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior):

```toml
["order_date"]
type = "date"
format = "%Y-%m-%d"  # 2024-12-31
```

Common format codes:
- `%Y` - 4-digit year (2024)
- `%m` - Month (01-12)
- `%d` - Day (01-31)
- `%H` - Hour (00-23)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)

### Date Parsing Performance

For best performance, choose the right date type:

1. **isodate** (fastest) - Use for ISO format dates (YYYY-MM-DD)
   - 100x faster than generic date parsing
   ```toml
   ["created_date"]
   type = "isodate"
   ```

2. **date/datetime with format** (fast) - Use when all dates have the same format
   ```toml
   ["order_date"]
   type = "datetime"
   format = "%Y-%m-%d %H:%M:%S"
   ```

3. **date/datetime without format** (slow) - Use only for inconsistent date formats
   ```toml
   ["flexible_date"]
   type = "date"  # No format - uses slow dateutil.parser
   ```

## Complete Documentation

For comprehensive documentation including all CLI options, advanced features, and examples, visit:

**📖 [Full Documentation at readthedocs.io](https://pyimport.readthedocs.io/)**

Documentation includes:
- **[Installation Guide](https://pyimport.readthedocs.io/en/latest/markdown/installation.html)** - Setup and configuration
- **[Quick Start](https://pyimport.readthedocs.io/en/latest/markdown/quickstart.html)** - Step-by-step tutorials
- **[CLI Reference](https://pyimport.readthedocs.io/en/latest/markdown/cli_reference.html)** - All 45+ command-line options
- **[Field Files Guide](https://pyimport.readthedocs.io/en/latest/markdown/fieldfiles.html)** - Complete `.tff` format reference
- **[Advanced Usage](https://pyimport.readthedocs.io/en/latest/markdown/advanced.html)** - Parallel processing, optimization, production tips

## Common Options

### Basic Options

```bash
-h, --help              Show help message
--version               Show version number
--database NAME         Database name [default: PYIM]
--collection NAME       Collection name [default: imported]
--mdburi URI           MongoDB connection URI [default: mongodb://localhost:27017]
```

### Field File Options

```bash
--genfieldfile          Generate field file from CSV
--fieldfile FILE        Specify custom field file path
--delimiter CHAR        Field delimiter [default: ,]
--hasheader             CSV has header line
```

### Performance Options

```bash
--multi                 Multi-process parallel import
--asyncpro             Async parallel import (high performance)
--threads              Thread-based parallel import
--poolsize N           Number of parallel workers [default: 4]
--batchsize N          Batch size for bulk inserts [default: 1000]
```

### File Splitting Options

```bash
--splitfile            Split file for parallel processing
--autosplit N          Split into N chunks
--keepsplits           Don't delete split files after import
```

### Audit Options

```bash
--audit                Enable audit tracking
--audithost URI        MongoDB URI for audit records
--auditdatabase NAME   Database for audit records [default: PYIM_AUDIT]
--auditcollection NAME Collection for audit records [default: audit]
```

### Data Enrichment Options

```bash
--addfilename          Add filename to each document
--addtimestamp now     Add current timestamp
--addtimestamp gen     Add generated ObjectId timestamp
--locator              Add filename and line number
--addfield key=value   Add custom field to all documents
```

### Error Handling Options

```bash
--onerror fail         Stop on first error
--onerror warn         Log errors and continue [default]
--onerror ignore       Silently skip errors
```

## Example Workflows

### Simple Import
```bash
pyimport --genfieldfile data.csv
pyimport --database mydb --collection mycol data.csv
```

### High-Performance Import
```bash
pyimport --multi --splitfile --autosplit 8 --poolsize 4 \
         --batchsize 5000 --database mydb --collection mycol \
         largefile.csv
```

### Import with Metadata
```bash
pyimport --addfilename --addtimestamp now --locator \
         --database mydb --collection mycol data.csv
```

### Import with Audit Tracking
```bash
pyimport --audit --audithost mongodb://localhost:27017 \
         --database mydb --collection mycol largefile.csv
```

This creates audit records in the audit collection tracking import metadata for monitoring and debugging.

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

### Development Setup

```bash
git clone https://github.com/jdrumgoole/pyimport.git
cd pyimport
poetry install --with dev

# Run tests
poetry run pytest

# Run all tests with coverage
invoke test-all
```

## Testing

PyImport has comprehensive test coverage (72%+):

```bash
# Run all tests
invoke test-all

# Run specific test suites
cd test/test_command && poetry run pytest
cd test/test_e2e && poetry run pytest

# Quick smoke tests
invoke quick-test
```

## Version History

**1.9.0** (Current)
- Comprehensive documentation (2,700+ lines)
- Version centralization with single source of truth
- Read the Docs integration
- Performance improvements (20-35% faster)
- Test coverage improvements (72%)
- Bug fixes for `--version` flag

**1.8.2**
- Previous stable release

See [CHANGELOG](https://github.com/jdrumgoole/pyimport/releases) for complete version history.

## Links

- **PyPI Package**: [pypi.org/project/pyimport](https://pypi.org/project/pyimport/)
- **Documentation**: [pyimport.readthedocs.io](https://pyimport.readthedocs.io/)
- **Source Code**: [github.com/jdrumgoole/pyimport](https://github.com/jdrumgoole/pyimport)
- **Issue Tracker**: [github.com/jdrumgoole/pyimport/issues](https://github.com/jdrumgoole/pyimport/issues)

## Support

- **Email**: [joe@joedrumgoole.com](mailto:joe@joedrumgoole.com)
- **BlueSky**: [@joedrumgoole.com](https://bsky.app/profile/joedrumgoole.com)
- **GitHub Issues**: [Report bugs or request features](https://github.com/jdrumgoole/pyimport/issues)

## License

Apache License 2.0 - See [LICENSE](LICENSE) file for details.

---

Made with ❤️ by [Joe Drumgoole](https://github.com/jdrumgoole) 
