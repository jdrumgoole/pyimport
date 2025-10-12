# Introduction

## What is PyImport?

PyImport is a powerful Python command-line tool for importing CSV data into MongoDB. Unlike MongoDB's native `mongoimport`, PyImport focuses on:

- **Handling "dirty" data gracefully** - Automatic type conversion with fallback to strings on errors
- **Automatic field type detection** - Infers types from CSV data and generates field files
- **Multiple execution strategies** - Sync, async, multi-process, and threaded imports
- **Parallel processing** - Split large files and import in parallel for maximum throughput
- **Restart capability** - Resume failed imports from where they left off
- **Flexible type conversion** - Support for dates, timestamps, ISO dates, and custom formats

## Key Features

### Automatic Type Detection
PyImport can analyze your CSV file and automatically generate a field file (`.tff`) that defines the type of each column:

```bash
pyimport --genfieldfile data.csv
```

This creates `data.tff` with inferred types for each field.

### Forgiving Type Conversion
If a field value cannot be converted to its specified type, PyImport falls back to storing it as a string rather than failing the entire import. This handles messy real-world data gracefully.

### Multiple Import Strategies

- **Synchronous** (default): Single-threaded, straightforward imports
- **Async** (`--asyncpro`): Event-loop based async imports using Motor driver
- **Multi-process** (`--multi`): Parallel import using multiple CPU cores
- **Threaded** (`--threads`): Thread-based parallel import

### Performance Optimizations

Recent performance improvements include:
- Pre-compiled type converters (15-25% faster)
- Optimized field validation (5-10% faster)
- Fast ISO date parsing (100x faster than generic date parsing)
- Expected overall improvement: 20-35%

Typical throughput:
- **Sync**: ~24,000-32,000 docs/sec
- **Async**: ~30,000-40,000 docs/sec
- **Multi-process**: ~50,000+ docs/sec

### File Splitting
Large CSV files can be automatically split into smaller chunks for parallel processing:

```bash
pyimport --splitfile --autosplit 10 --multi --poolsize 4 largefile.csv
```

This splits the file into 10 chunks and processes them with 4 parallel workers.

## When to Use PyImport

PyImport is ideal when you need to:

- Import CSV data with inconsistent types or dirty data
- Automatically detect and handle various date formats
- Import large files quickly using parallel processing
- Resume failed imports without starting over
- Add metadata (timestamps, filenames, line numbers) to imported documents
- Import from URLs or local files with the same command

## Quick Example

Basic import:
```bash
# Generate field file
pyimport --genfieldfile mydata.csv

# Import with generated field file
pyimport --database mydb --collection mycol mydata.csv
```

Fast parallel import:
```bash
pyimport --multi --splitfile --autosplit 8 --poolsize 4 \
         --database mydb --collection mycol largefile.csv
```

## Architecture Overview

PyImport follows a clean architecture:

1. **Field Files** (`.tff`): TOML files defining column types and formats
2. **CSV Reader**: Reads and type-converts CSV data per field definitions
3. **Enricher**: Optionally adds metadata (timestamps, filenames, line numbers)
4. **Database Writer**: Batches and writes documents to MongoDB (or PostgreSQL)
5. **Import Commands**: Orchestrates the import process using different strategies

## Compared to mongoimport

| Feature | PyImport | mongoimport |
|---------|----------|-------------|
| Type inference | Automatic with `--genfieldfile` | Manual `--columnsHaveTypes` |
| Dirty data handling | Graceful fallback to string | Strict, may fail |
| Date formats | Multiple formats, automatic detection | Limited |
| Parallel processing | Built-in with `--multi` or `--threads` | Requires external scripting |
| Restart capability | Built-in with `--restart` and `--audit` | Not built-in |
| CSV from URLs | Yes | No |
| File splitting | Built-in | Manual |

## Installation

See the [Installation Guide](installation.md) for setup instructions.

## Next Steps

- [Installation](installation.md) - Set up PyImport
- [Quick Start](quickstart.md) - Get started with basic imports
- [Command-Line Reference](cli_reference.md) - Complete CLI options documentation
- [Field Files](fieldfiles.md) - Understanding `.tff` field files
- [Advanced Usage](advanced.md) - Parallel processing, restart, and optimization
