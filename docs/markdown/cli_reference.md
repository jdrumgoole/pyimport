# Command-Line Reference

Complete reference for all PyImport command-line options.

## Usage

```bash
pyimport [OPTIONS] [FILES...]
```

## Basic Options

### `--version`, `-v`

Display PyImport version and exit.

```bash
pyimport --version
# Output: pyimport 1.10.0
```

### `--help`, `-h`

Show help message with all available options.

```bash
pyimport --help
```

### `filenames`

One or more CSV files to import. Can be local files or URLs.

```bash
# Single file
pyimport data.csv

# Multiple files
pyimport file1.csv file2.csv file3.csv

# From URL
pyimport https://example.com/data.csv
```

### `--filelist FILENAME`

Read list of files to import from a text file (one file per line).

```bash
# Create file list
cat > files.txt <<EOF
data1.csv
data2.csv
data3.csv
EOF

# Import all files
pyimport --filelist files.txt
```

## MongoDB Connection Options

### `--mdburi URI`

MongoDB connection URI.

**Default:** `mongodb://localhost:27017`

**Environment variable:** `MDB_URI`

```bash
# Local MongoDB
pyimport --mdburi mongodb://localhost:27017 data.csv

# MongoDB with authentication
pyimport --mdburi "mongodb://user:pass@localhost:27017/authDB" data.csv

# MongoDB Atlas
pyimport --mdburi "mongodb+srv://user:pass@cluster.mongodb.net/" data.csv

# Using environment variable
export MDB_URI="mongodb://localhost:27017"
pyimport data.csv
```

### `--database DATABASE`

Target database name.

**Default:** `PYIM`

```bash
pyimport --database mydb --collection mycol data.csv
```

### `--collection COLLECTION`

Target collection name.

**Default:** `imported`

```bash
pyimport --database mydb --collection users data.csv
```

### `--writeconcern LEVEL`

MongoDB write concern level (0, 1, 2, "majority").

**Default:** `0`

```bash
# Unacknowledged writes (fastest)
pyimport --writeconcern 0 data.csv

# Acknowledged writes
pyimport --writeconcern 1 data.csv

# Replicated writes
pyimport --writeconcern majority data.csv
```

### `--journal`

Enable journaling for writes.

**Default:** `False`

```bash
pyimport --journal --writeconcern 1 data.csv
```

### `--fsync`

Force fsync on write operations (sync all nodes to disk).

**Default:** `False`

```bash
pyimport --fsync --writeconcern 1 data.csv
```

## PostgreSQL Options (Experimental)

### `--pguri URI`

PostgreSQL connection URI (optional - uses environment variables if not specified).

**Default:** Constructed from standard PostgreSQL environment variables or defaults to `postgresql://localhost:5432/postgres`

**Environment variables:**
- `PGHOST` - PostgreSQL host (default: localhost)
- `PGPORT` - PostgreSQL port (default: 5432)
- `PGDATABASE` - Database name (default: postgres)
- `PGUSER` - Username (default: current OS user)

**Example:**

```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=mydb
export PGUSER=myuser

# Credentials in ~/.pgpass file:
# localhost:5432:mydb:myuser:password

pyimport --pgtable mytable data.csv
```

**Security:** Always store credentials in `~/.pgpass` file (format: `host:port:db:user:pass`) - PostgreSQL will automatically use this file for authentication.

### `--pgtable TABLE`

PostgreSQL table name.

**Default:** `imported`

```bash
pyimport --pgtable users data.csv
```

### `--pguser USER`

PostgreSQL username.

**Default:** `postgres`

### `--pgport PORT`

PostgreSQL port.

**Default:** `5432`

### `--pgdatabase DATABASE`

PostgreSQL database name.

**Default:** `postgres`

### `--pgpassword PASSWORD`

PostgreSQL password.

```bash
pyimport --pguser myuser --pgpassword mypass --pgtable data data.csv
```

## Field File Options

### `--fieldfile FILENAME`

Path to field file (`.tff`) that defines column types and formats.

If not specified, PyImport looks for a file with the same name as the CSV but with `.tff` extension.

```bash
# Explicit field file
pyimport --fieldfile custom.tff data.csv

# Auto-discovery (looks for data.tff)
pyimport data.csv
```

### `--genfieldfile`

Generate a field file from the CSV data and exit. Automatically sets `--hasheader` to `True`.

```bash
# Generate field file
pyimport --genfieldfile data.csv
# Creates data.tff

# Then import with generated field file
pyimport data.csv
```

### `--fieldinfo FILENAME`

Display information about a field file and exit.

```bash
pyimport --fieldinfo data.tff
```

## CSV Parsing Options

### `--delimiter DELIMITER`

Field delimiter character.

**Default:** `,` (comma)

**Special value:** `tab` for tab-delimited files

```bash
# Pipe-delimited
pyimport --delimiter "|" data.txt

# Tab-delimited
pyimport --delimiter tab data.tsv

# Semicolon-delimited
pyimport --delimiter ";" data.csv
```

### `--hasheader`

First line of CSV contains column names (header row).

**Default:** `False`

```bash
pyimport --hasheader data.csv
```

**Note:** When using `--genfieldfile`, this is automatically set to `True`.

### `--limit COUNT`

Limit number of records to import (0 = no limit).

**Default:** `0`

```bash
# Import first 1000 rows
pyimport --limit 1000 data.csv

# Import first 100 rows for testing
pyimport --limit 100 --database test --collection sample data.csv
```

## Data Enrichment Options

### `--noenrich`

Skip type conversion and enrichment. Import CSV data as-is without type conversion.

**Default:** `False`

```bash
# Import without type conversion (all fields as strings)
pyimport --noenrich data.csv
```

### `--addfilename`

Add a `filename` field to each document containing the source CSV filename.

**Default:** `False`

```bash
pyimport --addfilename data.csv

# Resulting documents will have:
# { "name": "Alice", "filename": "data.csv", ... }
```

### `--addtimestamp TYPE`

Add timestamp to each document.

**Options:**
- `no_timestamp` (default): No timestamp
- `doc`: Generate new timestamp per document
- `batch`: Use same timestamp for entire batch

```bash
# Add per-document timestamps
pyimport --addtimestamp doc data.csv

# Add per-batch timestamps (faster)
pyimport --addtimestamp batch data.csv

# Resulting documents will have:
# { "name": "Alice", "timestamp": ISODate("2024-01-15T10:30:00Z"), ... }
```

### `--addfield FIELD=VALUE`

Add a custom field with a constant value to all documents.

```bash
# Add source field
pyimport --addfield source=import data.csv

# Add numeric field
pyimport --addfield batch_id=42 data.csv

# Add multiple fields (use multiple --addfield flags)
pyimport --addfield region=US --addfield year=2024 data.csv

# Resulting documents will have:
# { "name": "Alice", "source": "import", "batch_id": 42, ... }
```

### `--cut FIELD1,FIELD2,...`

Comma-separated list of fields to exclude from import.

```bash
# Exclude sensitive fields
pyimport --cut ssn,password,credit_card data.csv

# Import only specific fields (cut all others)
pyimport --cut field1,field2,field3 data.csv
```

### `--locator`

Add a `locator` field containing filename and line number.

**Default:** `False`

```bash
pyimport --locator data.csv

# Resulting documents will have:
# { "name": "Alice", "locator": { "filename": "data.csv", "line": 2 }, ... }
```

## Import Performance Options

### `--batchsize SIZE`

Number of documents to insert in a single batch operation.

**Default:** `1000`

```bash
# Smaller batches (more frequent writes)
pyimport --batchsize 100 data.csv

# Larger batches (fewer write operations)
pyimport --batchsize 5000 data.csv
```

### `--asyncpro`

Use async I/O for processing files (Motor driver for MongoDB).

**Default:** `False`

```bash
pyimport --asyncpro data.csv

# Often combined with --multi or --threads
pyimport --asyncpro --multi data.csv
```

### `--multi`

Use multiprocessing for parallel import.

**Default:** `False`

```bash
# Use multiprocessing (splits work across CPU cores)
pyimport --multi data.csv

# Control number of processes
pyimport --multi --poolsize 4 data.csv
```

### `--threads`

Use threading for parallel import.

**Default:** `False`

```bash
# Use threading
pyimport --threads data.csv

# Control number of threads
pyimport --threads --poolsize 8 data.csv
```

### `--poolsize COUNT`

Number of parallel workers (processes or threads).

**Default:** CPU count (from `multiprocessing.cpu_count()`)

```bash
# Use 4 workers
pyimport --multi --poolsize 4 data.csv

# Use 8 threads
pyimport --threads --poolsize 8 data.csv
```

### `--forkmethod METHOD`

Method for creating subprocesses.

**Options:** `spawn`, `fork`, `forkserver`

**Default:** `fork`

```bash
# Use spawn (safer for some platforms)
pyimport --multi --forkmethod spawn data.csv
```

## File Splitting Options

### `--splitfile`

Split CSV file into chunks for parallel processing.

**Default:** `False`

```bash
# Split and process with multiprocessing
pyimport --splitfile --multi data.csv
```

### `--autosplit COUNT`

Automatically split file into COUNT chunks based on file size.

**Default:** `2`

```bash
# Split into 10 chunks
pyimport --splitfile --autosplit 10 --multi --poolsize 4 data.csv

# Split into 4 chunks (one per core)
pyimport --splitfile --autosplit 4 --multi data.csv
```

### `--splitsize BYTES`

Split file into chunks of specified size (in bytes).

**Default:** `10240` (10KB)

```bash
# Split into 1MB chunks
pyimport --splitfile --splitsize 1048576 data.csv

# Split into 100KB chunks
pyimport --splitfile --splitsize 102400 data.csv
```

### `--keepsplits`

Keep split files after import completes (don't delete them).

**Default:** `False`

```bash
# Keep split files for debugging
pyimport --splitfile --keepsplits data.csv

# Split files will be named: data.csv.1, data.csv.2, data.csv.3, etc.
```

## Audit Options

### `--audit`

Enable audit tracking (records import metadata to audit collection).

**Default:** `False`

```bash
pyimport --audit data.csv

# Audit records stored in separate collection
pyimport --audit --audithost mongodb://localhost:27017 data.csv
```

Audit records capture:
- Filename and command-line arguments
- Total records written and elapsed time
- Average records per second
- Import mode (sync, async, multi-process, threaded)
- Timestamp of import

### `--audithost URI`

MongoDB URI for storing audit records.

**Default:** `mongodb://localhost:27017`

**Environment variable:** `AUDIT_HOST`

```bash
export AUDIT_HOST="mongodb://localhost:27017"
pyimport --audit data.csv
```

### `--auditdatabase DATABASE`

Database name for audit collection.

**Default:** `PYIMPORT_AUDIT`

```bash
pyimport --audit --auditdatabase my_audit_db data.csv
```

### `--auditcollection COLLECTION`

Collection name for audit records.

**Default:** `audit`

```bash
pyimport --audit --auditcollection import_logs data.csv
```

### `--info STRING`

Add custom info string to audit record for tracking purposes.

```bash
pyimport --audit --info "Daily ETL job - 2024-01-15" data.csv
```

## Restart Options (NEW in v1.10.0)

### `--restart`

Resume an interrupted multi-file import from where it left off.

**Default:** `False`

**Requirements:** Requires `--audit` to be enabled for progress tracking

```bash
# Start import with audit
pyimport --audit --database mydb --collection mycol \
         file1.csv file2.csv file3.csv

# If interrupted, restart from where it stopped
pyimport --restart --database mydb --collection mycol \
         file1.csv file2.csv file3.csv
```

The system automatically:
- Detects the last incomplete batch (unless `--batch-id` is specified)
- Skips files that were already completed
- Resumes processing remaining files
- Continues tracking progress

### `--batch-id ID`

Specify the batch ID to restart (optional).

If not specified, PyImport automatically finds the last incomplete batch.

```bash
# Restart specific batch
pyimport --restart --batch-id abc123 \
         --database mydb --collection mycol \
         file1.csv file2.csv file3.csv
```

### `--checkpoint-interval COUNT`

Number of documents between progress checkpoints during import.

**Default:** `10000`

```bash
# Record progress every 5000 documents
pyimport --audit --checkpoint-interval 5000 data.csv

# Record progress more frequently (every 1000 docs)
pyimport --audit --checkpoint-interval 1000 data.csv
```

Lower values provide more granular restart points but create more audit records.

**Example: Restart Workflow**

```bash
# 1. Start multi-file import with audit
pyimport --audit --multi --database mydb --collection mycol \
         file1.csv file2.csv file3.csv file4.csv file5.csv

# Process gets interrupted after completing file1 and file2...

# 2. Restart - automatically skips completed files
pyimport --restart --multi --database mydb --collection mycol \
         file1.csv file2.csv file3.csv file4.csv file5.csv
# Only processes file3.csv, file4.csv, file5.csv
```

## Collection Management Options

### `--drop`

Drop (delete) target collection before importing.

**Default:** `False`

```bash
# WARNING: This deletes all existing data in the collection
pyimport --drop --database mydb --collection users data.csv
```

## Error Handling Options

### `--onerror ACTION`

Action to take when encountering parse errors.

**Options:**
- `Warn` (default): Log warning and continue
- `Fail`: Stop import on first error

```bash
# Stop on first error
pyimport --onerror Fail data.csv

# Continue on errors (log warnings)
pyimport --onerror Warn data.csv
```

## Logging and Output Options

### `--loglevel LEVEL`

Logging level.

**Options:** `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Default:** `INFO`

```bash
# Verbose logging
pyimport --loglevel DEBUG data.csv

# Minimal logging
pyimport --loglevel ERROR data.csv
```

### `--silent`

Suppress output except for log file.

**Default:** `False`

```bash
# Silent mode
pyimport --silent data.csv
```

### `--verbose`

Enable verbose output (more detailed progress information).

**Default:** `False`

```bash
pyimport --verbose data.csv
```

## Advanced Options

### `--argsource`

Show where command-line arguments are coming from (file, environment, command line).

**Default:** `False`

```bash
pyimport --argsource data.csv
```

### `--input`

Generate output formatted for another program (machine-readable).

**Default:** `False`

```bash
pyimport --input data.csv
```

## Common Usage Patterns

### Fast Import of Large File

```bash
pyimport --multi --splitfile --autosplit 8 --poolsize 4 \
         --batchsize 5000 \
         --database mydb --collection mycol \
         largefile.csv
```

### Safe Import with Audit Trail

```bash
pyimport --audit --writeconcern 1 --journal \
         --database mydb --collection mycol \
         data.csv
```

### Import with Metadata

```bash
pyimport --addfilename --addtimestamp doc --locator \
         --addfield source=ETL --addfield batch_id=123 \
         --database mydb --collection mycol \
         data.csv
```

### Test Import (First 100 Rows)

```bash
pyimport --limit 100 --loglevel DEBUG \
         --database test --collection sample \
         data.csv
```

### Async Parallel Import

```bash
pyimport --asyncpro --multi --splitfile --autosplit 10 \
         --poolsize 4 --batchsize 2000 \
         --database mydb --collection mycol \
         data.csv
```

### Import Multiple Files with Same Schema

```bash
pyimport --fieldfile schema.tff --addfilename \
         --database mydb --collection combined \
         file1.csv file2.csv file3.csv
```

### Clean Import (Replace Existing Data)

```bash
pyimport --drop --database mydb --collection users \
         users.csv
```

## Performance Tips

1. **Use `--multi` for large files** - Multiprocessing provides best throughput
2. **Combine with `--splitfile --autosplit`** - Split work across cores
3. **Increase `--batchsize`** - Larger batches = fewer write operations
4. **Use `--asyncpro` for I/O-bound imports** - Better concurrency
5. **Tune `--poolsize`** - Match your CPU core count
6. **Use `--writeconcern 0` for speed** - Unacknowledged writes are fastest
7. **Specify date formats in field files** - Avoid slow generic date parsing

## See Also

- [Field Files](fieldfiles.md) - Understanding `.tff` format and type conversion
- [Advanced Usage](advanced.md) - Optimization and troubleshooting
- [Quick Start](quickstart.md) - Basic usage examples
