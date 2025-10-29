# Advanced Usage

Advanced features, optimization techniques, and troubleshooting for PyImport.

## Parallel Processing Strategies

PyImport offers four execution strategies, each optimized for different scenarios.

### Synchronous (Default)

Single-threaded, straightforward processing.

```bash
pyimport data.csv
```

**When to use:**
- Small files (< 100MB)
- Simple imports
- Debugging issues
- Guaranteed order preservation

**Performance:** ~24,000-32,000 docs/sec

### Async I/O

Event-loop based async processing using Motor driver.

```bash
pyimport --asyncpro data.csv
```

**When to use:**
- I/O-bound operations
- Network-limited imports
- MongoDB Atlas (cloud) imports
- Combined with threading

**Performance:** ~30,000-40,000 docs/sec

**Best practice:** Combine with file splitting:
```bash
pyimport --asyncpro --splitfile --autosplit 8 data.csv
```

### Multi-Process

True parallel processing using multiple CPU cores.

```bash
pyimport --multi --splitfile --autosplit 8 --poolsize 4 data.csv
```

**When to use:**
- Large files (> 500MB)
- CPU-bound type conversion
- Maximum throughput needed
- Multiple CPU cores available

**Performance:** ~50,000+ docs/sec

**Requirements:**
- Must use with `--splitfile`
- Works best with `--autosplit`

### Threaded

Thread-based parallel processing.

```bash
pyimport --threads --splitfile --autosplit 8 --poolsize 8 data.csv
```

**When to use:**
- GIL-limited operations less critical
- Combined with async I/O
- Simpler than multiprocessing

**Performance:** ~29,000 docs/sec

**Best combination:**
```bash
pyimport --asyncpro --threads --splitfile --autosplit 8 --poolsize 8 data.csv
```

## File Splitting Strategies

### Auto-Split by Count

Split file into N chunks:

```bash
pyimport --splitfile --autosplit 10 --multi --poolsize 4 data.csv
```

Creates 10 equal-sized chunks: `data.csv.1` through `data.csv.10`

**Best practice:** Set `autosplit` to 2-3x `poolsize`:
```bash
# 4 workers, 12 chunks
pyimport --splitfile --autosplit 12 --multi --poolsize 4 data.csv
```

This allows load balancing if some chunks finish faster.

### Auto-Split by Size

Split into chunks of specific byte size:

```bash
# 10MB chunks
pyimport --splitfile --splitsize 10485760 --multi data.csv

# 1MB chunks
pyimport --splitfile --splitsize 1048576 --multi data.csv
```

**When to use:** Variable row sizes, unknown file size.

### Keeping Split Files

Keep split files for inspection or reuse:

```bash
pyimport --splitfile --keepsplits --autosplit 8 --multi data.csv

# Files remain: data.csv.1, data.csv.2, ..., data.csv.8
```

**Use cases:**
- Debugging split logic
- Reusing splits for multiple imports
- Manual processing of chunks

## Audit Tracking

### Enabling Audit Trail

Track import metadata for monitoring and debugging:

```bash
pyimport --audit --audithost mongodb://localhost:27017 data.csv
```

Audit records stored in `PYIMPORT_AUDIT.audit` collection:
```json
{
  "_id": ObjectId("..."),
  "command": "process one file",
  "version": "1.10.0",
  "filename": "data.csv",
  "elapsed_time": 8.3,
  "total_written": 200000,
  "avg_records_per_sec": 24096,
  "mode": {"single": true},
  "cmd_line": "pyimport --audit data.csv",
  "timestamp": ISODate("2025-10-12T13:45:00Z")
}
```

### What Audit Records Capture

- **Filename** and full command-line arguments
- **Total records** written and elapsed time
- **Average throughput** (records per second)
- **Import mode** (sync, async, multi-process, threaded)
- **Version** of PyImport used
- **Timestamp** of import completion

### Custom Audit Information

Add context to audit records for better tracking:

```bash
pyimport --audit --info "Daily ETL - Production - 2024-01-15" data.csv
```

The `--info` string is added to the audit record for your reference.

### Separate Audit Database

Keep audit logs separate from production data:

```bash
pyimport --audit \
         --audithost mongodb://audit-server:27017 \
         --auditdatabase audit_logs \
         --auditcollection import_tracking \
         data.csv
```

### Querying Audit Records

Find import history:

```javascript
// Recent imports
use PYIMPORT_AUDIT
db.audit.find().sort({timestamp: -1}).limit(10)

// Imports for specific file
db.audit.find({filename: "data.csv"})

// Failed imports (check for errors)
db.audit.find({"elapsed_time": {$exists: true}}).sort({avg_records_per_sec: 1})

// Performance analysis
db.audit.aggregate([
  {$group: {
    _id: "$mode",
    avg_throughput: {$avg: "$avg_records_per_sec"},
    count: {$sum: 1}
  }}
])
```

### Restart Capability (NEW in v1.10.0)

**Status:** ✅ Fully Implemented

PyImport now supports resuming interrupted multi-file imports from where they left off. The restart feature enables:

#### Progress Document Structure

```json
{
  "batch_id": 12345,
  "progress": {
    "filename": "data.csv.1",
    "docs_written": 125000,
    "last_line_number": 125000,
    "file_position": 5242880,
    "status": "in_progress"  // or "completed"
  },
  "timestamp": ISODate("2025-10-12T14:30:00Z")
}
```

#### Progress Tracking Features

The audit system now provides methods for:

- **`record_progress()`** - Record checkpoint every N documents (e.g., 10,000)
- **`get_file_progress()`** - Get latest checkpoint for a specific file
- **`get_completed_files()`** - Query which files have finished
- **`get_incomplete_files()`** - Find files that need resuming
- **`mark_file_completed()`** - Mark a file as done
- **`get_last_incomplete_batch()`** - Auto-detect incomplete imports

#### Restart Capability

PyImport now supports restarting interrupted imports. Here's how it works:

```bash
# Start an import with audit enabled
pyimport --audit --multi --splitfile --autosplit 8 \
         --database mydb --collection mycol largefile.csv
# ... import gets interrupted ...

# Resume from where it left off
pyimport --restart --batch-id <batch_id> \
         --database mydb --collection mycol largefile.csv

# Or auto-detect the last incomplete batch
pyimport --restart \
         --database mydb --collection mycol largefile.csv
```

The system:
1. Skips completed split files
2. Resumes incomplete files from last checkpoint
3. Continues tracking progress
4. Completes the import seamlessly

**Example with multiple files:**

```bash
# Start importing 5 files
pyimport --audit --database mydb --collection mycol \
         file1.csv file2.csv file3.csv file4.csv file5.csv

# Process is interrupted after completing file1 and file2...

# Restart - automatically skips completed files
pyimport --restart --database mydb --collection mycol \
         file1.csv file2.csv file3.csv file4.csv file5.csv
# Only processes file3.csv, file4.csv, file5.csv
```

**Key Features:**
- **Auto-detection**: If `--batch-id` is not specified, PyImport automatically finds the last incomplete batch
- **Progress checkpoints**: Records progress every 10,000 documents (configurable with `--checkpoint-interval`)
- **Works with all import modes**: Supports sync, async, multi-process, and threaded imports
- **File-level granularity**: Skips entire files that were completed, restarts incomplete ones

**Requirements:**
- Restart requires `--audit` to be enabled for progress tracking
- Pass the same file list on restart to identify which files were completed

#### Restart Granularity Options

Current implementation supports:

1. **File-level** (currently implemented): Skip completed files, restart incomplete ones
   - Simple and reliable
   - Best for multi-file imports with split files
   - No duplicate data

2. **Checkpoint-level** (future enhancement): Resume from last 10K-doc checkpoint
   - Would allow resuming within a single large file
   - Requires tracking file position/line number

3. **Idempotent** (future enhancement): Use deterministic IDs with upserts
   - Most reliable for network interruptions
   - Handles duplicate data gracefully

## Performance Optimization

### Benchmark: 200K Row Import

Test file: NYC taxi data (200,000 rows, ~30MB)

| Configuration | Time | Docs/sec | Command |
|--------------|------|----------|---------|
| Sync default | 8.3s | 24,000 | `pyimport data.csv` |
| Async | 6.6s | 30,000 | `pyimport --asyncpro data.csv` |
| Multi (4 cores) | 4.0s | 50,000 | `pyimport --multi --splitfile --autosplit 10 --poolsize 4 data.csv` |
| Threading | 6.8s | 29,000 | `pyimport --threads --splitfile --autosplit 8 --poolsize 8 data.csv` |

### Optimal Settings by File Size

**Small files (< 50MB):**
```bash
pyimport --batchsize 2000 data.csv
```

**Medium files (50MB - 500MB):**
```bash
pyimport --asyncpro --batchsize 5000 data.csv
```

**Large files (> 500MB):**
```bash
pyimport --multi --splitfile --autosplit 12 --poolsize 4 \
         --batchsize 10000 --writeconcern 0 \
         data.csv
```

**Huge files (> 5GB):**
```bash
pyimport --multi --splitfile --autosplit 20 --poolsize 8 \
         --batchsize 10000 --writeconcern 0 \
         --forkmethod spawn \
         data.csv
```

### Tuning Batch Size

Batch size affects memory usage and write frequency:

```bash
# Small batches (safer, more frequent writes)
pyimport --batchsize 500 data.csv

# Medium batches (balanced)
pyimport --batchsize 2000 data.csv

# Large batches (faster, more memory)
pyimport --batchsize 10000 data.csv
```

**Rule of thumb:**
- **100-1000**: Small files or low memory
- **1000-5000**: General purpose (default: 1000)
- **5000-10000**: Large files, ample memory
- **> 10000**: Risk of OOM errors

### Write Concern Trade-offs

Control MongoDB write acknowledgment:

```bash
# Fastest (no acknowledgment)
pyimport --writeconcern 0 data.csv

# Balanced (acknowledged)
pyimport --writeconcern 1 data.csv

# Safest (replicated)
pyimport --writeconcern majority --journal data.csv
```

**Performance impact:**
- `writeconcern 0`: Fastest, risk of data loss
- `writeconcern 1`: ~10-20% slower, safe for standalone
- `writeconcern majority`: ~30-50% slower, safe for replica sets

### Date Parsing Performance

Date parsing can be a bottleneck. Optimize with these techniques:

**1. Use ISO dates when possible:**
```toml
[date]
type = "isodate"  # 100x faster than generic parsing
```

**2. Always specify format:**
```toml
[date]
type = "date"
format = "%Y-%m-%d"  # Much faster than format-less parsing
```

**3. Avoid generic date parsing:**
```toml
# Slow (uses dateutil.parser)
[date]
type = "date"

# Fast (uses strptime)
[date]
type = "date"
format = "%m/%d/%Y"
```

**Performance comparison:**
- `isodate`: ~100 ns/date
- `date` with format: ~10 μs/date
- `date` without format: ~1 ms/date (100x slower!)

## Advanced Field File Techniques

### Multiple Date Formats in One File

Different columns can have different date formats:

```toml
[birth_date]
type = "date"
format = "%m/%d/%Y"  # US format

[hire_date]
type = "isodate"  # ISO format

[last_login]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"

[created_timestamp]
type = "timestamp"  # Unix timestamp
```

### Reusable Field Files

Create template field files for common schemas:

```bash
# templates/person.tff
[name]
type = "str"

[age]
type = "int"

[email]
type = "str"

# Use with multiple files
pyimport --fieldfile templates/person.tff users.csv
pyimport --fieldfile templates/person.tff employees.csv
pyimport --fieldfile templates/person.tff customers.csv
```

## Working with Large Files

### Memory-Efficient Imports

For files that don't fit in memory:

```bash
# Stream processing with controlled batch size
pyimport --batchsize 1000 \
         --multi --splitfile --autosplit 20 \
         --poolsize 4 \
         huge_file.csv
```

Split files are processed independently, keeping memory usage constant.

### Monitoring Progress

Use verbose mode to track progress:

```bash
pyimport --verbose --loglevel INFO \
         --multi --splitfile --autosplit 10 \
         large_file.csv
```

Output:
```
INFO: Processing: large_file.csv.1
INFO: Imported 50000 rows (50000 total)
INFO: Processing: large_file.csv.2
INFO: Imported 50000 rows (100000 total)
...
```

### Estimating Import Time

Quick formula: `time = rows / throughput`

Examples:
```
1M rows @ 30K docs/sec = ~33 seconds
10M rows @ 50K docs/sec = ~200 seconds = ~3.3 minutes
100M rows @ 50K docs/sec = ~2000 seconds = ~33 minutes
```

Add 20-30% overhead for:
- Type conversion
- Date parsing
- Network latency
- MongoDB indexing

## MongoDB Optimization

### Pre-Import

Before large imports:

```javascript
// Disable indexes temporarily
use mydb
db.mycollection.dropIndexes()

// Import
// ... pyimport runs ...

// Rebuild indexes
db.mycollection.createIndex({field1: 1})
db.mycollection.createIndex({field2: 1, field3: 1})
```

**Why:** Building indexes during import is slower than bulk building after.

### Post-Import

After import, optimize for queries:

```javascript
// Analyze collection
db.mycollection.stats()

// Add indexes based on query patterns
db.mycollection.createIndex({date: -1})
db.mycollection.createIndex({user_id: 1, timestamp: -1})

// Compound indexes for common queries
db.mycollection.createIndex({category: 1, price: 1, date: -1})
```

### Server Configuration

For maximum import performance:

```yaml
# mongod.conf
storage:
  journal:
    enabled: false  # Disable during bulk import
  wiredTiger:
    engineConfig:
      cacheSizeGB: 4  # Increase cache

replication:
  replSetName: null  # Import to standalone, then sync
```

**Warning:** Disabling journaling risks data loss. Only do this for non-critical bulk loads.

## Troubleshooting

### Out of Memory Errors

**Symptoms:** Process killed, "MemoryError" exceptions

**Solutions:**
1. Reduce batch size: `--batchsize 500`
2. Increase split count: `--autosplit 20`
3. Use multiprocessing (isolates memory): `--multi`

### Slow Date Parsing

**Symptoms:** Import much slower than expected

**Solutions:**
1. Check field file for date formats:
   ```bash
   cat data.tff
   ```
2. Add format specifications:
   ```toml
   [date]
   type = "date"
   format = "%Y-%m-%d"  # Add this!
   ```
3. Use `isodate` type when possible

### Connection Timeouts

**Symptoms:** "Connection refused", timeout errors

**Solutions:**
1. Check MongoDB is running:
   ```bash
   mongosh --eval "db.version()"
   ```
2. Test connection:
   ```bash
   pyimport --mdburi mongodb://localhost:27017 --loglevel DEBUG
   ```
3. Increase MongoDB connection timeout:
   ```bash
   pyimport --mdburi "mongodb://localhost:27017/?serverSelectionTimeoutMS=30000"
   ```

### Split File Issues

**Symptoms:** Split files not being cleaned up, "file not found" errors

**Solutions:**
1. Check permissions:
   ```bash
   ls -la data.csv*
   ```
2. Manual cleanup:
   ```bash
   rm data.csv.[0-9]*
   ```
3. Use `--keepsplits` to inspect splits:
   ```bash
   pyimport --splitfile --keepsplits --autosplit 4 data.csv
   head -n 10 data.csv.1  # Inspect first split
   ```

### Type Conversion Failures

**Symptoms:** Fields stored as strings instead of intended types

**Debug approach:**
```bash
# 1. Import with debug logging
pyimport --loglevel DEBUG --limit 100 data.csv 2>&1 | grep -i "error\|warn"

# 2. Check field file for type definitions
cat data.tff

# 3. Inspect specific rows
pyimport --limit 10 --verbose data.csv

# 4. Use strict mode to stop on errors
pyimport --onerror Fail --limit 100 data.csv
```

## Best Practices

### 1. Always Generate Field Files First

```bash
# DON'T: Import without verifying field file
pyimport data.csv

# DO: Generate and inspect field file first
pyimport --genfieldfile data.csv
cat data.tff  # Review types
pyimport --limit 100 data.csv  # Test with sample
pyimport data.csv  # Full import
```

### 2. Test with Limits

```bash
# Test import with first 100 rows
pyimport --limit 100 --database test --collection sample data.csv

# Verify results
mongosh test --eval "db.sample.find().limit(5).pretty()"

# If good, run full import
pyimport --database prod --collection data data.csv
```

### 3. Use Audit for Production Imports

```bash
# Always use audit for critical imports
pyimport --audit --writeconcern 1 --journal \
         --info "Production import - $(date)" \
         data.csv
```

### 4. Optimize by File Size

Choose strategy based on file size:

```bash
# < 50MB: Simple sync
pyimport data.csv

# 50MB - 500MB: Async
pyimport --asyncpro --batchsize 5000 data.csv

# > 500MB: Multi-process
pyimport --multi --splitfile --autosplit 10 --poolsize 4 data.csv
```

### 5. Monitor and Measure

Time your imports and adjust:

```bash
# Measure baseline
time pyimport data.csv

# Test optimizations
time pyimport --asyncpro data.csv
time pyimport --multi --splitfile --autosplit 8 --poolsize 4 data.csv

# Use best configuration
```

## Advanced Examples

### High-Performance Production Import

```bash
#!/bin/bash
# production_import.sh

# Configuration
DB="production"
COL="transactions"
FILE="transactions.csv"
AUDIT_URI="mongodb://audit-server:27017"

# Generate field file if missing
if [ ! -f "${FILE%.csv}.tff" ]; then
    pyimport --genfieldfile "$FILE"
fi

# Import with full optimization
pyimport \
    --database "$DB" \
    --collection "$COL" \
    --multi \
    --splitfile \
    --autosplit 12 \
    --poolsize 4 \
    --batchsize 10000 \
    --asyncpro \
    --writeconcern 1 \
    --journal \
    --audit \
    --audithost "$AUDIT_URI" \
    --addfilename \
    --addtimestamp batch \
    --addfield import_date="$(date +%Y-%m-%d)" \
    --info "Production import - $(date)" \
    --loglevel INFO \
    "$FILE"
```

### Incremental Daily Import

```bash
#!/bin/bash
# daily_import.sh

DATE=$(date +%Y-%m-%d)
FILE="data_${DATE}.csv"

# Download today's data
curl -o "$FILE" "https://api.example.com/data?date=$DATE"

# Generate field file (first time only)
[ ! -f "schema.tff" ] && pyimport --genfieldfile "$FILE" && mv "${FILE%.csv}.tff" schema.tff

# Import with metadata
pyimport \
    --fieldfile schema.tff \
    --database analytics \
    --collection daily_data \
    --multi --splitfile --autosplit 8 --poolsize 4 \
    --addfilename \
    --addfield import_date="$DATE" \
    --audit \
    --info "Daily import for $DATE" \
    "$FILE"

# Cleanup
rm "$FILE"
```

## See Also

- [Quick Start](quickstart.md) - Basic usage
- [Command-Line Reference](cli_reference.md) - All options
- [Field Files](fieldfiles.md) - Type conversion details
