# PyImport Python API Documentation

The PyImport Python API provides a clean, programmatic interface for importing CSV files to MongoDB. This allows third-party developers to integrate PyImport functionality directly into their Python applications without dealing with command-line arguments.

## Table of Contents

1. [Quick Start](#quick-start)
2. [API Classes](#api-classes)
3. [Basic Usage](#basic-usage)
4. [Advanced Usage](#advanced-usage)
5. [Field Files](#field-files)
6. [Parallel Processing](#parallel-processing)
7. [Audit and Restart](#audit-and-restart)
8. [Builder Pattern](#builder-pattern)
9. [API Reference](#api-reference)
10. [Examples](#examples)

## Quick Start

```python
from pyimport.api import PyImportAPI

# Create API instance
api = PyImportAPI(
    mongodb_uri="mongodb://localhost:27017",
    database="mydb",
    collection="mycol"
)

# Import a CSV file
result = api.import_csv("data.csv", has_header=True)
print(f"Imported {result.total_written} records in {result.elapsed_duration}")
```

## API Classes

### PyImportAPI

The main API class providing full access to PyImport functionality.

**Constructor Parameters:**
- `mongodb_uri` (str): MongoDB connection URI (default: "mongodb://localhost:27017")
- `database` (str): Default database name (default: "PYIM")
- `collection` (str): Default collection name (default: "imported")
- `write_concern` (int): Write concern level 0-majority (default: 0)
- `journal` (bool): Enable journaling (default: False)
- `fsync` (bool): Force fsync on writes (default: False)
- `log_level` (str): Logging level (default: "INFO")
- `use_color` (bool): Enable colorized output (default: True)

### PyImportBuilder

Fluent builder interface for chainable import configuration.

```python
from pyimport.api import PyImportBuilder

result = (PyImportBuilder()
    .connect("mongodb://localhost:27017")
    .database("mydb")
    .collection("mycol")
    .csv_file("data.csv")
    .has_header(True)
    .import_data())
```

## Basic Usage

### Simple Import

```python
from pyimport.api import PyImportAPI

api = PyImportAPI()
result = api.import_csv(
    "data.csv",
    database="testdb",
    collection="testcol",
    has_header=True
)

print(f"Records imported: {result.total_written}")
print(f"Time taken: {result.elapsed_duration}")
print(f"Rate: {result.avg_records_per_sec:.0f} docs/sec")
```

### Import Multiple Files

```python
api = PyImportAPI()
result = api.import_csv(
    ["file1.csv", "file2.csv", "file3.csv"],
    database="mydb",
    collection="combined"
)

for file_result in result.results:
    print(f"{file_result.filename}: {file_result.total_written} records")
```

### Custom Delimiter

```python
api = PyImportAPI()
result = api.import_csv(
    "data.tsv",
    delimiter="\t",  # Tab-separated
    has_header=True
)
```

### Drop Collection Before Import

```python
api = PyImportAPI()
result = api.import_csv(
    "fresh_data.csv",
    drop_collection=True,  # Clear existing data first
    has_header=True
)
```

## Advanced Usage

### Document Enrichment

Add metadata to each imported document:

```python
api = PyImportAPI()
result = api.import_csv(
    "data.csv",
    add_filename=True,      # Add source filename
    add_timestamp=True,     # Add import timestamp
    add_field=["source:web", "version:1.0"],  # Custom fields
    has_header=True
)
```

### Exclude Columns

```python
api = PyImportAPI()
result = api.import_csv(
    "data.csv",
    cut=[0, 3, 5],  # Skip columns at indices 0, 3, and 5
    has_header=True
)
```

### Custom MongoDB _id

```python
api = PyImportAPI()
result = api.import_csv(
    "data.csv",
    id_field="user_id",  # Use user_id column as _id
    has_header=True
)
```

### Batch Size Control

```python
api = PyImportAPI()
result = api.import_csv(
    "large_file.csv",
    batch_size=1000,  # Insert 1000 docs at a time
    has_header=True
)
```

## Field Files

Field files (.tff) define field types and formats for CSV columns.

### Generate Field File

```python
api = PyImportAPI()

# Auto-generate from CSV
field_file = api.generate_field_file("data.csv")
print(f"Generated {len(field_file.fields())} fields")

# Use field file for import
result = api.import_csv("data.csv", field_file="data.tff")
```

### Load Existing Field File

```python
api = PyImportAPI()
ff = api.load_field_file("data.tff")

# Inspect field types
for field in ff.fields():
    print(f"{field}: {ff.type_value(field)}")

# Use for import
result = api.import_csv("data.csv", field_file=ff)
```

### Custom Field File

```python
# Create data.tff manually:
# [user_id]
# type = "int"
# name = "user_id"
# format = ""
#
# [signup_date]
# type = "datetime"
# name = "signup_date"
# format = "%Y-%m-%d"

api = PyImportAPI()
result = api.import_csv("users.csv", field_file="data.tff")
```

## Parallel Processing

PyImport supports three parallel processing modes for large datasets.

### Multi-Process Mode

```python
api = PyImportAPI()
result = api.import_csv(
    "huge_file.csv",
    parallel_mode="multi",
    pool_size=4,  # Use 4 processes
    has_header=True
)
```

### Thread Mode

```python
api = PyImportAPI()
result = api.import_csv(
    "large_file.csv",
    parallel_mode="threads",
    pool_size=8,  # Use 8 threads
    has_header=True
)
```

### Async Mode

```python
api = PyImportAPI()
result = api.import_csv(
    "data.csv",
    parallel_mode="async",
    has_header=True
)
```

## Audit and Restart

Track import progress and restart incomplete imports.

### Enable Audit Tracking

```python
api = PyImportAPI()
result = api.import_csv(
    "data.csv",
    audit_host="mongodb://localhost:27017",
    has_header=True
)
```

### Restart Incomplete Import

```python
api = PyImportAPI()

# Restart last incomplete import
result = api.restart_import(
    audit_host="mongodb://localhost:27017",
    filename="data.csv",
    has_header=True
)

# Or restart specific batch
result = api.restart_import(
    batch_id="20231101_123456",
    audit_host="mongodb://localhost:27017",
    filename="data.csv"
)
```

### Check Audit Status

```python
api = PyImportAPI()

# Check for incomplete batches
status = api.get_audit_status("mongodb://localhost:27017")
if status['has_incomplete']:
    batch = status['last_incomplete_batch']
    print(f"Incomplete batch: {batch['batchID']}")

# Check specific batch
status = api.get_audit_status(
    "mongodb://localhost:27017",
    batch_id="20231101_123456"
)
print(f"Completed files: {status['completed_files']}")
```

## Builder Pattern

Use the fluent builder API for more readable configuration:

```python
from pyimport.api import PyImportBuilder

result = (PyImportBuilder()
    .connect("mongodb://localhost:27017")
    .database("analytics")
    .collection("events")
    .csv_file("events.csv")
    .delimiter(",")
    .has_header(True)
    .batch_size(1000)
    .add_timestamp()
    .add_field("environment", "production")
    .parallel("multi", workers=4)
    .write_concern(1)
    .import_data())

print(f"Imported {result.total_written} records")
```

### Builder with Multiple Files

```python
result = (PyImportBuilder()
    .connect("mongodb://localhost:27017")
    .database("mydb")
    .collection("logs")
    .csv_file("log1.csv")
    .csv_file("log2.csv")
    .csv_file("log3.csv")
    .has_header(True)
    .drop_first()  # Clear collection first
    .import_data())
```

### Builder with Audit

```python
result = (PyImportBuilder()
    .connect("mongodb://localhost:27017")
    .database("mydb")
    .collection("data")
    .csv_file("large_file.csv")
    .audit("mongodb://localhost:27017")
    .parallel("multi", workers=8)
    .import_data())
```

## API Reference

### PyImportAPI Methods

#### `import_csv()`

Import CSV file(s) to MongoDB.

**Parameters:**
- `filename` (str | List[str]): CSV file path(s)
- `database` (str, optional): Target database
- `collection` (str, optional): Target collection
- `delimiter` (str): Field delimiter (default: ",")
- `has_header` (bool): CSV has header row (default: False)
- `field_file` (str | FieldFile, optional): Field type definitions
- `batch_size` (int): Documents per batch (default: 500)
- `add_filename` (bool): Add source filename (default: False)
- `add_timestamp` (bool): Add import timestamp (default: False)
- `add_field` (List[str], optional): Custom "key:value" fields
- `id_field` (str, optional): Field to use as _id
- `noenrich` (bool): Skip enrichment (default: False)
- `cut` (List[int], optional): Column indices to exclude
- `parallel_mode` (str, optional): "multi", "threads", or "async"
- `pool_size` (int): Parallel workers (default: 8)
- `audit_host` (str, optional): Audit database URI
- `drop_collection` (bool): Drop before import (default: False)

**Returns:** `ImportResults` object

#### `generate_field_file()`

Generate field file from CSV structure.

**Parameters:**
- `csv_filename` (str): Source CSV file
- `output_filename` (str, optional): Output path (auto-generated if None)
- `delimiter` (str): CSV delimiter (default: ",")
- `has_header` (bool): CSV has header (default: True)
- `extension` (str): Output extension (default: ".tff")

**Returns:** `FieldFile` object

#### `load_field_file()`

Load existing field file.

**Parameters:**
- `filename` (str): Field file path

**Returns:** `FieldFile` object

#### `drop_collection()`

Drop a MongoDB collection.

**Parameters:**
- `database` (str, optional): Database name
- `collection` (str, optional): Collection name

**Returns:** None

#### `restart_import()`

Restart incomplete import.

**Parameters:**
- `batch_id` (str, optional): Batch to restart (auto-detects if None)
- `audit_host` (str): Audit database URI
- `**import_kwargs`: Additional arguments for import_csv()

**Returns:** `ImportResults` object

#### `get_audit_status()`

Check audit status.

**Parameters:**
- `audit_host` (str): Audit database URI
- `batch_id` (str, optional): Specific batch ID

**Returns:** Dictionary with audit status

### ImportResults Object

**Properties:**
- `total_written` (int): Total documents imported
- `total_results` (int): Number of successful files
- `total_errors` (int): Number of failed files
- `elapsed_time` (float): Total seconds
- `duration` (str): Formatted duration string
- `avg_records_per_sec` (float): Average import rate
- `results` (List[ImportResult]): Individual file results
- `errors` (List[ImportResult]): Failed imports
- `filenames` (List[str]): Successfully imported files

### ImportResult Object

**Properties:**
- `filename` (str): Source file
- `total_written` (int): Documents imported
- `elapsed_time` (float): Import duration in seconds
- `elapsed_duration` (str): Formatted duration
- `avg_records_per_sec` (float): Import rate
- `timestamp` (datetime): Import timestamp
- `error` (Exception): Error if import failed

## Examples

### Example 1: Simple Data Pipeline

```python
from pyimport.api import PyImportAPI

def import_daily_data(date_str):
    api = PyImportAPI(
        mongodb_uri="mongodb://localhost:27017",
        database="analytics"
    )

    # Generate field file if needed
    csv_file = f"data_{date_str}.csv"
    if not Path(f"{csv_file}.tff").exists():
        api.generate_field_file(csv_file)

    # Import with enrichment
    result = api.import_csv(
        csv_file,
        collection=f"data_{date_str}",
        field_file=f"{csv_file}.tff",
        add_timestamp=True,
        add_field=[f"import_date:{date_str}"],
        has_header=True
    )

    return result.total_written

# Use it
records = import_daily_data("2023-11-01")
print(f"Imported {records} records")
```

### Example 2: Batch Processing

```python
from pyimport.api import PyImportAPI
from pathlib import Path

def import_directory(directory, pattern="*.csv"):
    api = PyImportAPI(database="batch_import")

    files = list(Path(directory).glob(pattern))
    print(f"Found {len(files)} files")

    # Import all files in parallel
    result = api.import_csv(
        [str(f) for f in files],
        collection="combined_data",
        parallel_mode="multi",
        pool_size=4,
        add_filename=True,
        add_timestamp=True
    )

    # Report results
    print(f"\nImport Summary:")
    print(f"Total records: {result.total_written}")
    print(f"Time taken: {result.duration}")
    print(f"Rate: {result.avg_records_per_sec:.0f} docs/sec")

    for file_result in result.results:
        print(f"  {file_result.filename}: {file_result.total_written} records")

    return result

# Use it
import_directory("./data", "*.csv")
```

### Example 3: Resilient Import with Audit

```python
from pyimport.api import PyImportAPI
import sys

def resilient_import(csv_file, audit_uri):
    api = PyImportAPI(database="production")

    try:
        # Attempt import with audit
        result = api.import_csv(
            csv_file,
            collection="data",
            audit_host=audit_uri,
            parallel_mode="multi",
            pool_size=8,
            has_header=True
        )

        print(f"Successfully imported {result.total_written} records")
        return result

    except KeyboardInterrupt:
        print("\nImport interrupted. Progress has been saved.")
        print("Run with restart=True to continue.")

        # Check what was completed
        status = api.get_audit_status(audit_uri)
        if status['has_incomplete']:
            batch = status['last_incomplete_batch']
            print(f"Batch ID: {batch['batchID']}")

        sys.exit(1)

def continue_import(csv_file, audit_uri):
    api = PyImportAPI(database="production")

    # Resume from last checkpoint
    result = api.restart_import(
        audit_host=audit_uri,
        filename=csv_file,
        collection="data",
        parallel_mode="multi",
        pool_size=8,
        has_header=True
    )

    print(f"Resumed import: {result.total_written} total records")
    return result

# Use it
audit_uri = "mongodb://localhost:27017"
resilient_import("huge_file.csv", audit_uri)

# If interrupted, restart with:
# continue_import("huge_file.csv", audit_uri)
```

### Example 4: Using the Builder Pattern

```python
from pyimport.api import PyImportBuilder

def advanced_import():
    try:
        result = (PyImportBuilder()
            .connect("mongodb://localhost:27017")
            .database("analytics")
            .collection("user_events")
            .csv_file("events.csv")
            .field_file("events.tff")
            .delimiter(",")
            .has_header(True)
            .batch_size(2000)
            .add_timestamp()
            .add_filename()
            .add_field("processed_by", "pipeline_v2")
            .id_field("event_id")
            .parallel("multi", workers=6)
            .write_concern(1)
            .journal(True)
            .audit("mongodb://localhost:27017")
            .log_level("DEBUG")
            .import_data())

        print(f"Success! Imported {result.total_written} events")
        print(f"Rate: {result.avg_records_per_sec:.0f} events/sec")
        return result

    except Exception as e:
        print(f"Import failed: {e}")
        raise

advanced_import()
```

### Example 5: Error Handling

```python
from pyimport.api import PyImportAPI, FieldFileException
import sys

def safe_import(csv_file):
    api = PyImportAPI(database="mydb", log_level="ERROR")

    try:
        # Try to generate field file
        try:
            field_file = api.generate_field_file(csv_file)
            print(f"Generated field file with {len(field_file.fields())} fields")
        except Exception as e:
            print(f"Warning: Could not generate field file: {e}")
            field_file = None

        # Import with error handling
        result = api.import_csv(
            csv_file,
            collection="data",
            field_file=field_file,
            has_header=True
        )

        if result.total_errors > 0:
            print(f"Completed with {result.total_errors} errors:")
            for error in result.errors:
                print(f"  {error.filename}: {error.error}")

        print(f"Successfully imported {result.total_written} records")
        return result

    except OSError as e:
        print(f"File error: {e}")
        sys.exit(1)
    except FieldFileException as e:
        print(f"Field file error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

safe_import("data.csv")
```

## Best Practices

1. **Always specify `has_header=True`** if your CSV has a header row
2. **Use field files** for reliable type conversion on large datasets
3. **Enable audit tracking** for long-running imports that might be interrupted
4. **Use parallel mode** for large files (>100k rows)
5. **Set appropriate batch_size** based on document size (smaller batches for large documents)
6. **Add timestamps and filenames** for data lineage tracking
7. **Use write_concern=1** for production imports to ensure data safety
8. **Handle errors gracefully** with try/except blocks
9. **Test with small datasets** before running production imports
10. **Use the builder pattern** for complex configurations to improve code readability

## See Also

- [CLI Documentation](CLI.md)
- [Field File Format](FIELDFILE.md)
- [PyImport README](../README.md)
