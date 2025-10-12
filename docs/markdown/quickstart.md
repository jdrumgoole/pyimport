# Quick Start Guide

Get started with PyImport in minutes.

## Your First Import

### Step 1: Create a CSV File

Create a simple test file:

```bash
cat > people.csv <<EOF
name,age,city,salary,join_date
Alice,30,New York,75000,2020-01-15
Bob,25,Los Angeles,65000,2021-03-22
Charlie,35,Chicago,85000,2019-07-10
Diana,28,Houston,70000,2022-05-18
EOF
```

### Step 2: Generate Field File

Let PyImport analyze your CSV and create a field file:

```bash
pyimport --genfieldfile people.csv
```

This creates `people.tff` with inferred types:

```toml
[field.name]
type = "str"

[field.age]
type = "int"

[field.city]
type = "str"

[field.salary]
type = "int"

[field.join_date]
type = "date"
format = "%Y-%m-%d"
```

### Step 3: Import to MongoDB

```bash
pyimport --database mydb --collection employees people.csv
```

That's it! Your data is now in MongoDB.

### Step 4: Verify Import

Check your data using mongosh or your preferred MongoDB client:

```bash
mongosh mydb --eval "db.employees.find().pretty()"
```

Output:
```json
{
  "_id": ObjectId("..."),
  "name": "Alice",
  "age": 30,
  "city": "New York",
  "salary": 75000,
  "join_date": ISODate("2020-01-15T00:00:00Z")
}
...
```

## Common Scenarios

### Importing Pipe-Delimited Files

```bash
# Your data
cat > data.txt <<EOF
name|age|department
Alice|30|Engineering
Bob|25|Sales
EOF

# Generate field file with correct delimiter
pyimport --genfieldfile --delimiter "|" data.txt

# Import
pyimport --delimiter "|" --database mydb --collection staff data.txt
```

### Importing Tab-Delimited Files

```bash
pyimport --delimiter tab --database mydb --collection data data.tsv
```

### Importing Files Without Headers

If your CSV doesn't have a header row, create the field file manually:

```bash
# Your data (no header)
cat > data.csv <<EOF
Alice,30,NYC
Bob,25,LA
EOF

# Create field file manually
cat > data.tff <<EOF
[field.name]
type = "str"

[field.age]
type = "int"

[field.city]
type = "str"
EOF

# Import (don't use --hasheader)
pyimport --database mydb --collection people data.csv
```

### Adding Metadata to Imports

Track where your data came from:

```bash
pyimport --addfilename --addtimestamp doc \
         --addfield source=ETL --addfield batch=morning \
         --database mydb --collection data data.csv
```

Result:
```json
{
  "name": "Alice",
  "age": 30,
  "filename": "data.csv",
  "timestamp": ISODate("2024-01-15T10:30:00Z"),
  "source": "ETL",
  "batch": "morning"
}
```

### Importing Large Files Fast

For files with millions of rows, use parallel processing:

```bash
pyimport --multi --splitfile --autosplit 8 --poolsize 4 \
         --batchsize 5000 \
         --database mydb --collection bigdata \
         large_file.csv
```

This will:
1. Split the file into 8 chunks
2. Process with 4 parallel workers
3. Insert in batches of 5000 documents
4. Clean up split files automatically

### Importing Multiple Files

Import several files with the same schema:

```bash
# All files share the same field file
pyimport --fieldfile schema.tff \
         --database mydb --collection combined \
         jan.csv feb.csv mar.csv
```

Or use a file list:

```bash
# Create file list
ls *.csv > files.txt

# Import all
pyimport --filelist files.txt --database mydb --collection all_data
```

### Importing from URLs

PyImport can fetch CSV files from URLs:

```bash
pyimport --database mydb --collection web_data \
         https://example.com/data.csv
```

### Testing Before Full Import

Import just the first 100 rows to verify everything works:

```bash
pyimport --limit 100 --loglevel DEBUG \
         --database test --collection sample \
         data.csv
```

### Replacing Existing Data

Drop and recreate the collection:

```bash
pyimport --drop --database mydb --collection users users.csv
```

**Warning:** This deletes all existing data in the collection!

## Working with Dates

### ISO Dates (Fast)

If your dates are in ISO format (YYYY-MM-DD), PyImport will detect them automatically:

```csv
name,join_date
Alice,2020-01-15
Bob,2021-03-22
```

Field file:
```toml
[field.join_date]
type = "isodate"
```

This is **100x faster** than generic date parsing!

### Custom Date Formats

For non-ISO dates, PyImport can infer the format or you can specify it:

```csv
name,join_date
Alice,01/15/2020
Bob,03/22/2021
```

Auto-detected field file:
```toml
[field.join_date]
type = "date"
format = "%m/%d/%Y"
```

### Timestamps

Unix timestamps:

```csv
name,event_time
Alice,1678901234
Bob,1678902345
```

Field file:
```toml
[field.event_time]
type = "timestamp"
```

## Handling Errors

### Skip Bad Rows

By default, PyImport warns about errors but continues:

```bash
pyimport --onerror Warn data.csv
```

### Stop on First Error

For strict validation:

```bash
pyimport --onerror Fail data.csv
```

### Debug Import Issues

Use verbose logging to see what's happening:

```bash
pyimport --loglevel DEBUG --verbose data.csv
```

## Performance Comparison

Import of 200,000 rows (NYC taxi data):

| Method | Time | Docs/sec |
|--------|------|----------|
| Sync (default) | ~8.3s | ~24,000 |
| Async | ~6.6s | ~30,000 |
| Multi-process (4 cores) | ~4.0s | ~50,000 |

Commands:
```bash
# Default sync
pyimport data.csv

# Async
pyimport --asyncpro data.csv

# Multi-process
pyimport --multi --splitfile --autosplit 8 --poolsize 4 data.csv
```

## Configuration File

Create `~/.pyimport.conf` to avoid repeating options:

```ini
# MongoDB connection
mdburi = mongodb://localhost:27017
database = mydb

# Import settings
batchsize = 5000
hasheader = True
addfilename = True
addtimestamp = doc

# Parallel processing
poolsize = 4
```

Then simply:
```bash
pyimport --collection users users.csv
```

## Next Steps

Now that you know the basics:

- [Command-Line Reference](cli_reference.md) - Complete list of all options
- [Field Files](fieldfiles.md) - Deep dive into type conversion
- [Advanced Usage](advanced.md) - Optimization, troubleshooting, and advanced features

## Common Issues

### "Connection refused"

MongoDB isn't running. Start it:

```bash
# macOS
brew services start mongodb-community

# Linux
sudo systemctl start mongodb

# Docker
docker run -d -p 27017:27017 mongo
```

### "Field count mismatch"

Your CSV has inconsistent column counts. Check for:
- Missing commas
- Extra commas in data
- Wrong delimiter setting

Use `--loglevel DEBUG` to see which row is causing issues.

### "No field file found"

PyImport couldn't find a `.tff` file. Either:
1. Generate one: `pyimport --genfieldfile data.csv`
2. Specify explicitly: `pyimport --fieldfile myfields.tff data.csv`

### Dates Not Parsing

If dates aren't converting properly:

1. Check the format in your field file
2. Use ISO format (YYYY-MM-DD) when possible
3. Specify format explicitly:
   ```toml
   [field.date]
   type = "date"
   format = "%d/%m/%Y"  # DD/MM/YYYY
   ```

### Import Is Slow

Try these optimizations:
1. Use `--multi --splitfile`
2. Increase `--batchsize` to 5000-10000
3. Use `--writeconcern 0` for fastest writes
4. Ensure MongoDB has proper indexes (add after import)
5. Use SSD storage for MongoDB
