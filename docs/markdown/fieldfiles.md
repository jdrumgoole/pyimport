# Field Files (.tff)

Field files define how CSV columns should be parsed and converted to MongoDB types. They use TOML format (`.tff` extension).

## What is a Field File?

A field file maps each CSV column to a data type and optionally a format specification. When PyImport reads your CSV, it uses the field file to:

1. Convert strings to appropriate types (int, float, date, etc.)
2. Handle date/time formats
3. Validate data
4. Fall back to strings on conversion errors (unless `--onerror Fail` is used)

## Generating Field Files

The easiest way to create a field file is to let PyImport infer types:

```bash
pyimport --genfieldfile data.csv
```

This analyzes your CSV and creates `data.tff` with detected types.

## Field File Format

Field files use TOML syntax. Each field has its own section:

```toml
[field.column_name]
type = "type_name"
format = "format_string"  # Optional, for dates
```

### Basic Example

Given this CSV:
```csv
name,age,salary,active
Alice,30,75000.50,true
Bob,25,65000.00,false
```

Field file (`data.tff`):
```toml
[field.name]
type = "str"

[field.age]
type = "int"

[field.salary]
type = "float"

[field.active]
type = "str"  # MongoDB doesn't have native bool from CSV
```

## Supported Types

### String (`str`)

Text data. No conversion applied.

```toml
[field.name]
type = "str"
```

```csv
name
Alice
Bob
```

Result: `{"name": "Alice"}`

### Integer (`int`)

Whole numbers. Handles conversion from float strings.

```toml
[field.age]
type = "int"
```

```csv
age
30
42.0  # Converts to 42
```

Result: `{"age": 30}`

### Float (`float`)

Decimal numbers.

```toml
[field.salary]
type = "float"
```

```csv
salary
75000.50
80000
```

Result: `{"salary": 75000.50}`

### Date (`date`)

Date values without time component.

**Without format (slow):**
```toml
[field.birth_date]
type = "date"
```

Uses `dateutil.parser` - flexible but slow (~10-100x slower).

**With format (fast):**
```toml
[field.birth_date]
type = "date"
format = "%Y-%m-%d"
```

Uses `strptime` - fast and predictable.

```csv
birth_date
1990-05-15
1985-12-20
```

Result: `{"birth_date": ISODate("1990-05-15T00:00:00Z")}`

### DateTime (`datetime`)

Date with time component.

**Without format:**
```toml
[field.created_at]
type = "datetime"
```

**With format:**
```toml
[field.created_at]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"
```

```csv
created_at
2024-01-15 14:30:00
2024-01-16 09:15:30
```

Result: `{"created_at": ISODate("2024-01-15T14:30:00Z")}`

### ISO Date (`isodate`)

**Fastest date parsing** - specifically for ISO 8601 format (YYYY-MM-DD).

```toml
[field.date]
type = "isodate"
```

```csv
date
2024-01-15
2024-01-16
```

**Performance:** ~100x faster than generic date parsing. Use this whenever possible!

### Timestamp (`timestamp`)

Unix timestamp (seconds since epoch).

```toml
[field.event_time]
type = "timestamp"
```

```csv
event_time
1705334400
1705420800
```

Result: `{"event_time": ISODate("2024-01-15T12:00:00Z")}`

## Date Format Strings

Format strings use Python's `strptime` syntax:

| Code | Meaning | Example |
|------|---------|---------|
| `%Y` | 4-digit year | 2024 |
| `%y` | 2-digit year | 24 |
| `%m` | Month (01-12) | 01 |
| `%d` | Day (01-31) | 15 |
| `%H` | Hour 24h (00-23) | 14 |
| `%I` | Hour 12h (01-12) | 02 |
| `%M` | Minute (00-59) | 30 |
| `%S` | Second (00-59) | 45 |
| `%p` | AM/PM | PM |
| `%b` | Abbreviated month | Jan |
| `%B` | Full month | January |
| `%a` | Abbreviated day | Mon |
| `%A` | Full day | Monday |

### Common Date Format Examples

```toml
# US date: 01/15/2024
[field.date]
type = "date"
format = "%m/%d/%Y"

# European date: 15/01/2024
[field.date]
type = "date"
format = "%d/%m/%Y"

# ISO date: 2024-01-15
[field.date]
type = "isodate"  # No format needed!

# Date with time: 2024-01-15 14:30:00
[field.datetime]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"

# 12-hour time: 01/15/2024 02:30 PM
[field.datetime]
type = "datetime"
format = "%m/%d/%Y %I:%M %p"

# Long format: January 15, 2024
[field.date]
type = "date"
format = "%B %d, %Y"
```

## Defaults Section

The `DEFAULTS_SECTION` defines CSV parsing options:

```toml
[DEFAULTS_SECTION]
delimiter = ","
has_header = true
```

### Options

- `delimiter`: Field separator (default: `,`)
  - Use `\t` for tab
  - Any string: `|`, `;`, etc.
- `has_header`: Whether first row is header (default: `true`)

Example with pipe delimiter:
```toml
[DEFAULTS_SECTION]
delimiter = "|"
has_header = true

[field.name]
type = "str"

[field.age]
type = "int"
```

## Advanced Field File Examples

### Mixed Date Formats

You can have different date formats for different columns:

```toml
[field.birth_date]
type = "date"
format = "%m/%d/%Y"  # US format

[field.hire_date]
type = "isodate"  # ISO format (fastest)

[field.last_login]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"
```

### Optional Format Inference

Let PyImport infer date formats:

```bash
pyimport --genfieldfile data.csv
```

Result:
```toml
[field.date_column]
type = "date"
format = "%Y-%m-%d"  # Automatically detected!
```

### NULL and Empty String Handling

PyImport handles `NULL` and empty strings gracefully:

```csv
name,age,city
Alice,30,NYC
Bob,,  # Empty age, empty city
Charlie,35,NULL
```

With this field file:
```toml
[field.name]
type = "str"

[field.age]
type = "int"

[field.city]
type = "str"
```

Result:
```json
{"name": "Alice", "age": 30, "city": "NYC"}
{"name": "Bob", "age": "", "city": ""}  // Falls back to string
{"name": "Charlie", "age": 35, "city": "NULL"}  // Stored as string "NULL"
```

## Type Conversion Behavior

### Successful Conversion

```csv
age
30
42
25
```

```toml
[field.age]
type = "int"
```

Result: All values stored as integers.

### Failed Conversion (Default: Warn)

```csv
age
30
invalid
25
```

With `--onerror Warn` (default):
- Logs warning about "invalid"
- Stores "invalid" as string
- Continues processing

Result:
```json
{"age": 30}
{"age": "invalid"}  // Fallback to string
{"age": 25}
```

### Failed Conversion (Strict Mode)

With `--onerror Fail`:
```bash
pyimport --onerror Fail data.csv
```

Stops on first conversion error.

## Field File Discovery

PyImport looks for field files in this order:

1. **Explicit:** `--fieldfile custom.tff`
2. **Auto-discovery:** Same name as CSV with `.tff` extension
   - `data.csv` → looks for `data.tff`
   - `users.csv` → looks for `users.tff`
3. **Auto-generation:** If no field file found and has header, attempts to infer types

## Creating Field Files Manually

### Step 1: Look at Your CSV

```csv
name,age,salary,join_date,is_active
Alice,30,75000,2020-01-15,true
Bob,25,65000,2021-03-22,false
```

### Step 2: Determine Types

- `name`: String
- `age`: Integer
- `salary`: Integer (could be float if decimals)
- `join_date`: ISO date
- `is_active`: String (MongoDB has no native bool from CSV)

### Step 3: Create `.tff` File

```toml
[DEFAULTS_SECTION]
delimiter = ","
has_header = true

[field.name]
type = "str"

[field.age]
type = "int"

[field.salary]
type = "int"

[field.join_date]
type = "isodate"

[field.is_active]
type = "str"
```

### Step 4: Test

```bash
# Import first 10 rows to verify
pyimport --limit 10 --loglevel DEBUG \
         --database test --collection sample \
         --fieldfile myfields.tff \
         data.csv

# Check results
mongosh test --eval "db.sample.find().pretty()"
```

## Troubleshooting

### "Field count mismatch"

Your CSV has inconsistent column counts.

**Solution:** Check your data for:
- Missing delimiters
- Wrong delimiter (using `,` when data uses `|`)
- Extra delimiters in quoted fields

### Dates Not Converting

**Problem:** Dates stored as strings instead of Date objects.

**Solutions:**
1. Check format string matches your data exactly
2. Use `isodate` type for ISO format (YYYY-MM-DD)
3. Let PyImport infer format: `--genfieldfile`
4. Verify sample data with `--limit 10 --loglevel DEBUG`

### Numbers Stored as Strings

**Problem:** Numeric values stored as strings.

**Solution:** Check field file has correct type:
```toml
[field.price]
type = "float"  # Not "str"
```

### Field File Not Found

**Problem:** `pyimport` can't find `.tff` file.

**Solutions:**
```bash
# Generate field file
pyimport --genfieldfile data.csv

# Or specify explicitly
pyimport --fieldfile path/to/fields.tff data.csv
```

## Performance Tips

1. **Use `isodate` for ISO dates** - 100x faster than generic parsing
2. **Always specify date formats** - `format` parameter makes parsing much faster
3. **Use appropriate types** - Don't use `str` for everything
4. **Test with `--limit`** - Verify field file with small sample first

## Field File Examples

### Financial Data

```toml
[DEFAULTS_SECTION]
delimiter = ","
has_header = true

[field.transaction_id]
type = "str"

[field.date]
type = "isodate"

[field.amount]
type = "float"

[field.quantity]
type = "int"

[field.symbol]
type = "str"

[field.timestamp]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"
```

### Log Data

```toml
[DEFAULTS_SECTION]
delimiter = "|"
has_header = false

[field.timestamp]
type = "timestamp"

[field.level]
type = "str"

[field.message]
type = "str"

[field.user_id]
type = "int"
```

### NYC Taxi Data

```toml
[DEFAULTS_SECTION]
delimiter = ","
has_header = true

[field.VendorID]
type = "int"

[field.tpep_pickup_datetime]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"

[field.tpep_dropoff_datetime]
type = "datetime"
format = "%Y-%m-%d %H:%M:%S"

[field.passenger_count]
type = "int"

[field.trip_distance]
type = "float"

[field.fare_amount]
type = "float"

[field.tip_amount]
type = "float"

[field.total_amount]
type = "float"
```

## See Also

- [Quick Start](quickstart.md) - Basic usage examples
- [Command-Line Reference](cli_reference.md) - All CLI options
- [Advanced Usage](advanced.md) - Optimization and troubleshooting
