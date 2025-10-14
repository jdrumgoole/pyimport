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

## TFF v2.0: Nested Document Mapping (NEW!)

**Available in pyimport 2.0+**

TFF v2.0 format allows you to map flat CSV data to nested JSON/MongoDB documents. This is perfect for creating properly structured documents instead of flat key-value pairs.

### Why Use Nested Mapping?

**Before (v1.0 - Flat Structure):**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "street": "123 Main St",
  "city": "Boston",
  "state": "MA",
  "zip": "02101"
}
```

**After (v2.0 - Nested Structure):**
```json
{
  "name": {
    "first": "John",
    "last": "Doe"
  },
  "address": {
    "street": "123 Main St",
    "city": "Boston",
    "state": "MA",
    "postal_code": "02101"
  }
}
```

### Basic Nested Mapping

Add a `path` field to specify where the value should be placed in the nested structure:

```toml
[first_name]
type = "str"
name = "first_name"
path = "name.first"  # NEW: Nested path using dot notation
format = ""

[last_name]
type = "str"
name = "last_name"
path = "name.last"
format = ""

[city]
type = "str"
name = "city"
path = "address.city"
format = ""

[state]
type = "str"
name = "state"
path = "address.state"
format = ""

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
```

### Path Notation

Use dot notation to specify nested levels:

- `"name"` → Top-level field
- `"user.name"` → One level: `{user: {name: value}}`
- `"user.profile.name"` → Two levels: `{user: {profile: {name: value}}}`
- `"data.metrics.score.final"` → Three levels

**No limit** on nesting depth!

### Mixed v1.0 and v2.0 Fields

You can mix fields with and without paths in the same file:

```toml
[id]
type = "int"
name = "id"
# No path - stays at top level

[first_name]
type = "str"
name = "first_name"
path = "profile.name.first"  # Nested

[email]
type = "str"
name = "email"
# No path - stays at top level

[city]
type = "str"
name = "city"
path = "profile.location.city"  # Nested
```

Result:
```json
{
  "id": 123,                    // Top-level (no path)
  "email": "user@example.com",  // Top-level (no path)
  "profile": {                  // Nested (has path)
    "name": {
      "first": "John"
    },
    "location": {
      "city": "Boston"
    }
  }
}
```

### Real-World Example: Healthcare Data

Transform flat A&E (Accident & Emergency) data into structured documents:

```toml
[SHA]
type = "str"
name = "SHA"
path = "organization.sha_code"

[Code]
type = "str"
name = "Code"
path = "organization.code"

[Name]
type = "str"
name = "Name"
path = "organization.name"

[Type1_Attendances]
type = "int"
name = "Type1_Attendances"
path = "departments.type1.attendances"

[Type1_Over4Hours]
type = "int"
name = "Type1_Over4Hours"
path = "departments.type1.over_4_hours"

[Percentage_4Hours]
type = "float"
name = "Percentage_4Hours"
path = "performance.within_4_hours_pct"

[Admissions_Type1]
type = "int"
name = "Admissions_Type1"
path = "admissions.type1"

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
```

Result:
```json
{
  "organization": {
    "sha_code": "Q44",
    "code": "REM",
    "name": "BURTON HOSPITALS NHS TRUST"
  },
  "departments": {
    "type1": {
      "attendances": 7523,
      "over_4_hours": 1234
    }
  },
  "performance": {
    "within_4_hours_pct": 83.6
  },
  "admissions": {
    "type1": 1456
  }
}
```

### Real-World Example: NYC Taxi with Geospatial

Create GeoJSON-compatible nested structures for location data:

```toml
[VendorID]
type = "int"
name = "VendorID"
path = "vendor.id"

[tpep_pickup_datetime]
type = "datetime"
name = "tpep_pickup_datetime"
path = "pickup.datetime"
format = "%Y-%m-%d %H:%M:%S"

[pickup_longitude]
type = "float"
name = "pickup_longitude"
path = "pickup.location.coordinates.longitude"

[pickup_latitude]
type = "float"
name = "pickup_latitude"
path = "pickup.location.coordinates.latitude"

[dropoff_longitude]
type = "float"
name = "dropoff_longitude"
path = "dropoff.location.coordinates.longitude"

[dropoff_latitude]
type = "float"
name = "dropoff_latitude"
path = "dropoff.location.coordinates.latitude"

[passenger_count]
type = "int"
name = "passenger_count"
path = "trip.passengers"

[trip_distance]
type = "float"
name = "trip_distance"
path = "trip.distance"

[fare_amount]
type = "float"
name = "fare_amount"
path = "payment.fare"

[tip_amount]
type = "float"
name = "tip_amount"
path = "payment.tip"

[total_amount]
type = "float"
name = "total_amount"
path = "payment.total"

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
```

Result:
```json
{
  "vendor": {
    "id": 1
  },
  "pickup": {
    "datetime": ISODate("2024-01-15T10:30:00Z"),
    "location": {
      "coordinates": {
        "longitude": -73.9851,
        "latitude": 40.7589
      }
    }
  },
  "dropoff": {
    "location": {
      "coordinates": {
        "longitude": -73.9731,
        "latitude": 40.7644
      }
    }
  },
  "trip": {
    "passengers": 2,
    "distance": 1.5
  },
  "payment": {
    "fare": 12.5,
    "tip": 2.5,
    "total": 15.0
  }
}
```

**Note:** For proper GeoJSON, you'd need Phase 2 features (field composition) to create the `type: "Point"` structure. This example shows the nested coordinates structure.

### Querying Nested Documents

MongoDB queries work naturally with nested fields:

```javascript
// Find by nested field
db.taxi.find({"vendor.id": 1})

// Range query on nested field
db.taxi.find({"payment.total": {$gte: 20}})

// Query multiple nested levels
db.taxi.find({
  "pickup.location.coordinates.latitude": {$gte: 40.7, $lte: 40.8}
})

// Complex query
db.taxi.find({
  "trip.distance": {$gte: 5},
  "payment.tip": {$gte: 5}
})
```

### Create Indexes on Nested Fields

```javascript
// Index on nested field
db.taxi.createIndex({"vendor.id": 1})

// Compound index with nested fields
db.taxi.createIndex({
  "pickup.datetime": 1,
  "vendor.id": 1
})

// Geospatial index (after restructuring to GeoJSON)
db.taxi.createIndex({"pickup.location": "2dsphere"})
```

### Path Validation

PyImport validates paths to prevent conflicts:

**❌ Invalid - Prefix Conflict:**
```toml
[field1]
type = "str"
path = "address"

[field2]
type = "str"
path = "address.city"  # ERROR: Conflicts with "address"
```

**❌ Invalid - Duplicate Path:**
```toml
[field1]
type = "str"
path = "data.value"

[field2]
type = "str"
path = "data.value"  # ERROR: Duplicate path
```

**✅ Valid:**
```toml
[field1]
type = "str"
path = "address.city"

[field2]
type = "str"
path = "address.state"  # OK: Same parent, different fields
```

### Backward Compatibility

**100% backward compatible!** All existing v1.0 field files work unchanged:

- No `path` field? → Flat structure (v1.0 behavior)
- Has `path` field? → Nested structure (v2.0 behavior)
- Mixed? → Combination of both

**Migrating from v1.0 to v2.0:**

1. Start with existing v1.0 file
2. Add `path` to fields you want to nest
3. Leave others without `path` (they stay top-level)
4. Import works immediately - no other changes needed!

### Performance

- **v1.0 files**: No performance impact
- **v2.0 files**: < 5% overhead for nested document construction
- **Large datasets**: Minimal impact (tested with 100k+ rows)

### When to Use v2.0

**Use nested mapping when:**
- You want properly structured documents
- Data has logical groupings (name parts, address parts, etc.)
- You need to query nested fields efficiently
- You want MongoDB schema validation on nested structures

**Stick with v1.0 when:**
- Simple flat data is sufficient
- No logical grouping in your data
- Performance is absolutely critical (though v2.0 is only ~5% slower)

### Common Patterns

**Name Components:**
```toml
path = "name.first"
path = "name.last"
path = "name.middle"
```

**Address Components:**
```toml
path = "address.street"
path = "address.city"
path = "address.state"
path = "address.postal_code"
path = "address.country"
```

**Contact Information:**
```toml
path = "contact.email"
path = "contact.phone.mobile"
path = "contact.phone.home"
```

**Metrics/Stats:**
```toml
path = "stats.count"
path = "stats.average"
path = "stats.maximum"
path = "stats.minimum"
```

**Timestamps:**
```toml
path = "metadata.created_at"
path = "metadata.updated_at"
path = "metadata.deleted_at"
```

## See Also

- [Quick Start](quickstart.md) - Basic usage examples
- [Command-Line Reference](cli_reference.md) - All CLI options
- [Advanced Usage](advanced.md) - Optimization and troubleshooting
- [TFF v2.0 Migration Guide](v2_migration_guide.md) - Detailed migration guide
