# Enhanced TFF Format Design: CSV to Nested JSON Mapping

## Overview

This document proposes an enhanced TFF (TOML Field File) format that supports mapping flat CSV fields to nested JSON/MongoDB document structures. The design maintains backward compatibility with existing TFF files while adding powerful mapping capabilities.

## Current TFF Format (v1.0)

The current format maps CSV columns 1:1 to flat document fields:

```toml
[CSV_Column_Name]
type = "int"
name = "output_field_name"  # Optional: defaults to CSV_Column_Name
format = ""

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
"CSV File" = "data.csv"
```

**Current Behavior:**
- Each CSV column becomes a top-level field in the output document
- Simple type conversion (str, int, float, date, datetime, bool)
- 1:1 mapping only

## Proposed Enhanced TFF Format (v2.0)

### Key Concepts

1. **Path Notation**: Use dot notation for nested field paths
2. **Field Composition**: Combine multiple CSV fields into single output fields
3. **Transformation Functions**: Apply functions during mapping
4. **Backward Compatibility**: Existing TFF files work unchanged

### Basic Nested Mapping

Map CSV columns to nested paths using dot notation:

```toml
# CSV: first_name, last_name, street, city, state, zip, age

[first_name]
type = "str"
path = "personal.name.first"  # New: nested path

[last_name]
type = "str"
path = "personal.name.last"

[street]
type = "str"
path = "address.street"

[city]
type = "str"
path = "address.city"

[state]
type = "str"
path = "address.state"

[zip]
type = "str"
path = "address.postal_code"

[age]
type = "int"
path = "personal.age"
```

**Output Document:**
```json
{
  "personal": {
    "name": {
      "first": "John",
      "last": "Doe"
    },
    "age": 30
  },
  "address": {
    "street": "123 Main St",
    "city": "Boston",
    "state": "MA",
    "postal_code": "02101"
  }
}
```

### Field Composition

Combine multiple CSV fields into a single output field:

```toml
# CSV: first_name, last_name, street, city, state, zip

[full_name]
type = "str"
sources = ["first_name", "last_name"]
path = "name"
template = "{first_name} {last_name}"  # String interpolation

[full_address]
type = "str"
sources = ["street", "city", "state", "zip"]
path = "address.full"
template = "{street}, {city}, {state} {zip}"

# Coordinate example: combine lat/lon into GeoJSON point
[location]
type = "object"
sources = ["latitude", "longitude"]
path = "location"
structure = {
  "type": "Point",
  "coordinates": ["{longitude}", "{latitude}"]  # Note: GeoJSON is [lon, lat]
}
```

**CSV Input:**
```csv
first_name,last_name,street,city,state,zip,latitude,longitude
John,Doe,123 Main St,Boston,MA,02101,42.3601,-71.0589
```

**Output Document:**
```json
{
  "name": "John Doe",
  "address": {
    "full": "123 Main St, Boston, MA 02101"
  },
  "location": {
    "type": "Point",
    "coordinates": [-71.0589, 42.3601]
  }
}
```

### Array Fields

Create arrays from multiple CSV columns or delimited values:

```toml
# Option 1: Multiple columns into array
[tags]
type = "array"
sources = ["tag1", "tag2", "tag3"]
path = "tags"
item_type = "str"
skip_empty = true  # Don't include empty values

# Option 2: Split delimited value
[categories]
type = "array"
source = "category_list"  # CSV column with "cat1;cat2;cat3"
path = "categories"
item_type = "str"
split_on = ";"
trim = true  # Trim whitespace from items
```

**CSV Input:**
```csv
tag1,tag2,tag3,category_list
urgent,follow-up,,sales;marketing;support
```

**Output Document:**
```json
{
  "tags": ["urgent", "follow-up"],
  "categories": ["sales", "marketing", "support"]
}
```

### Conditional Mapping

Map fields conditionally based on values:

```toml
[status_code]
type = "str"
path = "status"
mapping = {
  "1": "active",
  "2": "inactive",
  "3": "pending",
  "_default": "unknown"  # Fallback value
}

[is_premium]
type = "bool"
source = "account_type"
path = "premium"
condition = { "values": ["premium", "enterprise"], "result": true, "default": false }
```

### Type Coercion with Fallbacks

Enhanced type handling for dirty data:

```toml
[price]
type = "float"
path = "product.price"
strip_chars = "$,"  # Remove $ and , before parsing
fallback = 0.0  # Use this if conversion fails
required = false  # Don't error if field is missing

[quantity]
type = "int"
path = "product.quantity"
fallback = 1
min_value = 0  # Validation
max_value = 10000
```

### Computed Fields

Add fields computed from other fields:

```toml
[total_price]
type = "computed"
path = "order.total"
expression = "{price} * {quantity}"
result_type = "float"

[full_name_upper]
type = "computed"
path = "name_upper"
expression = "{first_name} {last_name}"
transform = "upper"  # Built-in transforms: upper, lower, title, strip
```

### Metadata and Enrichment

Control metadata fields that pyimport currently adds:

```toml
[METADATA_SECTION]
add_filename = true
filename_path = "_metadata.source_file"  # Custom path

add_timestamp = true
timestamp_path = "_metadata.imported_at"  # Custom path

add_row_number = true
row_number_path = "_metadata.row"

custom_fields = [
  { path = "_metadata.version", value = "1.0" },
  { path = "_metadata.source", value = "import_2024" }
]
```

### Enhanced DEFAULTS_SECTION

```toml
[DEFAULTS_SECTION]
delimiter = ","
has_header = true
"CSV File" = "data.csv"

# New v2.0 options
tff_version = "2.0"  # Explicitly declare format version
strict_mode = false  # If true, fail on any mapping error
skip_unmapped = false  # If true, only include mapped fields (not unmapped columns)
preserve_original = false  # If true, keep original flat fields alongside nested
```

## Complete Example: Healthcare A&E Data

Transform the existing AandEData.csv into a properly structured nested document:

```toml
# CSV columns: SHA, Code, Name, Type 1 Departments - Major A&E | A&E attendances, ...

[SHA]
type = "str"
path = "organization.sha_code"

[Code]
type = "str"
path = "organization.code"

[Name]
type = "str"
path = "organization.name"

# Nested department statistics
["Type 1 Departments - Major A&E | A&E attendances"]
type = "int"
path = "departments.type1.attendances"

["Type 2 Departments - Single Specialty"]
type = "int"
path = "departments.type2.attendances"

["Type 3 Departments - Other A&E/Minor Injury Unit"]
type = "int"
path = "departments.type3.attendances"

["Total attendances"]
type = "int"
path = "totals.attendances"

["Type 1 Departments - Major A&E | A&E attendances > 4 hours from arrival to admission transfer or discharge"]
type = "int"
path = "departments.type1.over_4_hours"

["Percentage in 4 hours or less (type 1)"]
type = "float"
path = "performance.type1_within_4_hours_pct"

["Percentage in 4 hours or less (all)"]
type = "float"
path = "performance.all_within_4_hours_pct"

# Emergency admissions nested structure
["Emergency Admissions via Type 1 A&E | Emergency Admissions"]
type = "int"
path = "admissions.type1"

["Emergency Admissions via Type 2 A&E"]
type = "int"
path = "admissions.type2"

["Emergency Admissions via Type 3 and 4 A&E"]
type = "int"
path = "admissions.type3_4"

["Other Emergency admissions (i_e not via A&E)"]
type = "int"
path = "admissions.other"

[DEFAULTS_SECTION]
delimiter = ","
has_header = true
"CSV File" = "AandEData.csv"
tff_version = "2.0"
```

**Output Document Structure:**
```json
{
  "organization": {
    "sha_code": "Q44",
    "code": "REM",
    "name": "BURTON HOSPITALS NHS FOUNDATION TRUST"
  },
  "departments": {
    "type1": {
      "attendances": 7523,
      "over_4_hours": 1234
    },
    "type2": {
      "attendances": 0
    },
    "type3": {
      "attendances": 326
    }
  },
  "totals": {
    "attendances": 7849
  },
  "performance": {
    "type1_within_4_hours_pct": 83.6,
    "all_within_4_hours_pct": 85.2
  },
  "admissions": {
    "type1": 1456,
    "type2": 0,
    "type3_4": 12,
    "other": 234
  }
}
```

## Backward Compatibility

### Detection Logic

```python
def get_tff_version(toml_dict: dict) -> str:
    """Determine TFF format version"""
    if "DEFAULTS_SECTION" in toml_dict:
        if "tff_version" in toml_dict["DEFAULTS_SECTION"]:
            return toml_dict["DEFAULTS_SECTION"]["tff_version"]

    # Check if any v2.0 features are present
    for field_config in toml_dict.values():
        if isinstance(field_config, dict):
            if any(key in field_config for key in ["path", "sources", "template", "structure"]):
                return "2.0"

    return "1.0"  # Default: legacy format
```

### Migration Strategy

1. **Existing TFF files (v1.0)**: Continue to work exactly as before
   - No `path` field → use `name` field as flat field name
   - Simple 1:1 type conversion

2. **New TFF files (v2.0)**: Opt-in with explicit features
   - Presence of `path` field activates v2.0 behavior
   - Can mix v1.0 and v2.0 fields in same file

3. **Auto-generation**: `--genfieldfile` produces v1.0 format by default
   - New flag: `--genfieldfile-nested` to generate v2.0 format with suggested structure

## Implementation Considerations

### Phase 1: Core Nested Mapping
- Add `path` field support
- Implement dot-notation path parsing
- Build nested document construction
- Maintain v1.0 compatibility

### Phase 2: Field Composition
- Add `sources` field support
- Implement template-based composition
- Add `structure` for object composition

### Phase 3: Advanced Features
- Array field support
- Conditional mapping
- Computed fields
- Enhanced validation

### Performance Impact

- **Minimal**: Path parsing and nested document construction add negligible overhead
- **Memory**: Nested structures may use slightly more memory than flat documents
- **Compatibility**: Can still use all existing import modes (sync, async, multi, thread)

### Testing Strategy

1. Unit tests for path parsing and nested document building
2. Integration tests with v1.0 and v2.0 TFF files
3. Backward compatibility test suite (run all existing tests)
4. Performance benchmarks to ensure no regression

## CLI Changes

### New Flags

```bash
# Generate v2.0 field file with nested structure suggestions
pyimport --genfieldfile-nested data.csv

# Validate TFF file format
pyimport --validate-fieldfile data.tff

# Preview document structure without import
pyimport --preview-structure data.csv data.tff --limit 5
```

## Documentation Updates

1. Update fieldfiles.md with v2.0 format examples
2. Add migration guide for v1.0 → v2.0
3. Add cookbook with common mapping patterns
4. Update API documentation for FieldFile class

## Questions for Consideration

1. **Array handling**: Should we support CSV columns that contain JSON arrays directly?
2. **Type inference**: Should `--genfieldfile-nested` attempt to infer document structure from column names? (e.g., "address.city" → nested structure)
3. **MongoDB-specific**: Should we add MongoDB-specific features like `$type` operators for schema validation?
4. **Validation**: Should we support JSON Schema validation for output documents?
5. **Transformation library**: Should we integrate a transformation library (e.g., jmespath) for complex expressions?

## Next Steps

1. Review and approve this design
2. Create GitHub issue for feature tracking
3. Implement Phase 1 (core nested mapping)
4. Update documentation
5. Release as pyimport v2.0.0
