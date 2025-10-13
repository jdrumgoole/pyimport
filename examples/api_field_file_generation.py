#!/usr/bin/env python3
"""
Field File Generation Example

Demonstrates auto-generating field files for type-safe imports.

Usage:
    python examples/api_field_file_generation.py
"""

from pyimport.api import PyImportAPI
from pathlib import Path

def main():
    api = PyImportAPI(database="example_db")

    csv_file = "inventory.csv"
    print(f"Analyzing {csv_file}...")

    # Generate field file from CSV structure
    field_file = api.generate_field_file(
        csv_file,
        delimiter=",",
        has_header=True
    )

    # Inspect generated field types
    print(f"\nGenerated field file with {len(field_file.fields())} fields:\n")
    for field in field_file.fields():
        field_type = field_file.type_value(field)
        field_format = field_file.format_value(field)
        print(f"  {field:20} -> {field_type:10} {f'(format: {field_format})' if field_format else ''}")

    # Now use the field file for a type-safe import
    print(f"\nImporting {csv_file} with type conversion...")
    result = api.import_csv(
        csv_file,
        collection="typed_import",
        field_file=f"{Path(csv_file).stem}.tff",  # Uses generated .tff file
        has_header=True
    )

    print(f"\nImport completed!")
    print(f"Records imported: {result.total_written}")


if __name__ == "__main__":
    main()
