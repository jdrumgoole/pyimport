#!/usr/bin/env python3
"""
Builder Pattern Example

Demonstrates using the fluent PyImportBuilder API.

Usage:
    python examples/api_builder_pattern.py
"""

from pyimport.api import PyImportBuilder

def main():
    print("Starting import with builder pattern...\n")

    # Use fluent builder API for readable configuration
    result = (PyImportBuilder()
        .connect("mongodb://localhost:27017")
        .database("example_db")
        .collection("builder_import")
        .csv_file("inventory.csv")
        .delimiter(",")
        .has_header(True)
        .batch_size(100)
        .add_timestamp()
        .add_filename()
        .add_field("source", "example_script")
        .add_field("version", "1.0")
        .drop_first()  # Clear collection before import
        .log_level("INFO")
        .color(True)
        .import_data())

    print(f"\nImport Summary:")
    print(f"  Records: {result.total_written}")
    print(f"  Duration: {result.duration}")
    print(f"  Rate: {result.avg_records_per_sec:.0f} docs/sec")


if __name__ == "__main__":
    main()
