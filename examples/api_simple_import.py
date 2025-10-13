#!/usr/bin/env python3
"""
Simple PyImport API Example

Demonstrates basic CSV import using the PyImport API.

Usage:
    python examples/api_simple_import.py
"""

from pyimport.api import PyImportAPI

def main():
    # Create API instance with default settings
    api = PyImportAPI(
        mongodb_uri="mongodb://localhost:27017",
        database="example_db",
        collection="simple_import"
    )

    print("Starting simple CSV import...")

    # Import the inventory CSV file
    result = api.import_csv(
        "inventory.csv",
        has_header=True,
        add_timestamp=True
    )

    # Display results
    print(f"\nImport completed successfully!")
    print(f"Records imported: {result.total_written}")
    print(f"Time taken: {result.elapsed_duration}")
    print(f"Average rate: {result.avg_records_per_sec:.0f} records/sec")


if __name__ == "__main__":
    main()
