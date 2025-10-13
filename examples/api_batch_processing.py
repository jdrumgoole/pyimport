#!/usr/bin/env python3
"""
Batch Processing Example

Demonstrates importing multiple files and processing directories.

Usage:
    python examples/api_batch_processing.py
"""

from pyimport.api import PyImportAPI, PyImportBuilder
from pathlib import Path


def import_multiple_files():
    """Import multiple CSV files into a single collection."""

    api = PyImportAPI(database="example_db")

    files = ["file1.csv", "file2.csv", "file3.csv"]

    print(f"Importing {len(files)} files...\n")

    result = api.import_csv(
        files,
        collection="combined_data",
        has_header=True,
        add_filename=True,  # Track which file each record came from
        add_timestamp=True,
        drop_collection=True  # Start fresh
    )

    print(f"\nBatch import completed!")
    print(f"  Total records: {result.total_written}")
    print(f"  Files processed: {result.total_results}")
    print(f"  Duration: {result.duration}")

    # Show per-file results
    print(f"\nPer-file breakdown:")
    for file_result in result.results:
        print(f"  {file_result.filename}: {file_result.total_written} records")


def import_directory(directory, pattern="*.csv"):
    """Import all CSV files from a directory."""

    api = PyImportAPI(database="example_db")

    # Find all CSV files
    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"Directory not found: {directory}")
        return

    files = list(dir_path.glob(pattern))

    if not files:
        print(f"No {pattern} files found in {directory}")
        return

    print(f"Found {len(files)} files in {directory}")

    # Import all files in parallel
    result = api.import_csv(
        [str(f) for f in files],
        collection="directory_import",
        parallel_mode="multi",
        pool_size=4,
        has_header=True,
        add_filename=True,
        add_timestamp=True,
        drop_collection=True
    )

    print(f"\nDirectory import completed!")
    print(f"  Total records: {result.total_written}")
    print(f"  Files: {result.total_results}")
    print(f"  Errors: {result.total_errors}")
    print(f"  Rate: {result.avg_records_per_sec:.0f} docs/sec")

    if result.total_errors > 0:
        print(f"\nErrors:")
        for error in result.errors:
            print(f"  {error.filename}: {error.error}")


def import_with_preprocessing():
    """Import files with custom preprocessing."""

    print("Importing with preprocessing...\n")

    # Example: Import and add metadata based on filename
    files_with_dates = [
        ("sales_2023-01.csv", "2023-01"),
        ("sales_2023-02.csv", "2023-02"),
        ("sales_2023-03.csv", "2023-03"),
    ]

    api = PyImportAPI(database="example_db")

    for csv_file, period in files_with_dates:
        print(f"Processing {period}...")

        result = (PyImportBuilder()
            .connect("mongodb://localhost:27017")
            .database("example_db")
            .collection("sales")
            .csv_file(csv_file)
            .has_header(True)
            .add_timestamp()
            .add_field("period", period)
            .add_field("year", period[:4])
            .add_field("month", period[-2:])
            .import_data())

        print(f"  Imported {result.total_written} records")

    print("\nAll periods imported!")


def main():
    print("Batch Processing Examples\n")
    print("=" * 60)

    # Example 1: Multiple specific files
    print("\n1. Import Multiple Files")
    print("-" * 60)
    # import_multiple_files()  # Uncomment with actual files

    # Example 2: Import entire directory
    print("\n2. Import Directory")
    print("-" * 60)
    # import_directory("./data", "*.csv")  # Uncomment with actual directory

    # Example 3: Import with custom metadata
    print("\n3. Import with Preprocessing")
    print("-" * 60)
    # import_with_preprocessing()  # Uncomment with actual files

    print("\n(Uncomment examples with actual files to run)")


if __name__ == "__main__":
    main()
