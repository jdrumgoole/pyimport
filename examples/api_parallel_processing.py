#!/usr/bin/env python3
"""
Parallel Processing Example

Demonstrates parallel import modes for large datasets.

Usage:
    python examples/api_parallel_processing.py
"""

from pyimport.api import PyImportAPI
import time

def compare_import_modes(csv_file):
    """Compare different import modes on the same file."""

    modes = [
        ("Standard", None, 1),
        ("Multi-Process", "multi", 4),
        ("Threaded", "threads", 4),
        ("Async", "async", 1),
    ]

    print(f"Comparing import modes on {csv_file}\n")
    print(f"{'Mode':<20} {'Time':<15} {'Rate (docs/sec)':<20}")
    print("-" * 55)

    for mode_name, parallel_mode, pool_size in modes:
        api = PyImportAPI(
            database="example_db",
            collection=f"import_{mode_name.lower().replace('-', '_')}",
            log_level="WARNING"  # Reduce output
        )

        # Drop collection before each test
        api.drop_collection(
            database="example_db",
            collection=f"import_{mode_name.lower().replace('-', '_')}"
        )

        # Time the import
        start = time.time()
        result = api.import_csv(
            csv_file,
            has_header=True,
            parallel_mode=parallel_mode,
            pool_size=pool_size
        )
        elapsed = time.time() - start

        rate = result.total_written / elapsed if elapsed > 0 else 0
        print(f"{mode_name:<20} {elapsed:>10.2f}s     {rate:>15.0f}")

    print("\nNote: Results may vary based on file size and system resources.")


def parallel_import_example():
    """Example of parallel import with multiple workers."""

    print("\nParallel Import Example\n")

    api = PyImportAPI(database="example_db")

    # Import large file with multi-process mode
    result = api.import_csv(
        "inventory.csv",  # Replace with a larger file for real testing
        collection="parallel_data",
        parallel_mode="multi",
        pool_size=4,  # Use 4 worker processes
        has_header=True,
        add_timestamp=True
    )

    print(f"Parallel import completed!")
    print(f"  Records: {result.total_written}")
    print(f"  Time: {result.duration}")
    print(f"  Rate: {result.avg_records_per_sec:.0f} docs/sec")


def main():
    csv_file = "inventory.csv"

    # Run parallel import example
    parallel_import_example()

    # Uncomment to compare all modes (requires larger file for meaningful comparison)
    # compare_import_modes(csv_file)


if __name__ == "__main__":
    main()
