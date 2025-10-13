#!/usr/bin/env python3
"""
Audit and Restart Example

Demonstrates resilient imports with audit tracking and restart capability.

Usage:
    python examples/api_audit_restart.py [--restart]
"""

from pyimport.api import PyImportAPI
import sys

AUDIT_URI = "mongodb://localhost:27017"


def resilient_import(csv_file):
    """Import with audit tracking for restart capability."""

    api = PyImportAPI(database="example_db")

    print(f"Starting import of {csv_file} with audit tracking...")
    print("(Press Ctrl+C to interrupt and test restart capability)\n")

    try:
        result = api.import_csv(
            csv_file,
            collection="audited_data",
            audit_host=AUDIT_URI,
            parallel_mode="multi",
            pool_size=4,
            has_header=True,
            add_timestamp=True
        )

        print(f"\nImport completed successfully!")
        print(f"  Records: {result.total_written}")
        print(f"  Duration: {result.duration}")
        print(f"  Rate: {result.avg_records_per_sec:.0f} docs/sec")

        return result

    except KeyboardInterrupt:
        print("\n\nImport interrupted! Progress has been saved in audit database.")
        print("\nTo resume, run:")
        print(f"  python {sys.argv[0]} --restart")

        # Show status
        status = api.get_audit_status(AUDIT_URI)
        if status['has_incomplete']:
            batch = status['last_incomplete_batch']
            print(f"\nLast incomplete batch: {batch['batchID']}")

        sys.exit(1)


def restart_import(csv_file):
    """Restart an interrupted import."""

    api = PyImportAPI(database="example_db")

    # Check for incomplete batches
    status = api.get_audit_status(AUDIT_URI)

    if not status['has_incomplete']:
        print("No incomplete imports found.")
        return

    batch = status['last_incomplete_batch']
    batch_id = batch['batchID']

    print(f"Restarting incomplete batch: {batch_id}")

    # Check what's already done
    batch_status = api.get_audit_status(AUDIT_URI, batch_id=batch_id)
    completed_count = batch_status['completed_count']
    print(f"Already completed: {completed_count} files\n")

    # Resume the import
    result = api.restart_import(
        batch_id=batch_id,
        audit_host=AUDIT_URI,
        filename=csv_file,
        collection="audited_data",
        parallel_mode="multi",
        pool_size=4,
        has_header=True
    )

    print(f"\nRestart completed!")
    print(f"  Total records: {result.total_written}")
    print(f"  Duration: {result.duration}")


def main():
    csv_file = "inventory.csv"  # Replace with actual file

    if len(sys.argv) > 1 and sys.argv[1] == "--restart":
        restart_import(csv_file)
    else:
        resilient_import(csv_file)


if __name__ == "__main__":
    main()
