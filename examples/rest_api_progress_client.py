"""
Example client for monitoring PyImport REST API upload progress

Demonstrates how to:
1. Start an async import job
2. Poll for progress and upload rate
3. Stream real-time progress updates (Server-Sent Events)

Requirements:
    pip install requests sseclient-py

Author: Claude Code
"""

import requests
import time
import sys
from typing import Dict, Any


def start_import_job(api_url: str, filename: str, database: str, collection: str) -> str:
    """
    Start an async import job

    Args:
        api_url: Base URL of PyImport REST API (e.g., http://localhost:8000)
        filename: Path to CSV file
        database: Target database
        collection: Target collection

    Returns:
        job_id: Unique job identifier
    """
    response = requests.post(
        f"{api_url}/import/async",
        json={
            "filename": filename,
            "database": database,
            "collection": collection,
            "has_header": True,
            "delimiter": ",",
            "batch_size": 1000
        }
    )

    response.raise_for_status()
    result = response.json()

    print(f"✓ Import job started: {result['job_id']}")
    print(f"  Message: {result['message']}")

    return result["job_id"]


def get_job_progress(api_url: str, job_id: str) -> Dict[str, Any]:
    """
    Get current job progress

    Returns dict with:
        - status: Job status (pending/running/completed/failed)
        - lines_processed: Number of lines imported
        - lines_per_second: Current upload rate
        - percent_complete: Progress percentage
        - estimated_seconds_remaining: Estimated time to completion
    """
    response = requests.get(f"{api_url}/jobs/{job_id}")
    response.raise_for_status()
    return response.json()


def monitor_job_polling(api_url: str, job_id: str, poll_interval: float = 1.0):
    """
    Monitor job progress by polling

    Args:
        api_url: Base URL of API
        job_id: Job ID to monitor
        poll_interval: Seconds between polls
    """
    print(f"\nMonitoring job {job_id} (polling every {poll_interval}s)...")
    print("-" * 80)

    while True:
        progress = get_job_progress(api_url, job_id)

        # Display progress
        status = progress["status"]
        lines = progress["lines_processed"]
        rate = progress["lines_per_second"]
        percent = progress.get("percent_complete")
        eta = progress.get("estimated_seconds_remaining")

        if percent is not None:
            print(f"\r[{status.upper()}] {percent:.1f}% complete | "
                  f"{lines:,} lines | {rate:,.0f} lines/sec", end="")
            if eta:
                print(f" | ETA: {eta:.0f}s", end="")
        else:
            print(f"\r[{status.upper()}] {lines:,} lines | {rate:,.0f} lines/sec", end="")

        sys.stdout.flush()

        # Check if completed
        if status in ["completed", "failed"]:
            print()  # New line
            if status == "completed":
                print(f"\n✓ Import completed successfully!")
                print(f"  Total lines: {lines:,}")
                print(f"  Average rate: {rate:,.0f} lines/sec")
            else:
                print(f"\n✗ Import failed: {progress.get('error', 'Unknown error')}")
            break

        time.sleep(poll_interval)


def monitor_job_streaming(api_url: str, job_id: str):
    """
    Monitor job progress using Server-Sent Events (streaming)

    Requires: pip install sseclient-py
    """
    try:
        from sseclient import SSEClient
    except ImportError:
        print("Error: sseclient-py not installed. Run: pip install sseclient-py")
        return

    print(f"\nMonitoring job {job_id} (streaming)...")
    print("-" * 80)

    url = f"{api_url}/jobs/{job_id}/stream"
    client = SSEClient(url)

    for event in client.events():
        import json
        data = json.loads(event.data)

        if "error" in data:
            print(f"\n✗ Error: {data['error']}")
            break

        # Display progress
        status = data["status"]
        lines = data["lines_processed"]
        rate = data["lines_per_second"]
        percent = data.get("percent_complete")
        eta = data.get("estimated_seconds_remaining")

        if percent is not None:
            print(f"\r[{status.upper()}] {percent:.1f}% complete | "
                  f"{lines:,} lines | {rate:,.0f} lines/sec", end="")
            if eta:
                print(f" | ETA: {eta:.0f}s", end="")
        else:
            print(f"\r[{status.upper()}] {lines:,} lines | {rate:,.0f} lines/sec", end="")

        sys.stdout.flush()

        # Check if completed
        if status in ["completed", "failed"]:
            print()  # New line
            if status == "completed":
                print(f"\n✓ Import completed successfully!")
                print(f"  Total lines: {lines:,}")
            else:
                print(f"\n✗ Import failed")
            break


def list_jobs(api_url: str):
    """List all jobs"""
    response = requests.get(f"{api_url}/jobs")
    response.raise_for_status()
    return response.json()


def main():
    """Example usage"""

    # Configuration
    API_URL = "http://localhost:8000"
    CSV_FILE = "/path/to/your/data.csv"
    DATABASE = "test_db"
    COLLECTION = "test_collection"

    # Check API health
    print("Checking API health...")
    health = requests.get(f"{API_URL}/health").json()
    print(f"API Status: {health['status']}")
    print(f"MongoDB: {'✓ Connected' if health['mongodb_reachable'] else '✗ Not connected'}")

    # Start import job
    job_id = start_import_job(API_URL, CSV_FILE, DATABASE, COLLECTION)

    # Method 1: Poll for progress
    monitor_job_polling(API_URL, job_id, poll_interval=0.5)

    # Method 2: Stream progress (uncomment to use)
    # monitor_job_streaming(API_URL, job_id)

    # List all jobs
    print("\nAll jobs:")
    jobs = list_jobs(API_URL)
    for jid in jobs:
        progress = get_job_progress(API_URL, jid)
        print(f"  {jid}: {progress['status']}")


if __name__ == "__main__":
    # Simple usage example
    import sys

    if len(sys.argv) < 4:
        print("Usage: python rest_api_progress_client.py <csv_file> <database> <collection>")
        print("\nExample:")
        print("  python rest_api_progress_client.py data.csv mydb mycol")
        sys.exit(1)

    API_URL = "http://localhost:8000"
    CSV_FILE = sys.argv[1]
    DATABASE = sys.argv[2]
    COLLECTION = sys.argv[3]

    # Start job
    job_id = start_import_job(API_URL, CSV_FILE, DATABASE, COLLECTION)

    # Monitor with polling
    monitor_job_polling(API_URL, job_id, poll_interval=0.5)
