"""
Tests for audit progress tracking for restart capability.

@author: jdrumgoole
"""
import pytest
from pymongo import MongoClient

from pyimport.audit import Audit
from test.mdbtest import MDBTestDB


@pytest.fixture(scope="function")
def test_db():
    """Create a test database context"""
    with MDBTestDB(db_name="TEST_AUDIT", collection_name="audit") as db:
        yield db


@pytest.fixture(scope="function")
def audit():
    """Create an Audit instance for testing"""
    import pymongo
    audit_instance = Audit(
        host="mongodb://localhost:27017",
        database_name="TEST_AUDIT",
        collection_name="test_audit_progress"
    )
    # Clean up collection before each test
    audit_instance.audit_collection().delete_many({})
    yield audit_instance
    # Clean up after test
    audit_instance.audit_collection().delete_many({})


def test_record_progress_basic(audit):
    """Test basic progress recording"""
    batch_id = 12345
    filename = "test_data.csv"

    result = audit.record_progress(
        batch_id=batch_id,
        filename=filename,
        docs_written=1000,
        status="in_progress"
    )

    assert result.inserted_id is not None

    # Verify the document was inserted
    progress = audit.get_file_progress(batch_id, filename)
    assert progress is not None
    assert progress["batch_id"] == batch_id
    assert progress["progress"]["filename"] == filename
    assert progress["progress"]["docs_written"] == 1000
    assert progress["progress"]["status"] == "in_progress"
    assert "timestamp" in progress


def test_record_progress_with_optional_fields(audit):
    """Test progress recording with optional fields"""
    batch_id = 12346
    filename = "test_data2.csv"

    result = audit.record_progress(
        batch_id=batch_id,
        filename=filename,
        docs_written=5000,
        last_line_number=5000,
        file_position=2048576,
        status="in_progress"
    )

    assert result.inserted_id is not None

    progress = audit.get_file_progress(batch_id, filename)
    assert progress["progress"]["last_line_number"] == 5000
    assert progress["progress"]["file_position"] == 2048576


def test_get_file_progress_latest(audit):
    """Test that get_file_progress returns the most recent record"""
    import time
    batch_id = 12347
    filename = "test_data3.csv"

    # Insert multiple progress records
    audit.record_progress(batch_id, filename, docs_written=1000)
    time.sleep(0.001)
    audit.record_progress(batch_id, filename, docs_written=2000)
    time.sleep(0.001)
    audit.record_progress(batch_id, filename, docs_written=3000)

    # Should get the latest one
    progress = audit.get_file_progress(batch_id, filename)
    assert progress["progress"]["docs_written"] == 3000


def test_get_batch_progress(audit):
    """Test retrieving all progress for a batch"""
    batch_id = 12348

    # Record progress for multiple files
    audit.record_progress(batch_id, "file1.csv", docs_written=1000)
    audit.record_progress(batch_id, "file2.csv", docs_written=2000)
    audit.record_progress(batch_id, "file3.csv", docs_written=3000)

    progress_list = audit.get_batch_progress(batch_id)
    assert len(progress_list) == 3

    # Verify they all belong to the same batch
    for progress in progress_list:
        assert progress["batch_id"] == batch_id


def test_mark_file_completed(audit):
    """Test marking a file as completed"""
    batch_id = 12349
    filename = "completed_file.csv"

    result = audit.mark_file_completed(batch_id, filename, total_docs=10000)
    assert result.inserted_id is not None

    progress = audit.get_file_progress(batch_id, filename)
    assert progress["progress"]["status"] == "completed"
    assert progress["progress"]["docs_written"] == 10000


def test_get_completed_files(audit):
    """Test retrieving completed files"""
    batch_id = 12350

    # Mark some files as completed
    audit.mark_file_completed(batch_id, "file1.csv", total_docs=5000)
    audit.mark_file_completed(batch_id, "file2.csv", total_docs=6000)

    # Add an in-progress file
    audit.record_progress(batch_id, "file3.csv", docs_written=3000, status="in_progress")

    completed = audit.get_completed_files(batch_id)
    assert len(completed) == 2
    assert "file1.csv" in completed
    assert "file2.csv" in completed
    assert "file3.csv" not in completed


def test_get_incomplete_files(audit):
    """Test retrieving incomplete files"""
    batch_id = 12351

    # Add completed files
    audit.mark_file_completed(batch_id, "file1.csv", total_docs=5000)

    # Add incomplete files
    audit.record_progress(batch_id, "file2.csv", docs_written=3000, status="in_progress")
    audit.record_progress(batch_id, "file3.csv", docs_written=1000, status="in_progress")

    incomplete = audit.get_incomplete_files(batch_id)
    assert len(incomplete) == 2

    filenames = [doc["progress"]["filename"] for doc in incomplete]
    assert "file2.csv" in filenames
    assert "file3.csv" in filenames
    assert "file1.csv" not in filenames


def test_get_last_incomplete_batch(audit):
    """Test finding the last incomplete batch"""
    # Create a completed batch
    batch_id_1 = 12352
    audit.add_batch_info({"batchID": batch_id_1, "start": "2024-01-01"})
    audit.add_batch_info({"batchID": batch_id_1, "end": "2024-01-01"})

    # Create an incomplete batch (no end)
    batch_id_2 = 12353
    audit.add_batch_info({"batchID": batch_id_2, "start": "2024-01-02"})

    incomplete = audit.get_last_incomplete_batch()
    assert incomplete is not None
    assert incomplete["batchID"] == batch_id_2


def test_progress_tracking_workflow(audit):
    """Test a complete restart workflow"""
    import time
    batch_id = 12354

    # Simulate importing multiple files with checkpoints
    files = ["data.csv.1", "data.csv.2", "data.csv.3", "data.csv.4"]

    # File 1: Completed
    audit.record_progress(batch_id, files[0], docs_written=10000)
    time.sleep(0.001)  # Ensure different timestamps
    audit.record_progress(batch_id, files[0], docs_written=20000)
    time.sleep(0.001)
    audit.mark_file_completed(batch_id, files[0], total_docs=25000)

    # File 2: Completed
    time.sleep(0.001)
    audit.mark_file_completed(batch_id, files[1], total_docs=30000)

    # File 3: In progress (had some checkpoints)
    time.sleep(0.001)
    audit.record_progress(batch_id, files[2], docs_written=5000, last_line_number=5000)
    time.sleep(0.001)
    audit.record_progress(batch_id, files[2], docs_written=10000, last_line_number=10000)

    # File 4: Not started (no progress record)

    # Now simulate restart - check what we need to do
    completed = audit.get_completed_files(batch_id)
    assert len(completed) == 2
    assert files[0] in completed
    assert files[1] in completed

    # Get progress for file 3
    file3_progress = audit.get_file_progress(batch_id, files[2])
    assert file3_progress is not None
    assert file3_progress["progress"]["docs_written"] == 10000
    assert file3_progress["progress"]["last_line_number"] == 10000
    assert file3_progress["progress"]["status"] == "in_progress"

    # File 4 should have no progress
    file4_progress = audit.get_file_progress(batch_id, files[3])
    assert file4_progress is None


def test_multiple_batches_isolation(audit):
    """Test that batches are properly isolated"""
    batch_id_1 = 12355
    batch_id_2 = 12356

    # Add progress to both batches
    audit.record_progress(batch_id_1, "file1.csv", docs_written=1000)
    audit.record_progress(batch_id_2, "file1.csv", docs_written=2000)

    # Verify isolation
    progress_1 = audit.get_file_progress(batch_id_1, "file1.csv")
    progress_2 = audit.get_file_progress(batch_id_2, "file1.csv")

    assert progress_1["progress"]["docs_written"] == 1000
    assert progress_2["progress"]["docs_written"] == 2000

    # Check batch-specific queries
    batch_1_progress = audit.get_batch_progress(batch_id_1)
    batch_2_progress = audit.get_batch_progress(batch_id_2)

    assert len(batch_1_progress) == 1
    assert len(batch_2_progress) == 1


def test_checkpoint_intervals(audit):
    """Test recording checkpoints at intervals"""
    import time
    batch_id = 12357
    filename = "large_file.csv"
    checkpoint_interval = 10000

    # Simulate importing with checkpoints every 10K docs
    for i in range(1, 6):
        docs_written = i * checkpoint_interval
        audit.record_progress(
            batch_id=batch_id,
            filename=filename,
            docs_written=docs_written,
            last_line_number=docs_written,
            status="in_progress"
        )
        time.sleep(0.001)  # Ensure different timestamps

    # Mark as completed
    audit.mark_file_completed(batch_id, filename, total_docs=50000)

    # Verify we have multiple checkpoints plus completion
    all_progress = audit.get_batch_progress(batch_id)
    assert len(all_progress) == 6  # 5 checkpoints + 1 completion

    # Latest should be completed
    latest = audit.get_file_progress(batch_id, filename)
    assert latest["progress"]["status"] == "completed"
    assert latest["progress"]["docs_written"] == 50000
