"""
Tests for async audit progress tracking for restart capability.

@author: jdrumgoole
"""
import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from pyimport.asyncaudit import AsyncAudit
from pyimport.monotonicid import MonotonicID


@pytest_asyncio.fixture(scope="function")
async def mongo_client():
    """Create async MongoDB client"""
    client = AsyncIOMotorClient(host="mongodb://localhost:27017")
    yield client
    client.close()


@pytest_asyncio.fixture(scope="function")
async def audit_db(mongo_client):
    """Create test database"""
    db = mongo_client["TEST_ASYNC_AUDIT"]
    yield db
    # Cleanup
    await mongo_client.drop_database("TEST_ASYNC_AUDIT")


@pytest_asyncio.fixture(scope="function")
async def audit(audit_db):
    """Create AsyncAudit instance"""
    audit_instance = AsyncAudit(database=audit_db, collection_name="test_audit_progress")
    # Clean up collection before each test
    await audit_instance.collection.delete_many({})
    yield audit_instance
    # Clean up after test
    await audit_instance.collection.delete_many({})


@pytest.mark.asyncio
async def test_record_progress_basic(audit):
    """Test basic async progress recording"""
    batch_id = MonotonicID()
    filename = "test_data.csv"

    result = await audit.record_progress(
        batch_id=batch_id,
        filename=filename,
        docs_written=1000,
        status="in_progress"
    )

    assert result.inserted_id is not None

    # Verify the document was inserted
    progress = await audit.get_file_progress(batch_id, filename)
    assert progress is not None
    assert progress["batch_id"] == batch_id.id
    assert progress["progress"]["filename"] == filename
    assert progress["progress"]["docs_written"] == 1000
    assert progress["progress"]["status"] == "in_progress"
    assert "timestamp" in progress


@pytest.mark.asyncio
async def test_record_progress_with_optional_fields(audit):
    """Test async progress recording with optional fields"""
    batch_id = MonotonicID()
    filename = "test_data2.csv"

    result = await audit.record_progress(
        batch_id=batch_id,
        filename=filename,
        docs_written=5000,
        last_line_number=5000,
        file_position=2048576,
        status="in_progress"
    )

    assert result.inserted_id is not None

    progress = await audit.get_file_progress(batch_id, filename)
    assert progress["progress"]["last_line_number"] == 5000
    assert progress["progress"]["file_position"] == 2048576


@pytest.mark.asyncio
async def test_get_file_progress_latest(audit):
    """Test that get_file_progress returns the most recent record"""
    import asyncio
    batch_id = MonotonicID()
    filename = "test_data3.csv"

    # Insert multiple progress records
    await audit.record_progress(batch_id, filename, docs_written=1000)
    await asyncio.sleep(0.001)
    await audit.record_progress(batch_id, filename, docs_written=2000)
    await asyncio.sleep(0.001)
    await audit.record_progress(batch_id, filename, docs_written=3000)

    # Should get the latest one
    progress = await audit.get_file_progress(batch_id, filename)
    assert progress["progress"]["docs_written"] == 3000


@pytest.mark.asyncio
async def test_get_batch_progress(audit):
    """Test retrieving all progress for a batch"""
    batch_id = MonotonicID()

    # Record progress for multiple files
    await audit.record_progress(batch_id, "file1.csv", docs_written=1000)
    await audit.record_progress(batch_id, "file2.csv", docs_written=2000)
    await audit.record_progress(batch_id, "file3.csv", docs_written=3000)

    progress_list = await audit.get_batch_progress(batch_id)
    assert len(progress_list) == 3

    # Verify they all belong to the same batch
    for progress in progress_list:
        assert progress["batch_id"] == batch_id.id


@pytest.mark.asyncio
async def test_mark_file_completed(audit):
    """Test marking a file as completed"""
    batch_id = MonotonicID()
    filename = "completed_file.csv"

    result = await audit.mark_file_completed(batch_id, filename, total_docs=10000)
    assert result.inserted_id is not None

    progress = await audit.get_file_progress(batch_id, filename)
    assert progress["progress"]["status"] == "completed"
    assert progress["progress"]["docs_written"] == 10000


@pytest.mark.asyncio
async def test_get_completed_files(audit):
    """Test retrieving completed files"""
    batch_id = MonotonicID()

    # Mark some files as completed
    await audit.mark_file_completed(batch_id, "file1.csv", total_docs=5000)
    await audit.mark_file_completed(batch_id, "file2.csv", total_docs=6000)

    # Add an in-progress file
    await audit.record_progress(batch_id, "file3.csv", docs_written=3000, status="in_progress")

    completed = await audit.get_completed_files(batch_id)
    assert len(completed) == 2
    assert "file1.csv" in completed
    assert "file2.csv" in completed
    assert "file3.csv" not in completed


@pytest.mark.asyncio
async def test_get_incomplete_files(audit):
    """Test retrieving incomplete files"""
    batch_id = MonotonicID()

    # Add completed files
    await audit.mark_file_completed(batch_id, "file1.csv", total_docs=5000)

    # Add incomplete files
    await audit.record_progress(batch_id, "file2.csv", docs_written=3000, status="in_progress")
    await audit.record_progress(batch_id, "file3.csv", docs_written=1000, status="in_progress")

    incomplete = await audit.get_incomplete_files(batch_id)
    assert len(incomplete) == 2

    filenames = [doc["progress"]["filename"] for doc in incomplete]
    assert "file2.csv" in filenames
    assert "file3.csv" in filenames
    assert "file1.csv" not in filenames


@pytest.mark.asyncio
async def test_get_last_incomplete_batch(audit):
    """Test finding the last incomplete batch"""
    # Create a completed batch
    batch_id_1 = await audit.start_batch({"test": "batch1"})
    await audit.end_batch(batch_id_1, {"result": "success"})

    # Create an incomplete batch (no end)
    batch_id_2 = await audit.start_batch({"test": "batch2"})

    incomplete = await audit.get_last_incomplete_batch()
    assert incomplete is not None
    assert incomplete["batch_id"] == batch_id_2.id


@pytest.mark.asyncio
async def test_progress_tracking_workflow(audit):
    """Test a complete async restart workflow"""
    import asyncio
    batch_id = MonotonicID()

    # Simulate importing multiple files with checkpoints
    files = ["data.csv.1", "data.csv.2", "data.csv.3", "data.csv.4"]

    # File 1: Completed
    await audit.record_progress(batch_id, files[0], docs_written=10000)
    await asyncio.sleep(0.001)
    await audit.record_progress(batch_id, files[0], docs_written=20000)
    await asyncio.sleep(0.001)
    await audit.mark_file_completed(batch_id, files[0], total_docs=25000)

    # File 2: Completed
    await asyncio.sleep(0.001)
    await audit.mark_file_completed(batch_id, files[1], total_docs=30000)

    # File 3: In progress (had some checkpoints)
    await asyncio.sleep(0.001)
    await audit.record_progress(batch_id, files[2], docs_written=5000, last_line_number=5000)
    await asyncio.sleep(0.001)
    await audit.record_progress(batch_id, files[2], docs_written=10000, last_line_number=10000)

    # File 4: Not started (no progress record)

    # Now simulate restart - check what we need to do
    completed = await audit.get_completed_files(batch_id)
    assert len(completed) == 2
    assert files[0] in completed
    assert files[1] in completed

    # Get progress for file 3
    file3_progress = await audit.get_file_progress(batch_id, files[2])
    assert file3_progress is not None
    assert file3_progress["progress"]["docs_written"] == 10000
    assert file3_progress["progress"]["last_line_number"] == 10000
    assert file3_progress["progress"]["status"] == "in_progress"

    # File 4 should have no progress
    file4_progress = await audit.get_file_progress(batch_id, files[3])
    assert file4_progress is None


@pytest.mark.asyncio
async def test_multiple_batches_isolation(audit):
    """Test that batches are properly isolated"""
    batch_id_1 = MonotonicID()
    batch_id_2 = MonotonicID()

    # Add progress to both batches
    await audit.record_progress(batch_id_1, "file1.csv", docs_written=1000)
    await audit.record_progress(batch_id_2, "file1.csv", docs_written=2000)

    # Verify isolation
    progress_1 = await audit.get_file_progress(batch_id_1, "file1.csv")
    progress_2 = await audit.get_file_progress(batch_id_2, "file1.csv")

    assert progress_1["progress"]["docs_written"] == 1000
    assert progress_2["progress"]["docs_written"] == 2000

    # Check batch-specific queries
    batch_1_progress = await audit.get_batch_progress(batch_id_1)
    batch_2_progress = await audit.get_batch_progress(batch_id_2)

    assert len(batch_1_progress) == 1
    assert len(batch_2_progress) == 1


@pytest.mark.asyncio
async def test_checkpoint_intervals(audit):
    """Test recording checkpoints at intervals"""
    import asyncio
    batch_id = MonotonicID()
    filename = "large_file.csv"
    checkpoint_interval = 10000

    # Simulate importing with checkpoints every 10K docs
    for i in range(1, 6):
        docs_written = i * checkpoint_interval
        await audit.record_progress(
            batch_id=batch_id,
            filename=filename,
            docs_written=docs_written,
            last_line_number=docs_written,
            status="in_progress"
        )
        await asyncio.sleep(0.001)  # Ensure different timestamps

    # Mark as completed
    await audit.mark_file_completed(batch_id, filename, total_docs=50000)

    # Verify we have multiple checkpoints plus completion
    all_progress = await audit.get_batch_progress(batch_id)
    assert len(all_progress) == 6  # 5 checkpoints + 1 completion

    # Latest should be completed
    latest = await audit.get_file_progress(batch_id, filename)
    assert latest["progress"]["status"] == "completed"
    assert latest["progress"]["docs_written"] == 50000


@pytest.mark.asyncio
async def test_integration_with_batch_lifecycle(audit):
    """Test progress tracking integrated with batch start/end"""
    # Start a batch
    batch_id = await audit.start_batch({"operation": "import", "files": 3})

    # Record progress for files
    await audit.record_progress(batch_id, "file1.csv", docs_written=5000)
    await audit.mark_file_completed(batch_id, "file1.csv", total_docs=5000)

    await audit.record_progress(batch_id, "file2.csv", docs_written=3000)
    await audit.mark_file_completed(batch_id, "file2.csv", total_docs=3000)

    await audit.record_progress(batch_id, "file3.csv", docs_written=7000)
    await audit.mark_file_completed(batch_id, "file3.csv", total_docs=7000)

    # End the batch
    await audit.end_batch(batch_id, {"total_docs": 15000})

    # Verify all files completed
    completed = await audit.get_completed_files(batch_id)
    assert len(completed) == 3

    # Verify batch is complete
    batch = await audit.get_batch(batch_id)
    assert "end" in batch or await audit.is_complete(batch_id)


@pytest.mark.asyncio
async def test_concurrent_progress_recording(audit):
    """Test that concurrent progress recording works correctly"""
    import asyncio

    batch_id = MonotonicID()
    files = [f"file{i}.csv" for i in range(10)]

    # Simulate concurrent progress recording
    tasks = [
        audit.record_progress(batch_id, filename, docs_written=1000 * (i + 1))
        for i, filename in enumerate(files)
    ]

    results = await asyncio.gather(*tasks)
    assert len(results) == 10

    # Verify all were recorded
    progress_list = await audit.get_batch_progress(batch_id)
    assert len(progress_list) == 10
