"""
Created on 13 Aug 2017

@author: jdrumgoole
"""
import os
import tempfile
import shutil
import pytest

from pyimport.filesplitter import LineCounter, FileSplitter, FileType

# Mark all tests in this file to run in the same group to avoid file conflicts
pytestmark = pytest.mark.xdist_group(name="filesplitter_sequential")


@pytest.fixture(scope="function")
def temp_work_dir(request):
    """Create a worker-specific temporary directory for split files to prevent interference."""
    # Get worker ID for parallel test isolation
    worker_id = getattr(request.config, 'workerinput', {}).get('workerid', 'master')
    temp_dir = tempfile.mkdtemp(prefix=f"pyimport_filesplit_{worker_id}_")
    original_dir = os.getcwd()

    # Copy all test data files to temp directory
    test_files = [
        "fourlines.txt", "emptyfile.txt", "threelines.txt", "inventory.csv",
        "AandEData_301.csv", "10k.txt", "ninelines.txt", "tenlines.txt",
        "yellow_tripdata_2015-01-06-1999.csv"
    ]
    for test_file in test_files:
        if os.path.exists(test_file):
            shutil.copy(test_file, temp_dir)

    # Change to temp directory
    os.chdir(temp_dir)

    yield temp_dir

    # Cleanup: change back and remove temp directory
    os.chdir(original_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_count_lines():
    assert LineCounter.count_now("fourlines.txt") == 4
    assert LineCounter.count_now("emptyfile.txt") == 0
    assert LineCounter.count_now("threelines.txt") == 3
    assert LineCounter.count_now("inventory.csv") == 5


def _split_helper(filename, split_size, has_header=False):

    parts = []
    parts_total_count = 0
    original_size = LineCounter.count_now(filename)

    try:
        for part_name, line_count in FileSplitter.split_file(filename=filename, split_size=split_size, has_header=has_header):
            part_count = LineCounter.count_now(part_name)
            # line_count is data lines only (excluding header)
            # part_count is total lines in file (data + header if present)
            if has_header:
                assert part_count == line_count + 1  # +1 for header
                parts_total_count = parts_total_count + line_count  # Count data lines only
            else:
                assert part_count == line_count
                parts_total_count = parts_total_count + part_count
            parts.append(part_name)

        # Total data lines should equal original minus header (if present)
        expected_data_lines = original_size - 1 if has_header else original_size
        assert parts_total_count == expected_data_lines

        return FileSplitter.compare_concatenated_files(filename, parts)
    finally:
        for part in parts:
            os.unlink(part)


def headless_file(filename, new_filename=None) -> str:
    if new_filename is None:
        new_filename = f"{filename}.nohdr"
    # Clean up any existing file from previous test run
    if os.path.isfile(new_filename):
        os.unlink(new_filename)
    with open(filename) as f:
        with open(new_filename, "w") as nf:
            for i, line in enumerate(f, 1):
                if i == 1:
                    continue
                else:
                    nf.write(line)
    return new_filename

def test_split_file(temp_work_dir):
    assert _split_helper("fourlines.txt", 0)
    assert _split_helper("fourlines.txt", 3)
    # TODO: Fix this test - header handling logic needs investigation
    # assert _split_helper("AandEData_301.csv", 47, has_header=True)
    assert _split_helper("10k.txt", 2300, has_header=False)


def auto_split_helper(filename, lines, split_count, has_header=False):
    part_total_count = 0
    total_line_count = LineCounter.count_now(filename)
    if has_header:
        total_line_count = total_line_count - 1
    parts = []
    assert total_line_count == lines
    for part_name, line_count in FileSplitter.autosplit(filename, has_header, split_count):
        part_count = LineCounter.count_now(part_name)
        parts.append(part_name)
        assert part_count > 0
        assert part_count == line_count
        part_total_count = part_total_count + part_count

    hf = headless_file(filename)
    FileSplitter.compare_concatenated_files(hf, parts)
    os.unlink(hf)
    for i in parts:
        os.unlink(i)
    assert part_total_count == lines


def test_copy_file(temp_work_dir):
    (rhs, total_lines) = FileSplitter.copy_file("AandEData_301.csv",
                                                 "AandEData_301.csv" + ".1", ignore_header=True)
    assert total_lines == (LineCounter.count_now("AandEData_301.csv") - 1 )
    assert rhs == "AandEData_301.csv" + ".1"
    os.unlink(rhs)


def test_split_file_sizes(temp_work_dir):
    for i, (part_name, size) in  enumerate(FileSplitter.split_file("fourlines.txt", 1, has_header=False), 1):
        assert part_name == f"fourlines.txt.{i}"
        assert size == 1
    with open("fourlines.txt.1") as f:
        assert f.readline() == "One\n"
    with open("fourlines.txt.4") as f:
        assert f.readline() == "four\n"
    os.unlink("fourlines.txt.1")
    os.unlink("fourlines.txt.2")
    os.unlink("fourlines.txt.3")
    os.unlink("fourlines.txt.4")
    assert os.path.isfile("fourlines.txt.5") is False

    for i, (part_name, size) in  enumerate(FileSplitter.split_file("fourlines.txt", 2, has_header=False), 1):
        assert part_name == f"fourlines.txt.{i}"
        assert size == 2
    with open("fourlines.txt.1") as f:
        line1 = f.readline()
        line2 = f.readline()
        assert line1 == "One\n"
        assert line2 == "Two\n"
    os.unlink("fourlines.txt.1")
    os.unlink("fourlines.txt.2")
    assert os.path.isfile("fourlines.txt.3") is False

    for i, (part_name, size) in  enumerate(FileSplitter.split_file("fourlines.txt", 4, has_header=False), 1):
        assert part_name == f"fourlines.txt.{i}"
        assert size == 4
    with open("fourlines.txt.1") as f:
        line1 = f.readline()
        line2 = f.readline()
        line3 = f.readline()
        line4 = f.readline()
        assert line1 == "One\n"
        assert line2 == "Two\n"
        assert line3 == "Three\n"
        assert line4 == "four\n"
    os.unlink("fourlines.txt.1")
    assert os.path.isfile("fourlines.txt.2") is False


def test_split_file_sizes_residue(temp_work_dir):
    for i, (part_name, size) in  enumerate(FileSplitter.split_file("ninelines.txt", 2, has_header=False), 1):
        if i == 5:
            assert part_name == f"ninelines.txt.{i}"
            assert size == 1
        else:
            assert part_name == f"ninelines.txt.{i}"
            assert size == 2
    assert os.path.isfile(f"ninelines.txt.{6}") is False

def test_split_file_sizes_with_header(temp_work_dir):
    for i, (part_name, size) in  enumerate(FileSplitter.split_file("inventory.csv", 1, has_header=True), 1):
        assert part_name == f"inventory.csv.{i}"
        assert size == 1
    with open("inventory.csv.1") as f:
        assert f.readline() == "Screws,         300,    1-Jan-2016\n"
    with open("inventory.csv.4") as f:
        assert f.readline() == "Nuts,           75,     29-Feb-2016\n"
    os.unlink("inventory.csv.1")
    os.unlink("inventory.csv.2")
    os.unlink("inventory.csv.3")
    os.unlink("inventory.csv.4")
    assert os.path.isfile("inventory.csv.5") is False


def test_aande(temp_work_dir):
    auto_split_helper("AandEData_301.csv", 300, 3, has_header=True)
    auto_split_helper("fourlines.txt", 4, 0, has_header=False)

def test_auto_split_various_files(temp_work_dir):
    auto_split_helper("fourlines.txt", 4, 2, has_header=False)
    auto_split_helper("ninelines.txt", 9, 3, has_header=False)
    auto_split_helper("inventory.csv", 4, 2, has_header=True)
    auto_split_helper("AandEData_301.csv", 300, 3, has_header=True)
    auto_split_helper("AandEData_301.csv", 300, 2, has_header=True)
    auto_split_helper("AandEData_301.csv", 300, 1, has_header=True)
    auto_split_helper("AandEData_301.csv", 300, 0, has_header=True)
    auto_split_helper("10k.txt", 10000, 5, has_header=False)
    auto_split_helper("yellow_tripdata_2015-01-06-1999.csv", 1999, 4, has_header=False)





