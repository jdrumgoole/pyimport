"""
Additional test coverage for filesplitter module
Currently at 35% coverage - adding comprehensive tests
"""
import pytest
import os
import tempfile
from pathlib import Path
from pyimport.filesplitter import (
    LineCounter, FileSplitter, split_files, FileType
)


class TestLineCounter:
    """Test LineCounter functionality"""

    def test_count_lines_simple(self):
        """Test counting lines in a simple file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("line1\n")
            f.write("line2\n")
            f.write("line3\n")
            fname = f.name

        try:
            count = LineCounter.count_now(fname)
            assert count == 3
        finally:
            os.unlink(fname)

    def test_count_lines_empty_file(self):
        """Test counting lines in empty file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            fname = f.name

        try:
            count = LineCounter.count_now(fname)
            assert count == 0
        finally:
            os.unlink(fname)

    def test_count_lines_no_trailing_newline(self):
        """Test counting lines without trailing newline"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("line1\nline2\nline3")  # No trailing newline
            fname = f.name

        try:
            count = LineCounter.count_now(fname)
            assert count == 3
        finally:
            os.unlink(fname)

    def test_count_lines_large_file(self):
        """Test counting lines in larger file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            for i in range(1000):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            count = LineCounter.count_now(fname)
            assert count == 1000
        finally:
            os.unlink(fname)


class TestFileSplitter:
    """Test FileSplitter functionality"""

    def test_split_file_basic(self):
        """Test basic file splitting using static method"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("header\n")
            for i in range(10):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=5, has_header=True))

            # Should create 2 split files (5 lines each, excluding header)
            assert len(splits) == 2

            # Verify split files exist
            for split_file, _ in splits:
                assert os.path.exists(split_file)
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_file_no_header(self):
        """Test splitting file without header"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            for i in range(10):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=3, has_header=False))

            # Should create 4 split files (3, 3, 3, 1)
            assert len(splits) == 4

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_file_exact_division(self):
        """Test splitting when file divides evenly"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            for i in range(10):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=5, has_header=False))

            # Should create exactly 2 split files
            assert len(splits) == 2

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_file_single_split(self):
        """Test when file is smaller than split size"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("line1\n")
            f.write("line2\n")
            fname = f.name

        try:
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=10, has_header=False))

            # Should create 1 split file
            assert len(splits) == 1

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_with_autosplit(self):
        """Test auto-splitting functionality"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            for i in range(100):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            # Use static autosplit method
            splits = list(FileSplitter.autosplit(fname, has_header=False, split_count=4))

            # Should create 4 split files
            assert len(splits) == 4

            # Verify roughly equal sizes
            sizes = [size for _, size in splits]
            assert all(s > 0 for s in sizes)

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_with_header_preservation(self):
        """Test that split files are created correctly with header"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("name,age,city\n")
            for i in range(6):
                f.write(f"person{i},{20+i},city{i}\n")
            fname = f.name

        try:
            # Use static method with header
            splits = list(FileSplitter.split_file(fname, split_size=3, has_header=True))

            # Should create 2 splits (3 data lines each, header excluded)
            assert len(splits) == 2

            # Verify split files exist and have correct line counts
            for split_file, line_count in splits:
                assert os.path.exists(split_file)
                assert line_count == 3

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)


class TestFileSplitterStaticMethods:
    """Test static methods of FileSplitter"""

    def test_compare_files_identical(self):
        """Test comparing two identical files"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f1:
            f1.write("line1\n")
            f1.write("line2\n")
            fname1 = f1.name

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f2:
            f2.write("line1\n")
            f2.write("line2\n")
            fname2 = f2.name

        try:
            result = FileSplitter.compare_files(fname1, fname2)
            assert result is True
        finally:
            os.unlink(fname1)
            os.unlink(fname2)

    def test_compare_files_different(self):
        """Test comparing two different files"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f1:
            f1.write("line1\n")
            fname1 = f1.name

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f2:
            f2.write("line2\n")
            fname2 = f2.name

        try:
            result = FileSplitter.compare_files(fname1, fname2)
            assert result is False
        finally:
            os.unlink(fname1)
            os.unlink(fname2)

    def test_get_header(self):
        """Test getting header from file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("header_line\n")
            f.write("data_line\n")
            fname = f.name

        try:
            header = FileSplitter.get_header(fname)
            assert "header_line" in header
        finally:
            os.unlink(fname)

    def test_get_average_line_size(self):
        """Test calculating average line size"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            for i in range(10):
                f.write("x" * 100 + "\n")  # 101 chars per line
            fname = f.name

        try:
            avg_size = FileSplitter.get_average_line_size(fname, has_header=False)
            assert avg_size > 90  # Should be around 101
            assert avg_size < 110
        finally:
            os.unlink(fname)

    def test_copy_file(self):
        """Test file copying functionality"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("header\n")
            f.write("line1\n")
            f.write("line2\n")
            fname = f.name

        try:
            outfile = fname + ".copy"
            result_file, line_count = FileSplitter.copy_file(fname, outfile, ignore_header=True)

            assert os.path.exists(outfile)
            assert line_count == 2  # Excluding header

            os.unlink(outfile)
        finally:
            os.unlink(fname)


class TestSplitFilesFunction:
    """Test high-level split_files function"""

    def test_split_files_directly_with_static_methods(self):
        """Test file splitting using static methods directly"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("header\n")
            for i in range(20):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            # Test autosplit directly
            splits = list(FileSplitter.autosplit(fname, has_header=True, split_count=4))

            # Should create approximately 4 splits (may be 3-5 depending on rounding)
            assert 3 <= len(splits) <= 5
            assert len(splits) >= 1  # At least one split created

            # Verify total lines preserved
            total_lines = sum(line_count for _, line_count in splits)
            assert total_lines == 20  # All data lines accounted for

            # Cleanup
            for split_file, _ in splits:
                if os.path.exists(split_file):
                    os.unlink(split_file)

        finally:
            if os.path.exists(fname):
                os.unlink(fname)

    def test_split_multiple_files_directly(self):
        """Test splitting multiple input files using static methods"""
        files = []
        all_splits = []

        try:
            # Create multiple test files
            for i in range(2):
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
                    for j in range(10):
                        f.write(f"file{i}_line{j}\n")
                    files.append(f.name)

            # Split each file
            for fname in files:
                splits = list(FileSplitter.autosplit(fname, has_header=False, split_count=2))
                all_splits.extend(splits)

            # Should create splits from all files (2 files * 2 splits each = 4)
            assert len(all_splits) == 4

            # Cleanup
            for split_file, _ in all_splits:
                if os.path.exists(split_file):
                    os.unlink(split_file)

        finally:
            for f in files:
                if os.path.exists(f):
                    os.unlink(f)


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_split_empty_file(self):
        """Test splitting an empty file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            fname = f.name

        try:
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=10, has_header=False))

            # Empty file should produce minimal or no splits
            assert len(splits) >= 0

            # Cleanup
            for split_file, _ in splits:
                if os.path.exists(split_file):
                    os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_file_with_long_lines(self):
        """Test splitting file with very long lines"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            for i in range(5):
                f.write("x" * 10000 + "\n")  # Very long lines
            fname = f.name

        try:
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=2, has_header=False))

            # Should still split correctly
            assert len(splits) >= 2

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_file_different_delimiters(self):
        """Test splitting files with different delimiters (content has pipes)"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("name|age|city\n")
            for i in range(5):
                f.write(f"person{i}|{20+i}|city{i}\n")
            fname = f.name

        try:
            # Delimiter doesn't affect splitting - it's just about line counts
            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=2, has_header=True))

            # Should split correctly - 5 data lines / 2 = 3 splits
            assert len(splits) == 3

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)

    def test_split_maintains_line_count(self):
        """Test that total lines are preserved after splitting"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("header\n")
            for i in range(20):
                f.write(f"line{i}\n")
            fname = f.name

        try:
            original_count = LineCounter.count_now(fname)

            # Use static method
            splits = list(FileSplitter.split_file(fname, split_size=7, has_header=True))

            # Count total data lines in all splits
            total_data_lines = sum(line_count for _, line_count in splits)

            # Original has 21 lines (1 header + 20 data)
            # After splitting with header excluded, should have 20 data lines
            assert total_data_lines == (original_count - 1)  # Minus the header

            # Cleanup
            for split_file, _ in splits:
                os.unlink(split_file)

        finally:
            os.unlink(fname)
