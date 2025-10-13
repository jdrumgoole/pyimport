"""
Pytest configuration for test_command directory.

This conftest.py file ensures tests run in the correct directory
so they can find their test data files.
"""
import os
import pytest


@pytest.fixture(scope="function", autouse=True)
def change_test_dir(request, monkeypatch):
    """
    Change to the test file's directory for each test.

    This allows tests to reference data files without full paths.
    The fixture automatically reverts to the original directory after the test.
    """
    # Get the directory containing the test file
    test_dir = os.path.dirname(request.fspath)

    # Change to that directory for the test
    monkeypatch.chdir(test_dir)
