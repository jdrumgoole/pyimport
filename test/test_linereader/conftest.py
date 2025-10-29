"""
Pytest configuration for this test suite.
Changes working directory to the test directory so relative paths work.
"""
import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def change_test_dir():
    """Change to test directory for relative path compatibility"""
    original_dir = os.getcwd()
    test_dir = os.path.dirname(__file__)
    os.chdir(test_dir)
    yield
    os.chdir(original_dir)
