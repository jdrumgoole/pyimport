"""
Pytest configuration for end-to-end tests.
Changes working directory to test/test_e2e so relative paths work.
"""
import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def change_test_dir():
    """Change to test_e2e directory for relative path compatibility"""
    original_dir = os.getcwd()
    test_dir = os.path.dirname(__file__)
    os.chdir(test_dir)
    yield
    os.chdir(original_dir)
