"""
Pytest configuration and fixtures for GUI tests using Playwright

This module provides shared fixtures for testing the PyImport web interface.
"""

import pytest
import subprocess
import time
import requests
import tempfile
import csv
from pathlib import Path


# Test data constants
TEST_SERVER_URL = "http://localhost:8000"
TEST_USERNAME = "testuser"
TEST_PASSWORD = "testpassword123"
TEST_ADMIN_USERNAME = "admin"
TEST_ADMIN_PASSWORD = "admin"


@pytest.fixture(scope="session")
def server_url():
    """Return the base URL for the test server"""
    return TEST_SERVER_URL


@pytest.fixture(scope="session")
def pyimport_server():
    """
    Start PyImport server for the test session.

    The server is started in the background and cleaned up after all tests.
    """
    # Check if server is already running
    try:
        response = requests.get(f"{TEST_SERVER_URL}/health", timeout=2)
        if response.status_code == 200:
            print(f"Server already running at {TEST_SERVER_URL}")
            yield TEST_SERVER_URL
            return
    except requests.exceptions.RequestException:
        pass

    # Start the server
    print(f"Starting PyImport server at {TEST_SERVER_URL}...")
    process = subprocess.Popen(
        ["poetry", "run", "pyimport-server", "--host", "0.0.0.0", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait for server to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{TEST_SERVER_URL}/health", timeout=2)
            if response.status_code == 200:
                print(f"Server ready after {i+1} attempts")
                break
        except requests.exceptions.RequestException:
            if i == max_retries - 1:
                process.terminate()
                raise RuntimeError("Server failed to start within timeout")
            time.sleep(1)

    yield TEST_SERVER_URL

    # Cleanup: stop the server
    print("Stopping PyImport server...")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Configure browser context for all tests"""
    return {
        **browser_context_args,
        "viewport": {
            "width": 1280,
            "height": 720,
        },
        "ignore_https_errors": True,
    }


@pytest.fixture
def test_csv_file():
    """
    Create a temporary CSV file for testing imports.

    Returns:
        Path: Path to the temporary CSV file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['id', 'name', 'email', 'age', 'signup_date'])
        # Write test data
        writer.writerow(['1', 'Alice Smith', 'alice@example.com', '28', '2024-01-15'])
        writer.writerow(['2', 'Bob Jones', 'bob@example.com', '35', '2024-02-20'])
        writer.writerow(['3', 'Charlie Brown', 'charlie@example.com', '42', '2024-03-10'])
        writer.writerow(['4', 'Diana Prince', 'diana@example.com', '31', '2024-04-05'])
        writer.writerow(['5', 'Eve Wilson', 'eve@example.com', '29', '2024-05-12'])

        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def large_test_csv_file():
    """
    Create a larger temporary CSV file for testing progress tracking.

    Returns:
        Path: Path to the temporary CSV file with 1000 rows
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['id', 'name', 'value', 'timestamp'])
        # Write 1000 rows of test data
        for i in range(1, 1001):
            writer.writerow([str(i), f'Item_{i}', str(i * 10.5), f'2024-01-{(i % 28) + 1:02d}'])

        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def authenticated_page(page, pyimport_server, server_url):
    """
    Provide a Playwright page that is already authenticated.

    This fixture logs in as the test user before yielding the page.
    """
    # Navigate to the application
    page.goto(server_url)

    # Register a test user first (if not already registered)
    page.click('button.tab:has-text("Register")')
    page.wait_for_selector('#register.tab-content.active', timeout=5000)

    page.fill('#reg-username', TEST_USERNAME)
    page.fill('#reg-password', TEST_PASSWORD)
    page.fill('#reg-email', f'{TEST_USERNAME}@example.com')
    page.fill('#reg-fullname', 'Test User')

    try:
        # Click the submit button within the register form
        page.locator('#register form button[type="submit"]').click()
        # Wait a moment for registration
        page.wait_for_timeout(1000)
    except Exception:
        # User might already exist, that's okay
        pass

    # Now login
    page.click('button.tab:has-text("Login")')
    page.wait_for_selector('#login.tab-content.active', timeout=5000)

    page.fill('#login-username', TEST_USERNAME)
    page.fill('#login-password', TEST_PASSWORD)
    # Click the submit button within the login form
    page.locator('#login form button[type="submit"]').click()

    # Wait for authentication to complete and authenticated view to show
    page.wait_for_selector('body.authenticated', timeout=10000)
    page.wait_for_selector('.nav-logout', state='visible', timeout=10000)

    yield page

    # Cleanup: logout
    try:
        page.click('.nav-logout')
    except Exception:
        pass


@pytest.fixture
def admin_page(page, pyimport_server, server_url):
    """
    Provide a Playwright page authenticated as admin user.

    This fixture logs in as admin before yielding the page.
    """
    # Navigate to the application
    page.goto(server_url)

    # Login as admin
    page.click('button:has-text("Login")')
    page.fill('#login-username', TEST_ADMIN_USERNAME)
    page.fill('#login-password', TEST_ADMIN_PASSWORD)
    page.click('button[type="submit"]')

    # Wait for authentication to complete
    page.wait_for_selector('.nav-logout', timeout=5000)

    # Check if first login - if so, change password
    try:
        if page.locator('#profile-status').is_visible(timeout=1000):
            # First login, need to change password
            page.click('.nav-profile')
            page.fill('#current-password', TEST_ADMIN_PASSWORD)
            page.fill('#new-password', 'NewAdminPassword123')
            page.fill('#confirm-password', 'NewAdminPassword123')
            page.click('button[type="submit"]:has-text("Change Password")')
            page.wait_for_selector('.status.success', timeout=5000)
    except Exception:
        # Not first login, continue
        pass

    yield page

    # Cleanup: logout
    try:
        page.click('.nav-logout')
    except Exception:
        pass
