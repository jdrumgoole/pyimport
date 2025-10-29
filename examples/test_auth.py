"""
Test script for PyImport REST API authentication

This script demonstrates and tests the authentication workflow:
1. Start the API server
2. Register a new user
3. Login and get access token
4. Use token to access protected endpoints

Usage:
    # Make sure the API server is running first:
    # poetry run pyimport-api --port 8000

    # Then run this script:
    python examples/test_auth.py
"""

import requests
import sys
from time import sleep

API_BASE = "http://localhost:8000"


def test_health():
    """Test health endpoint (no auth required)"""
    print("\n1. Testing health endpoint...")
    response = requests.get(f"{API_BASE}/health")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    return response.status_code == 200


def test_register(username, password):
    """Test user registration"""
    print(f"\n2. Registering user '{username}'...")
    response = requests.post(
        f"{API_BASE}/users/register",
        json={
            "username": username,
            "password": password,
            "email": f"{username}@example.com",
            "full_name": f"Test User {username}"
        }
    )
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(f"   User created: {response.json()}")
        return True
    else:
        print(f"   Error: {response.json()}")
        # If user already exists, that's okay for testing
        return response.status_code == 400 and "already registered" in response.json().get("detail", "")


def test_login(username, password):
    """Test user login and get access token"""
    print(f"\n3. Logging in as '{username}'...")
    response = requests.post(
        f"{API_BASE}/token",
        data={
            "username": username,
            "password": password
        }
    )
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        token = data["access_token"]
        print(f"   Access token: {token[:20]}...{token[-20:]}")
        return token
    else:
        print(f"   Error: {response.json()}")
        return None


def test_me(token):
    """Test getting current user info"""
    print(f"\n4. Getting current user info...")
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{API_BASE}/users/me", headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(f"   User info: {response.json()}")
        return True
    else:
        print(f"   Error: {response.json()}")
        return False


def test_protected_without_auth():
    """Test accessing protected endpoint without authentication"""
    print(f"\n5. Testing protected endpoint WITHOUT authentication (should fail)...")
    response = requests.get(f"{API_BASE}/users/me")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    return response.status_code == 401


def test_protected_with_invalid_token():
    """Test accessing protected endpoint with invalid token"""
    print(f"\n6. Testing protected endpoint with INVALID token (should fail)...")
    headers = {"Authorization": "Bearer invalid_token_here"}
    response = requests.get(f"{API_BASE}/users/me", headers=headers)
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    return response.status_code == 401


def main():
    print("=" * 60)
    print("PyImport REST API Authentication Test")
    print("=" * 60)

    # Check if server is running
    try:
        requests.get(f"{API_BASE}/health", timeout=2)
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: API server is not running!")
        print("   Please start the server first:")
        print("   poetry run pyimport-api --port 8000")
        sys.exit(1)

    # Test credentials
    username = "testuser"
    password = "testpassword123"

    # Run tests
    results = []

    results.append(("Health Check", test_health()))
    results.append(("User Registration", test_register(username, password)))

    token = test_login(username, password)
    results.append(("User Login", token is not None))

    if token:
        results.append(("Get Current User", test_me(token)))
    else:
        results.append(("Get Current User", False))

    results.append(("Protected Without Auth", test_protected_without_auth()))
    results.append(("Protected With Invalid Token", test_protected_with_invalid_token()))

    # Print summary
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)

    passed = 0
    failed = 0

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
        if result:
            passed += 1
        else:
            failed += 1

    print("\n" + "=" * 60)
    print(f"Total: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed == 0:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
