"""
Tests for authentication endpoints in the REST API

Tests user registration, login, and authentication flow.
"""

import pytest
from fastapi.testclient import TestClient
from pyimport.rest_api import app
from pyimport.auth import users_db


@pytest.fixture
def client():
    """Create a test client"""
    return TestClient(app)


@pytest.fixture(autouse=True)
def clear_users():
    """Clear users database before each test"""
    users_db.clear()
    yield
    users_db.clear()


def test_health_endpoint(client):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data


def test_register_user(client):
    """Test user registration"""
    response = client.post(
        "/users/register",
        json={
            "username": "testuser",
            "password": "testpass123",
            "email": "test@example.com",
            "full_name": "Test User"
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert data["username"] == "testuser"
    assert data["email"] == "test@example.com"
    assert data["full_name"] == "Test User"
    assert data["disabled"] == False
    assert "password" not in data
    assert "hashed_password" not in data


def test_register_duplicate_user(client):
    """Test that duplicate usernames are rejected"""
    # Register first user
    client.post(
        "/users/register",
        json={
            "username": "testuser",
            "password": "testpass123"
        }
    )

    # Try to register same username
    response = client.post(
        "/users/register",
        json={
            "username": "testuser",
            "password": "different123"
        }
    )

    assert response.status_code == 400
    assert "already registered" in response.json()["detail"]


def test_login_success(client):
    """Test successful login"""
    # Register user first
    client.post(
        "/users/register",
        json={
            "username": "testuser",
            "password": "testpass123"
        }
    )

    # Login
    response = client.post(
        "/token",
        data={
            "username": "testuser",
            "password": "testpass123"
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert len(data["access_token"]) > 0


def test_login_wrong_password(client):
    """Test login with wrong password"""
    # Register user
    client.post(
        "/users/register",
        json={
            "username": "testuser",
            "password": "testpass123"
        }
    )

    # Try to login with wrong password
    response = client.post(
        "/token",
        data={
            "username": "testuser",
            "password": "wrongpassword"
        }
    )

    assert response.status_code == 401
    assert "Incorrect username or password" in response.json()["detail"]


def test_login_nonexistent_user(client):
    """Test login with non-existent user"""
    response = client.post(
        "/token",
        data={
            "username": "nonexistent",
            "password": "password123"
        }
    )

    assert response.status_code == 401


def test_get_current_user(client):
    """Test getting current user info"""
    # Register and login
    client.post(
        "/users/register",
        json={
            "username": "testuser",
            "password": "testpass123",
            "email": "test@example.com"
        }
    )

    login_response = client.post(
        "/token",
        data={
            "username": "testuser",
            "password": "testpass123"
        }
    )
    token = login_response.json()["access_token"]

    # Get user info
    response = client.get(
        "/users/me",
        headers={"Authorization": f"Bearer {token}"}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["username"] == "testuser"
    assert data["email"] == "test@example.com"


def test_get_current_user_without_token(client):
    """Test that /users/me requires authentication"""
    response = client.get("/users/me")
    assert response.status_code == 401


def test_get_current_user_with_invalid_token(client):
    """Test /users/me with invalid token"""
    response = client.get(
        "/users/me",
        headers={"Authorization": "Bearer invalid_token"}
    )
    assert response.status_code == 401


def test_protected_import_endpoint_requires_auth(client):
    """Test that import endpoint requires authentication"""
    response = client.post(
        "/import",
        json={
            "filename": "/path/to/file.csv",
            "database": "testdb",
            "collection": "testcol"
        }
    )
    assert response.status_code == 401


def test_protected_import_endpoint_with_auth(client):
    """Test import endpoint with valid authentication"""
    # Register and login
    client.post(
        "/users/register",
        json={"username": "testuser", "password": "testpass123"}
    )

    login_response = client.post(
        "/token",
        data={"username": "testuser", "password": "testpass123"}
    )
    token = login_response.json()["access_token"]

    # Try import with auth (will fail due to invalid file, but should pass auth)
    response = client.post(
        "/import",
        json={
            "filename": "/nonexistent/file.csv",
            "database": "testdb",
            "collection": "testcol"
        },
        headers={"Authorization": f"Bearer {token}"}
    )

    # Should not be 401 (unauthorized), should be 404 or 500 (file error)
    assert response.status_code != 401


def test_admin_only_endpoint_requires_admin(client):
    """Test that drop collection endpoint requires admin"""
    # Register regular user and login
    client.post(
        "/users/register",
        json={"username": "regularuser", "password": "testpass123"}
    )

    login_response = client.post(
        "/token",
        data={"username": "regularuser", "password": "testpass123"}
    )
    token = login_response.json()["access_token"]

    # Try to drop collection as regular user
    response = client.request(
        "DELETE",
        "/collection",
        json={"database": "testdb", "collection": "testcol"},
        headers={"Authorization": f"Bearer {token}"}
    )

    # Should be forbidden (403)
    assert response.status_code == 403


def test_admin_endpoint_with_admin_user(client):
    """Test that admin user can access admin endpoints"""
    # Register admin user and login
    client.post(
        "/users/register",
        json={"username": "admin", "password": "adminpass123"}
    )

    login_response = client.post(
        "/token",
        data={"username": "admin", "password": "adminpass123"}
    )
    token = login_response.json()["access_token"]

    # Try to drop collection as admin (will fail due to invalid collection, but should pass auth)
    response = client.request(
        "DELETE",
        "/collection",
        json={"database": "testdb", "collection": "testcol"},
        headers={"Authorization": f"Bearer {token}"}
    )

    # Should not be 403 (forbidden), should be 500 (operation error)
    assert response.status_code != 403
