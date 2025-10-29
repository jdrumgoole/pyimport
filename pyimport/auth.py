"""
Authentication and authorization module for PyImport REST API

Provides user registration, login, and JWT token-based authentication.

Usage:
    # In your FastAPI app
    from pyimport.auth import get_current_user, create_user, authenticate_user

    @app.post("/users/register")
    async def register(username: str, password: str):
        return await create_user(username, password)

    @app.post("/users/login")
    async def login(username: str, password: str):
        user = await authenticate_user(username, password)
        token = create_access_token({"sub": user["username"]})
        return {"access_token": token, "token_type": "bearer"}

    @app.get("/protected")
    async def protected_route(current_user: dict = Depends(get_current_user)):
        return {"message": f"Hello {current_user['username']}"}

@author: Claude Code
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import os

from fastapi import Depends, HTTPException
from fastapi import status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

# Configuration (can be overridden via environment variables)
SECRET_KEY = os.getenv("SECRET_KEY", "pyimport-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

# Password hashing
# Use argon2 as primary algorithm (no 72-byte password limit) with bcrypt as fallback
pwd_context = CryptContext(
    schemes=["argon2", "bcrypt"],
    deprecated="auto",
    argon2__rounds=2  # Fast for development, increase for production
)

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# In-memory user storage (replace with database in production)
users_db: Dict[str, Dict[str, Any]] = {}


# Pydantic models

class User(BaseModel):
    """User model"""
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: bool = False
    first_login: bool = False


class UserInDB(User):
    """User model with hashed password"""
    hashed_password: str


class UserCreate(BaseModel):
    """User creation model"""
    username: str
    password: str
    email: Optional[str] = None
    full_name: Optional[str] = None


class Token(BaseModel):
    """Token response model"""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """Token payload model"""
    username: Optional[str] = None


# Password utilities

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    Hash a password

    Note: bcrypt has a 72-byte password limit, so we truncate if necessary
    """
    # Truncate password to 72 bytes if needed (bcrypt limitation)
    password_bytes = password.encode('utf-8')
    if len(password_bytes) > 72:
        password = password_bytes[:72].decode('utf-8', errors='ignore')

    return pwd_context.hash(password)


# User management

def get_user(username: str) -> Optional[UserInDB]:
    """Get user from database"""
    if username in users_db:
        user_dict = users_db[username]
        return UserInDB(**user_dict)
    return None


def create_user(user_data: UserCreate) -> User:
    """
    Create a new user

    Args:
        user_data: User creation data including username and password

    Returns:
        Created user (without password)

    Raises:
        HTTPException: If username already exists
    """
    if user_data.username in users_db:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    hashed_password = get_password_hash(user_data.password)
    user_dict = {
        "username": user_data.username,
        "email": user_data.email,
        "full_name": user_data.full_name,
        "disabled": False,
        "first_login": False,  # New users don't need forced password change
        "hashed_password": hashed_password
    }
    users_db[user_data.username] = user_dict

    return User(**{k: v for k, v in user_dict.items() if k != "hashed_password"})


def authenticate_user(username: str, password: str) -> Optional[UserInDB]:
    """
    Authenticate a user

    Args:
        username: Username
        password: Plain text password

    Returns:
        User if authentication succeeds, None otherwise
    """
    user = get_user(username)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


# JWT token utilities

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token

    Args:
        data: Token payload data
        expires_delta: Token expiration time (default: 30 minutes)

    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Get current user from JWT token

    This is a FastAPI dependency that can be used to protect routes.

    Args:
        token: JWT token from Authorization header

    Returns:
        Current user

    Raises:
        HTTPException: If token is invalid or user not found
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return User(**user.dict())


async def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """
    Get current active user (non-disabled)

    This is a FastAPI dependency that ensures the user is not disabled.

    Args:
        current_user: Current user from get_current_user

    Returns:
        Current active user

    Raises:
        HTTPException: If user is disabled
    """
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


# Admin utilities

def is_admin(user: User) -> bool:
    """Check if user is an admin (placeholder for future role-based access)"""
    # For now, just check if username is 'admin'
    # In production, check against a roles table
    return user.username == "admin"


def require_admin(current_user: User = Depends(get_current_active_user)) -> User:
    """
    Dependency that requires admin privileges

    Usage:
        @app.delete("/admin/users/{username}")
        async def delete_user(username: str, admin: User = Depends(require_admin)):
            # Only admins can access this route
            pass
    """
    if not is_admin(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough privileges"
        )
    return current_user


def initialize_default_users():
    """
    Initialize default users (e.g., admin) on startup

    This creates a default admin user with username 'admin' and password 'admin'.
    The admin user is required to change their password on first login.
    """
    if "admin" not in users_db:
        admin_password_hash = get_password_hash("admin")
        users_db["admin"] = {
            "username": "admin",
            "email": "admin@pyimport.local",
            "full_name": "Administrator",
            "disabled": False,
            "first_login": True,  # Force password change on first login
            "hashed_password": admin_password_hash
        }


# Initialize default users on module import
initialize_default_users()
