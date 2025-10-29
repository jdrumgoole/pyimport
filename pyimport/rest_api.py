"""
FastAPI REST API for PyImport

Provides a RESTful HTTP interface to the PyImport CSV-to-MongoDB import tool.
This allows remote clients to trigger imports, check status, and manage data
via HTTP requests.

Quick Start:
    # Start the server
    uvicorn pyimport.rest_api:app --reload --host 0.0.0.0 --port 8000

    # Or use the CLI command
    python -m pyimport.rest_api

Usage Examples:
    # Import a CSV file
    curl -X POST "http://localhost:8000/import" \\
        -H "Content-Type: application/json" \\
        -d '{
            "filename": "/path/to/data.csv",
            "database": "mydb",
            "collection": "mycol",
            "has_header": true
        }'

    # Generate field file
    curl -X POST "http://localhost:8000/fieldfile/generate" \\
        -H "Content-Type: application/json" \\
        -d '{
            "filename": "/path/to/data.csv",
            "delimiter": ","
        }'

    # Health check
    curl "http://localhost:8000/health"

@author: Claude Code
"""

from __future__ import annotations

from typing import Optional, List, Literal, Dict, Any
from pathlib import Path
import logging
import uuid
import threading
from datetime import datetime
import time

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Depends, status
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse, HTMLResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
from pathlib import Path

from pyimport.api import PyImportAPI, PyImportBuilder
from pyimport.importresult import ImportResults
from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.auth import (
    User, UserCreate, Token, create_user, authenticate_user,
    create_access_token, get_current_active_user, require_admin,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from datetime import timedelta


# Pydantic models for request/response validation

class ChangePasswordRequest(BaseModel):
    """Request model for password change"""
    current_password: str = Field(..., description="Current password for verification")
    new_password: str = Field(..., min_length=8, description="New password (min 8 characters)")

    class Config:
        json_schema_extra = {
            "example": {
                "current_password": "OldPassword123",
                "new_password": "NewPassword456"
            }
        }

class ImportRequest(BaseModel):
    """Request model for CSV import operation"""
    filename: str | List[str] = Field(..., description="Path to CSV file(s)")
    database: Optional[str] = Field(None, description="Target database name")
    collection: Optional[str] = Field(None, description="Target collection name")
    delimiter: str = Field(",", description="CSV delimiter")
    has_header: bool = Field(False, description="CSV has header row")
    field_file: Optional[str] = Field(None, description="Path to field file (.tff)")
    batch_size: int = Field(500, description="Batch size for inserts")
    add_filename: bool = Field(False, description="Add source filename to documents")
    add_timestamp: bool = Field(False, description="Add import timestamp to documents")
    add_fields: Optional[Dict[str, str]] = Field(None, description="Additional fields to add")
    id_field: Optional[str] = Field(None, description="Field to use as _id")
    noenrich: bool = Field(False, description="Skip document enrichment")
    cut: Optional[List[int]] = Field(None, description="Column indices to exclude")
    parallel_mode: Optional[Literal["multi", "threads", "async"]] = Field(None, description="Parallel processing mode")
    pool_size: int = Field(8, description="Number of parallel workers")
    audit_host: Optional[str] = Field(None, description="MongoDB URI for audit tracking")
    drop_collection: bool = Field(False, description="Drop collection before import")

    class Config:
        json_schema_extra = {
            "example": {
                "filename": "/path/to/data.csv",
                "database": "mydb",
                "collection": "mycol",
                "delimiter": ",",
                "has_header": True,
                "batch_size": 1000
            }
        }


class ImportResponse(BaseModel):
    """Response model for import operation"""
    status: str = Field(..., description="Import status (success/error)")
    total_written: int = Field(..., description="Total documents written")
    total_errors: int = Field(..., description="Total errors encountered")
    elapsed_time: str = Field(..., description="Total elapsed time")
    files_processed: int = Field(..., description="Number of files processed")
    results: List[Dict[str, Any]] = Field(..., description="Per-file results")


class GenerateFieldFileRequest(BaseModel):
    """Request model for field file generation"""
    filename: str = Field(..., description="Path to CSV file")
    output_filename: Optional[str] = Field(None, description="Output field file path")
    delimiter: str = Field(",", description="CSV delimiter")
    has_header: bool = Field(True, description="CSV has header row")

    class Config:
        json_schema_extra = {
            "example": {
                "filename": "/path/to/data.csv",
                "delimiter": ",",
                "has_header": True
            }
        }


class FieldFileResponse(BaseModel):
    """Response model for field file operations"""
    filename: str = Field(..., description="Field file path")
    fields: List[str] = Field(..., description="List of field names")
    field_count: int = Field(..., description="Number of fields")


class TempUploadResponse(BaseModel):
    """Response model for temporary file upload"""
    temp_path: str = Field(..., description="Path to temporary file")
    original_filename: str = Field(..., description="Original filename")
    file_size: int = Field(..., description="File size in bytes")


class CleanupRequest(BaseModel):
    """Request model for temp file cleanup"""
    temp_path: str = Field(..., description="Path to temporary file to delete")


class ImportWithFieldFileRequest(BaseModel):
    """Request model for import with custom field file"""
    temp_path: str = Field(..., description="Path to temporary file")
    database: str = Field(..., description="Target database name")
    collection: str = Field(..., description="Target collection name")
    fields: Dict[str, Dict[str, Any]] = Field(..., description="Field definitions (name: {type, format})")
    has_header: bool = Field(True, description="CSV has header row")
    delimiter: str = Field(",", description="CSV delimiter")
    batch_size: int = Field(500, description="Batch size for inserts")


class ChangePasswordRequest(BaseModel):
    """Change password request model"""
    current_password: str = Field(..., description="Current password for verification")
    new_password: str = Field(..., min_length=8, description="New password (minimum 8 characters)")


class DropCollectionRequest(BaseModel):
    """Request model for dropping collection"""
    database: Optional[str] = Field(None, description="Database name")
    collection: Optional[str] = Field(None, description="Collection name")


class AuditStatusResponse(BaseModel):
    """Response model for audit status"""
    batch_id: Optional[str] = None
    completed_files: Optional[List[str]] = None
    completed_count: Optional[int] = None
    last_incomplete_batch: Optional[Dict[str, Any]] = None
    has_incomplete: Optional[bool] = None


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str
    version: str
    mongodb_reachable: bool


class JobProgressResponse(BaseModel):
    """Response model for job progress tracking"""
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    filename: Optional[str] = None
    total_lines: Optional[int] = None
    lines_processed: int = 0
    lines_per_second: float = 0.0
    elapsed_seconds: float = 0.0
    estimated_seconds_remaining: Optional[float] = None
    percent_complete: Optional[float] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


# Job tracking storage
class JobTracker:
    """Thread-safe job progress tracker"""

    def __init__(self):
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create_job(self, filename: str) -> str:
        """Create a new job and return job ID"""
        job_id = str(uuid.uuid4())
        with self._lock:
            self._jobs[job_id] = {
                "status": "pending",
                "filename": filename,
                "total_lines": None,
                "lines_processed": 0,
                "started_at": None,
                "completed_at": None,
                "error": None,
                "start_time": None
            }
        return job_id

    def start_job(self, job_id: str, total_lines: Optional[int] = None):
        """Mark job as started"""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = "running"
                self._jobs[job_id]["total_lines"] = total_lines
                self._jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
                self._jobs[job_id]["start_time"] = time.time()

    def update_progress(self, job_id: str, lines_processed: int):
        """Update job progress"""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["lines_processed"] = lines_processed

    def complete_job(self, job_id: str, success: bool = True, error: Optional[str] = None):
        """Mark job as completed or failed"""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = "completed" if success else "failed"
                self._jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
                self._jobs[job_id]["error"] = error

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status with calculated metrics"""
        with self._lock:
            if job_id not in self._jobs:
                return None

            job = self._jobs[job_id].copy()

            # Calculate rate and ETA
            if job["start_time"] and job["status"] == "running":
                elapsed = time.time() - job["start_time"]
                job["elapsed_seconds"] = elapsed

                if elapsed > 0:
                    job["lines_per_second"] = job["lines_processed"] / elapsed

                    if job["total_lines"] and job["lines_processed"] > 0:
                        job["percent_complete"] = (job["lines_processed"] / job["total_lines"]) * 100
                        remaining_lines = job["total_lines"] - job["lines_processed"]
                        if job["lines_per_second"] > 0:
                            job["estimated_seconds_remaining"] = remaining_lines / job["lines_per_second"]

            return job

    def list_jobs(self) -> List[str]:
        """List all job IDs"""
        with self._lock:
            return list(self._jobs.keys())

    def delete_job(self, job_id: str):
        """Delete a job"""
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]


# Global job tracker
job_tracker = JobTracker()


# FastAPI application

app = FastAPI(
    title="PyImport REST API",
    description="RESTful HTTP interface for PyImport CSV-to-MongoDB import tool",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware to allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Global API instance (can be configured via environment variables)
api = PyImportAPI()

# Logger
logger = logging.getLogger("pyimport.rest_api")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint - Serve web frontend"""
    index_path = static_dir / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    else:
        return JSONResponse({
            "name": "PyImport REST API",
            "version": "1.0.0",
            "description": "RESTful HTTP interface for PyImport",
            "docs": "/docs",
            "health": "/health"
        })


@app.get("/api", response_class=JSONResponse)
async def api_info():
    """API information endpoint"""
    return {
        "name": "PyImport REST API",
        "version": "1.0.0",
        "description": "RESTful HTTP interface for PyImport",
        "docs": "/docs",
        "health": "/health",
        "web_ui": "/"
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint

    Returns API status and MongoDB connectivity
    """
    from pyimport.version import __VERSION__

    # Try to connect to MongoDB
    mongodb_reachable = False
    try:
        from pymongo import MongoClient
        client = MongoClient(api.mongodb_uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        mongodb_reachable = True
        client.close()
    except Exception as e:
        logger.warning(f"MongoDB health check failed: {e}")

    return HealthResponse(
        status="healthy" if mongodb_reachable else "degraded",
        version=__VERSION__,
        mongodb_reachable=mongodb_reachable
    )


# Authentication endpoints

@app.post("/users/register", response_model=User, tags=["Authentication"])
async def register_user(user_data: UserCreate):
    """
    Register a new user

    Creates a new user account with username and password.
    Passwords are securely hashed using bcrypt before storage.

    Args:
        user_data: User registration data (username, password, optional email/full_name)

    Returns:
        Created user information (without password)

    Raises:
        HTTPException 400: Username already exists
    """
    try:
        user = create_user(user_data)
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"User registration failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Registration failed: {e}"
        )


@app.post("/token", tags=["Authentication"])
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Login to get access token (OAuth2 compatible)

    Authenticates user and returns a JWT access token.
    Use this token in the Authorization header for protected endpoints:
    `Authorization: Bearer <token>`

    Args:
        form_data: OAuth2 form with username and password

    Returns:
        Access token, token type, and first_login flag

    Raises:
        HTTPException 401: Invalid credentials
    """
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "first_login": user.first_login
    }


@app.get("/users/me", response_model=User, tags=["Authentication"])
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """
    Get current user information

    Returns information about the currently authenticated user.
    Requires valid JWT token in Authorization header.

    Args:
        current_user: Automatically injected from JWT token

    Returns:
        Current user information
    """
    return current_user


@app.post("/users/change-password", tags=["Authentication"])
async def change_password(
    request: ChangePasswordRequest,
    current_user: User = Depends(get_current_active_user)
):
    """
    Change user password

    Allows authenticated users to change their password.
    Requires valid JWT token and current password verification.

    Args:
        request: ChangePasswordRequest containing current and new passwords
        current_user: Automatically injected from JWT token

    Returns:
        Success message

    Raises:
        HTTPException 401: Current password is incorrect
        HTTPException 400: New password is invalid
    """
    from pyimport.auth import authenticate_user, get_password_hash, users_db

    # Verify current password
    user = authenticate_user(current_user.username, request.current_password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect"
        )

    # Validate new password (Pydantic already validates min_length=8)
    if len(request.new_password) < 8:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New password must be at least 8 characters"
        )

    # Update password
    hashed_password = get_password_hash(request.new_password)
    users_db[current_user.username]["hashed_password"] = hashed_password

    # Clear first_login flag if it was set
    if users_db[current_user.username].get("first_login", False):
        users_db[current_user.username]["first_login"] = False

    return {"message": "Password changed successfully"}


@app.post("/import", response_model=ImportResponse)
async def import_csv(
    request: ImportRequest,
    current_user: User = Depends(get_current_active_user)
):
    """
    Import CSV file(s) to MongoDB

    Accepts CSV files and imports them to MongoDB with configurable options.
    Supports single or multiple files, parallel processing, and field type conversion.

    **Authentication Required**: This endpoint requires a valid JWT token.
    """
    try:
        # Convert add_fields dict to list of "key=value" strings
        add_field_list = None
        if request.add_fields:
            add_field_list = [f"{k}={v}" for k, v in request.add_fields.items()]

        # Perform import in thread pool to avoid blocking async event loop
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            results = await loop.run_in_executor(
                executor,
                lambda: api.import_csv(
                    filename=request.filename,
                    database=request.database,
                    collection=request.collection,
                    delimiter=request.delimiter,
                    has_header=request.has_header,
                    field_file=request.field_file,
                    batch_size=request.batch_size,
                    add_filename=request.add_filename,
                    add_timestamp=request.add_timestamp,
                    add_field=add_field_list,
                    id_field=request.id_field,
                    noenrich=request.noenrich,
                    cut=request.cut,
                    parallel_mode=request.parallel_mode,
                    pool_size=request.pool_size,
                    audit_host=request.audit_host,
                    drop_collection=request.drop_collection
                )
            )

        # Convert results to response format
        # results.results is a list of ImportResult objects, not a dict
        file_results = []
        for result in results.results:
            file_results.append({
                "filename": result.filename,
                "docs_written": result.total_written,
                "elapsed_time": str(result.elapsed_duration),
                "errors": [str(result.error)] if result.error else []
            })

        return ImportResponse(
            status="success" if results.total_errors == 0 else "completed_with_errors",
            total_written=results.total_written,
            total_errors=results.total_errors,
            elapsed_time=str(results.duration) if results.duration else "0:00:00",
            files_processed=len(results.results),
            results=file_results
        )

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except FieldFileException as e:
        raise HTTPException(status_code=400, detail=f"Field file error: {e}")
    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Import failed: {e}")


@app.post("/upload", response_model=ImportResponse)
async def upload_and_import_csv(
    file: UploadFile = File(...),
    database: str = None,
    collection: str = None,
    has_header: bool = True,
    delimiter: str = ",",
    batch_size: int = 500,
    current_user: User = Depends(get_current_active_user)
):
    """
    Upload CSV file and import to MongoDB

    Accepts a file upload, saves it temporarily, and imports it to MongoDB.
    The temporary file is cleaned up after import.

    **Authentication Required**: This endpoint requires a valid JWT token.
    """
    import tempfile
    import os

    # Validate parameters
    if not database or not collection:
        raise HTTPException(status_code=400, detail="database and collection are required")

    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    # Create temp file
    temp_file = None
    try:
        # Save uploaded file to temporary location
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_filename = temp_file.name

        logger.info(f"Uploaded file saved to {temp_filename}")

        # Import the file
        results = api.import_csv(
            filename=temp_filename,
            database=database,
            collection=collection,
            delimiter=delimiter,
            has_header=has_header,
            batch_size=batch_size
        )

        # Convert results to response format
        file_results = []
        for filename in results.results.keys():
            result = results.results[filename]
            file_results.append({
                "filename": file.filename,  # Use original filename
                "docs_written": result.docs_written,
                "elapsed_time": str(result.elapsed_duration),
                "errors": result.errors
            })

        return ImportResponse(
            status="success" if results.total_errors == 0 else "completed_with_errors",
            total_written=results.total_written,
            total_errors=results.total_errors,
            elapsed_time=str(results.duration) if results.duration else "0:00:00",
            files_processed=len(results.results),
            results=file_results
        )

    except Exception as e:
        logger.error(f"Upload and import failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Upload and import failed: {e}")

    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_filename):
            try:
                os.unlink(temp_filename)
                logger.info(f"Cleaned up temporary file {temp_filename}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp file: {e}")


@app.post("/upload/temp", response_model=TempUploadResponse)
async def upload_temp(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user)
):
    """
    Upload CSV file temporarily (without importing)

    Saves the uploaded file to a temporary location and returns the path.
    This is used for field file generation workflow.

    **Authentication Required**: This endpoint requires a valid JWT token.
    """
    import tempfile
    import os

    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        # Save uploaded file to temporary location
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_filename = temp_file.name

        logger.info(f"Uploaded file saved temporarily to {temp_filename}")

        return TempUploadResponse(
            temp_path=temp_filename,
            original_filename=file.filename,
            file_size=len(content)
        )

    except Exception as e:
        logger.error(f"Temporary upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Temporary upload failed: {e}")


@app.post("/upload/temp/cleanup")
async def cleanup_temp(
    request: CleanupRequest,
    current_user: User = Depends(get_current_active_user)
):
    """
    Clean up temporary file

    Deletes a previously uploaded temporary file.

    **Authentication Required**: This endpoint requires a valid JWT token.
    """
    import os

    try:
        if os.path.exists(request.temp_path):
            os.unlink(request.temp_path)
            logger.info(f"Cleaned up temporary file {request.temp_path}")
            return {"status": "success", "message": "Temporary file deleted"}
        else:
            return {"status": "not_found", "message": "File not found"}

    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {e}")


@app.post("/import/with-fieldfile", response_model=ImportResponse)
async def import_with_fieldfile(
    request: ImportWithFieldFileRequest,
    current_user: User = Depends(get_current_active_user)
):
    """
    Import CSV with custom field file configuration

    Takes a temporary file path and custom field type definitions,
    creates a field file, and imports the CSV to MongoDB.
    Cleans up the temporary file after import.

    **Authentication Required**: This endpoint requires a valid JWT token.
    """
    import tempfile
    import os
    import toml

    # Verify temp file exists
    if not os.path.exists(request.temp_path):
        raise HTTPException(status_code=404, detail="Temporary file not found")

    field_file_path = None
    try:
        # Create field file from user's field definitions
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tff', delete=False) as ff:
            field_file_path = ff.name

            # Build field file content
            field_file_content = {
                "DEFAULTS_SECTION": {
                    "delimiter": request.delimiter,
                    "has_header": request.has_header,
                    "CSV File": request.temp_path
                }
            }

            # Add field type definitions
            for field_name, field_config in request.fields.items():
                field_file_content[field_name] = {
                    "type": field_config["type"]
                }
                # Add format if provided and not null/empty
                if field_config.get("format"):
                    field_file_content[field_name]["format"] = field_config["format"]

            # Write field file
            toml.dump(field_file_content, ff)

        logger.info(f"Created field file at {field_file_path}")

        # Import with the generated field file
        # Run in thread pool to avoid blocking async event loop
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            results = await loop.run_in_executor(
                executor,
                lambda: api.import_csv(
                    filename=request.temp_path,
                    database=request.database,
                    collection=request.collection,
                    delimiter=request.delimiter,
                    has_header=request.has_header,
                    field_file=field_file_path,
                    batch_size=request.batch_size
                )
            )

        # Convert results to response format
        # results.results is a list of ImportResult objects, not a dict
        file_results = []
        for result in results.results:
            file_results.append({
                "filename": request.temp_path,
                "docs_written": result.total_written,
                "elapsed_time": str(result.elapsed_duration),
                "errors": [str(result.error)] if result.error else []
            })

        return ImportResponse(
            status="success" if results.total_errors == 0 else "completed_with_errors",
            total_written=results.total_written,
            total_errors=results.total_errors,
            elapsed_time=str(results.duration) if results.duration else "0:00:00",
            files_processed=len(results.results),
            results=file_results
        )

    except Exception as e:
        logger.error(f"Import with field file failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Import failed: {e}")

    finally:
        # Clean up temporary CSV file
        if os.path.exists(request.temp_path):
            try:
                os.unlink(request.temp_path)
                logger.info(f"Cleaned up temporary CSV file {request.temp_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp CSV file: {e}")

        # Clean up temporary field file
        if field_file_path and os.path.exists(field_file_path):
            try:
                os.unlink(field_file_path)
                logger.info(f"Cleaned up temporary field file {field_file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp field file: {e}")


@app.post("/import/async", response_model=Dict[str, str])
async def import_csv_async(
    request: ImportRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user)
):
    """
    Start CSV import as background task

    Returns immediately with job ID while import runs in background.
    Use GET /jobs/{job_id} to check progress and upload rate.

    **Authentication Required**: This endpoint requires a valid JWT token.
    """

    # Create job
    filename = request.filename if isinstance(request.filename, str) else request.filename[0]
    job_id = job_tracker.create_job(filename)

    # Define background task
    def run_import():
        try:
            # Get file line count for progress tracking
            from pyimport.filesplitter import LineCounter
            try:
                total_lines = LineCounter.count_now(filename)
                if request.has_header:
                    total_lines -= 1  # Subtract header
            except:
                total_lines = None

            job_tracker.start_job(job_id, total_lines)

            # TODO: Implement progress callback to update job_tracker during import
            # For now, just run the import
            results = api.import_csv(
                filename=request.filename,
                database=request.database,
                collection=request.collection,
                delimiter=request.delimiter,
                has_header=request.has_header,
                field_file=request.field_file,
                batch_size=request.batch_size,
                add_filename=request.add_filename,
                add_timestamp=request.add_timestamp,
                add_field=[f"{k}={v}" for k, v in request.add_fields.items()] if request.add_fields else None,
                id_field=request.id_field,
                noenrich=request.noenrich,
                cut=request.cut,
                parallel_mode=request.parallel_mode,
                pool_size=request.pool_size,
                audit_host=request.audit_host,
                drop_collection=request.drop_collection
            )

            # Update progress to completion
            if total_lines:
                job_tracker.update_progress(job_id, total_lines)

            job_tracker.complete_job(job_id, success=True)

        except Exception as e:
            logger.error(f"Background import failed: {e}", exc_info=True)
            job_tracker.complete_job(job_id, success=False, error=str(e))

    # Start background task
    background_tasks.add_task(run_import)

    return {
        "job_id": job_id,
        "status": "accepted",
        "message": f"Import job started. Check progress at /jobs/{job_id}"
    }


@app.post("/fieldfile/generate", response_model=FieldFileResponse)
async def generate_field_file(request: GenerateFieldFileRequest):
    """
    Generate field file from CSV

    Analyzes CSV header and first row to automatically generate a field file (.tff)
    with inferred types for each column.
    """
    try:
        field_file = api.generate_field_file(
            csv_filename=request.filename,
            output_filename=request.output_filename,
            delimiter=request.delimiter,
            has_header=request.has_header
        )

        # Get the actual filename from the field file
        ff_path = request.output_filename or FieldFile.make_default_tff_name(request.filename)

        return FieldFileResponse(
            filename=ff_path,
            fields=field_file.fields(),
            field_count=len(field_file.fields())
        )

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Field file generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Field file generation failed: {e}")


@app.get("/fieldfile/{filename}", response_class=FileResponse)
async def download_field_file(filename: str):
    """
    Download field file

    Returns the contents of a field file (.tff) for download.
    """
    path = Path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Field file not found: {filename}")

    return FileResponse(
        path=str(path),
        media_type="text/plain",
        filename=path.name
    )


@app.post("/fieldfile/load", response_model=FieldFileResponse)
async def load_field_file(filename: str):
    """
    Load and inspect field file

    Loads a field file and returns its structure.
    """
    try:
        field_file = api.load_field_file(filename)

        return FieldFileResponse(
            filename=filename,
            fields=field_file.fields(),
            field_count=len(field_file.fields())
        )

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Field file not found: {filename}")
    except FieldFileException as e:
        raise HTTPException(status_code=400, detail=f"Invalid field file: {e}")


@app.delete("/collection")
async def drop_collection(
    request: DropCollectionRequest,
    admin: User = Depends(require_admin)
):
    """
    Drop MongoDB collection

    Permanently deletes a MongoDB collection.

    **Admin Only**: This endpoint requires admin privileges.
    """
    try:
        api.drop_collection(
            database=request.database,
            collection=request.collection
        )

        return {
            "status": "success",
            "message": f"Collection {request.database}.{request.collection} dropped"
        }

    except Exception as e:
        logger.error(f"Drop collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Drop failed: {e}")


@app.get("/audit/status", response_model=AuditStatusResponse)
async def get_audit_status(audit_host: str, batch_id: Optional[str] = None):
    """
    Get audit status

    Check status of import batches for restart capability.
    """
    try:
        status = api.get_audit_status(audit_host, batch_id)

        return AuditStatusResponse(**status)

    except Exception as e:
        logger.error(f"Audit status check failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Audit check failed: {e}")


@app.post("/import/restart")
async def restart_import(
    audit_host: str,
    batch_id: Optional[str] = None,
    request: Optional[ImportRequest] = None
):
    """
    Restart incomplete import

    Resumes a previously incomplete import using audit tracking.
    """
    try:
        import_kwargs = {}
        if request:
            import_kwargs = request.dict(exclude_none=True)

        results = api.restart_import(
            batch_id=batch_id,
            audit_host=audit_host,
            **import_kwargs
        )

        return {
            "status": "success",
            "total_written": results.total_written,
            "batch_id": batch_id
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Restart failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Restart failed: {e}")


# Job progress tracking endpoints

@app.get("/jobs", response_model=List[str])
async def list_jobs():
    """
    List all job IDs

    Returns a list of all job IDs (active and completed).
    """
    return job_tracker.list_jobs()


@app.get("/jobs/{job_id}", response_model=JobProgressResponse)
async def get_job_status(job_id: str):
    """
    Get job progress and upload rate

    Returns real-time progress information including:
    - Lines processed
    - Upload rate (lines per second)
    - Percentage complete
    - Estimated time remaining
    """
    job = job_tracker.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JobProgressResponse(
        job_id=job_id,
        status=job["status"],
        filename=job.get("filename"),
        total_lines=job.get("total_lines"),
        lines_processed=job.get("lines_processed", 0),
        lines_per_second=job.get("lines_per_second", 0.0),
        elapsed_seconds=job.get("elapsed_seconds", 0.0),
        estimated_seconds_remaining=job.get("estimated_seconds_remaining"),
        percent_complete=job.get("percent_complete"),
        error=job.get("error"),
        started_at=job.get("started_at"),
        completed_at=job.get("completed_at")
    )


@app.get("/jobs/{job_id}/stream")
async def stream_job_progress(job_id: str):
    """
    Stream job progress updates (Server-Sent Events)

    Continuously streams progress updates until job completes.
    Use with EventSource in browser or SSE client.

    Example:
        const eventSource = new EventSource('/jobs/abc-123/stream');
        eventSource.onmessage = (event) => {
            const progress = JSON.parse(event.data);
            console.log(`Progress: ${progress.percent_complete}%`);
            console.log(`Rate: ${progress.lines_per_second} lines/sec`);
        };
    """
    import json
    import asyncio

    async def event_generator():
        while True:
            job = job_tracker.get_job(job_id)

            if not job:
                yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                break

            # Send progress update
            progress_data = {
                "job_id": job_id,
                "status": job["status"],
                "lines_processed": job.get("lines_processed", 0),
                "lines_per_second": job.get("lines_per_second", 0.0),
                "percent_complete": job.get("percent_complete"),
                "elapsed_seconds": job.get("elapsed_seconds", 0.0),
                "estimated_seconds_remaining": job.get("estimated_seconds_remaining")
            }

            yield f"data: {json.dumps(progress_data)}\n\n"

            # Stop streaming if job is completed or failed
            if job["status"] in ["completed", "failed"]:
                break

            # Wait before next update
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """
    Delete a job from tracking

    Removes job history. Does not cancel running jobs.
    """
    job = job_tracker.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job_tracker.delete_job(job_id)

    return {"status": "deleted", "job_id": job_id}


# CLI entry point
def main():
    """Run the FastAPI server"""
    import sys

    # Parse command-line arguments
    host = "0.0.0.0"
    port = 8000
    reload = False

    if "--host" in sys.argv:
        idx = sys.argv.index("--host")
        if idx + 1 < len(sys.argv):
            host = sys.argv[idx + 1]

    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    if "--reload" in sys.argv:
        reload = True

    print(f"Starting PyImport REST API on http://{host}:{port}")
    print(f"API documentation: http://{host}:{port}/docs")

    uvicorn.run(
        "pyimport.rest_api:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    main()
