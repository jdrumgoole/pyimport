# PyImport REST API

A FastAPI-based REST API that provides HTTP access to PyImport's CSV-to-MongoDB import functionality.

## Installation

Install PyImport with REST API support:

```bash
# Install with REST API extras
pip install pyimport[rest-api]

# Or with poetry
poetry install --extras rest-api
```

## Quick Start

### Starting the Server

PyImport provides convenient command-line scripts for managing the server:

```bash
# Start server (recommended)
pyimport-server

# Start on custom port
pyimport-server --port 8080

# Start with auto-reload for development
pyimport-server --reload

# Start and automatically open browser
pyimport-server --open

# Full example
pyimport-server --host 0.0.0.0 --port 8000 --reload --open
```

**Alternative methods:**
```bash
# Using pyimport-api (legacy)
pyimport-api --host 0.0.0.0 --port 8000

# Using uvicorn directly
uvicorn pyimport.rest_api:app --reload --host 0.0.0.0 --port 8000
```

### Opening the Web Interface

```bash
# Open web interface in browser
pyimport-web

# Check if server is running first
pyimport-web --check

# Automatically start server if not running
pyimport-web --start

# Open on custom port
pyimport-web --port 8080
```

### Stopping the Server

```bash
# Stop server gracefully
pyimport-stop

# Stop server on specific port
pyimport-stop --port 8080

# Stop all PyImport servers
pyimport-stop --all

# Force kill (immediate termination)
pyimport-stop --force

# List running servers without stopping
pyimport-stop --list
```

### Closing the Web Interface

```bash
# Close browser tabs (macOS: auto-close Safari/Chrome)
pyimport-close

# Close specific browser (macOS only)
pyimport-close --browser safari

# Close browser and stop server
pyimport-close --stop-server
```

### Access API Documentation

Once the server is running, visit:
- **Web Interface**: http://localhost:8000/ (User registration, login, testing)
- **Interactive API docs (Swagger UI)**: http://localhost:8000/docs
- **Alternative API docs (ReDoc)**: http://localhost:8000/redoc

## API Endpoints

### Authentication

The API uses JWT (JSON Web Token) based authentication for protected endpoints.

#### Register a New User

**POST** `/users/register`

Create a new user account.

```bash
curl -X POST "http://localhost:8000/users/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "johndoe",
    "password": "secretpassword",
    "email": "john@example.com",
    "full_name": "John Doe"
  }'
```

**Response:**
```json
{
  "username": "johndoe",
  "email": "john@example.com",
  "full_name": "John Doe",
  "disabled": false
}
```

#### Login (Get Access Token)

**POST** `/token`

Authenticate and receive a JWT access token. Use this token for protected endpoints.

```bash
curl -X POST "http://localhost:8000/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=johndoe&password=secretpassword"
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

Save the `access_token` and include it in the Authorization header for protected endpoints:

```bash
export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

# Use token in requests
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/users/me
```

#### Get Current User

**GET** `/users/me`

Get information about the currently authenticated user.

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/users/me
```

**Response:**
```json
{
  "username": "johndoe",
  "email": "john@example.com",
  "full_name": "John Doe",
  "disabled": false
}
```

### Health Check

**GET** `/health`

Check API and MongoDB connectivity status. **No authentication required.**

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "version": "2.0.6",
  "mongodb_reachable": true
}
```

### Import CSV (Synchronous)

**POST** `/import`

Import CSV file(s) to MongoDB. Blocks until import completes.

**Authentication Required**: Include JWT token in Authorization header.

```bash
curl -X POST "http://localhost:8000/import" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "filename": "/path/to/data.csv",
    "database": "mydb",
    "collection": "mycol",
    "has_header": true,
    "delimiter": ",",
    "batch_size": 1000
  }'
```

**Request Body:**
```json
{
  "filename": "string or array",       // Required: CSV file path(s)
  "database": "string",                // Target database
  "collection": "string",              // Target collection
  "delimiter": ",",                    // CSV delimiter
  "has_header": false,                 // CSV has header row
  "field_file": "string",              // Path to .tff field file
  "batch_size": 500,                   // Batch insert size
  "add_filename": false,               // Add filename to documents
  "add_timestamp": false,              // Add timestamp to documents
  "add_fields": {"key": "value"},      // Additional fields
  "id_field": "string",                // Field to use as _id
  "noenrich": false,                   // Skip enrichment
  "cut": [0, 1],                       // Exclude column indices
  "parallel_mode": "multi|threads|async", // Parallel processing
  "pool_size": 8,                      // Number of workers
  "audit_host": "string",              // Audit tracking URI
  "drop_collection": false             // Drop before import
}
```

**Response:**
```json
{
  "status": "success",
  "total_written": 10000,
  "total_errors": 0,
  "elapsed_time": "0:00:05.123456",
  "files_processed": 1,
  "results": [
    {
      "filename": "/path/to/data.csv",
      "docs_written": 10000,
      "elapsed_time": "0:00:05.123456",
      "errors": 0
    }
  ]
}
```

### Import CSV (Asynchronous with Progress Tracking)

**POST** `/import/async`

Start CSV import as background job. Returns immediately with job ID for progress tracking.

**Authentication Required**: Include JWT token in Authorization header.

```bash
curl -X POST "http://localhost:8000/import/async" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "filename": "/path/to/large_file.csv",
    "database": "mydb",
    "collection": "mycol",
    "has_header": true
  }'
```

**Response:**
```json
{
  "job_id": "abc-123-def-456",
  "status": "accepted",
  "message": "Import job started. Check progress at /jobs/abc-123-def-456"
}
```

### Monitor Upload Progress

**GET** `/jobs/{job_id}`

Get real-time progress and upload rate for an import job.

```bash
curl "http://localhost:8000/jobs/abc-123-def-456"
```

**Response:**
```json
{
  "job_id": "abc-123-def-456",
  "status": "running",
  "filename": "/path/to/large_file.csv",
  "total_lines": 1000000,
  "lines_processed": 450000,
  "lines_per_second": 15234.5,
  "elapsed_seconds": 29.5,
  "estimated_seconds_remaining": 36.1,
  "percent_complete": 45.0,
  "error": null,
  "started_at": "2025-10-15T12:00:00",
  "completed_at": null
}
```

**Key Metrics:**
- `lines_per_second`: Current upload rate (rows/sec)
- `percent_complete`: Progress percentage
- `estimated_seconds_remaining`: Estimated time to completion
- `elapsed_seconds`: Time since job started

### Stream Progress Updates (Server-Sent Events)

**GET** `/jobs/{job_id}/stream`

Stream real-time progress updates. Updates sent every second until job completes.

```javascript
// Browser example using EventSource
const eventSource = new EventSource('http://localhost:8000/jobs/abc-123-def-456/stream');

eventSource.onmessage = (event) => {
    const progress = JSON.parse(event.data);
    console.log(`Progress: ${progress.percent_complete}%`);
    console.log(`Upload rate: ${progress.lines_per_second} lines/sec`);
    console.log(`ETA: ${progress.estimated_seconds_remaining}s`);

    if (progress.status === 'completed') {
        eventSource.close();
        console.log('Import complete!');
    }
};
```

```python
# Python example using requests
import requests
import json

response = requests.get(
    'http://localhost:8000/jobs/abc-123-def-456/stream',
    stream=True
)

for line in response.iter_lines():
    if line.startswith(b'data: '):
        data = json.loads(line[6:])
        print(f"Progress: {data['percent_complete']}% | "
              f"Rate: {data['lines_per_second']} lines/sec")
        if data['status'] in ['completed', 'failed']:
            break
```

### List All Jobs

**GET** `/jobs`

List all job IDs (active and completed).

```bash
curl "http://localhost:8000/jobs"
```

**Response:**
```json
[
  "abc-123-def-456",
  "xyz-789-ghi-012"
]
```

### Delete Job

**DELETE** `/jobs/{job_id}`

Remove job from tracking history.

```bash
curl -X DELETE "http://localhost:8000/jobs/abc-123-def-456"
```

### Generate Field File

**POST** `/fieldfile/generate`

Auto-generate a field file (.tff) from CSV by analyzing header and first row.

```bash
curl -X POST "http://localhost:8000/fieldfile/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "/path/to/data.csv",
    "delimiter": ",",
    "has_header": true
  }'
```

**Response:**
```json
{
  "filename": "/path/to/data.tff",
  "fields": ["id", "name", "email", "age"],
  "field_count": 4
}
```

### Load Field File

**POST** `/fieldfile/load?filename=/path/to/file.tff`

Load and inspect an existing field file.

```bash
curl -X POST "http://localhost:8000/fieldfile/load?filename=data.tff"
```

### Download Field File

**GET** `/fieldfile/{filename}`

Download a field file.

```bash
curl -O "http://localhost:8000/fieldfile/data.tff"
```

### Drop Collection

**DELETE** `/collection`

Drop a MongoDB collection.

**Admin Only**: This endpoint requires admin privileges. Only users with username "admin" can access.

```bash
curl -X DELETE "http://localhost:8000/collection" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "database": "mydb",
    "collection": "mycol"
  }'
```

### Audit Status

**GET** `/audit/status?audit_host=mongodb://localhost&batch_id=123`

Check import batch status for restart capability.

```bash
curl "http://localhost:8000/audit/status?audit_host=mongodb://localhost:27017"
```

### Restart Import

**POST** `/import/restart?audit_host=mongodb://localhost&batch_id=123`

Resume a previously incomplete import.

```bash
curl -X POST "http://localhost:8000/import/restart?audit_host=mongodb://localhost:27017&batch_id=abc123" \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "/path/to/data.csv",
    "database": "mydb"
  }'
```

## Usage Examples

### Authentication Workflow

```python
import requests

# 1. Register a new user
register_response = requests.post(
    "http://localhost:8000/users/register",
    json={
        "username": "datauser",
        "password": "secure_password_123",
        "email": "user@example.com"
    }
)
print(f"User registered: {register_response.json()}")

# 2. Login to get access token
login_response = requests.post(
    "http://localhost:8000/token",
    data={
        "username": "datauser",
        "password": "secure_password_123"
    }
)
token = login_response.json()["access_token"]
print(f"Access token: {token[:20]}...")

# 3. Use token for authenticated requests
headers = {"Authorization": f"Bearer {token}"}

# 4. Verify authentication
me_response = requests.get(
    "http://localhost:8000/users/me",
    headers=headers
)
print(f"Logged in as: {me_response.json()['username']}")
```

### Basic Import

```python
import requests

# Get authentication token first
token_response = requests.post(
    "http://localhost:8000/token",
    data={"username": "datauser", "password": "secure_password_123"}
)
token = token_response.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Perform import
response = requests.post(
    "http://localhost:8000/import",
    headers=headers,
    json={
        "filename": "/data/sales.csv",
        "database": "analytics",
        "collection": "sales",
        "has_header": True,
        "delimiter": ","
    }
)

result = response.json()
print(f"Imported {result['total_written']} records")
```

### Import with Field File

```python
# First, generate field file
field_response = requests.post(
    "http://localhost:8000/fieldfile/generate",
    json={
        "filename": "/data/sales.csv",
        "delimiter": ",",
        "has_header": True
    }
)

field_file = field_response.json()["filename"]

# Then import using field file
import_response = requests.post(
    "http://localhost:8000/import",
    json={
        "filename": "/data/sales.csv",
        "database": "analytics",
        "collection": "sales",
        "field_file": field_file,
        "has_header": True
    }
)
```

### Parallel Import

```python
response = requests.post(
    "http://localhost:8000/import",
    json={
        "filename": "/data/large_file.csv",
        "database": "analytics",
        "collection": "big_data",
        "has_header": True,
        "parallel_mode": "multi",  # Use multiprocessing
        "pool_size": 8,           # 8 parallel workers
        "batch_size": 1000
    }
)
```

### Import with Enrichment

```python
response = requests.post(
    "http://localhost:8000/import",
    json={
        "filename": "/data/events.csv",
        "database": "logs",
        "collection": "events",
        "has_header": True,
        "add_filename": True,    # Add source filename
        "add_timestamp": True,   # Add import timestamp
        "add_fields": {          # Add custom fields
            "environment": "production",
            "version": "2.0"
        }
    }
)
```

## Configuration

The API uses environment variables or can be configured programmatically:

```python
# In pyimport/rest_api.py, modify the global api instance:
api = PyImportAPI(
    mongodb_uri="mongodb://localhost:27017",
    database="default_db",
    collection="default_col",
    write_concern=1,
    journal=True,
    log_level="INFO"
)
```

## Error Handling

The API returns standard HTTP status codes:

- **200**: Success
- **400**: Bad Request (invalid parameters)
- **404**: Not Found (file doesn't exist)
- **500**: Internal Server Error
- **501**: Not Implemented

Error response format:
```json
{
  "detail": "Error message describing what went wrong"
}
```

## Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install PyImport with REST API support
RUN pip install pyimport[rest-api]

# Expose API port
EXPOSE 8000

# Run the API server
CMD ["pyimport-api", "--host", "0.0.0.0", "--port", "8000"]
```

Build and run:

```bash
docker build -t pyimport-api .
docker run -p 8000:8000 pyimport-api
```

## Production Considerations

### Security

**Authentication is now built-in!** The API includes JWT-based authentication with the following security features:

#### Environment Variables

Set these environment variables in production:

```bash
# REQUIRED: Change this to a secure random string in production!
export SECRET_KEY="your-secret-key-at-least-32-characters-long"

# Optional: Token expiration time in minutes (default: 30)
export ACCESS_TOKEN_EXPIRE_MINUTES=60

# MongoDB connection
export MONGODB_URI="mongodb://localhost:27017"
```

Generate a secure secret key:
```bash
# Using Python
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Or using OpenSSL
openssl rand -hex 32
```

#### Security Best Practices

1. **Secret Key Management**:
   - NEVER use the default secret key in production
   - Store secret keys in environment variables or secrets management systems
   - Rotate keys periodically

2. **User Management**:
   - Currently uses in-memory storage (users_db in `auth.py`)
   - **For production**: Replace with MongoDB or PostgreSQL storage
   - Implement user email verification
   - Add password reset functionality
   - Implement account lockout after failed attempts

3. **Admin Access**:
   - Admin check is currently username == "admin"
   - **For production**: Implement proper role-based access control (RBAC)
   - Store roles in database
   - Add granular permissions

4. **Additional Security Measures**:
   - Validate file paths to prevent directory traversal
   - Rate limit endpoints to prevent abuse (use `slowapi` or similar)
   - Use HTTPS in production (TLS/SSL certificates)
   - Implement CORS policies appropriately
   - Add audit logging for sensitive operations
   - Set appropriate token expiration times
   - Implement token refresh mechanism

#### Example: Database-Backed User Storage

Replace the in-memory `users_db` with MongoDB:

```python
# In pyimport/auth.py
from pymongo import MongoClient

client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
users_collection = client["pyimport"]["users"]

def get_user(username: str) -> Optional[UserInDB]:
    user_dict = users_collection.find_one({"username": username})
    if user_dict:
        return UserInDB(**user_dict)
    return None

def create_user(user_data: UserCreate) -> User:
    if users_collection.find_one({"username": user_data.username}):
        raise HTTPException(status_code=400, detail="Username already registered")

    user_dict = {
        "username": user_data.username,
        "email": user_data.email,
        "full_name": user_data.full_name,
        "disabled": False,
        "hashed_password": get_password_hash(user_data.password)
    }
    users_collection.insert_one(user_dict)
    return User(**{k: v for k, v in user_dict.items() if k != "hashed_password"})
```

### Performance

- Consider using Gunicorn with multiple Uvicorn workers:
  ```bash
  gunicorn pyimport.rest_api:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000
  ```

### Monitoring

- Use FastAPI's built-in request logging
- Add Prometheus metrics endpoint
- Monitor MongoDB connection pool
- Track import job durations

## Development

### Running Tests

```bash
# Install dev dependencies
poetry install --with dev --extras rest-api

# Run tests
pytest test/test_rest_api/
```

### Hot Reload

For development with auto-reload:

```bash
pyimport-api --reload
```

## Command-Line Scripts Reference

PyImport provides a complete set of command-line tools for managing the REST API server and web interface.

### Server Management

#### `pyimport-server` - Start the Server

Start the PyImport REST API server with sensible defaults.

**Options:**
- `--host HOST` - Server host (default: 0.0.0.0)
- `--port PORT` - Server port (default: 8000)
- `--reload` - Enable auto-reload for development
- `--open` - Open web browser after server starts

**Environment Variables:**
- `PYIMPORT_HOST` - Default host
- `PYIMPORT_PORT` - Default port
- `SECRET_KEY` - JWT secret key (required for production)
- `ACCESS_TOKEN_EXPIRE_MINUTES` - Token expiration time

**Examples:**
```bash
pyimport-server                          # Start with defaults
pyimport-server --port 8080              # Custom port
pyimport-server --reload                 # Development mode
pyimport-server --open                   # Auto-open browser
pyimport-server --host 0.0.0.0 --port 8000 --reload --open
```

#### `pyimport-stop` - Stop the Server

Stop running PyImport REST API server processes.

**Options:**
- `--port PORT` - Stop server on specific port
- `--all` - Stop all PyImport servers regardless of port
- `--force` - Force kill (SIGKILL) instead of graceful shutdown
- `--list` - List running servers without stopping them

**Examples:**
```bash
pyimport-stop                 # Stop server on port 8000
pyimport-stop --port 8080     # Stop server on port 8080
pyimport-stop --all           # Stop all servers
pyimport-stop --force         # Force kill
pyimport-stop --list          # List running servers
```

### Web Interface Management

#### `pyimport-web` - Open Web Interface

Open the PyImport web interface in your default browser.

**Options:**
- `--port PORT` - Server port (default: 8000)
- `--check` - Check if server is running before opening
- `--start` - Automatically start server if not running

**Examples:**
```bash
pyimport-web                  # Open web interface
pyimport-web --port 8080      # Custom port
pyimport-web --check          # Check server first
pyimport-web --start          # Auto-start server
```

#### `pyimport-close` - Close Web Interface

Close browser tabs showing the PyImport web interface.

**Options:**
- `--port PORT` - Server port (default: 8000)
- `--browser {safari,chrome,firefox}` - Specific browser (macOS only)
- `--stop-server` - Also stop the server after closing browser

**Platform Support:**
- **macOS**: Automatically closes Safari and Chrome tabs using AppleScript
- **Windows/Linux**: Provides manual instructions (browser security limitations)

**Examples:**
```bash
pyimport-close                      # Close browser tabs
pyimport-close --browser safari     # Close specific browser
pyimport-close --stop-server        # Close browser and stop server
```

### Common Workflows

**Quick Start (Everything):**
```bash
pyimport-server --open          # Start server and open browser
```

**Development Workflow:**
```bash
# Start server with auto-reload
pyimport-server --reload --open

# Make changes to code...

# Server automatically reloads

# When done
pyimport-close --stop-server    # Close browser and stop server
```

**Production Deployment:**
```bash
# Set environment variables
export SECRET_KEY="your-secure-secret-key"
export PYIMPORT_HOST="0.0.0.0"
export PYIMPORT_PORT="8000"

# Start server (no reload, no auto-open)
pyimport-server

# Stop server gracefully
pyimport-stop
```

**Managing Multiple Servers:**
```bash
# Start servers on different ports
pyimport-server --port 8000 &
pyimport-server --port 8080 &
pyimport-server --port 8090 &

# List all running servers
pyimport-stop --list

# Stop specific server
pyimport-stop --port 8080

# Stop all servers
pyimport-stop --all
```

## See Also

- [PyImport Python API Documentation](API.md)
- [Field File Format](docs/markdown/FIELD_FILES.md)
- [PyImport CLI Documentation](README.md)
