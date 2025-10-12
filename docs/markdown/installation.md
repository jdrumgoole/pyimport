# Installation

## Requirements

- **Python**: 3.11 or higher
- **MongoDB**: 4.0 or higher (for imports)
- **Poetry**: For dependency management (recommended)

## Installation Methods

### From PyPI (Recommended)

```bash
pip install pyimport
```

### From Source

```bash
# Clone the repository
git clone https://github.com/jdrumgoole/pyimport.git
cd pyimport

# Install with Poetry
poetry install

# Or install with pip
pip install -e .
```

### Using Poetry (Development)

If you're developing or contributing to PyImport:

```bash
# Clone and enter directory
git clone https://github.com/jdrumgoole/pyimport.git
cd pyimport

# Install dependencies
poetry install

# Run pyimport
poetry run pyimport --help
```

## Verify Installation

Check that PyImport is installed correctly:

```bash
pyimport --version
# Output: pyimport 1.9.0
```

Get help:

```bash
pyimport --help
```

## MongoDB Setup

PyImport requires a running MongoDB instance. By default, it connects to `mongodb://localhost:27017`.

### Local MongoDB

**macOS (Homebrew):**
```bash
brew install mongodb-community
brew services start mongodb-community
```

**Ubuntu/Debian:**
```bash
sudo apt-get install mongodb
sudo systemctl start mongodb
```

**Docker:**
```bash
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### MongoDB Atlas

For MongoDB Atlas (cloud), use the `--mdburi` parameter:

```bash
pyimport --mdburi "mongodb+srv://user:pass@cluster.mongodb.net/" \
         --database mydb --collection mycol data.csv
```

Or set the environment variable:

```bash
export MDB_URI="mongodb+srv://user:pass@cluster.mongodb.net/"
pyimport --database mydb --collection mycol data.csv
```

## Optional Dependencies

### PostgreSQL Support

PyImport also supports importing to PostgreSQL (experimental):

```bash
pip install pyimport[postgres]
```

Set the PostgreSQL connection:

```bash
export PGURI="postgresql://user:pass@localhost:5432/dbname"
pyimport --pguri "$PGURI" --pgtable mytable data.csv
```

### Development Dependencies

For development and testing:

```bash
poetry install --with dev
```

This includes:
- pytest and pytest-asyncio
- coverage
- sphinx (documentation)
- black, flake8 (code quality)

## Configuration File

PyImport can read settings from configuration files in these locations (in order of priority):

1. `./pyimport.conf` (current directory)
2. `~/.pyimport.conf` (home directory)
3. `~/.config/pyimport.conf` (config directory)

Example `pyimport.conf`:

```ini
# MongoDB settings
mdburi = mongodb://localhost:27017
database = mydb
collection = imported

# Import settings
batchsize = 1000
hasheader = True

# Parallel processing
poolsize = 4
```

## Environment Variables

PyImport respects these environment variables:

- `MDB_URI`: MongoDB connection string (overrides `--mdburi`)
- `AUDIT_HOST`: MongoDB URI for audit records (overrides `--audithost`)
- `PGURI`: PostgreSQL connection string (overrides `--pguri`)

Set them in your shell or `.env` file:

```bash
export MDB_URI="mongodb://localhost:27017"
export AUDIT_HOST="mongodb://localhost:27017"
```

## Quick Start

Once installed, try a basic import:

```bash
# Create a test CSV file
echo "name,age,city" > test.csv
echo "Alice,30,NYC" >> test.csv
echo "Bob,25,LA" >> test.csv

# Generate field file (type definitions)
pyimport --genfieldfile test.csv

# Import to MongoDB
pyimport --database testdb --collection people test.csv

# Verify import
mongo testdb --eval "db.people.find().pretty()"
```

## Troubleshooting

### Connection Issues

If you get "connection refused" errors:

1. Verify MongoDB is running:
   ```bash
   # Check if MongoDB is listening
   netstat -an | grep 27017

   # Or use mongosh/mongo
   mongosh --eval "db.version()"
   ```

2. Check your connection string:
   ```bash
   pyimport --mdburi mongodb://localhost:27017 --database test --collection test data.csv
   ```

### Permission Errors

If you get permission errors during installation:

```bash
# Use --user flag
pip install --user pyimport

# Or use a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install pyimport
```

### Import Path Issues

If Python can't find pyimport:

```bash
# Add to PYTHONPATH
export PYTHONPATH=/path/to/pyimport:$PYTHONPATH

# Or reinstall in development mode
pip install -e .
```

## Next Steps

- [Quick Start Guide](quickstart.md) - Your first import
- [Command-Line Reference](cli_reference.md) - All available options
- [Field Files](fieldfiles.md) - Understanding type definitions
