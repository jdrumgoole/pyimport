from datetime import datetime
import os

import pytest
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import Integer, Float, String, DateTime

from pyimport.db.postgresuri import PostgresURI

from pyimport.db.rdbmanager import RDBManager, RDBManagerError

# Skip all tests in this module if PostgreSQL is not configured
# Requires PGHOST environment variable (PGPORT, PGDATABASE, PGUSER optional - have defaults)
# Credentials should be in ~/.pgpass file
pytestmark = pytest.mark.skipif(
    not os.getenv('PGHOST'),
    reason="PostgreSQL tests require PGHOST environment variable and credentials in ~/.pgpass"
)


@pytest.fixture
def db_url():
    return PostgresURI.get_pguri().uri


@pytest.fixture
def rdb_manager(db_url):
    return RDBManager(db_url)


@pytest.fixture
def test_table_name(request, rdb_manager):
    """Generate worker-specific table name for parallel test isolation and ensure cleanup"""
    worker_id = getattr(request.config, 'workerinput', {}).get('workerid', 'master')
    table_name = f"test_table_{worker_id}"

    # Clean up before test (in case previous test failed to clean up)
    if rdb_manager.is_table(table_name):
        try:
            rdb_manager.drop_table(table_name)
        except Exception:
            pass  # Ignore errors if table doesn't exist

    yield table_name

    # Clean up after test
    if rdb_manager.is_table(table_name):
        try:
            rdb_manager.drop_table(table_name)
        except Exception:
            pass  # Ignore errors if table doesn't exist


@pytest.fixture
def test_index_name(request):
    """Generate worker-specific index name for parallel test isolation"""
    worker_id = getattr(request.config, 'workerinput', {}).get('workerid', 'master')
    return f"test_index_{worker_id}"


def test_sanitize_identifier():
    assert RDBManager.sanitize_identifier('valid_name') == 'valid_name'
    with pytest.raises(ValueError):
        RDBManager.sanitize_identifier('invalid-name')


def test_map_python_type_to_sqlalchemy():
    assert RDBManager.map_python_type_to_sqlalchemy(int) == Integer
    assert RDBManager.map_python_type_to_sqlalchemy(float) == Float
    assert RDBManager.map_python_type_to_sqlalchemy(str) == String
    assert RDBManager.map_python_type_to_sqlalchemy(datetime) == DateTime
    assert RDBManager.map_python_type_to_sqlalchemy(bytes) == String  # Default case


def test_creates_table_successfully(rdb_manager, test_table_name):
    schema = {"id": int, "name": str}
    table = rdb_manager.create_table(test_table_name, schema)
    assert table.name == test_table_name
    assert rdb_manager.is_table(test_table_name)
    rdb_manager.drop_table(test_table_name)


def test_raises_error_if_table_exists(rdb_manager, test_table_name):
    schema = {"id": int, "name": str}
    rdb_manager.create_table(test_table_name, schema)
    with pytest.raises(RDBManagerError):
        rdb_manager.create_table(test_table_name, schema)
    rdb_manager.drop_table(test_table_name)


def test_drops_table_successfully(rdb_manager, test_table_name):
    schema = {"id": int, "name": str}
    rdb_manager.create_table(test_table_name, schema)
    rdb_manager.drop_table(test_table_name)
    assert not rdb_manager.is_table(test_table_name)


def test_raises_error_if_table_does_not_exist(rdb_manager):
    with pytest.raises(ProgrammingError):
        rdb_manager.drop_table("nonexistent_table")


def test_creates_index_successfully(rdb_manager, test_table_name, test_index_name):
    schema = {"id": int, "name": str}
    rdb_manager.create_table(test_table_name, schema)
    rdb_manager.create_index(test_index_name, test_table_name, ["id"])
    inspector = rdb_manager.get_inspector()
    indexes = inspector.get_indexes(test_table_name)
    assert any(index['name'] == test_index_name for index in indexes)
    rdb_manager.drop_table(test_table_name)


def test_drops_index_successfully(rdb_manager, test_table_name, test_index_name):
    schema = {"id": int, "name": str}
    rdb_manager.create_table(test_table_name, schema)
    rdb_manager.create_index(test_index_name, test_table_name, ["id"])
    rdb_manager.drop_index(test_table_name, test_index_name)
    inspector = rdb_manager.get_inspector()
    indexes = inspector.get_indexes(test_table_name)
    assert not any(index['name'] == test_index_name for index in indexes)
    rdb_manager.drop_table(test_table_name)