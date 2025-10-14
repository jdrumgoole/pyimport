# test_rdbmaker.py
import psycopg2
import pytest
from psycopg2 import DatabaseError, OperationalError, ProgrammingError

from pyimport.db.postgresuri import PostgresURI
from pyimport.db.rdbmaker import RDBMaker


@pytest.fixture
def postgres_uri():
    return PostgresURI.get_pguri().uri


@pytest.fixture
def test_db_name(request):
    """Generate worker-specific database name for parallel test isolation"""
    worker_id = getattr(request.config, 'workerinput', {}).get('workerid', 'master')
    return f"test_db_{worker_id}"


def test_connection(postgres_uri: str) -> bool:
    assert RDBMaker.test_db_connection(postgres_uri)


def test_database_exists(postgres_uri, test_db_name):
    assert not RDBMaker.is_database(postgres_uri, test_db_name)


def test_create_test_database(postgres_uri, test_db_name):
    RDBMaker.create_database(postgres_uri, test_db_name)
    assert RDBMaker.is_database(postgres_uri, test_db_name)
    RDBMaker.delete_database(postgres_uri, test_db_name)


def test_delete_test_database(postgres_uri, test_db_name):
    RDBMaker.create_database(postgres_uri, test_db_name)
    RDBMaker.delete_database(postgres_uri, test_db_name)
    assert not RDBMaker.is_database(postgres_uri, test_db_name)


def test_creates_database_successfully(postgres_uri, test_db_name):
    RDBMaker.create_database(postgres_uri, test_db_name)
    assert RDBMaker.is_database(postgres_uri, test_db_name)
    RDBMaker.delete_database(postgres_uri, test_db_name)


def test_deletes_database_successfully(postgres_uri, test_db_name):
    RDBMaker.create_database(postgres_uri, test_db_name)
    assert RDBMaker.is_database(postgres_uri, test_db_name)
    RDBMaker.delete_database(postgres_uri, test_db_name)
    assert not RDBMaker.is_database(postgres_uri, test_db_name)


def test_handles_existing_database_creation(postgres_uri, test_db_name):
    RDBMaker.create_database(postgres_uri, test_db_name)
    with pytest.raises(DatabaseError):
        RDBMaker.create_database(postgres_uri, test_db_name)
    RDBMaker.delete_database(postgres_uri, test_db_name)


def test_handles_nonexistent_database_deletion(postgres_uri):
    dbname = "nonexistent_db"
    with pytest.raises(ProgrammingError):  # psycopg2.errors.lookup(3D000) InvalidCatalogName
        RDBMaker.delete_database(postgres_uri, dbname)
    assert not RDBMaker.is_database(postgres_uri, dbname)


if __name__ == "__main__":
    pytest.main()
