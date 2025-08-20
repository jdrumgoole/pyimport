# test_rdbmaker.py
import psycopg2
import pytest
from psycopg2 import DatabaseError, OperationalError, ProgrammingError

from pyimport.db.postgresuri import PostgresURI
from pyimport.db.rdbmaker import RDBMaker


@pytest.fixture
def postgres_uri():
    return PostgresURI.get_pguri().uri


def test_connection(postgres_uri: str) -> bool:
    assert RDBMaker.test_db_connection(postgres_uri)


def test_database_exists(postgres_uri):
    dbname = "test_db"
    assert not RDBMaker.is_database(postgres_uri, dbname)


def test_create_test_database(postgres_uri):
    dbname = "test_db"
    RDBMaker.create_database(postgres_uri, dbname)
    assert RDBMaker.is_database(postgres_uri, dbname)
    RDBMaker.delete_database(postgres_uri, dbname)


def test_delete_test_database(postgres_uri):
    dbname = "test_db"
    RDBMaker.create_database(postgres_uri, dbname)
    RDBMaker.delete_database(postgres_uri, dbname)
    assert not RDBMaker.is_database(postgres_uri, dbname)


def test_creates_database_successfully(postgres_uri):
    dbname = "test_db"
    RDBMaker.create_database(postgres_uri, dbname)
    assert RDBMaker.is_database(postgres_uri, dbname)
    RDBMaker.delete_database(postgres_uri, dbname)


def test_deletes_database_successfully(postgres_uri):
    dbname = "test_db"
    RDBMaker.create_database(postgres_uri, dbname)
    assert RDBMaker.is_database(postgres_uri, dbname)
    RDBMaker.delete_database(postgres_uri, dbname)
    assert not RDBMaker.is_database(postgres_uri, dbname)


def test_handles_existing_database_creation(postgres_uri):
    dbname = "test_db"
    RDBMaker.create_database(postgres_uri, dbname)
    with pytest.raises(DatabaseError):
        RDBMaker.create_database(postgres_uri, dbname)
    RDBMaker.delete_database(postgres_uri, dbname)


def test_handles_nonexistent_database_deletion(postgres_uri):
    dbname = "nonexistent_db"
    with pytest.raises(ProgrammingError):  # psycopg2.errors.lookup(3D000) InvalidCatalogName
        RDBMaker.delete_database(postgres_uri, dbname)
    assert not RDBMaker.is_database(postgres_uri, dbname)


if __name__ == "__main__":
    pytest.main()
