from datetime import datetime

import pytest
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import Integer, Float, String, DateTime

from pyimport.db.postgresuri import PostgresURI

from pyimport.db.rdbmanager import RDBManager, RDBManagerError


@pytest.fixture
def db_url():
    return PostgresURI.get_pguri().uri


@pytest.fixture
def rdb_manager(db_url):
    return RDBManager(db_url)

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


def test_creates_table_successfully(rdb_manager):
    schema = {"id": int, "name": str}
    table = rdb_manager.create_table("test_table", schema)
    assert table.name == "test_table"
    assert rdb_manager.is_table("test_table")
    rdb_manager.drop_table("test_table")


def test_raises_error_if_table_exists(rdb_manager):
    schema = {"id": int, "name": str}
    rdb_manager.create_table("test_table", schema)
    with pytest.raises(RDBManagerError):
        rdb_manager.create_table("test_table", schema)
    rdb_manager.drop_table("test_table")


def test_drops_table_successfully(rdb_manager):
    schema = {"id": int, "name": str}
    rdb_manager.create_table("test_table", schema)
    rdb_manager.drop_table("test_table")
    assert not rdb_manager.is_table("test_table")


def test_raises_error_if_table_does_not_exist(rdb_manager):
    with pytest.raises(ProgrammingError):
        rdb_manager.drop_table("nonexistent_table")


def test_creates_index_successfully(rdb_manager):
    schema = {"id": int, "name": str}
    rdb_manager.create_table("test_table", schema)
    rdb_manager.create_index("test_index", "test_table", ["id"])
    inspector = rdb_manager.get_inspector()
    indexes = inspector.get_indexes("test_table")
    assert any(index['name'] == "test_index" for index in indexes)
    rdb_manager.drop_table("test_table")


def test_drops_index_successfully(rdb_manager):
    schema = {"id": int, "name": str}
    rdb_manager.create_table("test_table", schema)
    rdb_manager.create_index("test_index", "test_table", ["id"])
    rdb_manager.drop_index("test_table", "test_index")
    inspector = rdb_manager.get_inspector()
    indexes = inspector.get_indexes("test_table")
    assert not any(index['name'] == "test_index" for index in indexes)
    rdb_manager.drop_table("test_table")