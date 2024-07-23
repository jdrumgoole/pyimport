# test_rdb_writer.py
import os

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from pyimport.argparser import ArgMgr
from pyimport.rdbmanager import RDBManager
from pyimport.rdbwiter import RDBWriter
from test.rdbtest import RDBTestDB


@pytest.fixture
def default_args():
    args = ArgMgr.default_args()
    return args.ns


def test_env() -> None:
    args = ArgMgr.default_args()
    assert args.ns.pguri is not None


def test_connect(default_args) -> None:
    with RDBTestDB(default_args.pguri) as tr:
        assert tr.mgr.engine is not None


def test_rdbwriter(default_args) -> None:
    with RDBTestDB(default_args.pguri) as tr:
        assert tr.mgr.is_table("pyimport_test") is True
        tr.create_table("pyimport_test_rdb", RDBTestDB.test_schema_dict)
        assert tr.mgr.is_table("pyimport_test_rdb") is True
        tr.mgr.drop_table("pyimport_test_rdb")
        assert tr.mgr.is_table("pyimport_test_rdb") is False


def test_create_table() -> None:
    with RDBTestDB() as tr:

        tr.writer.create_table("test_table_name", tr.test_schema)
        # Verify the table creation
        inspector = inspect(tr.writer._engine)
        assert "test_table_name" in inspector.get_table_names()


def test_mdb_resource() -> None:
    with RDBTestDB() as tr:
        assert tr.test_table_name == tr.get_test_table().name
        assert tr.test_table_name in tr.table_names


def test_insert() -> None:
    with RDBTestDB() as tr:
        # Verify the table creation
        assert tr.test_table_name == tr.get_test_table().name
        data = [
            {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com", "salary": 60000.0,
             "hire_date": datetime(2020, 5, 1)},
            {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com", "salary": 50000.0,
             "hire_date": datetime(2019, 7, 23)}
        ]
        total_written = tr.writer.insert(tr.test_table_name, data)
        assert total_written == 2
        result = tr.writer.find_one(tr.test_table_name, "name", "Alice")
        assert result.name == "Alice"

def test_dbwriter_generator():
    with RDBTestDB() as tr:
        writer = RDBWriter(tr.args.ns)
        assert writer.database.name == "DBWRITER_TEST_DB"
        assert writer.collection.name == "DBWRITER_TEST_COLLECTION"
        assert writer.docs_per_second == 0
        d = {"a": 1, "b": 2}
        writer.write(d)
        writer.write(None)
