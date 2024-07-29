# test_rdb_writer.py
import os

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from pyimport.argparser import ArgMgr
from pyimport.rdbmanager import RDBManager
from pyimport.rdbwiter import RDBWriter
from test.rdbtest import RDBTestDB


@pytest.fixture
def default_args():
    args = ArgMgr.default_args()
    return args


def test_env() -> None:
    args = ArgMgr.default_args()
    assert args.ns.pguri is not None


def test_connect(default_args) -> None:
    with RDBTestDB(default_args.ns.pguri) as tr:
        assert tr.mgr.engine is not None


def test_rdbwriter(default_args) -> None:
    with RDBTestDB(default_args.ns.pguri) as tr:
        if tr.is_table("pyimport_test_rdb"):
            tr.drop_table("pyimport_test_rdb")
        assert tr.is_table("pyimport_test_rdb") is False
        tr.create_table("pyimport_test_rdb", RDBTestDB.test_schema_dict)
        assert tr.mgr.is_table("pyimport_test_rdb") is True
        tr.drop_table("pyimport_test_rdb")
        assert tr.is_table("pyimport_test_rdb") is False
        assert tr.mgr.is_table("pyimport_test_rdb") is False


def test_create_table(default_args) -> None:
    table_name = "test_table_name"
    args = default_args.add_arguments(table=table_name)
    mgr = None
    try:
        with RDBTestDB(default_args.ns.pguri) as tr:
            mgr = tr.mgr
            mgr.create_table(args.ns.table, tr.test_schema)
            # Verify the table creation
            inspector = inspect(mgr.engine)
            assert table_name in inspector.get_table_names()
    except ProgrammingError:
        pytest.skip(f"Error: table {args.ns.table} already exists")
    finally:
        if mgr.is_table(args.ns.table):
            mgr.drop_table(args.ns.table)


def test_mdb_resource(default_args) -> None:
    with RDBTestDB(default_args.ns.pguri) as tr:
        assert RDBTestDB.test_table_name == tr.get_test_table().name
        assert tr.test_table_name in tr.table_names


def test_insert(default_args) -> None:
    with RDBTestDB(default_args.ns.pguri) as tr:
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


def test_dbwriter_generator(default_args):
    with RDBTestDB(default_args.ns.pguri) as tr:
        d1 = {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com", "salary": 60000.0,
             "hire_date": datetime(2020, 5, 1)}
        d2 = {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com", "salary": 50000.0,
              "hire_date": datetime(2019, 7, 23)}
        tr.writer.write(d1)
        tr.writer.write(d2)
        tr.writer.write(None)
        result = tr.writer.find_one(tr.test_table_name, "name", "Alice")
        assert result.name == "Alice"
        result = tr.writer.find_one(tr.test_table_name, "name", "Bob")
        assert result.name == "Bob"

