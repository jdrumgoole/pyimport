from datetime import datetime

import pytest
from unittest.mock import patch, MagicMock

from sqlalchemy import Integer, Float, String, DateTime
from sqlalchemy.exc import ProgrammingError

from pyimport.argparser import ArgMgr
from pyimport.rdbmanager import RDBManager  # Adjust the import according to your module

DB_URL = 'postgresql://user:password@localhost:5432/testdb'


@pytest.fixture
def default_args():
    args = ArgMgr.default_args()
    return args.ns


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


def test_create_table(default_args):
    mgr = RDBManager(default_args.pguri)
    schema = {'id': int, 'name': str}

    assert not mgr.is_table('test_table')
    mgr.create_table('test_table', schema)
    assert mgr.is_table('test_table')
    mgr.drop_table('test_table')
    assert not mgr.is_table('test_table')


def test_get_table(default_args):
    mgr = RDBManager(default_args.pguri)
    schema = {'id': int, 'name': str}
    assert not mgr.is_table('test_table')
    mgr.create_table('test_table', schema)
    table = mgr.get_table('test_table')
    assert table.name == 'test_table'
    mgr.drop_table('test_table')


def test_create_index(default_args):

    try:
        mgr = RDBManager(default_args.pguri)
        schema = {'id': int, 'name': str}
        assert not mgr.is_table('test_table')
        mgr.create_table('test_table', schema)
        tbl = mgr.get_table('test_table')
        assert tbl.name == 'test_table'
        assert mgr.is_table('test_table')
        tbl = mgr.get_table('test_table')
        assert tbl.name == 'test_table'
        mgr.create_index('test_index', 'test_table', ['name'])
        mgr.drop_index("test_table", 'test_index')
    finally:
        if mgr.is_table('test_table'):
            mgr.drop_table('test_table')


if __name__ == '__main__':
    pytest.main()
