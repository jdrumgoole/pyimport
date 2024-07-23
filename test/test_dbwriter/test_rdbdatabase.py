from unittest.mock import patch, MagicMock

import pytest
import psycopg2
from psycopg2 import sql

from pyimport.argparser import ArgMgr
from pyimport.rdbmaker import RDBMaker

DB_URI = 'postgresql://user:password@localhost:5432/postgres'


@pytest.fixture
def default_args():
    args = ArgMgr.default_args()
    return args.ns


def test_connect(default_args):

    conn = psycopg2.connect(default_args.pguri)
    assert conn is not None
    conn.close()


def test_create_database(default_args):

    assert RDBMaker.is_database(default_args.pguri, 'testdb') is False
    RDBMaker.create_database(default_args.pguri, 'testdb')
    assert RDBMaker.is_database(default_args.pguri, 'testdb') is True
    RDBMaker.delete_database(default_args.pguri, 'testdb')
    assert RDBMaker.is_database(default_args.pguri, 'testdb') is False


@patch('psycopg2.connect')
def test_delete_database(mock_connect, default_args):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
