import pytest
from pyimport.postgresuri import PostgresURI


def test_parse_postgres_url():
    uri = 'postgresql://user:password@localhost:5432/mydatabase'
    parsed_uri = PostgresURI.parse_postgres_url(uri)
    assert parsed_uri['scheme'] == 'postgresql'
    assert parsed_uri['username'] == 'user'
    assert parsed_uri['password'] == 'password'
    assert parsed_uri['host'] == 'localhost'
    assert parsed_uri['port'] == 5432
    assert parsed_uri['database'] == 'mydatabase'
    assert parsed_uri['query'] is None


def test_make_uri():
    uri = PostgresURI.make_uri(username='user', password='password', host='localhost', port=5432, database='mydatabase')
    assert uri == 'postgresql://user:password@localhost:5432/mydatabase'
    pg_uri = PostgresURI(uri)
    assert pg_uri.uri == uri
    pg_uri.database = 'newdatabase'
    assert pg_uri.uri == 'postgresql://user:password@localhost:5432/newdatabase'



def test_properties():
    uri = 'postgresql://user:password@localhost:5432/mydatabase'
    postgres_uri = PostgresURI(uri)
    assert postgres_uri.scheme == 'postgresql'
    assert postgres_uri.username == 'user'
    assert postgres_uri.password == 'password'
    assert postgres_uri.host == 'localhost'
    assert postgres_uri.port == 5432
    assert postgres_uri.database == 'mydatabase'
    assert postgres_uri.query is None


def test_invalid_scheme():
    uri = 'http://user:password@localhost:5432/mydatabase'
    with pytest.raises(ValueError, match="Invalid PostgreSQL URL: bad scheme, expected 'postgres'"):
        PostgresURI.parse_postgres_url(uri)


def test_no_host():
    uri = 'postgresql://user:password@:5432/mydatabase'
    with pytest.raises(ValueError, match="Invalid PostgreSQL URL, no host defined"):
        PostgresURI.parse_postgres_url(uri)


if __name__ == '__main__':
    pytest.main()
