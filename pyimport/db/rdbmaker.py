import os

import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError

from pyimport.db.postgresuri import PostgresURI


class RDBMaker:
    """Class to create and delete PostgreSQL databases using psycopg2"""

    def test_db_connection(postgres_uri: str) -> bool:
        try:
            conn = psycopg2.connect(postgres_uri)
            conn.close()
            return True
        except OperationalError:
            return False

    @staticmethod
    def create_database(postgres_uri: str, new_db: str):
        pg_uri = PostgresURI(postgres_uri)
        pg_uri.database = 'postgres'
        conn = psycopg2.connect(pg_uri.uri)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db)))
        cursor.close()
        conn.close()

    @staticmethod
    def delete_database(postgres_uri: str, dbname: str):
        pg_uri = PostgresURI(postgres_uri)
        pg_uri.database = 'postgres'
        conn = psycopg2.connect(pg_uri.uri)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(dbname)))
        cursor.close()
        conn.close()

    @staticmethod
    def is_database(postgres_url: str, dbname: str) -> bool:
        pg_uri = PostgresURI(postgres_url)
        pg_uri.database = 'postgres'
        conn = psycopg2.connect(pg_uri.uri)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (dbname,))
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists
