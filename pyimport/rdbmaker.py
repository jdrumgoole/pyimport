import psycopg2
from psycopg2 import sql

from pyimport.postgresuri import PostgresURI


class RDBMaker:

    @staticmethod
    def create_database(postgres_uri: str, new_db: str):
        pg_uri = PostgresURI(postgres_uri)
        pg_uri.database = 'postgres'
        try:
            conn = psycopg2.connect(pg_uri.uri)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(new_db)
            ))
        finally:
            if conn:
                if cursor:
                    cursor.close()
                conn.close()

    @staticmethod
    def delete_database(postgres_uri:str, dbname: str):
        pg_uri = PostgresURI(postgres_uri)
        pg_uri.database = 'postgres'
        try:
            conn = psycopg2.connect(pg_uri.uri)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(sql.SQL("DROP DATABASE {}").format(
                sql.Identifier(dbname)
            ))
        finally:
            if conn:
                if cursor:
                    cursor.close()
                conn.close()

    @staticmethod
    def is_database(postgres_url: str, dbname: str) -> bool:
        """
        Check if a PostgreSQL database exists.

        :param postgres_url: PostgreSQL URL string
        :param dbname: Name of the database to check
        :return: True if the database exists, False otherwise
        """
        try:
            pg_uri = PostgresURI(postgres_url)
            pg_uri.database = 'postgres'
            # Connect to the 'postgres' database to check for the existence of the target database
            conn = psycopg2.connect(pg_uri.uri)
            conn.autocommit = True
            cursor = conn.cursor()

            # Use a parameterized query to prevent SQL injection
            query = sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s")
            cursor.execute(query, (dbname,))
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()

            return exists
        finally:
            if conn:
                if cursor:
                    cursor.close()
                conn.close()
