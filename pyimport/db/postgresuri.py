import os
from typing import Dict
from urllib.parse import urlparse, parse_qs


class PostgresURI:

    def __init__(self, uri: str):
        self.uri_dict = self.parse_postgres_url(uri)
        self._uri =  uri

    @classmethod
    def get_pguri(cls, default=None) -> "PostgresURI":
        uri = os.getenv('PGURI', default)
        if uri is None:
            raise ValueError("No PostgreSQL URI found in the environment variable PGURI")
        return PostgresURI(uri)

    @property
    def uri(self):
        return self._uri

    @property
    def scheme(self):
        return self.uri_dict['scheme']

    @property
    def username(self):
        return self.uri_dict['username']

    @property
    def password(self):
        return self.uri_dict['password']

    @property
    def host(self):
        return self.uri_dict['host']

    @property
    def port(self):
        return self.uri_dict['port']

    @property
    def database(self):
        return self.uri_dict['database']

    @database.setter
    def database(self, value):
        self.uri_dict['database'] = value
        self._uri = self.make_uri(**self.uri_dict)

    @property
    def query(self):
        return self.uri_dict['query']

    @staticmethod
    def make_uri(scheme: str = "postgresql",
                 username: str = "",
                 password: str = "",
                 host: str = "localhost",
                 port: int = 5432, database: str = "postgres",
                 query: Dict[str, str] = None):

        if scheme != 'postgresql':
            raise ValueError("Invalid scheme, expected 'postgres'")
        query_str = "&".join([f"{k}={v}" for k, v in query.items()]) if query else ""
        if query_str:
            query_str = f"?{query_str}"
        else:
            query_str = ""
        if username and password:
            auth_str = f"{username}:{password}@"
        elif username:
            auth_str = f"{username}@"
        else:
            auth_str = ""
        return f"{scheme}://{auth_str}{host}:{port}/{database}{query_str}"

    @staticmethod
    def parse_postgres_url(url):
        """
        Parse a PostgreSQL URL into its components.

        :param url: PostgreSQL URL string
        :return: Dictionary with URL components
        """
        parsed_url = urlparse(url)
        if parsed_url.scheme != 'postgresql':
            raise ValueError("Invalid PostgreSQL URL: bad scheme, expected 'postgres'")
        if not parsed_url.hostname:
            raise ValueError("Invalid PostgreSQL URL, no host defined")

        return {
            'scheme': parsed_url.scheme,
            'username': parsed_url.username if parsed_url.username else None,
            'password': parsed_url.password if parsed_url.password else None,
            'host': parsed_url.hostname,
            'port': int(parsed_url.port) if parsed_url.port else 5432,
            'database': parsed_url.path.lstrip('/'),
            'query': parse_qs(parsed_url.query) if parsed_url.query else None,
        }
