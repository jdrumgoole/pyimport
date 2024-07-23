import re

import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData, Table, inspect
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Dict, Type

from pyimport.rdbmaker import RDBMaker


class RDBManager:

    sqlalchemy_type_map = {
        int: Integer,
        float: Float,
        str: String,
        datetime: DateTime
    }

    def __init__(self, db_url: str):
        self.db_url = db_url
        self._engine = create_engine(db_url)

        self._metadata = MetaData()
        self._session_factory = sessionmaker(bind=self._engine)
        self._session = self._session_factory()


    @property
    def engine(self):
        return self._engine

    @property
    def metadata(self):
        return self._metadata

    @property
    def session_factory(self):
        return self._session_factory

    def get_session(self):
        return self._session_factory()

    def get_inspector(self):
        inspector = inspect(self._engine)
        return inspector

    @property
    def metadata(self):
        return self._metadata

    @staticmethod
    def get_metadata():
        return MetaData()
    @staticmethod
    def sanitize_identifier(name: str) -> str:
        # Allow only alphanumeric characters and underscores in identifiers
        if not re.match(r'^[A-Za-z0-9_]+$', name):
            raise ValueError("Invalid identifier")
        return name

    @classmethod
    def map_python_type_to_sqlalchemy(cls, py_type: Type) -> Column:

        return cls.sqlalchemy_type_map.get(py_type, String)

    def create_database(self, db_url:str, db_name: str):
        RDBMaker.create_database(db_url, db_name)
        self._engine = create_engine(db_url)

    def create_table(self, table_name: str, schema: Dict[str, Type]):
        table_name = self.sanitize_identifier(table_name)
        columns = [Column(name, self.map_python_type_to_sqlalchemy(py_type)) for name, py_type in schema.items()]
        table = Table(table_name, self.get_metadata(), *columns)
        table.create(self._engine)

    def get_table(self, table_name) -> Table:
        return Table(table_name, self._metadata, autoload_with=self._engine)

    def is_table(self, table_name):
        inspector = inspect(self._engine)
        return table_name in inspector.get_table_names()

    def drop_table(self, table_name: str):
        table_name = self.sanitize_identifier(table_name)
        if self.is_table(table_name):
            table = Table(table_name, self._metadata, autoload_with=self._engine)
            table.drop(self._engine)
        else:
            raise ProgrammingError(f"Table {table_name} does not exist")

    def create_index(self, index_name: str, table_name: str, columns: list):
        index_name = self.sanitize_identifier(index_name)
        table_name = self.sanitize_identifier(table_name)
        table = Table(table_name, self._metadata, autoload_with=self._engine)
        index_columns = [table.c[column] for column in columns]
        index = sqlalchemy.Index(index_name, *index_columns)
        index.create(self._engine)

    def drop_index(self, table_name:str, index_name: str):
        index_name = self.sanitize_identifier(index_name)
        metadata = MetaData()
        # Reflect the table from the database
        table = Table(table_name, metadata, autoload_with=self._engine)
        index = sqlalchemy.Index(name=index_name, _table=table)
        index.drop(self._engine)
