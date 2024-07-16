# rdb_writer.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData, Table, inspect, select, Text, \
    Boolean, VARCHAR
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from typing import Dict, List, Any, Type

from pyimport.mdbwriter import start_generator
from pyimport.fieldfile import FieldFile

Base = declarative_base()

class RDBWriter:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._engine = create_engine(db_url)
        self._metadata = MetaData()
        self._session_factory = sessionmaker(bind=self.engine)
        self._metadata = MetaData()
        self._session = self.session_factory()
        self._inspector = inspect(self._engine)
        self._writer = self.write_generator()

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
    def map_python_type(py_type: Type) -> Any:
        if py_type == int:
            return Integer
        elif py_type == float:
            return Float
        elif py_type == str:
            return String
        elif py_type == datetime:
            return DateTime
        else:
            raise ValueError(f"Unsupported type: {py_type}")

    def create_table(self, table_name: str, ff: dict) -> Table:
        columns = []
        metadata = self.get_metadata()
        for name, py_type in ff.items():
            col_type = self.map_python_type(py_type)
            if name == "id":
                column = Column(name, col_type, primary_key=True)
            else:
                column = Column(name, col_type)
            columns.append(column)

        table = Table(table_name, metadata, *columns)
        metadata.create_all(self.engine)
        return table

    def get_table(self, table_name) -> Table:
        return Table(table_name, self._metadata, autoload_with=self._engine)

    def is_table(self, table_name):
        return table_name in self.get_inspector().get_table_names()

    def drop_table(self, table):
        if self.is_table(table.name):
            table.drop(self._engine)
        else:
            raise ValueError(f"Table {table.name} does not exist")

    def drop_table_by_name(self, table_name):
        if self.is_table(table_name):
            table = self.get_table(table_name)
            table.drop(self._engine)
        else:
            raise ValueError(f"Table {table_name} does not exist")

    def insert(self, table_name: str, list_of_dicts: List[Dict[str, Any]]) -> int:
        total_written = len(list_of_dicts)
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        session = self.session_factory()
        session.execute(table.insert(), list_of_dicts)
        session.commit()
        return total_written

    def write(self, doc):
        try:
            self._writer.send(doc)
            self._total_written += 1
            return self._total_written
        except StopIteration:
            return self._total_written

    @start_generator
    def write_generator(self, table_name:str):
        buffer = []
        total_written = 0
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        session = self.session_factory()
        while True:
            doc = yield
            if doc is None:
                break

            buffer.append(doc)
            len_buffer = len(buffer)
            if len_buffer >= 1000:

                session.execute(table.insert(), buffer)
                total_written = total_written + len_buffer
                session.commit()
                buffer = []

        if len(buffer) > 0:
            session.execute(table.insert(), buffer)
            session.commit()

    def find_one(self, table_name: str, column_name: str, key: Any) -> Any:
        metadata = self.get_metadata()
        # Reflect the table from the database
        table = Table(table_name, metadata, autoload_with=self.engine)
        session = self.get_session()
        try:
            # Access the column dynamically
            column = table.c[column_name]
            if column.type.python_type == type(key):
                # Create a select statement to query by column
                stmt = select(table).where(column == key)
                result = session.execute(stmt).fetchone()
            else:
                raise TypeError(f"Key type {type(key)} does not match column type {column.type}")
            return result
        finally:
            session.close()

    def close(self):
        if self._conn and self._engine:
            self._engine.dispose()
            self._engine = None


# Example Usage
# if __name__ == "__main__":
#     schema = {
#         "id": int,
#         "name": str,
#         "age": int,
#         "email": str,
#         "salary": float,
#         "hire_date": datetime
#     }
#
#     data = [
#         {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com", "salary": 60000.0,
#          "hire_date": datetime(2020, 5, 1)},
#         {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com", "salary": 50000.0,
#          "hire_date": datetime(2019, 7, 23)}
#     ]
#
#     db_url = "postgresql://username:password@localhost/exampledb"
#     writer = RDBWriter(db_url)
#     writer.create_table("employees", schema)
#     writer.insert("employees", data)
