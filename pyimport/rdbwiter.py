

from sqlalchemy import Table, select

from typing import Dict, List, Any

from sqlalchemy.orm import declarative_base

from pyimport.mdbwriter import start_generator

from pyimport.rdbmanager import RDBManager

Base = declarative_base()


class RDBWriter:
    def __init__(self, mgr: RDBManager, table_name: str):
        self._mgr = mgr
        self._writer = self.write_generator(table_name)
        self._total_written = 0

    def insert(self, table_name: str, list_of_dicts: List[Dict[str, Any]]) -> int:
        total_written = len(list_of_dicts)
        metadata = self._mgr.get_metadata()
        table = Table(table_name, metadata, autoload_with=self._mgr.engine)
        session = self._mgr.session_factory()
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
        table = self._mgr.get_table(table_name)
        session = self._mgr.session_factory()
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
        metadata = self._mgr.get_metadata()
        # Reflect the table from the database
        table = Table(table_name, metadata, autoload_with=self._mgr.engine)
        session = self._mgr.get_session()
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
