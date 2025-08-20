from typing import List, Dict, Any

from sqlalchemy import Table, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from pyimport.db.syncmdbwriter import start_generator
from pyimport.db.rdbmanager import RDBManager


class AsyncRDBWriter:

    def __init__(self, mgr: RDBManager, table_name: str):
        self._mgr = mgr
        self._writer = self.write_generator(table_name)
        self._total_written = 0

    @property
    def total_written(self):
        return self._total_written

    async def insert(self, table_name: str, list_of_dicts: List[Dict[str, Any]]) -> int:
        total_written = len(list_of_dicts)
        metadata = self._mgr.get_metadata()
        table = Table(table_name, metadata, autoload_with=self._mgr.engine)
        async with self._mgr.async_session_factory() as session:
            async with session.begin():
                await session.execute(table.insert(), list_of_dicts)
        return total_written

    async def insert(self, table_name: str, dicts: List[Dict[str, Any]]) -> int:
        async with AsyncSession(self._mgr.engine) as session:
            async with session.begin():
                for data in dicts:
                    stmt = insert(table).values(**data)
                    await session.execute(stmt)
            await session.commit()
            
    async def write(self, doc):
        try:
            await self._writer.asend(doc)
            self._total_written += 1
            return self._total_written
        except StopIteration:
            return self._total_written

    @start_generator
    async def write_generator(self, table_name:str):
        buffer = []
        total_written = 0
        table = self._mgr.get_table(table_name)
        async with self._mgr.async_session_factory() as session:
            while True:
                doc = yield
                if doc is None:
                    break

                buffer.append(doc)
                len_buffer = len(buffer)
                if len_buffer >= 1000:
                    async with session.begin():
                        await session.execute(table.insert(), buffer)
                    total_written = total_written + len_buffer
                    buffer = []

            if len(buffer) > 0:
                async with session.begin():
                    await session.execute(table.insert(), buffer)


    async def find_one(self, table_name: str, column_name: str, key: Any) -> Any:
        metadata = self._mgr.get_metadata()
        # Reflect the table from the database
        table = Table(table_name, metadata, autoload_with=self._mgr.engine)
        async with self._mgr.async_session_factory() as session:
            try:
                # Access the column dynamically
                column = table.c[column_name]
                if column.type.python_type == type(key):
                    # Create a select statement to query by column
                    stmt = select(table).where(column == key)
                    result = await session.execute(stmt)
                    return result.fetchone()
                else:
                    raise TypeError(f"Key type {type(key)} does not match column type {column.type}")
            finally:
                await session.close
