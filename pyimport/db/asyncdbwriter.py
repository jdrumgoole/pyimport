from pyimport.db.syncmdbwriter import AsyncMDBWriter
from pyimport.db.asyncrdbwriter import AsyncRDBWriter


class AsyncDBWriter:

    def __init__(self, args):

        if args.mdburi:
            self._async_mdb_writer = AsyncMDBWriter(args)
            self._async_rdb_writer = None
        elif args.pguri:
            self._async_rdb_writer = AsyncRDBWriter(args)
            self._async_mdb_writer = None

    @property
    def mdb_writer(self):
        return self._async_mdb_writer

    @property
    def rdb_writer(self):
        return self._async_rdb_writer

    @property
    def writer(self):
        if self._async_mdb_writer:
            return self._async_mdb_writer
        elif self._async_rdb_writer:
            return self._async_rdb_writer
        else:
            raise ValueError("No writer created")

    async def write(self, doc):
        try:
            if self._async_mdb_writer:
                await self._async_mdb_writer.write(doc)
            elif self._async_rdb_writer:
                await self._async_rdb_writer.write(doc)
            self._total_written += 1
            return self._total_written
        except StopIteration:
            return self._total_written
