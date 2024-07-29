import asyncio
import functools
import time

import pymongo

from motor import motor_asyncio

from pyimport.argparser import ArgMgr
from pyimport.mdbwriter import MDBWriter, AsyncMDBWriter
from pyimport.rdbwiter import RDBWriter


class DBWriter:
    def __init__(self, args):

        if args.mdburi:
            self._mdb_writer = MDBWriter(args)
        if args.pguri:
            self._rdb_writer = RDBWriter(args)

    @property
    def mdb_writer(self):
        return self._mdb_writer

    @property
    def rdb_writer(self):
        return self._rdb_writer

    def write(self, doc):
        try:
            if self._mdb_writer:
                self._mdb_writer.write(doc)
            if self._rdb_writer:
                self._rdb_writer.write(doc)
            self._total_written += 1
            return self._total_written
        except StopIteration:
            return self._total_written

    @property
    def total_written(self):
        return self._total_written

    def close(self):
        if self._mdb_writer:
            self._mdb_writer.write(None)
        if self._rdb_writer:
            self._rdb_writer.write(None)

    def drop(self):
        return self._client.drop_database(self._args.database)


class AsyncDBWriter:

    def __init__(self, args):

        if args.mdburi:
            self._mdb_writer = AsyncMDBWriter(args)
        if args.pguri:
            self._rdb_writer = AsyncRDBWriter(args)


    @property
    def mdb_writer(self):
        return self._mdb_writer

    @property
    def rdb_writer(self):
        return self._rdb_writer

    async def write(self, doc):
        try:
            if self._mdb_writer:
                await self._mdb_writer.write(doc)
            if self._rdb_writer:
                await self._rdb_writer.write(doc)
            self._total_written += 1
            return self._total_written
        except StopIteration:
            return self._total_written



