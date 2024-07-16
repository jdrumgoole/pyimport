import asyncio
import functools
import time

import pymongo
import pytest
import pytest_asyncio
from motor import motor_asyncio

from pyimport.argparser import ArgMgr


def start_generator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)  # Initialize the generator
        return gen

    return wrapper


class MDBWriter:
    def __init__(self, args):

        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            self._client = pymongo.MongoClient(args.mdburi, w=args.writeconcern)
        else:
            self._client = pymongo.MongoClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)

        self._database = self._client[args.database]
        self._collection = self._database[args.collection]
        self._args = args
        self._writer = self.write_generator()
        self._total_written = 0
        self._buffer = []
        self._batch_size = args.batchsize

    @property
    def collection(self):
        return self._collection

    @property
    def database(self):
        return self._database

    @property
    def buffer_len(self):
        return len(self._buffer)

    @property
    def batch_size(self):
        return self._args.batchsize

    def write(self, doc):
        try:
            self._writer.send(doc)
            self._total_written += 1
            return self._total_written
        except StopIteration:
            return self._total_written

    @property
    def total_written(self):
        return self._total_written

    def close(self):
        self.write(None)

    def drop(self):
        return self._client.drop_database(self._args.database)

    @start_generator
    def write_generator(self):
        buffer = []
        while True:
            doc = yield
            if doc is None:
                break

            buffer.append(doc)
            len_buffer = len(buffer)
            if len_buffer >= self._batch_size:
                self._collection.insert_many(buffer)
                buffer = []
        if len(buffer) > 0:
            self._collection.insert_many(buffer)


def start_coroutine(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        await gen.asend(None)  # Initialize the generator
        return gen

    return wrapper


class AsyncDBWriter:

    def __init__(self, args):
        if not hasattr(self, '_initialized'):
            raise RuntimeError("Use the `create` class method to create an instance.")

        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            self._client = motor_asyncio.AsyncIOMotorClient(args.mdburi, w=args.writeconcern)
        else:
            self._client = motor_asyncio.AsyncIOMotorClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)

        self._database = self._client[args.database]
        self._collection = self._database[args.collection]
        self._args = args
        self._total_written = 0
        self._buffer = []
        self._batch_size = args.batchsize
        self._first_time = True
        self._writer = None

    @classmethod
    async def create(cls, args):
        self = cls.__new__(cls)
        self._initialized = True
        self.__init__(args)
        self._writer = await self.writer_generator()
        self._total_written = 0
        return self

    async def write(self, doc):

        try:
            await self._writer.asend(doc)
            self._total_written += 1
            return self._total_written
        except StopAsyncIteration:
            return self._total_written

    async def close(self):
        try:
            await self._writer.asend(None)
        except StopAsyncIteration:
            pass

    @start_coroutine
    async def writer_generator(self):
        buffer = []
        i=1
        while True:
            doc = (yield)
            if doc is None:
                break

            buffer.append(doc)
            len_buffer = len(buffer)
            if len_buffer >= 1000:
                await self._collection.insert_many(buffer)
                buffer = []

        if len(buffer) > 0:
            await self._collection.insert_many(buffer)


# if __name__ == "__main__":
#
#     args = ArgMgr.default_args()
#
#     async def runner(args):
#
#         async_db_writer = await AsyncDBWriter.create(args)
#         total_written = await async_db_writer.write({"name": "John", "age": 25})
#         print(f"Total written: {total_written}")
#         total_written = await async_db_writer.write({"name": "Jane", "age": 30})
#         print(f"Total written: {total_written}")
#         await async_db_writer.close()
#         print(f"Total written: {total_written}")
#
#     asyncio.run(runner(args.ns))
#
#     sync_db_writer = MDBWriter(args.ns)
#     total_written = sync_db_writer.write({"name": "John", "age": 25})
#     total_written = sync_db_writer.write({"name": "John", "age": 25})
#     total_written = sync_db_writer.write({"name": "John", "age": 25})
#     print(f"Total written sync : {total_written}")


