import asyncio
import functools
import time

import pymongo
from motor import motor_asyncio

from pyimport.argmgr import ArgMgr


def start_generator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)  # Initialize the generator
        return gen

    return wrapper


class SyncMDBWriter:
    def __init__(self, args, audit=None, batch_id=None, filename=None):

        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            self._client = pymongo.MongoClient(args.mdburi, w=args.writeconcern)
        else:
            self._client = pymongo.MongoClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)

        self._database = self._client[args.database]
        self._collection = self._database[args.collection]
        self._args = args
        self._audit = audit
        self._batch_id = batch_id
        self._filename = filename
        self._checkpoint_interval = getattr(args, 'checkpoint_interval', 10000)
        self._writer = self.write_generator()
        self._total_written = 0
        self._buffer = []
        self._batch_size = args.batchsize

    @property
    def client(self):
        return self._client

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
        docs_written = 0
        last_checkpoint = 0
        while True:
            doc = yield
            if doc is None:
                break

            buffer.append(doc)
            len_buffer = len(buffer)
            if len_buffer >= self._batch_size:
                self._collection.insert_many(buffer)
                docs_written += len(buffer)
                buffer = []

                # Record checkpoints for all intervals crossed since last checkpoint
                if self._audit and self._batch_id and self._filename:
                    while docs_written - last_checkpoint >= self._checkpoint_interval:
                        last_checkpoint += self._checkpoint_interval
                        last_line = doc.get('_line_number') if isinstance(doc, dict) else None
                        self._audit.record_progress(
                            batch_id=self._batch_id,
                            filename=self._filename,
                            docs_written=last_checkpoint,
                            last_line_number=last_line,
                            status="in_progress"
                        )

        if len(buffer) > 0:
            self._collection.insert_many(buffer)
            docs_written += len(buffer)

            # Record checkpoints for all intervals crossed since last checkpoint
            if self._audit and self._batch_id and self._filename:
                while docs_written - last_checkpoint >= self._checkpoint_interval:
                    last_checkpoint += self._checkpoint_interval
                    # Get the last doc from buffer for line number
                    last_doc = buffer[-1] if buffer else doc
                    last_line = last_doc.get('_line_number') if isinstance(last_doc, dict) else None
                    self._audit.record_progress(
                        batch_id=self._batch_id,
                        filename=self._filename,
                        docs_written=last_checkpoint,
                        last_line_number=last_line,
                        status="in_progress"
                    )


def start_coroutine(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        await gen.asend(None)  # Initialize the generator
        return gen

    return wrapper


class AsyncMDBWriter:

    def __init__(self, args, audit=None, batch_id=None, filename=None):
        if not hasattr(self, '_initialized'):
            raise RuntimeError("Use the `create` class method to create an instance.")

        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            self._client = motor_asyncio.AsyncIOMotorClient(args.mdburi, w=args.writeconcern)
        else:
            self._client = motor_asyncio.AsyncIOMotorClient(args.mdburi, w=args.writeconcern, fsync=args.fsync, j=args.journal)

        self._database = self._client[args.database]
        self._collection = self._database[args.collection]
        self._args = args
        self._audit = audit
        self._batch_id = batch_id
        self._filename = filename
        self._checkpoint_interval = getattr(args, 'checkpoint_interval', 10000)
        self._total_written = 0
        self._buffer = []
        self._batch_size = args.batchsize
        self._first_time = True
        self._writer = None

    @classmethod
    async def create(cls, args, audit=None, batch_id=None, filename=None):
        self = cls.__new__(cls)
        self._initialized = True
        self.__init__(args, audit, batch_id, filename)
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
        docs_written = 0
        last_checkpoint = 0
        while True:
            doc = (yield)
            if doc is None:
                break

            buffer.append(doc)
            len_buffer = len(buffer)
            if len_buffer >= 1000:
                await self._collection.insert_many(buffer)
                docs_written += len(buffer)
                buffer = []

                # Record checkpoints for all intervals crossed since last checkpoint
                if self._audit and self._batch_id and self._filename:
                    while docs_written - last_checkpoint >= self._checkpoint_interval:
                        last_checkpoint += self._checkpoint_interval
                        last_line = doc.get('_line_number') if isinstance(doc, dict) else None
                        await self._audit.record_progress(
                            batch_id=self._batch_id,
                            filename=self._filename,
                            docs_written=last_checkpoint,
                            last_line_number=last_line,
                            status="in_progress"
                        )

        if len(buffer) > 0:
            await self._collection.insert_many(buffer)
            docs_written += len(buffer)

            # Record checkpoints for all intervals crossed since last checkpoint
            if self._audit and self._batch_id and self._filename:
                while docs_written - last_checkpoint >= self._checkpoint_interval:
                    last_checkpoint += self._checkpoint_interval
                    # Get the last doc from buffer for line number
                    last_doc = buffer[-1] if buffer else doc
                    last_line = last_doc.get('_line_number') if isinstance(last_doc, dict) else None
                    await self._audit.record_progress(
                        batch_id=self._batch_id,
                        filename=self._filename,
                        docs_written=last_checkpoint,
                        last_line_number=last_line,
                        status="in_progress"
                    )


# if __name__ == "__main__":
#
#     args = ArgMgr.default_args()
#
#     async def runner(args):
#
#         async_db_writer = await AsyncMDBWriter.create(args)
#         total_written = await async_db_writer.write({"name": "John", "age": 25})
#         print(f"Total written: {total_written}")
#         total_written = await async_db_writer.write({"name": "Jane", "age": 30})
#         print(f"Total written: {total_written}")
#         await async_db_writer.close()
#         print(f"Total written: {total_written}")
#
#     asyncio.run(runner(args.ns))
#
#     sync_db_writer = SyncMDBWriter(args.ns)
#     total_written = sync_db_writer.write({"name": "John", "age": 25})
#     total_written = sync_db_writer.write({"name": "John", "age": 25})
#     total_written = sync_db_writer.write({"name": "John", "age": 25})
#     print(f"Total written sync : {total_written}")


