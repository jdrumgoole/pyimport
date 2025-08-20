import asyncio
import functools

from motor import motor_asyncio

from pyimport.argmgr import ArgMgr


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
        self._batchsize = args.batchsize
        self._first_time = True
        self._writer = None

    @classmethod
    async def create(cls, args):
        self = cls.__new__(cls)
        self._initialized = True
        self.__init__(args)
        self._writer = await self.writer_generator()
        return self

    async def write(self, doc):
        try:
            await self._writer.asend(doc)
        except StopAsyncIteration:
            pass

    async def close(self):
        try:
            self._writer.asend(None)
        except StopAsyncIteration:
            pass

    @start_coroutine
    async def writer_generator(self):
        buffer = []
        total_written = 0
        while True:
            doc = (yield)

            if doc is None:
                break

            buffer.append(doc)
            len_buffer = len(buffer)
            if len_buffer >= 1000:
                await self._collection.insert_many(buffer)
                total_written += len_buffer
                buffer = []

        if len(buffer) > 0:
            await self._collection.insert_many(buffer)


async def runner():
    args = ArgMgr.default_args().add_arguments(datatbase="ASYNC_TEST", collection="test")
    db_writer = await AsyncDBWriter.create(args.ns)
    try:
        await db_writer.write({"hello": "world"})
        await db_writer.write({"hello": "world"})
        await db_writer.write({"hello": "world!"})
        await db_writer.close()
    except StopAsyncIteration:
        print("Done")

asyncio.run(runner())