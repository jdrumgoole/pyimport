import functools

import pymongo

from pyimport.argparser import ArgMgr
from pyimport.timer import Timer


def auto_start_generator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        # Ensure it's a generator
        if not isinstance(gen, (list, tuple)) and callable(getattr(gen, 'send', None)):
            next(gen)
        return gen
    return wrapper


class DBWriter:
    def __init__(self, args, time_period=1.0):

        if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
            self._client = pymongo.MongoClient(args.host, w=args.writeconcern)
        else:
            self._client = pymongo.MongoClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

        self._database = self._client[args.database]
        self._collection = self._database[args.collection]
        self._args = args
        self._time_period = time_period
        self._docs_per_second = 0
        self._writer = self._write_gen()

    @property
    def collection(self):
        return self._collection

    @property
    def database(self):
        return self._database

    @property
    def docs_per_second(self):
        return self._docs_per_second

    def write(self, doc):
        try:
            self._writer.send(doc)
        except StopIteration:
            pass

    @auto_start_generator
    def _write_gen(self):
        buffer = []
        total_written = 0
        inserted_this_quantum = 0
        loop_timer = Timer(start_now=True)
        while True:
            doc = (yield)
            if doc is None:
                break
            buffer.append(doc)
            if len(buffer) >= self._args.batchsize:
                self._collection.insert_many(buffer)
                total_written = total_written + len(buffer)
                inserted_this_quantum = inserted_this_quantum + len(buffer)
                buffer = []
                elapsed = loop_timer.elapsed()
                if elapsed >= self._time_period:
                    self._docs_per_second = inserted_this_quantum / elapsed
                    loop_timer.reset()
                    inserted_this_quantum = 0

        if len(buffer) > 0:
            self._collection.insert_many(buffer)

    def drop(self):
        return self._client.drop_database(self._args.database)


if __name__ == "__main__":
    args = ArgMgr.default_args().add_arguments(database="TEST_WRITE", collection="test")
    writer = DBWriter(args=args.ns)
    writer.write({"hello": "world"})
    writer.write({"goodbye": "world"})
    writer.write(None)


