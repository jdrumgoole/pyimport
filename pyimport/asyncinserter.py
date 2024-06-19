import asyncio
import logging
import pprint

from pyimport.timer import Timer


class AsyncInserter:
    def __init__(self, collection, q:asyncio.Queue, filename=None):
        self._collection = collection
        self._q = q
        self._timer = Timer()
        self._time_period = 1.0
        self._log = logging.getLogger(__name__)
        self._filename = filename if filename is not None else "AsyncInserter"

    async def __call__(self, batch_size=1000) -> int:
        buffer = []
        inserted = 0
        total_written = 0
        inserted_this_quantum = 0

        time_start = self._timer.start()
        while True:
            doc = await self._q.get()
            #pprint.pprint(doc)
            if doc is None:
                self._q.task_done()
                break
            else:
                buffer.append(doc)
                self._q.task_done()
                if len(buffer) == batch_size:
                    await self._collection.insert_many(buffer)
                    total_written = total_written + len(buffer)
                    inserted_this_quantum = inserted_this_quantum + len(buffer)
                    buffer = []
                    elapsed = self._timer.elapsed()
                    if elapsed > self._time_period:
                        docs_per_second = inserted_this_quantum / elapsed
                        self._timer.reset()
                        inserted_this_quantum = 0
                        self._log.info(f"Input:'{self._filename}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")
        if len(buffer) > 0:
            # pprint.pprint(buffer)
            await self._collection.insert_many(buffer)

        self._log.info(f"imported file: '{self._filename}' ({total_written} rows)")
        return total_written

