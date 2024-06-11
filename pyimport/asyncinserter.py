import asyncio
from motor.motor_asyncio import AsyncIOMotorClient


class AsyncInserter:
    def __init__(self, collection_name, q:asyncio.Queue):
        self._collection_name = collection_name
        self._q = q

    async def __call__(self, batch_size=1000) -> int:
        buffer = []
        inserted = 0

        while True:
            doc = self._queue.get()
            inserted = inserted + 1
            if doc is None:
                break
            else:
                self._buffer.append(doc)
                if len(buffer) == batch_size:
                    await self._collection.insert_many(buffer)
                    buffer = []
        if len(buffer) > 0:
            await self._collection.insert_many(buffer)

        return inserted

