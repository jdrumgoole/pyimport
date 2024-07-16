import asyncio

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from test.fakedata import generate_user_profiles

from pyimport.asyncinserter import AsyncInserter


@pytest.mark.asyncio
async def test_generator():
    q: asyncio.Queue = asyncio.Queue()
    for doc in generate_user_profiles(10):
        await q.put(doc)
    assert q.qsize() == 10
    await q.put(None)


@pytest.mark.asyncio
async def test_inserter():
    client = AsyncIOMotorClient()
    db = client["TEST_ASYNC_INSERTER"]
    col = db["test"]
    q: asyncio.Queue = asyncio.Queue()
    inserter = AsyncInserter(col, q)
    for doc in generate_user_profiles(10):
        await q.put(doc)
    await q.put(None)
    await inserter()
    assert await col.count_documents({}) == 10
    print("count")
    assert q.qsize() == 0
    await q.join()
    await client.drop_database(db)


def test_run_inserter():
    asyncio.gather(test_inserter(), test_generator())
