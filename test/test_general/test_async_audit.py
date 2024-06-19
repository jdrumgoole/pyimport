"""
Created on 13-Apr-2018

@author: jdrumgoole
"""
import asyncio
import string
import time
import unittest
import random
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import timezone as tz, datetime as dt, datetime
import pymongo

from pyimport.asyncaudit import AsyncAudit
import pytest


@pytest.fixture(scope="function")
def mongo_client():
    return AsyncIOMotorClient(host="mongodb://localhost/TEST_AUDIT")


@pytest.fixture(scope="function")
def audit_db(mongo_client):
    return mongo_client["TEST_AUDIT"]


@pytest.fixture(scope="function")
def audit(audit_db):
    return AsyncAudit(database=audit_db, collection_name="audit")


@pytest.mark.asyncio
async def test_get_current_batch_id(audit):
    assert audit.in_batch() is False
    random_string = ''.join(random.choice(string.ascii_letters) for i in range(8))
    batch_id = await audit.start_batch(info={"test": random_string})
    assert audit.in_batch() is True
    await audit.end_batch(batch_id,info={"test": random_string})
    assert audit.in_batch() is False
    assert type(await audit.get_last_valid_batch()) is dict
    result = await audit.get_last_valid_batch()
    assert result["info"]["test"] == random_string
    assert await audit.get_last_valid_batch_id() == batch_id
    await audit.drop_collection()


@pytest.mark.asyncio
async def test_offset_aware(audit):
    test_start_time = dt.now(tz=tz.utc)
    time.sleep(0.001)
    await audit.collection.insert_one({"test": datetime.now(tz=tz.utc), "id": 1888})
    d = await audit.collection.find_one({'id': 1888})
    assert d["test"].tzinfo is not None
    assert d["test"] > test_start_time


@pytest.mark.asyncio
async def test_get_valid_batches(audit):
    await audit.drop_collection()
    test_start_time = dt.now(tz=tz.utc)
    id1 = await audit.start_batch(info={"test": "doc"})
    id2 = await audit.start_batch(info={"test": "doc"})
    await asyncio.sleep(0.1)
    assert id1 != id2
    assert audit.in_batch() is True
    await audit.end_batch(id2)
    assert audit.in_batch() is True
    await audit.end_batch(id1)
    assert audit.in_batch() is False
    async for i in audit.get_valid_batches():
        assert i["end"] >= i["start"]
        assert i["end"] >= test_start_time
        assert i["end"] <= dt.now(tz=tz.utc)
    # assert len(list(i async for i in audit.get_valid_batches())) == 2
    batch_end = await audit.get_batch_end(id1)
    assert batch_end["end"] > batch_end["start"]
    assert batch_end["end"] > test_start_time
    assert batch_end["end"] < dt.now(tz=tz.utc)

    ids = []
    async for i in audit.get_valid_batches():
        ids.append(i["batch_id"])

    assert id1.id in ids
    assert id2.id in ids

@pytest.mark.asyncio
async def test_get_last_batch_id(audit):
    id1 = await audit.start_batch(info={"test": "doc"})
    id2 = await audit.start_batch(info={"test": "doc"})
    id3 = await audit.start_batch(info={"test": "doc"})
    assert audit.in_batch() is True
    assert id1 != id2
    assert await audit.get_last_batch_id() == id3
    await audit.end_batch(id3)
    time.sleep(0.001)
    await audit.end_batch(id2)
    x = await audit.get_last_batch_id()
    assert x == id3
    time.sleep(0.001)
    await audit.end_batch(id1)
    y = await audit.get_last_valid_batch_id()
    assert y == id1
    assert audit.in_batch() is False
    id1 = await audit.start_batch(info={"test": "doc"})
    assert await audit.get_last_batch_id() == id1
    await audit.end_batch(id1)
    assert await audit.get_last_valid_batch_id() == id1
    assert audit.in_batch() is False



