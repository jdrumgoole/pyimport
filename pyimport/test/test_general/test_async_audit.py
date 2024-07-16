"""
Created on 13-Apr-2018

@author: jdrumgoole
"""
import asyncio
import time

from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
import pytz

from pyimport.asyncaudit import AsyncAudit
from test.mdbtest import AsyncMDBTestDB
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
async def xtest_offset_aware(audit):
    async with AsyncMDBTestDB() as tr:
        test_start_time = datetime.now(tz=timezone.utc)
        await tr.test_col.insert_one({"test_time": test_start_time, "id": 1})
        d = await tr.test_col.find_one({'id': 1})
        assert d["test_time"] == test_start_time
        asyncio.sleep(0.1)
        await audit.collection.insert_one({"test": datetime.now(tz=tz.utc), "id": 2})
        d = await tr.test_col.find_one({'id': 1888})
        tz_aware_d = d["test"].replace(tzinfo=pytz.utc)
        assert tz_aware_d.tzinfo is not None
        assert tz_aware_d > test_start_time








