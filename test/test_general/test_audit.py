"""
Created on 13-Apr-2018

@author: jdrumgoole
"""
import string
import time
import unittest
import random
from datetime import timezone as tz, datetime as dt, datetime
import pymongo

from pyimport.audit import Audit
import pytest


@pytest.fixture(scope="module")
def mongo_client():
    client = pymongo.MongoClient(host="mongodb://localhost/TEST_AUDIT")
    yield client
    client.close()


@pytest.fixture(scope="module")
def audit_db(mongo_client):
    db = mongo_client["TEST_AUDIT"]
    yield db
    mongo_client.drop_database("TEST_AUDIT")


@pytest.fixture(scope="function")
def audit(audit_db):
    a = Audit(database=audit_db, collection_name="audit")
    yield a
    a.drop_collection()


def test_get_current_batch_id(audit):
    assert audit.in_batch() is False
    random_string = ''.join(random.choice(string.ascii_letters) for i in range(8))
    batch_id = audit.start_batch(info={"test": random_string})
    assert audit.in_batch() is True
    audit.end_batch(batch_id,info={"test": random_string})
    assert audit.in_batch() is False
    assert type(audit.get_last_valid_batch()) is dict
    assert audit.get_last_valid_batch()["info"]["test"] == random_string
    assert audit.get_last_valid_batch_id() == batch_id


def test_offset_aware(audit):
    test_start_time = dt.now(tz=tz.utc)
    time.sleep(0.001)
    audit.collection.insert_one({"test": datetime.now(tz=tz.utc), "id": 1888})
    d = audit.collection.find_one({'id': 1888})
    assert d["test"].tzinfo is not None
    assert d["test"] > test_start_time


def test_get_valid_batches(audit):
    test_start_time = dt.now(tz=tz.utc)
    id1 = audit.start_batch(info={"test": "doc"})
    id2 = audit.start_batch(info={"test": "doc"})

    assert id1 != id2
    assert audit.in_batch() is True
    audit.end_batch(id2)
    assert audit.in_batch() is True
    audit.end_batch(id1)
    assert audit.in_batch() is False
    assert len(list(audit.get_valid_batches())) == 2
    batch_end = audit.get_batch_end(id1)
    assert batch_end["end"] > batch_end["start"]
    assert batch_end["end"] > test_start_time
    assert batch_end["end"] < dt.now(tz=tz.utc)

    id_list = list(audit.get_valid_batch_ids())
    assert id1 in id_list
    assert id2 in id_list


def test_get_last_batch_id(audit):
    id1 = audit.start_batch(info={"test": "doc"})
    id2 = audit.start_batch(info={"test": "doc"})
    id3 = audit.start_batch(info={"test": "doc"})
    assert audit.in_batch() is True
    assert id1 != id2
    assert audit.get_last_batch_id() == id3
    audit.end_batch(id3)
    time.sleep(0.001)
    audit.end_batch(id2)
    x = audit.get_last_batch_id()
    assert x == id3
    time.sleep(0.001)
    audit.end_batch(id1)
    y = audit.get_last_valid_batch_id()
    assert y == id1
    assert audit.in_batch() is False
    id1 = audit.start_batch(info={"test": "doc"})
    assert audit.get_last_batch_id() == id1
    audit.end_batch(id1)
    assert audit.get_last_valid_batch_id() == id1
    assert audit.in_batch() is False


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
