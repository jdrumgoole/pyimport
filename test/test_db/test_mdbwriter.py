import pytest

from pyimport.argmgr import ArgMgr
from pyimport.db.syncmdbwriter import SyncMDBWriter


@pytest.fixture(scope="module")
def setup_args():
    args = ArgMgr.default_args().add_arguments(database="DBWRITER_TEST_DB", collection="DBWRITER_TEST_COLLECTION")
    return args


def test_dbwriter(setup_args):
    writer = SyncMDBWriter(setup_args.ns)
    assert writer.database.name == "DBWRITER_TEST_DB"
    assert writer.collection.name == "DBWRITER_TEST_COLLECTION"
    d = {"a": 1, "b": 2}
    writer.write(d)
    writer.write(None)
