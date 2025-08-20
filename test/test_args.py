import pytest
from pyimport.argmgr import ArgMgr


def test_args():
    a = ArgMgr.default_args()
    assert a is not None
    assert "mdburi" in a.ns
    assert "database" in a.ns
    assert "collection" in a.ns



def test_add():
    am = ArgMgr.default_args()
    am.add_arguments(mdburi="mongodb://localhost:27017/testy", database="testy", collection="testy")
    assert am.d["mdburi"] == "mongodb://localhost:27017/testy"
    assert am.d["database"] == "testy"
    assert am.d["collection"] == "testy"


def test_length():
    am = ArgMgr.default_args()
    original_length = len(am)
    am.add_arguments(mdburi="mongodb://localmdburi:27017/testy", database="testy", collection="testy")
    new_length = len(am)
    assert new_length == original_length
    am.add_arguments(bingo="bongo")
    assert len(am) == original_length + 1


def test_merge_namespaces():
    am1 = ArgMgr.default_args()
    assert "mdburi" in am1.d
    def_len = len(am1)
    am2 = ArgMgr.default_args()
    am3 = ArgMgr.default_args()
    am2.add_arguments(new_arg="is a test", listof=["1", "2", "3"])
    assert len(am2) == def_len + 2
    am3.add_arguments(a=50, b=60, c=70)
    assert len(am3) == len(am1) + 3
    am2.merge(am3)
    assert len(am2) == len(am1) + 5
    assert am1.d["mdburi"] == "mongodb://localhost:27017"
    assert am1.d["database"] == "PYIM"
    assert am1.d["collection"] == "imported"
    assert am2.d["mdburi"] == "mongodb://localhost:27017"
    assert am2.d["new_arg"] == "is a test"
    assert am2.d["listof"] == ["1", "2", "3"]
    assert am2.d["a"] == 50
    assert am2.d["b"] == 60
    assert am2.d["c"] == 70
    assert len(am2) == len(am1) + 5




