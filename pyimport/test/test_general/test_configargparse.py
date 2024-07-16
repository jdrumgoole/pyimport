import os.path

import pytest


from pyimport import argparser
from pyimport.argparser import parse_args_and_cfg_files, make_parser

home_dir = os.getenv("HOME")
def create_config_file(cfg_filename, **kwargs):
    with open(cfg_filename, "w") as file:
        for key, value in kwargs.items():
            file.write(f"{key}={value}\n")
    return os.path.abspath(cfg_filename)


def create_config_hierarchy():
    cfg_file = "pyimport.conf"
    f1 = create_config_file(cfg_file, mdburi="localhost1", database="test", collection="test")

    home_cfg = f"{home_dir}/.pyimport.conf"
    f2 = create_config_file(home_cfg, mdburi="localhost2", database="home", collection="home")
    return f1, f2


def test_configargparse():

    f1, f2 = create_config_hierarchy()

    try:
        args = parse_args_and_cfg_files(make_parser(), input_args=["--mdburi", "localhost"])
        assert args.mdburi == "localhost"
    except Exception as e:
        pytest.fail(f"Exception: {e}")
    finally:
        os.remove(f1)
        os.remove(f2)


def test_configargparse_env(monkeypatch):

    monkeypatch.setenv("MDB_URI", "localhost2")
    assert os.getenv("MDB_URI") == "localhost2"
    try:
        args = parse_args_and_cfg_files(make_parser())
        assert args.mdburi == "localhost2"
    except Exception as e:
        pytest.fail(f"Exception: {e}")

