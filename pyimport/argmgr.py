import configargparse

from pyimport.argparser import make_parser, parse_args_and_cfg_files


class ArgMgr:

    def __init__(self, ns: configargparse.Namespace):
        self._args = ns

    def merge_namespace(self, ns: configargparse.Namespace) -> configargparse.Namespace:
        merged = configargparse.Namespace()
        merged.__dict__.update(vars(self._args))
        merged.__dict__.update(vars(ns))
        self._args = merged
        return merged

    def merge(self, am: "ArgMgr") -> "ArgMgr":
        return ArgMgr(self.merge_namespace(am._args))

    def __len__(self):
        return len(vars(self._args))

    @property
    def d(self) -> dict:
        return vars(self._args)

    @property
    def ns(self) -> configargparse.Namespace:
        return self._args

    @classmethod
    def default_args(cls, input_args=None) -> "ArgMgr":
        p = make_parser()
        args = parse_args_and_cfg_files(p, input_args=input_args)
        return ArgMgr(args)

    @classmethod
    def test_args(cls) -> "ArgMgr":
        """Create default args for testing with sensible defaults."""
        import multiprocessing
        from pyimport.doctimestamp import DocTimeStamp
        from pyimport.logger import ErrorResponse

        defaults = {
            'locator': False,
            'batchsize': 1000,
            'drop': False,
            'fieldfile': None,
            'delimiter': ',',
            'filenames': [],
            'filelist': None,
            'addfilename': False,
            'cut': None,
            'addtimestamp': DocTimeStamp.NO_TIMESTAMP,
            'addfield': None,
            'hasheader': False,
            'genfieldfile': False,
            'onerror': ErrorResponse.Warn,
            'loglevel': 'INFO',
            'silent': False,
            'no_color': False,
            'audit': False,
            'audithost': 'mongodb://localhost:27017',
            'auditcollection': 'audit',
            'auditdatabase': 'PYIMPORT_AUDIT',
            'info': '',
            'restart': False,
            'batch_id': None,
            'checkpoint_interval': 10000,
            'noenrich': False,
            'fieldinfo': None,
            'limit': 0,
            'database': 'PYIM',
            'collection': 'imported',
            'mdburi': 'mongodb://localhost:27017',
            'writeconcern': 0,
            'journal': False,
            'fsync': False,
            'pgtable': 'imported',
            'pguser': 'postgres',
            'pguri': 'postgresql://localhost:5432/postgres',
            'pgport': 5432,
            'pgdatabase': 'postgres',
            'pgpassword': None,
            'asyncpro': False,
            'multi': False,
            'poolsize': multiprocessing.cpu_count(),
            'forkmethod': 'fork',
            'splitfile': False,
            'autosplit': 2,
            'splitsize': 1024 * 10,
            'verbose': False,
            'input': False,
            'threads': False,
            'keepsplits': False,
            'argsource': False,
        }

        ns = configargparse.Namespace(**defaults)
        return ArgMgr(ns)

    @classmethod
    def args(cls, **kwargs) -> "ArgMgr":
        ns = configargparse.Namespace(**kwargs)
        return ArgMgr(ns)

    @staticmethod
    def default_args_dict() -> dict:
        return ArgMgr.ns_to_dict(ArgMgr.default_args())

    def add_arguments(self, **kwargs) -> "ArgMgr":
        new_ns = configargparse.Namespace(**kwargs)
        self.merge_namespace(new_ns)
        return self

    def copy(self) -> "ArgMgr":
        """Create a copy of this ArgMgr with the same arguments."""
        import copy as copy_module
        new_ns = copy_module.deepcopy(self._args)
        return ArgMgr(new_ns)

    @staticmethod
    def dict_to_ns(d: dict) -> configargparse.Namespace:
        """
        Convert a dictionary to an configargparse.Namespace object.

        :param d: Dictionary to convert
        :return: Namespace object with attributes corresponding to dictionary keys and values
        """
        return configargparse.Namespace(**d)

    @staticmethod
    def ns_to_dict(namespace: configargparse.Namespace) -> dict:
        """
        Convert an configargparse.Namespace object to a dictionary.

        :param namespace: Namespace object to convert
        :return: Dictionary with keys and values corresponding to Namespace attributes
        """
        return vars(namespace)

    def __getitem__(self, key):
        return self._args.__dict__[key]

    def __setitem__(self, key, value):
        self._args.__dict__[key] = value

    def __delitem__(self, key):
        del self._args.__dict__[key]

    def __contains__(self, item):
        return item in self._args.__dict__
