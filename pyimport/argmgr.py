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
    def default_args(cls) -> "ArgMgr":
        p = make_parser()
        args = parse_args_and_cfg_files(p)
        return ArgMgr(args)

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
