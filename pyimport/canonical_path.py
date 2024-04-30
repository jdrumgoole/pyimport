"""
Created on 10 Aug 2017

@author: jdrumgoole
"""
import os
import socket


class Canonical_Path(object):
    """
    Maintain a path as a canonical path. A canonical path is
    a path which contains the host filename an absolute path for a file.

    """

    def __init__(self, path):
        """
        Constructor
        """
        self._path = Canonical_Path.make_path(path)

    @staticmethod
    def make_path(path):
        return "%s:%s" % (socket.gethostname(), os.path.abspath(path))

    def __str__(self):
        return self._path

    def __repr__(self):
        return self.__str__()

    def __call__(self):
        return self._path

    def path(self):
        return ":".split(self._path)[1]

    def host(self):
        return ":".split(self._path)[0]
