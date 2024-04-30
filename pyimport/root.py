"""
Created on 19 Aug 2017

@author: jdrumgoole
"""
import os


class Root(object):
    """
    What is the root directory for this project
    """

    def __init__(self):
        """
        Constructor
        """
        self._root = os.path.join(os.getenv("HOME"), "GIT", "pyimport")

    def root(self):
        return self._root

    def root_path(self, *path):
        return os.path.join(self._root, *path)


if __name__ == "__main__":
    r = Root()
    print("root : '%s'" % r.root())
    # print( "root path: '%s'" % r.root_path( "a", "b"))
