import logging

import pymongo

from pyimport.command import Command


class DropCollectionCommand(Command):

    def __init__(self, audit=None, database=pymongo.database.Database):
        super().__init__(audit)
        self._name = "drop"
        self._log = logging.getLogger(__name__)
        self._database = database

    def execute(self,  args):
        # print( "arg:'{}'".format(arg))

        self._database.drop_collection(args.collection)


