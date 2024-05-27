import logging

from pyimport.command import Command


class DropCollectionCommand(Command):

    def __init__(self, database, audit=None, id=None):
        super().__init__(audit, id)
        self._name = "drop"
        self._log = logging.getLogger(__name__)
        self._database = database

    def post_execute(self, arg):
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"database": self._database.name,
                                                            "collection_name": arg})
        self._log.info(f"dropped collection: {self._database.name}.{arg}")

    def execute(self, arg):
        # print( "arg:'{}'".format(arg))
        self._database.drop_collection(arg)
