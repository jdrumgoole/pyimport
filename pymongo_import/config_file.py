"""
Read a ConfigFile into a dict so we can access without ConfigParser overhead.
"""

from collections import OrderedDict

from configparser import RawConfigParser

class Config_File(object):

    def __init__(self, filename=None):

        self._cfg = RawConfigParser()
        self._tags = ["name", "type", "format"]
        self._filename = filename
        self._fieldDict = None
        self._idField = None
        if self._filename:
            self._fieldDict = self.read(self._filename)


    def field_dict(self):
        return self._fieldDict

    def read(self, filename):

        fieldDict = OrderedDict()

        result = self._cfg.read(filename)
        if len(result) == 0:
            raise OSError("Couldn't open '{}'".format(filename))

        self._fields = self._cfg.sections()

        for s in self._fields:
            # print( "section: '%s'" % s )
            fieldDict[s] = {}
            for o in self._cfg.options(s):
                # print("option : '%s'" % o )
                if not o in self._tags:
                    raise ValueError("No such field type: %s in section: %s" % (o, s))
                if (o == "name"):
                    if (self._cfg.get(s, o) == "_id"):
                        if self._idField == None:
                            self._idField = s
                        else:
                            raise ValueError(self.duplicateIDMsg(self._idField, s))

                fieldDict[s][o] = self._cfg.get(s, o)

            if not "name" in fieldDict[s]:
                fieldDict[s]["name"] = s

            if not "format" in fieldDict[s]: #Need to exist for date/datetime fields
                fieldDict[s]["format"] = None

        return fieldDict

    def fieldDict(self):
        if self._fieldDict is None:
            raise ValueError("trying retrieve a fieldDict which has a 'None' value")
        else:
            return self._fieldDict

    def fields(self):
        return self._fields

    def hasNewName(self, section):
        return section != self._fieldDict[section]['name']


    def type_value(self, fieldName):
        return self._fieldDict[fieldName]["type"]
        #return self._cfg.get(fieldName, "type")

    def format_value(self, fieldName):
        return self._fieldDict[fieldName]["format"]
        #return self._cfg.get(fieldName, "format")

    def name_value(self, fieldName):
        return self._fieldDict[fieldName]["format"]
        #return self._cfg.get(fieldName, "name")
