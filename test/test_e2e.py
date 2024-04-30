import os
import unittest
from typing import Dict
from datetime import datetime

import pymongo
import dateutil

from pyimport.fieldfile import FieldFile, FieldNames
from pyimport.filereader import FileReader
from pyimport.csvlinetodictparser import CSVLineToDictParser

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class TestEndToEnd(unittest.TestCase):

    def test_in(self):
        #x = list(FieldNames.values)
        self.assertFalse(FieldNames.is_valid("hell"))
        self.assertTrue(FieldNames.is_valid("type"))
        self.assertTrue(FieldNames.is_valid("format"))
        self.assertTrue(FieldNames.is_valid("name"))

    def test_mot(self):
        gfc = FieldFile.generate_field_file(f('e2edata/mot.txt'), delimiter="|", has_header=False)
        self.assertEqual(gfc.field_filename, f("e2edata/mot.tff"))
        fc = FieldFile(f("e2edata/mot.tff"))
        self.assertEqual(len(fc.fields()), 14)
        self.assertEqual(fc.fields()[0], "No Header 1")
        self.assertEqual(fc.fields()[13], "No Header 14")
        reader = FileReader(f("e2edata/mot.txt"), delimiter="|", has_header=False)
        parser = CSVLineToDictParser(fc)
        for i, line in enumerate(reader.readline(), 1):
            doc = parser.parse_line(line, i)
            for field in fc.fields():
                self.assertTrue(field in doc, f"'{field}'")


    def test_aande(self):
        gfc = FieldFile.generate_field_file(f('e2edata/AandEData.csv'), delimiter=",", has_header=True)
        self.assertEqual(gfc.field_filename, f("e2edata/AandEData.tff"))
        fc = FieldFile(f("e2edata/AandEData.tff"))
        self.assertEqual(len(fc.fields()), 20)
        self.assertEqual(fc.fields()[0], "No Header 1")
        self.assertEqual(fc.fields()[1], "No Header 2")
        self.assertEqual(fc.fields()[2], "SHA")
        self.assertEqual(fc.fields()[19], "Number of patients spending >12 hours from decision to admit to admission")
        reader = FileReader(f("e2edata/AandEData.csv"), delimiter=",", has_header=True)
        parser = CSVLineToDictParser(fc)
        for i, line in enumerate(reader.readline(), 1):
            doc = parser.parse_line(line, i)
            for field in fc.fields():
                self.assertTrue(field in doc, f"'{field}'")

    def test_plants(self):
        gfc = FieldFile.generate_field_file(f('e2edata/plants.txt'), delimiter="tab", has_header=True)
        self.assertEqual(gfc.field_filename, f("e2edata/plants.tff"))
        fc = FieldFile(f("e2edata/plants.tff"))
        self.assertEqual(len(fc.fields()), 29)
        # cls.assertEqual(fc.fields()[0], "No Header 1")
        # cls.assertEqual(fc.fields()[1], "No Header 2")
        # cls.assertEqual(fc.fields()[2], "SHA")
        # cls.assertEqual(fc.fields()[19], "Number of patients spending >12 hours from decision to admit to admission")
