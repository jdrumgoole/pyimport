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
        gfc = FieldFile.generate_field_file('mot.txt', delimiter="|", has_header=False)
        self.assertEqual(gfc.field_filename, "mot.tff")
        fc = FieldFile("mot.tff")
        self.assertEqual(len(fc.fields()), 14)
        self.assertEqual(fc.fields()[0], "No Header 1")
        self.assertEqual(fc.fields()[13], "No Header 14")
        reader = FileReader("mot.txt", delimiter="|", has_header=False)
        parser = CSVLineToDictParser(fc)
        for i, line in enumerate(reader.readline(), 1):
            doc = parser.parse_line(line, i)
            for field in fc.fields():
                self.assertTrue(field in doc, f"'{field}'")


    def test_aande(self):
        gfc = FieldFile.generate_field_file('AandEData.csv', delimiter=",", has_header=True)
        self.assertEqual(gfc.field_filename, "AandEData.tff")
        fc = FieldFile("AandEData.tff")
        self.assertEqual(len(fc.fields()), 20)
        self.assertEqual(fc.fields()[0], "No Header 1")
        self.assertEqual(fc.fields()[1], "No Header 2")
        self.assertEqual(fc.fields()[2], "SHA")
        self.assertEqual(fc.fields()[19], "Number of patients spending >12 hours from decision to admit to admission")
        reader = FileReader("AandEData.csv", delimiter=",", has_header=True)
        parser = CSVLineToDictParser(fc)
        for i, line in enumerate(reader.readline(), 1):
            doc = parser.parse_line(line, i)
            for field in fc.fields():
                self.assertTrue(field in doc, f"'{field}'")

    def test_plants(self):
        gfc = FieldFile.generate_field_file('plants.txt', delimiter="tab", has_header=True)
        self.assertEqual(gfc.field_filename, "plants.tff")
        fc = FieldFile("plants.tff")
        self.assertEqual(len(fc.fields()), 29)
