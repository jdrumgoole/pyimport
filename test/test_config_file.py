import os
import unittest

from pyimport.fieldfile import FieldFile, dict_to_fields

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def test_Config_File(self):
        ff= FieldFile(f("data/10k.tff"))
        self.assertTrue("test_id" in ff.fields())
        self.assertTrue("cylinder_capacity" in ff.fields())

        self.assertEqual(ff.type_value("test_id"), "int")
        self.assertEqual(ff.type_value("test_date"), "datetime")

    def test_property_prices(self):
        ff = FieldFile(f("data/uk_property_prices.tff"))
        self.assertTrue(ff.has_new_name("txn"))
        self.assertFalse(ff.name_value("txn") is None)

    def test_dict_to_fields(self):
        a = {"a": 1, "b": 2, "c": 3}
        b = {"w": 5, "z": a}
        c = {"m": a, "n": b}

        fields = dict_to_fields(a)
        self.assertEqual(len(fields), 3)
        self.assertEqual(["a", "b", "c"], fields)

        fields = dict_to_fields(b)
        self.assertEqual(len(fields), 4)
        self.assertEqual(["w", "a", "b", "c"], fields)

        fields = dict_to_fields(c)
        self.assertEqual(len(fields), 7)
        self.assertEqual(["a", "b", "c", "w", "a", "b", "c"], fields)


if __name__ == "__main__":
    unittest.main()
