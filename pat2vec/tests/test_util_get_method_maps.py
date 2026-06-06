import unittest
from pat2vec.util.get_method_default_fields_map import (
    get_default_fields_for_method,
    get_all_method_default_fields,
)
from pat2vec.util.get_method_index_map import get_index_for_method


class TestMethodMaps(unittest.TestCase):
    def test_get_default_fields_for_method(self):
        self.assertIsNotNone(get_default_fields_for_method("get_current_pat_bloods"))
        self.assertIsNone(get_default_fields_for_method("non_existent"))

    def test_get_all_method_default_fields(self):
        res = get_all_method_default_fields()
        self.assertIn("get_appointments", res)

    def test_get_index_for_method(self):
        self.assertEqual(get_index_for_method("get_demo"), "epr_documents")
