import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import os
import shutil
import tempfile

# Import functions to be tested
from pat2vec.util.generate_elastic_schema import (
    generate_elastic_schema,
    generate_mapping_for_dataframe,
    create_schema_from_dataframe,
)


class TestGenerateElasticSchema(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = MagicMock()
        self.config.test_schema_path = os.path.join(
            self.test_dir, "elastic_schemas.json"
        )
        self.config.verbosity = 0

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_generate_mapping_for_dataframe_basic_types(self):
        """Test mapping generation for basic data types."""
        df = pd.DataFrame(
            {
                "str_col": ["a", "b"],
                "int_col": [1, 2],
                "float_col": [1.1, 2.2],
                "bool_col": [True, False],
                "date_col": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            }
        )
        mapping = generate_mapping_for_dataframe(df)

        self.assertEqual(mapping["str_col"]["type"], "keyword")
        self.assertEqual(mapping["int_col"]["type"], "long")
        self.assertEqual(mapping["float_col"]["type"], "double")
        self.assertEqual(mapping["bool_col"]["type"], "boolean")
        self.assertEqual(mapping["date_col"]["type"], "date")

    def test_generate_mapping_for_dataframe_nested_json(self):
        """Test mapping generation for columns containing JSON strings."""
        df = pd.DataFrame(
            {
                "json_col": ['{"a": 1, "b": "text"}', '{"a": 2, "c": true}'],
            }
        )
        mapping = generate_mapping_for_dataframe(df)
        self.assertEqual(mapping["json_col"]["type"], "nested")
        self.assertEqual(mapping["json_col"]["properties"]["a"]["type"], "long")
        self.assertEqual(mapping["json_col"]["properties"]["b"]["type"], "keyword")
        self.assertEqual(mapping["json_col"]["properties"]["c"]["type"], "boolean")

    def test_generate_mapping_for_dataframe_mixed_types_in_json(self):
        """Test mapping generation for mixed types within nested JSON."""
        df = pd.DataFrame(
            {
                "json_col": ['{"val": 1}', '{"val": "text"}'],
            }
        )
        mapping = generate_mapping_for_dataframe(df)
        # Should default to keyword if mixed types are detected
        self.assertEqual(mapping["json_col"]["type"], "nested")
        self.assertEqual(mapping["json_col"]["properties"]["val"]["type"], "keyword")

    def test_generate_elastic_schema_basic(self):
        """Test basic schema generation with a single index."""
        df = pd.DataFrame({"col1": ["a"], "col2": [1]})
        index_name = "test_index"
        schema = generate_elastic_schema(df, index_name)

        self.assertIn(index_name, schema)
        self.assertIn("mappings", schema[index_name])
        self.assertIn("properties", schema[index_name]["mappings"])
        self.assertIn("col1", schema[index_name]["mappings"]["properties"])
        self.assertEqual(
            schema[index_name]["mappings"]["properties"]["col1"]["type"], "keyword"
        )

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_create_schema_from_dataframe_new_file(
        self, mock_json_dump, mock_open_file
    ):
        """Test creating a schema from a DataFrame and saving to a new file."""
        df = pd.DataFrame({"col_a": ["val"], "col_b": [123]})
        index_name = "new_index"

        create_schema_from_dataframe(df, index_name, self.config)

        mock_open_file.assert_called_once_with(self.config.test_schema_path, "w")
        mock_json_dump.assert_called_once()
        args, kwargs = mock_json_dump.call_args
        generated_schema = args[0]
        self.assertIn(index_name, generated_schema)
        self.assertEqual(
            generated_schema[index_name]["mappings"]["properties"]["col_a"]["type"],
            "keyword",
        )

    @patch("os.path.exists", return_value=True)
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"existing_index": {"mappings": {"properties": {"old_col": {"type": "text"}}}}}',
    )
    @patch("json.load")
    @patch("json.dump")
    def test_create_schema_from_dataframe_existing_file(
        self, mock_json_dump, mock_json_load, mock_open_file, mock_exists
    ):
        """Test creating a schema and merging with an existing schema file."""
        mock_json_load.return_value = {
            "existing_index": {
                "mappings": {"properties": {"old_col": {"type": "text"}}}
            }
        }
        df = pd.DataFrame({"new_col": ["data"]})
        index_name = "another_index"

        create_schema_from_dataframe(df, index_name, self.config)

        mock_json_load.assert_called_once()
        mock_json_dump.assert_called_once()
        args, kwargs = mock_json_dump.call_args
        generated_schema = args[0]
        self.assertIn("existing_index", generated_schema)
        self.assertIn(index_name, generated_schema)
        self.assertEqual(
            generated_schema["existing_index"]["mappings"]["properties"]["old_col"][
                "type"
            ],
            "text",
        )
        self.assertEqual(
            generated_schema[index_name]["mappings"]["properties"]["new_col"]["type"],
            "keyword",
        )


if __name__ == "__main__":
    unittest.main()
