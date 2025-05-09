import unittest
import json
from unittest import mock
from src.knowledge_graph import loaders

class TestDefinitionLoaders(unittest.TestCase):

    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load')
    def test_get_astro_definition_success(self, mock_json_load, mock_open_file):
        """Test successful loading of an astro definition file."""
        expected_data = {"key": "value"}
        mock_json_load.return_value = expected_data
        
        definition_name = "test_astro_def"
        result = loaders.get_astro_definition(definition_name)
        
        mock_open_file.assert_called_once_with(
            mock.ANY, 'r'
        )
        # Check that the path contains the correct directory and filename
        call_args = mock_open_file.call_args[0][0]
        self.assertTrue(loaders.ASTRO_DEFINITIONS_DIR in call_args)
        self.assertTrue(f"{definition_name}.json" in call_args)

        mock_json_load.assert_called_once_with(mock_open_file())
        self.assertEqual(result, expected_data)

    @mock.patch('src.knowledge_graph.loaders.open', side_effect=FileNotFoundError)
    def test_get_astro_definition_file_not_found(self, mock_open_file):
        """Test handling of a missing astro definition file."""
        definition_name = "non_existent_astro_def"
        result = loaders.get_astro_definition(definition_name)
        
        mock_open_file.assert_called_once()
        self.assertIsNone(result)

    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load', side_effect=json.JSONDecodeError("Error", "doc", 0))
    def test_get_astro_definition_json_decode_error(self, mock_json_load, mock_open_file):
        """Test handling of a malformed JSON astro definition file."""
        definition_name = "malformed_astro_def"
        result = loaders.get_astro_definition(definition_name)
        
        mock_open_file.assert_called_once()
        mock_json_load.assert_called_once()
        self.assertIsNone(result)

    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load', side_effect=Exception("Unexpected error"))
    def test_get_astro_definition_unexpected_error(self, mock_json_load, mock_open_file):
        """Test handling of an unexpected error during astro definition loading."""
        definition_name = "error_astro_def"
        result = loaders.get_astro_definition(definition_name)

        mock_open_file.assert_called_once()
        mock_json_load.assert_called_once()
        self.assertIsNone(result)

    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load')
    def test_get_hd_definition_success(self, mock_json_load, mock_open_file):
        """Test successful loading of an HD definition file."""
        expected_data = {"hd_key": "hd_value"}
        mock_json_load.return_value = expected_data
        
        definition_name = "test_hd_def"
        result = loaders.get_hd_definition(definition_name)
        
        mock_open_file.assert_called_once_with(
            mock.ANY, 'r'
        )
        call_args = mock_open_file.call_args[0][0]
        self.assertTrue(loaders.HD_DEFINITIONS_DIR in call_args)
        self.assertTrue(f"{definition_name}.json" in call_args)
        
        mock_json_load.assert_called_once_with(mock_open_file())
        self.assertEqual(result, expected_data)

    @mock.patch('src.knowledge_graph.loaders.open', side_effect=FileNotFoundError)
    def test_get_hd_definition_file_not_found(self, mock_open_file):
        """Test handling of a missing HD definition file."""
        definition_name = "non_existent_hd_def"
        result = loaders.get_hd_definition(definition_name)
        
        mock_open_file.assert_called_once()
        self.assertIsNone(result)

    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load', side_effect=json.JSONDecodeError("Error", "doc", 0))
    def test_get_hd_definition_json_decode_error(self, mock_json_load, mock_open_file):
        """Test handling of a malformed JSON HD definition file."""
        definition_name = "malformed_hd_def"
        result = loaders.get_hd_definition(definition_name)
        
        mock_open_file.assert_called_once()
        mock_json_load.assert_called_once()
        self.assertIsNone(result)

    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load', side_effect=Exception("Unexpected error"))
    def test_get_hd_definition_unexpected_error(self, mock_json_load, mock_open_file):
        """Test handling of an unexpected error during HD definition loading."""
        definition_name = "error_hd_def"
        result = loaders.get_hd_definition(definition_name)

        mock_open_file.assert_called_once()
        mock_json_load.assert_called_once()
        self.assertIsNone(result)

    # Example of a test verifying structure (if loaders were more complex)
    # For current loaders, content verification is simply checking mock_json_load.return_value
    @mock.patch('src.knowledge_graph.loaders.open', new_callable=mock.mock_open)
    @mock.patch('json.load')
    def test_get_astro_definition_content_structure(self, mock_json_load, mock_open_file):
        """Test the expected structure of loaded astro definition data."""
        # This test is more conceptual for the current simple loaders,
        # as json.load directly returns the parsed structure.
        # If loaders performed transformation, this would be more critical.
        sample_data = {
            "planets": {
                "Sun": {"sign": "Aries"},
                "Moon": {"sign": "Taurus"}
            },
            "aspects": [
                {"type": "Conjunction", "planets": ["Sun", "Moon"]}
            ]
        }
        mock_json_load.return_value = sample_data
        
        result = loaders.get_astro_definition("sample_astro_def")
        self.assertEqual(result, sample_data)
        self.assertIn("planets", result)
        self.assertIn("Sun", result["planets"])
        self.assertIsInstance(result["aspects"], list)

if __name__ == '__main__':
    unittest.main()