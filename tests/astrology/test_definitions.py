import unittest
from unittest.mock import patch, call
import logging

# Module to be tested
from src.astrology import definitions as astro_defs

# Disable logging for cleaner test output
logging.disable(logging.CRITICAL)

class TestAstrologyDefinitions(unittest.TestCase):

    def setUp(self):
        # Clear the cache before each test to ensure test isolation
        astro_defs.ASTROLOGY_DEFINITIONS_CACHE.clear()
        # Reset logger level if it was changed by other tests/modules
        logging.disable(logging.CRITICAL)


    def tearDown(self):
        # Clear the cache after each test
        astro_defs.ASTROLOGY_DEFINITIONS_CACHE.clear()
        logging.disable(logging.NOTSET) # Re-enable logging

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_success_first_load(self, mock_load_json):
        """Test successful retrieval of a definition, loading and caching the file."""
        mock_load_json.return_value = {"Sun in Aries": {"definition": "Bold and pioneering"}}
        
        # Patch ASTRO_DEFINITION_FILES for this test
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', ["planet_in_sign"]):
            definition = astro_defs.get_astro_definition("Sun in Aries")

        self.assertIsNotNone(definition)
        self.assertEqual(definition, {"definition": "Bold and pioneering"})
        mock_load_json.assert_called_once_with("planet_in_sign")
        self.assertIn("planet_in_sign", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        self.assertEqual(astro_defs.ASTROLOGY_DEFINITIONS_CACHE["planet_in_sign"], {"Sun in Aries": {"definition": "Bold and pioneering"}})

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_success_from_cache(self, mock_load_json):
        """Test successful retrieval of a definition from cache."""
        # Pre-populate cache
        astro_defs.ASTROLOGY_DEFINITIONS_CACHE["planet_in_sign"] = {"Sun in Aries": {"definition": "Cached and bold"}}
        
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', ["planet_in_sign"]):
            definition = astro_defs.get_astro_definition("Sun in Aries")

        self.assertIsNotNone(definition)
        self.assertEqual(definition, {"definition": "Cached and bold"})
        mock_load_json.assert_not_called() # Should not load from file

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_term_not_found_in_loaded_file(self, mock_load_json):
        """Test term not found in a successfully loaded file."""
        mock_load_json.return_value = {"Moon in Taurus": {"definition": "Stable and calm"}}
        
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', ["planet_in_sign"]):
            definition = astro_defs.get_astro_definition("Sun in Aries")

        self.assertIsNone(definition)
        mock_load_json.assert_called_once_with("planet_in_sign")
        self.assertIn("planet_in_sign", astro_defs.ASTROLOGY_DEFINITIONS_CACHE) # File is cached

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_term_not_found_in_any_file(self, mock_load_json):
        """Test term not found after checking multiple files."""
        mock_load_json.side_effect = [
            {"Moon in Taurus": {"definition": "Stable"}}, # Content for file1
            {"Mars in Leo": {"definition": "Fiery"}}      # Content for file2
        ]
        
        test_files = ["file1", "file2"]
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', test_files):
            definition = astro_defs.get_astro_definition("Sun in Aries")

        self.assertIsNone(definition)
        self.assertEqual(mock_load_json.call_count, 2)
        mock_load_json.assert_has_calls([call("file1"), call("file2")])
        self.assertIn("file1", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        self.assertIn("file2", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)

    def test_get_astro_definition_empty_term_name(self):
        """Test providing an empty term name."""
        definition = astro_defs.get_astro_definition("")
        self.assertIsNone(definition)

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_loader_returns_none(self, mock_load_json):
        """Test when the loader returns None (e.g., file not found or malformed)."""
        mock_load_json.return_value = None
        
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', ["missing_file"]):
            definition = astro_defs.get_astro_definition("Any Term")

        self.assertIsNone(definition)
        mock_load_json.assert_called_once_with("missing_file")
        # Cache should store an empty dict to prevent re-attempts for this file
        self.assertIn("missing_file", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        self.assertEqual(astro_defs.ASTROLOGY_DEFINITIONS_CACHE["missing_file"], {})

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_loader_returns_not_a_dict(self, mock_load_json):
        """Test when the loader returns content that is not a dictionary."""
        mock_load_json.return_value = ["This is a list, not a dict"] # Invalid content type
        
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', ["list_file"]):
            definition = astro_defs.get_astro_definition("Any Term")

        self.assertIsNone(definition)
        mock_load_json.assert_called_once_with("list_file")
        self.assertIn("list_file", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        # Cache stores the problematic content
        self.assertEqual(astro_defs.ASTROLOGY_DEFINITIONS_CACHE["list_file"], ["This is a list, not a dict"])


    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_loader_raises_exception(self, mock_load_json):
        """Test when the loader raises an unexpected exception."""
        mock_load_json.side_effect = Exception("Unexpected loader error!")
        
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', ["error_file"]):
            definition = astro_defs.get_astro_definition("Any Term")

        self.assertIsNone(definition)
        mock_load_json.assert_called_once_with("error_file")
        # Cache should store an empty dict to prevent re-attempts for this file on error
        self.assertIn("error_file", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        self.assertEqual(astro_defs.ASTROLOGY_DEFINITIONS_CACHE["error_file"], {})

    @patch('src.astrology.definitions.load_astro_json_file')
    def test_get_astro_definition_searches_multiple_files_finds_in_second(self, mock_load_json):
        """Test finding a term in the second file after not finding it in the first."""
        mock_load_json.side_effect = [
            {"TermA": {"desc": "From file1"}},  # Content for file1
            {"TermB": {"desc": "From file2"}, "Sun in Aries": {"definition": "Found it!"}} # Content for file2
        ]
        
        test_files = ["file1_astro", "file2_astro"]
        with patch('src.astrology.definitions.ASTRO_DEFINITION_FILES', test_files):
            definition = astro_defs.get_astro_definition("Sun in Aries")

        self.assertIsNotNone(definition)
        self.assertEqual(definition, {"definition": "Found it!"})
        self.assertEqual(mock_load_json.call_count, 2)
        mock_load_json.assert_has_calls([call("file1_astro"), call("file2_astro")])
        self.assertIn("file1_astro", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        self.assertIn("file2_astro", astro_defs.ASTROLOGY_DEFINITIONS_CACHE)
        self.assertEqual(astro_defs.ASTROLOGY_DEFINITIONS_CACHE["file2_astro"]["Sun in Aries"], {"definition": "Found it!"})

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)