import os
import unittest
from unittest.mock import patch
from config.settings import Settings

class TestSettingsValidation(unittest.TestCase):
    def test_missing_connection_string(self):
        # Settings should work without connection strings (they're optional)
        # Note: This test needs to clear .env.local loading
        with patch.dict(os.environ, {}, clear=True):
            # Create a new Settings instance without loading .env files
            from pydantic_settings import SettingsConfigDict
            from config.settings import Settings as BaseSettings
            
            class TestSettings(BaseSettings):
                model_config = SettingsConfigDict(
                    env_file=None,  # Don't load any .env files
                    extra='ignore',
                    case_sensitive=False
                )
            
            settings = TestSettings()
            # Should have empty connection_strings dict
            self.assertEqual(settings.connection_strings, {})

    @patch.dict(os.environ, {
        'DB_CONNECTION_STRING': 'Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=master;Uid=sa;Pwd=Pass;'
    })
    def test_valid_settings(self):
        settings = Settings()
        self.assertIsNotNone(settings.DB_CONNECTION_STRING)
        self.assertEqual(settings.MAX_QUERY_LENGTH, 10000)

if __name__ == '__main__':
    unittest.main()
