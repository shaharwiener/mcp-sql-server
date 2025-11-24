import sys
import os
import unittest
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings

class TestSettings(unittest.TestCase):
    @patch.dict(os.environ, {
        'PANGO_ENV': 'Test',
        'DB_NAME': 'LocalDB',
        'DB_CONNECTION_STRING': 'Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=master;Uid=sa;Pwd=Pass;',
        'DB_CONN_ANOTHER': 'Driver={...};Server=...;Database=Another;...'
    })
    def test_load_settings_from_env(self):
        print("\n--- Testing Settings Loading ---")
        
        # Initialize settings (will load from mocked env)
        settings = Settings()
        
        self.assertEqual(settings.PANGO_ENV, 'Test')
        self.assertEqual(settings.DB_NAME, 'LocalDB')
        self.assertEqual(settings.DB_CONNECTION_STRING.get_secret_value(), 'Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=master;Uid=sa;Pwd=Pass;')
        
        # Check connection strings aggregation
        conns = settings.connection_strings
        self.assertIn('localdb', conns)
        self.assertIn('another', conns)
        
        print("✅ Settings Loading Test Passed")

    @patch.dict(os.environ, {'AWS_REGION': 'us-west-2'})
    def test_default_values(self):
        print("\n--- Testing Default Values ---")
        settings = Settings()
        self.assertEqual(settings.AWS_REGION, 'us-west-2')
        self.assertEqual(settings.MAX_QUERY_LENGTH, 10000)
        print("✅ Default Values Test Passed")

if __name__ == '__main__':
    unittest.main()
