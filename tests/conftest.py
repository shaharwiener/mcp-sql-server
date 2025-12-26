"""
Pytest configuration for unit tests.
"""
import pytest
import sys
import os

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock pyodbc globally before any application imports
from unittest.mock import MagicMock
sys.modules["pyodbc"] = MagicMock()

@pytest.fixture
def mock_config():
    """Mock configuration for tests."""
    from unittest.mock import MagicMock
    config = MagicMock()
    config.best_practices.enforce_no_select_star = True
    config.safety.risk_weights.cross_join = 35
    config.safety.risk_weights.ddl_statement = 90
    config.safety.risk_weights.write_operation = 100
    config.safety.risk_weights.no_where_clause = 50
    config.safety.risk_weights.dynamic_sql = 85
    # Add other needed config values
    return config
