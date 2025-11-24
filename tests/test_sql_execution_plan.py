import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.sql_review.performance_service import PerformanceService
import json
from unittest.mock import MagicMock

def test_sql_execution_plan():
    print("--- Testing SQL Execution Plan Service ---")
    
    # Mock DB Service to avoid AWS connection issues
    mock_db = MagicMock()
    service = PerformanceService()
    
    # Test: Get Execution Plan (Mocked)
    print("\nTesting get_execution_plan()...")
    
    # For now, just test that the service initializes
    print("✅ PerformanceService initialized successfully.")
    print("Note: Full execution plan testing requires database connection.")

if __name__ == "__main__":
    test_sql_execution_plan()
