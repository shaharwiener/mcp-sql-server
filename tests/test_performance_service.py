# tests/test_performance_service.py
"""Unit tests for the PerformanceService (Tools 4-8).
These tests verify that the service returns the expected JSON structure,
handles validation errors, and works with a simple SELECT query against the
LocalDB validation database.
"""

import os
from dotenv import load_dotenv

from services.sql_review.performance_service import PerformanceService

load_dotenv('.env.local')

class TestPerformanceService:
    @classmethod
    def setup_class(cls):
        cls.service = PerformanceService()
        # Use the same validation DB as other tests
        cls.database = os.getenv('LOCAL_DB_NAME', 'LocalDB')

    def test_get_execution_plan_success(self):
        result = self.service.get_execution_plan(
            database=self.database,
            query="SELECT 1 AS test",
            timeout_seconds=30,
        )
        assert result["success"] is True
        # Plan may be empty string if SHOWPLAN not supported; ensure key exists
        assert "plan" in result
        assert result["error"] is None

    def test_get_query_cost_estimate_success(self):
        result = self.service.get_query_cost_estimate(
            database=self.database,
            query="SELECT 1",
            timeout_seconds=30,
        )
        assert result["success"] is True
        assert isinstance(result["estimated_cost"], float)
        assert result["error"] is None

    def test_get_index_usage_statistics_success(self):
        # Test without filters (top 100)
        result = self.service.get_index_usage_statistics(
            database=self.database,
            timeout_seconds=30,
        )
        assert result["success"] is True
        assert isinstance(result["indexes"], list)
        # We might not have usage stats on a fresh DB, but structure should be correct
        if result["indexes"]:
            idx = result["indexes"][0]
            assert "index_name" in idx
            assert "user_seeks" in idx

    def test_get_index_usage_statistics_with_table(self):
        # Test with table filter
        # Use a system table that exists, e.g. sys.objects, but sys tables are excluded in query (object_id > 100)
        # So we might get empty list if no user tables.
        # But we can check success.
        result = self.service.get_index_usage_statistics(
            database=self.database,
            schema_name="sys",
            table_name="objects",
            timeout_seconds=30,
        )
        assert result["success"] is True
        assert isinstance(result["indexes"], list)

    def test_get_index_usage_statistics_invalid_schema(self):
        result = self.service.get_index_usage_statistics(
            database=self.database,
            schema_name="Invalid;Schema",
            timeout_seconds=30,
        )
        assert result["success"] is False
        assert "Invalid schema name" in result["error"]

    def test_get_missing_index_suggestions_success(self):
        # Test basic retrieval
        result = self.service.get_missing_index_suggestions(
            database=self.database,
            timeout_seconds=30,
        )
        assert result["success"] is True
        assert isinstance(result["suggestions"], list)
        # Even if empty, structure is verified. 
        # If we had suggestions, we'd check keys:
        if result["suggestions"]:
            sug = result["suggestions"][0]
            assert "schema_name" in sug
            assert "table_name" in sug
            assert "equality_columns" in sug
            assert isinstance(sug["equality_columns"], list)

    def test_get_missing_index_suggestions_with_filter(self):
        # Test with table filter
        result = self.service.get_missing_index_suggestions(
            database=self.database,
            schema_name="dbo",
            table_name="NonExistentTable", # Should return empty list, no error
            timeout_seconds=30,
        )
        assert result["success"] is True
        assert result["suggestions"] == []

    def test_get_missing_index_suggestions_invalid_db(self):
        result = self.service.get_missing_index_suggestions(
            database="InvalidDB",
            timeout_seconds=30,
        )
        assert result["success"] is False
        assert "not in allowed list" in result["error"] or "Unsupported database" in result["error"]

    def test_get_query_statistics_success(self):
        result = self.service.get_query_statistics(
            database=self.database,
            query="SELECT 1 AS a, 2 AS b",
            timeout_seconds=30,
        )
        assert result["success"] is True
        assert result["row_count"] == 1
        assert isinstance(result["execution_ms"], float)
        assert result["error"] is None
