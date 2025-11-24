# tests/test_utility_service.py
"""Unit tests for UtilityService (Tool 26)."""

import os
from dotenv import load_dotenv
from services.sql_review.utility_service import UtilityService

load_dotenv('.env.local')

class TestUtilityService:
    @classmethod
    def setup_class(cls):
        cls.service = UtilityService()
        cls.database = os.getenv('LOCAL_DB_NAME', 'LocalDB')

    def test_sample_query_results_success(self):
        # Simple SELECT query
        query = "SELECT 1 AS test_col, 'test' AS text_col"
        result = self.service.sample_query_results(database=self.database, query=query, timeout_seconds=30)
        
        assert result["success"] is True
        assert isinstance(result["rows"], list)
        assert isinstance(result["columns"], list)
        assert len(result["rows"]) > 0
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "test_col"
        assert result["rows"][0]["test_col"] == 1

    def test_sample_query_results_with_top(self):
        # Query already has TOP
        query = "SELECT TOP 5 name FROM sys.tables"
        result = self.service.sample_query_results(database=self.database, query=query, max_rows=10, timeout_seconds=30)
        
        if result["success"]:
            assert isinstance(result["rows"], list)
            assert len(result["rows"]) <= 5

    def test_sample_query_results_invalid_db(self):
        query = "SELECT 1"
        result = self.service.sample_query_results(database="nonexistent_db", query=query, timeout_seconds=30)
        assert result["success"] is False
        assert isinstance(result["error"], str)

    def test_sample_query_results_invalid_query(self):
        # Query too long
        query = "SELECT " + "a" * 10000
        result = self.service.sample_query_results(database=self.database, query=query, timeout_seconds=30)
        assert result["success"] is False
        assert "Query too long" in result["error"] or "exceeds maximum" in result["error"]

    def test_sample_query_results_syntax_error(self):
        query = "SELECT * FROM nonexistent_table_xyz"
        result = self.service.sample_query_results(database=self.database, query=query, timeout_seconds=30)
        assert result["success"] is False
        assert isinstance(result["error"], str)

    def test_search_objects_success(self):
        # Search for system tables (should find some)
        result = self.service.search_objects(database=self.database, pattern="%", timeout_seconds=30)
        
        assert result["success"] is True
        assert isinstance(result["objects"], list)
        # Should find at least some objects
        if result["objects"]:
            obj = result["objects"][0]
            assert "schema_name" in obj
            assert "object_name" in obj
            assert "object_type" in obj

    def test_search_objects_with_type_filter(self):
        # Search for tables only
        result = self.service.search_objects(
            database=self.database, 
            pattern="%", 
            object_type="table",
            timeout_seconds=30
        )
        
        assert result["success"] is True
        if result["objects"]:
            # All results should be tables
            for obj in result["objects"]:
                assert obj["object_type"] == "table"

    def test_search_objects_specific_pattern(self):
        # Search for specific pattern
        result = self.service.search_objects(database=self.database, pattern="sys%", timeout_seconds=30)
        assert result["success"] is True

    def test_search_objects_invalid_db(self):
        result = self.service.search_objects(database="nonexistent_db", pattern="%", timeout_seconds=30)
        assert result["success"] is False
        assert isinstance(result["error"], str)

    def test_search_objects_invalid_pattern(self):
        # Pattern with invalid characters
        result = self.service.search_objects(database=self.database, pattern="test';DROP TABLE--", timeout_seconds=30)
        assert result["success"] is False
        assert "Invalid pattern" in result["error"]
