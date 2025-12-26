"""
Unit tests for ExecutionService.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, ANY
from services.core.execution_service import ExecutionService
from services.common.exceptions import DatabaseError
from services.security.concurrency_throttler import TooManyConcurrentQueriesError
from services.security.nolock_injector import NolockInjectionError, NolockInjector

class TestExecutionServiceUnit:
    @pytest.fixture
    def mock_db_connection(self):
        # DbConnectionService instance mock
        mock_db = MagicMock()
        return mock_db

    @pytest.fixture
    def mock_analyzer(self):
        return MagicMock()

    @pytest.fixture
    def mock_config(self):
        config_mock = MagicMock()
        config_mock.environment = "Prd"
        config_mock.safety.max_rows = 100
        config_mock.safety.max_execution_time_seconds = 30
        config_mock.safety.enable_cost_check = False
        config_mock.safety.max_query_cost = 50.0
        config_mock.safety.max_concurrent_queries = 5
        config_mock.safety.max_concurrent_queries_per_user = 2
        config_mock.safety.max_payload_size_bytes = 10 * 1024 * 1024 # 10MB default
        
        # Setup get_env_setting behavior
        def get_env_setting(env, key, default):
            return default
        config_mock.safety.get_env_setting.side_effect = get_env_setting
        
        return config_mock

    @pytest.fixture
    def service(self, mock_db_connection, mock_analyzer, mock_config):
        with patch('services.core.execution_service.DbConnectionService', return_value=mock_db_connection), \
             patch('services.core.execution_service.SqlAnalyzer', return_value=mock_analyzer), \
             patch('services.core.execution_service.get_config', return_value=mock_config), \
             patch('services.core.execution_service.ConcurrencyThrottler'), \
             patch('services.core.execution_service.NolockInjector'):
             
            service = ExecutionService()
            # Restore mock config explicitly if patch didn't propagate to instance
            service.config = mock_config
            service.db = mock_db_connection
            service.analyzer = mock_analyzer
            
            # Setup successful acquiring for convenience
            service.concurrency_throttler.acquire.return_value.__enter__.return_value = None
            service.nolock_injector.should_inject.return_value = False
            
            return service

    def test_get_execution_plan_success(self, service):
        """Test successful execution plan retrieval."""
        service.db.execute_query.return_value = "<Plan/>"
        
        result = service.get_execution_plan("SELECT 1")
        
        assert result["success"] is True
        assert result["plan_xml"] == "<Plan/>"
        # Verify wrapper SQL
        call_args = service.db.execute_query.call_args
        assert "SET SHOWPLAN_XML ON" in call_args[0][0]

    def test_get_execution_plan_failure(self, service):
        """Test failure in execution plan retrieval."""
        service.db.execute_query.side_effect = Exception("DB Error")
        
        result = service.get_execution_plan("SELECT 1")
        
        assert result["success"] is False
        assert "element" in result or "error" in result # "error" field likely
        assert "Failed to get execution plan" in result["error"]

    def test_execute_readonly_security_check(self, service):
        """Test that validation failure blocks execution."""
        service.analyzer.validate_readonly.return_value = (False, "Unsafe SQL")
        
        result = service.execute_readonly("DROP TABLE Users")
        
        assert result["success"] is False
        assert "Security Violation" in result["error"]
        service.db.execute_query.assert_not_called()

    def test_execute_readonly_success(self, service):
        """Test successful execution flow."""
        service.analyzer.validate_readonly.return_value = (True, None)
        service.db.execute_query.return_value = [{"col": "val"}]
        
        result = service.execute_readonly("SELECT 1")
        
        assert result["success"] is True
        assert result["row_count"] == 1
        service.concurrency_throttler.acquire.assert_called()

    def test_execute_readonly_large_payload_truncation(self, service, mock_db_connection, mock_config):
        """Test that large strings are truncated."""
        service.analyzer.validate_readonly.return_value = (True, None)
        
        # Setup large rows for inner fetch method logic.
        # execute_query in service takes a 'fetch_method' callback.
        # We need to simulate execute_query calling this callback.
        
        long_string = "a" * 2000
        mock_cursor = MagicMock()
        mock_cursor.description = [("col_long",)]
        # fetchmany returns batch then empty
        mock_cursor.fetchmany.side_effect = [[(long_string,)], []] 
        
        # Side effect to invoke the callback
        def side_effect_execute(query, env=None, db=None, fetch_method=None, command_timeout=None):
            return fetch_method(mock_cursor, None)
            
        mock_db_connection.execute_query.side_effect = side_effect_execute
        
        result = service.execute_readonly("SELECT 1")
        
        assert result["success"] is True
        row = result["data"][0]
        assert len(row["col_long"]) < 2000
        assert "...(truncated)" in row["col_long"]

    def test_execute_readonly_payload_limit_exceeded(self, service, mock_db_connection, mock_config):
        """Test that exception is raised when payload limit exceeded."""
        service.analyzer.validate_readonly.return_value = (True, None)
        
        # 11 MB string
        huge_string = "a" * 1024 * 1024 * 11 
        mock_cursor = MagicMock()
        mock_cursor.description = [("col_huge",)]
        mock_cursor.fetchmany.side_effect = [[(huge_string,)], []]
        
        def side_effect_execute(query, env=None, db=None, fetch_method=None, command_timeout=None):
            fetch_method(mock_cursor, None)
            
        mock_db_connection.execute_query.side_effect = side_effect_execute
        
        result = service.execute_readonly("SELECT HUGE")
        
        assert result["success"] is False
        assert "Execution error" in result["error"] 
        assert "Query result too large" in result["error"]

    def test_cost_check_failure(self, service, mock_config):
        """Test failure when query cost is high."""
        service.analyzer.validate_readonly.return_value = (True, None)
        mock_config.safety.enable_cost_check = True
        
        # Mock get_execution_plan
        with patch.object(service, 'get_execution_plan', return_value={"success": True, "plan_xml": "<Plan/>"}):
            with patch('services.core.execution_service.QueryCostChecker') as MockChecker:
                instance = MockChecker.return_value
                instance.check_query_cost.return_value = (False, 100.0)
                
                result = service.execute_readonly("SELECT * FROM Expensive")
                
                assert result["success"] is False
                assert "Query cost (100.00) exceeds threshold" in result["error"]

    def test_nolock_injection_success(self, service):
        """Test NOLOCK injection when enabled."""
        service.analyzer.validate_readonly.return_value = (True, None)
        service.nolock_injector.should_inject.return_value = True
        service.nolock_injector.inject_nolock_hints.return_value = "SELECT * FROM T WITH (NOLOCK)"
        
        service.execute_readonly("SELECT * FROM T")
        
        # Verify injection called
        service.nolock_injector.inject_nolock_hints.assert_called_with("SELECT * FROM T")
        # Verify execute called with modified query
        call_args = service.db.execute_query.call_args
        assert call_args[0][0] == "SELECT * FROM T WITH (NOLOCK)"

    def test_nolock_injection_error(self, service):
        """Test error handling during NOLOCK injection."""
        service.analyzer.validate_readonly.return_value = (True, None)
        service.nolock_injector.should_inject.return_value = True
        service.nolock_injector.inject_nolock_hints.side_effect = NolockInjectionError("Mock Injection Fail")
        
        result = service.execute_readonly("SELECT * FROM T")
        
        assert result["success"] is False
        assert "Security enforcement failed" in result["error"]
        assert "Mock Injection Fail" in result["error"]

    def test_row_limit_enforcement(self, service, mock_db_connection, mock_config):
        """Test that row limit stops fetching."""
        service.analyzer.validate_readonly.return_value = (True, None)
        mock_config.safety.max_rows = 2
        
        # Return 3 rows
        mock_cursor = MagicMock()
        mock_cursor.description = [("col",)]
        # fetchmany called repeatedly: 2 rows (batch), then 1 row, then empty
        # Wait, implementation uses batch_size=100.
        # But we want to simulate getting more than max_rows.
        # Logic: 
        # while True:
        #   results = fetchmany(100)
        #   loops...
        #   if len(rows) >= max_rows: break
        
        # So we return 1 batch of 3 rows (exceeds 2)
        mock_cursor.fetchmany.side_effect = [[(1,), (2,), (3,)], []]
        
        def side_effect_execute(query, env=None, db=None, fetch_method=None, command_timeout=None):
            return fetch_method(mock_cursor, None)
            
        mock_db_connection.execute_query.side_effect = side_effect_execute
        
        result = service.execute_readonly("SELECT * FROM T")
        
        assert result["success"] is True
        
        assert len(result["data"]) == 2

    def test_concurrency_error_handling(self, service):
        """Test handling of concurrency limits."""
        # Mock acquire to raise error
        service.concurrency_throttler.acquire.side_effect = TooManyConcurrentQueriesError("Busy")
        
        result = service.execute_readonly("SELECT 1")
        
        assert result["success"] is False
        assert "Busy" in result["error"]
        assert result["retry_after_seconds"] == 5

    def test_non_string_payload_size(self, service, mock_db_connection, mock_config):
        """Test payload size estimation for non-strings."""
        service.analyzer.validate_readonly.return_value = (True, None)
        mock_config.safety.max_rows = 10
        
        mock_cursor = MagicMock()
        mock_cursor.description = [("num",)]
        mock_cursor.fetchmany.side_effect = [[(123,), (456,)], []]
        
        def side_effect_execute(query, env=None, db=None, fetch_method=None, command_timeout=None):
            return fetch_method(mock_cursor, None)
            
        mock_db_connection.execute_query.side_effect = side_effect_execute
        
        result = service.execute_readonly("SELECT 1")
        assert result["success"] is True
        assert result["data"] == [{"num": 123}, {"num": 456}]

    def test_execution_plan_fetching_logic(self, service):
        """Test the fetch_plan helper function details."""
        # We need to extract the inner function `fetch_plan` or verify logic via side effect
        # Logic: Concatenates row[0] if present
        
        # Mock execute_query to run the callback with specific data
        def side_effect_execute(query, env=None, db=None, fetch_method=None, command_timeout=None):
            mock_cursor = MagicMock()
            # Rows: [("Part1",), ("Part2",), (None,)]
            mock_cursor.fetchall.return_value = [("Part1",), ("Part2",), (None,)]
            return fetch_method(mock_cursor, None)
            
        service.db.execute_query.side_effect = side_effect_execute
        
        result = service.get_execution_plan("SELECT 1")
        
        assert result["success"] is True
        assert result["plan_xml"] == "Part1Part2"

