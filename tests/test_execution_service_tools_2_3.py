"""
Test Suite for ExecutionService - Tools 2-3

Tests transactional script execution and syntax parsing.
"""
import os
import pytest
from dotenv import load_dotenv
from services.sql_review.execution_service import ExecutionService

# Load environment variables
load_dotenv()
load_dotenv('.env.local')


class TestExecuteSqlScriptTransactional:
    """Test cases for execute_sql_script_transactional tool."""
    
    @classmethod
    def setup_class(cls):
        """Setup test environment."""
        cls.service = ExecutionService()
        cls.database = os.getenv('DB_NAME', 'LocalDB')
    
    def test_simple_create_table_rollback(self):
        """Test that CREATE TABLE is executed and rolled back."""
        script = """
        CREATE TABLE test_table_12345 (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )
        """
        
        result = self.service.execute_sql_script_transactional(
            database=self.database,
            script=script,
            timeout_seconds=60
        )
        
        assert result["success"] is True
        assert result["error"] is None
        assert len(result["statement_results"]) > 0
        
        # Verify table was NOT created (rolled back)
        verify_result = self.service.execute_sql_query(
            database=self.database,
            query="SELECT * FROM sys.tables WHERE name = 'test_table_12345'",
            max_rows=10,
            timeout_seconds=30
        )
        assert verify_result["row_count"] == 0  # Table should not exist
    
    def test_insert_statements_rollback(self):
        """Test that INSERT statements are rolled back."""
        # First create a temp table, then insert
        script = """
        CREATE TABLE #temp_test (id INT, value VARCHAR(50));
        INSERT INTO #temp_test VALUES (1, 'test1');
        INSERT INTO #temp_test VALUES (2, 'test2');
        """
        
        result = self.service.execute_sql_script_transactional(
            database=self.database,
            script=script,
            timeout_seconds=60
        )
        
        assert result["success"] is True
        assert len(result["statement_results"]) >= 2  # CREATE + INSERTs
    
    def test_syntax_error_in_script(self):
        """Test handling of syntax errors in script."""
        script = "CREATE TABEL bad_syntax (id INT)"  # Typo: TABEL
        
        result = self.service.execute_sql_script_transactional(
            database=self.database,
            script=script,
            timeout_seconds=60
        )
        
        assert result["success"] is False
        assert result["error"] is not None
        assert "failed" in result["error"].lower() or "unknown" in result["error"].lower()
    
    def test_multiple_statements(self):
        """Test script with multiple statements."""
        script = """
        SELECT 1 AS test;
        SELECT 2 AS test;
        SELECT 3 AS test;
        """
        
        result = self.service.execute_sql_script_transactional(
            database=self.database,
            script=script,
            timeout_seconds=60
        )
        
        assert result["success"] is True
        assert len(result["statement_results"]) >= 3
    
    def test_go_separator(self):
        """Test script with GO separators."""
        script = """
        SELECT 1 AS test
        GO
        SELECT 2 AS test
        GO
        """
        
        result = self.service.execute_sql_script_transactional(
            database=self.database,
            script=script,
            timeout_seconds=60
        )
        
        assert result["success"] is True
        assert len(result["statement_results"]) >= 2


class TestParseSqlScript:
    """Test cases for parse_sql_script tool."""
    
    @classmethod
    def setup_class(cls):
        """Setup test environment."""
        cls.service = ExecutionService()
        cls.database = os.getenv('DB_NAME', 'LocalDB')
    
    def test_valid_select_syntax(self):
        """Test parsing valid SELECT statement."""
        script = "SELECT * FROM sys.objects WHERE type = 'U'"
        
        result = self.service.parse_sql_script(
            database=self.database,
            script=script
        )
        
        assert result["valid"] is True
        assert len(result["errors"]) == 0
    
    def test_valid_create_table_syntax(self):
        """Test parsing valid CREATE TABLE statement."""
        script = """
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            created_date DATETIME DEFAULT GETDATE()
        )
        """
        
        result = self.service.parse_sql_script(
            database=self.database,
            script=script
        )
        
        assert result["valid"] is True
        assert len(result["errors"]) == 0
    
    def test_invalid_syntax(self):
        """Test parsing invalid syntax."""
        script = "SELECT * FORM sys.objects"  # Typo: FORM
        
        result = self.service.parse_sql_script(
            database=self.database,
            script=script
        )
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
    
    def test_missing_table_name(self):
        """Test parsing with missing table name."""
        script = "SELECT * FROM"
        
        result = self.service.parse_sql_script(
            database=self.database,
            script=script
        )
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
    
    def test_complex_valid_script(self):
        """Test parsing complex but valid script."""
        script = """
        CREATE PROCEDURE test_proc
        AS
        BEGIN
            SELECT * FROM sys.objects;
            UPDATE sys.objects SET name = name WHERE 1=0;
        END
        """
        
        result = self.service.parse_sql_script(
            database=self.database,
            script=script
        )
        
        assert result["valid"] is True
        assert len(result["errors"]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
