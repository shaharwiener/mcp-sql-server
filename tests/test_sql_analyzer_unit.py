"""
Unit tests for SqlAnalyzer.
Tests orchestration, risk scoring, and model population.
"""
import pytest
from unittest.mock import Mock, patch
from services.analysis.sql_analyzer import SqlAnalyzer
from services.analysis.models import ReviewResult

class TestSqlAnalyzerUnit:
    @pytest.fixture
    def mock_bp_engine(self):
        engine = Mock()
        engine.check_rules.return_value = []
        return engine

    @pytest.fixture
    def analyzer(self, mock_bp_engine, mock_config):
        with patch('services.analysis.sql_analyzer.BestPracticesEngine', return_value=mock_bp_engine), \
             patch('services.analysis.sql_analyzer.get_config', return_value=mock_config):
            analyzer = SqlAnalyzer()
            analyzer.bp_engine = mock_bp_engine # Ensure instance is replaced
            return analyzer

    def test_analyze_clean_query(self, analyzer):
        """Test analysis of a clean query."""
        sql = "SELECT id FROM dbo.Users WHERE id = 1"
        result = analyzer.analyze(sql)
        
        assert result.summary.status == "APPROVED"
        assert result.summary.risk_score == 0
        assert len(result.issues) == 0

    def test_analyze_syntax_error(self, analyzer):
        """Test handling of syntax errors."""
        sql = "SELECT * FORM Users"
        result = analyzer.analyze(sql)
        
        assert result.summary.status == "REJECTED"
        assert result.summary.risk_score == 100
        assert len(result.issues) == 1
        assert result.issues[0].code == "SYN001"

    def test_analyze_write_operation(self, analyzer):
        """Test scoring of write operations."""
        sql = "UPDATE dbo.Users SET status = 'active'"
        result = analyzer.analyze(sql)
        
        # Write op adds 100 to risk (capped at 100)
        assert result.summary.risk_score == 100
        assert result.safety_checks.has_write_ops is True
        assert result.issues[0].code == "SEC001"

    def test_analyze_bp_violations_aggregation(self, analyzer):
        """Test that BP violations are correctly aggregated."""
        # Mock BP engine to return specific violations
        analyzer.bp_engine.check_rules.return_value = [
            "BP001: Avoid SELECT *",
            "BP002: Missing schema prefix"
        ]
        
        sql = "SELECT * FROM Users"
        result = analyzer.analyze(sql)
        
        # Risk score: 5 per BP violation = 10 total
        assert result.summary.risk_score == 10
        assert len(result.issues) == 2
        
        # Check explicit codes are extracted
        codes = [i.code for i in result.issues]
        assert "BP001" in codes
        assert "BP002" in codes

    def test_risk_score_status_thresholds(self, analyzer):
        """Test status determination based on risk score."""
        # Mock BP engine to return many violations to boost score
        analyzer.bp_engine.check_rules.return_value = [f"BP0{i}: Reason" for i in range(10)]
        
        # Score approx 50 -> WARNING (>= 30, < 80)
        sql = "SELECT * FROM Users"
        result = analyzer.analyze(sql)
        
        assert result.summary.status == "WARNING"
        assert 30 <= result.summary.risk_score < 80

    def test_referenced_tables(self, analyzer):
        """Test table extraction."""
        sql = "SELECT * FROM dbo.Users JOIN dbo.Orders ON u.id = o.id"
        result = analyzer.analyze(sql)
        
        tables = result.schema_context.valid_objects
        assert "dbo.Users" in tables
        assert "dbo.Orders" in tables

    def test_detect_ddl_statement(self, analyzer):
        """Test detection of DDL statements."""
        sql = "CREATE TABLE NewTable (id int)"
        result = analyzer.analyze(sql)
        
        assert result.summary.status == "REJECTED"
        assert result.safety_checks.has_ddl is True
        # Check codes
        codes = [i.code for i in result.issues]
        assert "SEC003" in codes

    def test_detect_dynamic_sql_command(self, analyzer):
        """Test detection of dynamic SQL."""
        sql = "EXEC('SELECT 1')"
        result = analyzer.analyze(sql)
        
        # Risk score > 0 (weight) + Dynamic SQL
        assert "SEC004" in [i.code for i in result.issues]

    def test_detect_cross_join(self, analyzer):
        """Test detection of Cross Join."""
        sql = "SELECT * FROM A CROSS JOIN B"
        result = analyzer.analyze(sql)
        
        assert "PERF001" in [i.code for i in result.issues]

    def test_validate_readonly_empty(self, analyzer):
        """Test validate_readonly with empty query."""
        valid, msg = analyzer.validate_readonly("")
        assert valid is False
        assert "Empty" in msg

        valid, msg = analyzer.validate_readonly("-- Just a comment")
        assert valid is False
        assert "Empty" in msg

    def test_validate_readonly_multistatement(self, analyzer):
        """Test validate_readonly with batch."""
        valid, msg = analyzer.validate_readonly("SELECT 1; SELECT 2")
        assert valid is False
        assert "Multi-statement" in msg

    def test_validate_readonly_non_select(self, analyzer):
        """Test validate_readonly with UPDATE."""
        valid, msg = analyzer.validate_readonly("UPDATE T SET C=1")
        assert valid is False
        assert "Only SELECT" in msg

    def test_validate_readonly_select_into(self, analyzer):
        """Test validate_readonly with SELECT INTO."""
        valid, msg = analyzer.validate_readonly("SELECT * INTO NewT FROM OldT")
        assert valid is False
        assert "SELECT INTO" in msg

    def test_validate_readonly_parse_error(self, analyzer):
        """Test validate_readonly with syntax error."""
        valid, msg = analyzer.validate_readonly("SELECT * FORM T") # Syntax
        assert valid is False
        assert "Parsing error" in msg

    def test_analyze_no_where_delete(self, analyzer):
        """Test DELETE without WHERE."""
        sql = "DELETE FROM Users" # No WHERE
        result = analyzer.analyze(sql)
        assert "SEC002" in [i.code for i in result.issues]

    def test_validate_readonly_exception(self, analyzer):
        """Test general exception in validate_readonly."""
        with patch('services.analysis.sql_analyzer.sqlglot.parse', side_effect=Exception("Major Fail")):
            valid, msg = analyzer.validate_readonly("SELECT 1")
            assert valid is False
            assert "Major Fail" in msg

    def test_validate_readonly_success(self, analyzer):
        """Test successful validation of readonly query."""
        valid, msg = analyzer.validate_readonly("SELECT * FROM dbo.Users")
        assert valid is True
        assert msg == ""
