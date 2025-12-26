"""
Unit tests for BestPracticesEngine.
Tests individual rules against SQL snippets.
"""
import pytest
import sqlglot
from services.analysis.best_practices import BestPracticesEngine

class TestBestPracticesUnit:
    @pytest.fixture
    def engine(self, mock_config):
        """Create engine with mock config."""
        # Patch get_config used inside the class
        from unittest.mock import patch
        with patch('services.analysis.best_practices.get_config', return_value=mock_config):
            return BestPracticesEngine()

    def _parse(self, sql):
        return sqlglot.parse(sql, read="tsql")[0]

    def test_select_star_detection(self, engine):
        """Test BP001: SELECT *"""
        sql = "SELECT * FROM Users"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP001" in v for v in violations)
        
        sql_clean = "SELECT id, name FROM Users"
        violations = engine.check_rules(self._parse(sql_clean))
        assert not any("BP001" in v for v in violations)

    def test_schema_prefix_detection(self, engine):
        """Test BP002: Schema prefix"""
        sql = "SELECT * FROM Users"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP002" in v for v in violations)
        
        sql_clean = "SELECT * FROM dbo.Users"
        # Note: SELECT * (BP001) will still trigger, but BP002 should not
        violations = engine.check_rules(self._parse(sql_clean))
        assert not any("BP002" in v for v in violations)

    def test_cross_join_detection(self, engine):
        """Test BP003: Cross Join"""
        sql = "SELECT * FROM dbo.Users, dbo.Orders"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP003" in v for v in violations)
        
        sql_explicit = "SELECT * FROM dbo.Users CROSS JOIN dbo.Orders"
        violations = engine.check_rules(self._parse(sql_explicit))
        assert any("BP003" in v for v in violations)

    def test_functions_in_where(self, engine):
        """Test BP004: Function in WHERE"""
        sql = "SELECT * FROM dbo.Users WHERE YEAR(created_date) = 2024"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP004" in v for v in violations)

    def test_or_in_where(self, engine):
        """Test BP005: OR in WHERE"""
        sql = "SELECT * FROM dbo.Users WHERE status = 'active' OR status = 'pending'"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP005" in v for v in violations)

    def test_dynamic_sql(self, engine):
        """Test BP018: Dynamic SQL"""
        sql = "EXEC('SELECT * FROM Users')"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP018" in v for v in violations)

    def test_distinct_usage(self, engine):
        """Test BP006: DISTINCT."""
        violations = engine.check_rules(self._parse("SELECT DISTINCT col FROM tbl"))
        assert any("BP006" in v for v in violations)

    def test_in_subquery(self, engine):
        """Test BP007: IN with Subquery."""
        violations = engine.check_rules(self._parse("SELECT * FROM tbl WHERE col IN (SELECT c FROM t2)"))
        assert any("BP007" in v for v in violations)

    def test_cursor_usage(self, engine):
        """Test BP008: Cursors."""
        violations = engine.check_rules(self._parse("DECLARE CURSOR foo"))
        assert any("BP008" in v for v in violations)

    def test_scalar_function_in_select(self, engine):
        """Test BP009: Scalar function in SELECT."""
        violations = engine.check_rules(self._parse("SELECT dbo.myfunc(col) FROM tbl"))
        assert any("BP009" in v for v in violations)

    def test_scalar_function_unqualified(self, engine):
        """Test BP009: Unqualified scalar function."""
        # Unqualified function might be parsed as Func or Anonymous depending on dialect/registry
        # We want to hit 'isinstance(func, exp.Func)' branch
        # GETDATE() is often parsed as Func
        violations = engine.check_rules(self._parse("SELECT GETDATE()"))
        # Wait, GETDATE might be 0 args.
        # Try custom UDF "myfunc(x)"
        # Note: If sqlglot doesn't know it, it might be Anonymous?
        # Let's try to ensure it hits line 136.
        # If I mock the expression node manually I can guarantee type.
        # But parsing is better integration.
        # "SELECT FORMAT(d, 'D')": FORMAT is standard scalar func.
        violations = engine.check_rules(self._parse("SELECT FORMAT(d, 'D')"))
        assert any("BP009" in v for v in violations)

    def test_large_in_list(self, engine):
        """Test BP010: Large IN list (>100 items)."""
        items = ",".join([str(i) for i in range(105)])
        sql = f"SELECT * FROM tbl WHERE col IN ({items})"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP010" in v for v in violations)

    def test_union_vs_union_all(self, engine):
        """Test BP011: UNION vs UNION ALL."""
        violations = engine.check_rules(self._parse("SELECT * FROM t1 UNION SELECT * FROM t2"))
        assert any("BP011" in v for v in violations)
        
        safe = engine.check_rules(self._parse("SELECT * FROM t1 UNION ALL SELECT * FROM t2"))
        assert not any("BP011" in v for v in safe)

    def test_implicit_conversion(self, engine):
        """Test BP012: Implicit conversion."""
        violations = engine.check_rules(self._parse("SELECT * FROM t WHERE col = '123'"))
        assert any("BP012" in v for v in violations)

    def test_set_nocount_missing(self, engine):
        """Test BP013: Missing SET NOCOUNT ON."""
        violations = engine.check_rules(self._parse("CREATE PROCEDURE test AS SELECT 1"))
        assert any("BP013" in v for v in violations)

    def test_xact_abort_missing(self, engine):
        """Test BP014: Missing XACT_ABORT."""
        # Wrap in BEGIN/END block to ensure parsed as one command/block containing full text
        violations = engine.check_rules(self._parse("BEGIN BEGIN TRAN; UPDATE T SET C=1; COMMIT END"))
        assert any("BP014" in v for v in violations)

    def test_try_catch_missing(self, engine):
        """Test BP015: Missing TRY/CATCH."""
        violations = engine.check_rules(self._parse("CREATE PROCEDURE p AS SELECT 1"))
        assert any("BP015" in v for v in violations)

    def test_outer_joins(self, engine):
        """Test BP016: Outer joins."""
        violations = engine.check_rules(self._parse("SELECT * FROM t1 LEFT JOIN t2 ON t1.id=t2.id"))
        assert any("BP016" in v for v in violations)

    def test_table_variables(self, engine):
        """Test BP017: Table variables."""
        violations = engine.check_rules(self._parse("DECLARE @T TABLE (id int)"))
        assert any("BP017" in v for v in violations)

    def test_unclosed_transaction(self, engine):
        """Test BP019: Unclosed transaction."""
        violations = engine.check_rules(self._parse("BEGIN BEGIN TRAN; SELECT 1 END"))
        assert any("BP019" in v for v in violations)

    def test_multiple_subqueries(self, engine):
        """Test BP020: Multiple subqueries."""
        sql = "SELECT * FROM t WHERE c IN (SELECT 1) AND d IN (SELECT 2) AND e IN (SELECT 3)"
        violations = engine.check_rules(self._parse(sql))
        assert any("BP020" in v for v in violations)

    def test_no_top_limit(self, engine):
        """Test BP021: Missing TOP/LIMIT."""
        violations = engine.check_rules(self._parse("SELECT col FROM tbl"))
        assert any("BP021" in v for v in violations)
        
        safe = engine.check_rules(self._parse("SELECT COUNT(*) FROM tbl"))
        assert not any("BP021" in v for v in safe)

    def test_sp_prefix(self, engine):
        """Test BP022: sp_ prefix."""
        violations = engine.check_rules(self._parse("CREATE PROCEDURE sp_bad AS SELECT 1"))
        assert any("BP022" in v for v in violations)

    def test_get_docs_json_load(self, engine):
        """Test documentation loading."""
        docs = engine.get_all_practices_documentation()
        assert isinstance(docs, dict)

    def test_select_count_star_ignore(self, engine):
        """Test that COUNT(*) is ignored by select star rule."""
        violations = engine.check_rules(self._parse("SELECT COUNT(*) FROM tbl"))
        assert not any("BP001" in v for v in violations)

    def test_get_docs_json_load_error(self, engine):
        """Test error handling in docs loading."""
        from unittest.mock import patch, mock_open
        with patch('builtins.open', side_effect=Exception("Read Error")):
             docs = engine.get_all_practices_documentation()
             docs = engine.get_all_practices_documentation()
             assert docs == {}

    def test_get_docs_missing_file(self, engine):
        """Test missing file handling."""
        from unittest.mock import patch
        # Patch Path.exists to False
        with patch('services.analysis.best_practices.Path.exists', return_value=False):
             docs = engine.get_all_practices_documentation()
             assert docs == {}
