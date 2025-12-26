"""
Unit tests for Security modules: ConcurrencyThrottler, NolockInjector, QueryCostChecker.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from services.security.concurrency_throttler import ConcurrencyThrottler, TooManyConcurrentQueriesError
from services.security.nolock_injector import NolockInjector
from services.security.query_cost_checker import QueryCostChecker

class TestConcurrencyThrottler:
    def test_acquire_release_success(self):
        """Test successful acquire and release."""
        throttler = ConcurrencyThrottler(max_concurrent_queries=2, max_concurrent_queries_per_user=1)
        
        with throttler.acquire("Prd", "user1"):
            assert throttler.get_active_count("Prd") == 1
            assert throttler.get_user_active_count("Prd", "user1") == 1
        
        assert throttler.get_active_count("Prd") == 0

    def test_max_concurrent_queries_limit(self):
        """Test blocking on global limit."""
        throttler = ConcurrencyThrottler(max_concurrent_queries=1, max_concurrent_queries_per_user=1)
        
        # Manually fill the slot
        throttler.active_queries = {"Prd": {"other_user": 1}}
        
        with pytest.raises(TooManyConcurrentQueriesError) as exc:
            with throttler.acquire("Prd", "user1"):
                pass
        
        assert "Too many concurrent queries on Prd" in str(exc.value)

    def test_max_concurrent_user_limit(self):
        """Test blocking on user limit."""
        throttler = ConcurrencyThrottler(max_concurrent_queries=10, max_concurrent_queries_per_user=1)
        
        # Manually fill slot for user
        throttler.active_queries = {"Prd": {"user1": 1}}
        
        with pytest.raises(TooManyConcurrentQueriesError) as exc:
            with throttler.acquire("Prd", "user1"):
                pass
        
        assert "Too many concurrent queries for user 'user1'" in str(exc.value)

    def test_throttler_empty_env(self):
        """Test getters for unused environment."""
        throttler = ConcurrencyThrottler()
        assert throttler.get_active_count("Unknown") == 0
        assert throttler.get_user_active_count("Unknown", "u") == 0

class TestNolockInjector:
    def test_inject_nolock_hints(self):
        """Test injecting WITH (NOLOCK) hints."""
        injector = NolockInjector()
        sql = "SELECT * FROM Users"
        
        # Need to mock sqlglot if we want to isolate, but it's a library, so integration testing it is fine for unit level of our code
        # However, for pure unit test speed/reliability, we rely on sqlglot behaving correctly.
        # Given the implementation uses sqlglot directly, we test the outcome.
        
        result = injector.inject_nolock_hints(sql)
        assert "WITH (NOLOCK)" in result
        assert "Users" in result

    def test_inject_nolock_existing(self):
        """Test it doesn't duplicate hints."""
        injector = NolockInjector()
        sql = "SELECT * FROM Users WITH (NOLOCK)"
        result = injector.inject_nolock_hints(sql)
        # Should not add another NOLOCK
        # Counting occurrences might be tricky with formatting changes, but let's check basic structure
        # sqlglot might reformat case, so normalize
        # sqlglot might reformat case, so normalize
        assert result.upper().count("NOLOCK") == 1

    def test_inject_nolock_append_to_existing(self):
        """Test appending NOLOCK to existing hints."""
        injector = NolockInjector()
        # "WITH (INDEX(1))" but no NOLOCK
        sql = "SELECT * FROM Users WITH (INDEX(1))"
        result = injector.inject_nolock_hints(sql)
        assert "NOLOCK" in result
        assert "INDEX(1)" in result

class TestQueryCostChecker:
    def test_check_query_cost_safe(self):
        """Test query within cost limits."""
        checker = QueryCostChecker(threshold=50.0)
        
        # Mock XML plan
        plan_xml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
            <BatchSequence>
                <Batch>
                    <Statements>
                        <StmtSimple StatementSubTreeCost="10.5" />
                    </Statements>
                </Batch>
            </BatchSequence>
        </ShowPlanXML>
        """
        
        allowed, cost = checker.check_query_cost(plan_xml, "SELECT 1")
        assert allowed is True
        assert cost == 10.5

    def test_check_query_cost_exceeded(self):
        """Test query exceeding cost limits."""
        checker = QueryCostChecker(threshold=5.0)
        
        plan_xml = """<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
        <StmtSimple StatementSubTreeCost="10.5" /></ShowPlanXML>"""
        # Note: Previous XML was minimal, let's match the logic in _extract_cost_from_plan
        # It looks for .//p:StmtSimple
        
        allowed, cost = checker.check_query_cost(plan_xml, "SELECT 1")
        assert allowed is False
        assert cost == 10.5

    def test_extract_cost_fallback(self):
        """Test fallback to RelOp max cost if StmtSimple missing."""
        checker = QueryCostChecker()
        
        plan_xml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
            <RelOp EstimatedTotalSubtreeCost="25.0" />
            <RelOp EstimatedTotalSubtreeCost="5.0" />
        </ShowPlanXML>
        """
        
        cost = checker._extract_cost_from_plan(plan_xml)
        assert cost == 25.0

    def test_extract_cost_parsing_error(self):
        """Test handling of invalid XML."""
        checker = QueryCostChecker()
        cost = checker._extract_cost_from_plan("INVALID XML")
        assert cost == 0.0

    def test_extract_cost_empty(self):
        """Test handling of empty plan."""
        checker = QueryCostChecker()
        assert checker._extract_cost_from_plan("") == 0.0
        assert checker._extract_cost_from_plan(None) == 0.0

class TestNolockInjectorHelpers:
    def test_should_inject(self):
        injector = NolockInjector()
        assert injector.should_inject(env="Prd", enable_nolock_hint=True) is True
        assert injector.should_inject(env="Int", enable_nolock_hint=True) is False
        assert injector.should_inject(env="Prd", enable_nolock_hint=False) is False

        assert injector.should_inject(env="Prd", enable_nolock_hint=False) is False

    def test_inject_nolock_parse_error(self):
        """Test exception during injection."""
        from services.security.nolock_injector import NolockInjectionError
        injector = NolockInjector()
        with patch("services.security.nolock_injector.sqlglot.parse_one", side_effect=Exception("Parse Fail")):
            with pytest.raises(NolockInjectionError):
                injector.inject_nolock_hints("SELECT * FROM T")

    def test_check_query_cost_exception(self):
        """Test generic exception during cost check."""
        checker = QueryCostChecker()
        with patch.object(checker, "_extract_cost_from_plan", side_effect=Exception("Cost Fail")):
             allowed, cost = checker.check_query_cost("<xml/>", "SELECT 1")
             assert allowed is True
             assert cost == 0.0

    def test_extract_cost_generic_error(self):
        """Test generic error in extraction."""
        checker = QueryCostChecker()
        with patch('services.security.query_cost_checker.ET.fromstring', side_effect=Exception("Boom")):
             assert checker._extract_cost_from_plan("<xml/>") == 0.0

    def test_cost_invalid_value(self):
        """Test non-numeric cost in XML."""
        checker = QueryCostChecker()
        xml = """<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">
        <RelOp EstimatedTotalSubtreeCost="Invalid" />
        </ShowPlanXML>"""
        # Should catch ValueError and continue (return 0.0 if max_cost=0)
        assert checker._extract_cost_from_plan(xml) == 0.0
