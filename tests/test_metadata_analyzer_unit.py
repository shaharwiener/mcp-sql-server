"""
Unit tests for MetadataAnalyzer.
Tests interpretation of system view queries for best practices.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from services.analysis.metadata_analyzer import MetadataAnalyzer

class TestMetadataAnalyzerUnit:
    @pytest.fixture
    def mock_cursor(self):
        cursor = MagicMock()
        return cursor

    @pytest.fixture
    def mock_db_service(self, mock_cursor):
        service = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        service.get_connection.return_value.__enter__.return_value = mock_conn
        return service

    @pytest.fixture
    def analyzer(self, mock_db_service):
        with patch('services.analysis.metadata_analyzer.DbConnectionService', return_value=mock_db_service):
            analyzer = MetadataAnalyzer()
            analyzer.db_service = mock_db_service
            return analyzer

    def test_analyze_metadata_empty(self, analyzer, mock_cursor):
        """Test that no violations are returned if queries return nothing."""
        mock_cursor.fetchall.return_value = []
        violations = analyzer.analyze_metadata()
        assert len(violations) == 0

    def test_missing_statistics_detection(self, analyzer, mock_cursor):
        """Test detection of missing statistics (BP034)."""
        # Setup specific query response
        # We need to distinguish calls based on query text or order.
        # Simplification: use side_effect to return specific data for specific checks
        
        # Determine which check is called by inspecting call args?
        # Or simpler: The analyzer runs checks sequentially.
        # But mocking sequential returns is brittle if order changes.
        
        # Instead, I'll test the internal private method directly for logic, 
        # or mock execute to check query string patterns.
        
        # Let's test _check_missing_statistics directly
        row = Mock()
        row.table_name = "dbo.NoStatsTable"
        mock_cursor.fetchall.return_value = [row]
        
        violations = analyzer._check_missing_statistics(mock_cursor)
        
        assert len(violations) == 1
        assert "BP034" in violations[0]
        assert "dbo.NoStatsTable" in violations[0]

    def test_index_fragmentation_detection(self, analyzer, mock_cursor):
        """Test detection of index fragmentation (BP033)."""
        row = Mock()
        row.table_name = "dbo.FragTable"
        row.index_name = "IX_Frag"
        row.avg_fragmentation_in_percent = 45.5
        mock_cursor.fetchall.return_value = [row]
        
        violations = analyzer._check_index_fragmentation(mock_cursor)
        
        assert len(violations) == 1
        assert "BP033" in violations[0]
        assert "45.5%" in violations[0]
        
    def test_exception_handling(self, analyzer, mock_cursor):
        """Test that analysis continues if individual check fails."""
        # Mock global execute failure for the main analyze_metadata
        mock_cursor.execute.side_effect = Exception("DB Error")
        
        violations = analyzer.analyze_metadata()
        
        # Should catch and return empty list, not raise
        # Should catch and return empty list, not raise
        assert len(violations) == 0

    def test_analyze_metadata_connection_error(self, analyzer):
        """Test handling of connection failure."""
        analyzer.db_service.get_connection.side_effect = Exception("Conn Failed")
        violations = analyzer.analyze_metadata()
        assert len(violations) == 0

    def test_all_individual_checks(self, analyzer, mock_cursor):
        """Test all specific check methods with data."""
        # BP032
        mock_cursor.fetchall.return_value = [Mock(table_name="T", stats_name="S", days_old=10)]
        assert "BP032" in analyzer._check_statistics_freshness(mock_cursor)[0]
        
        # BP035
        mock_cursor.fetchall.return_value = [Mock(table_name="T", index_name="I")]
        assert "BP035" in analyzer._check_unused_indexes(mock_cursor)[0]

        # BP036
        mock_cursor.fetchall.return_value = [Mock(table_name="T", index1="I1", index2="I2")]
        assert "BP036" in analyzer._check_duplicate_indexes(mock_cursor)[0]

        # BP037
        mock_cursor.fetchall.return_value = [Mock(table_name="T", row_count=10000001)]
        assert "BP037" in analyzer._check_table_partitioning(mock_cursor)[0]

        # BP038
        mock_cursor.fetchall.return_value = [Mock(table_name="T", row_count=6000000)]
        assert "BP038" in analyzer._check_columnstore_indexes(mock_cursor)[0]

        # BP039
        mock_cursor.fetchall.return_value = [Mock(table_name="T", column_name="C")]
        assert "BP039" in analyzer._check_data_types(mock_cursor)[0]

        # BP040
        mock_cursor.fetchall.return_value = [Mock(table_name="T")]
        assert "BP040" in analyzer._check_heap_tables(mock_cursor)[0]

        # BP041
        mock_cursor.fetchall.return_value = [Mock(table_name="T", column_count=51)]
        assert "BP041" in analyzer._check_wide_tables(mock_cursor)[0]

        # BP042
        mock_cursor.fetchall.return_value = [Mock(table_name="T", fk_name="FK", column_name="C")]
        assert "BP042" in analyzer._check_foreign_key_indexes(mock_cursor)[0]
