# tests/test_locking_service.py
"""Unit tests for LockingService (Tool 9).
These tests verify that the service returns a structured JSON response for the
current blocking snapshot and handles validation errors.
"""

import os
from dotenv import load_dotenv

from services.sql_review.locking_service import LockingService

load_dotenv('.env.local')

class TestLockingService:
    @classmethod
    def setup_class(cls):
        cls.service = LockingService()
        cls.database = os.getenv('LOCAL_DB_NAME', 'LocalDB')

    def test_get_current_blocking_snapshot_success(self):
        # Test default (include_query_text=True)
        result = self.service.get_current_blocking_snapshot(database=self.database, timeout_seconds=30)
        assert result["success"] is True
        assert isinstance(result["sessions"], list)
        assert result["error"] is None
        # If we had blocking sessions, we'd check structure
        if result["sessions"]:
            session = result["sessions"][0]
            assert "session_id" in session
            assert "blocking_session_id" in session
            assert "wait_time_ms" in session
            assert "statement_text" in session

    def test_get_current_blocking_snapshot_no_text(self):
        # Test include_query_text=False
        result = self.service.get_current_blocking_snapshot(
            database=self.database, 
            include_query_text=False,
            timeout_seconds=30
        )
        assert result["success"] is True
        assert isinstance(result["sessions"], list)
        if result["sessions"]:
            session = result["sessions"][0]
            assert "session_id" in session
            assert "statement_text" not in session

    def test_get_current_blocking_snapshot_invalid_db(self):
        result = self.service.get_current_blocking_snapshot(database="nonexistent_db", timeout_seconds=30)
        assert result["success"] is False
        assert result["sessions"] == []
        assert isinstance(result["error"], str)
