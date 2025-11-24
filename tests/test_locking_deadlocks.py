# tests/test_locking_deadlocks.py
"""Unit tests for LockingService deadlock retrieval (Tool 10)."""

import os
from dotenv import load_dotenv

from services.sql_review.locking_service import LockingService

load_dotenv('.env.local')

class TestLockingDeadlocks:
    @classmethod
    def setup_class(cls):
        cls.service = LockingService()
        cls.database = os.getenv('LOCAL_DB_NAME', 'LocalDB')

    def test_get_recent_deadlocks_success(self):
        # Test default
        result = self.service.get_recent_deadlocks(database=self.database, timeout_seconds=30)
        assert result["success"] is True
        assert isinstance(result["deadlocks"], list)
        # If we had deadlocks, we'd check structure
        if result["deadlocks"]:
            dl = result["deadlocks"][0]
            assert "event_time" in dl
            assert "victim_spid" in dl
            assert "process_info" in dl

    def test_get_recent_deadlocks_invalid_db(self):
        result = self.service.get_recent_deadlocks(database="nonexistent_db", timeout_seconds=30)
        assert result["success"] is False
        assert result["deadlocks"] == []
        assert isinstance(result["error"], str)
