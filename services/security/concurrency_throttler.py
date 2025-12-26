"""
Concurrency Throttler for SQL Server queries.
Limits concurrent queries per environment and per user.
"""
import threading
from typing import Dict, Set, Optional
from contextlib import contextmanager
import structlog

logger = structlog.get_logger()


class ConcurrencyThrottler:
    """Thread-safe throttler for concurrent query execution."""
    
    def __init__(
        self,
        max_concurrent_queries: int = 5,
        max_concurrent_queries_per_user: int = 2
    ):
        """
        Initialize concurrency throttler.
        
        Args:
            max_concurrent_queries: Max total concurrent queries
            max_concurrent_queries_per_user: Max concurrent queries per user
        """
        self.max_concurrent_queries = max_concurrent_queries
        self.max_concurrent_queries_per_user = max_concurrent_queries_per_user
        
        # Track active queries: {env: {user: count}}
        self.active_queries: Dict[str, Dict[str, int]] = {}
        self.lock = threading.Lock()
    
    @contextmanager
    def acquire(self, env: str, user: str = "anonymous"):
        """
        Acquire slot for query execution.
        
        Args:
            env: Environment (Int, Stg, Prd)
            user: User identifier
            
        Raises:
            TooManyConcurrentQueriesError: If limits exceeded
            
        Usage:
            with throttler.acquire("Prd", "user123"):
                # Execute query
                pass
        """
        # Try to acquire
        acquired = False
        try:
            with self.lock:
                # Initialize environment tracking
                if env not in self.active_queries:
                    self.active_queries[env] = {}
                
                # Check total concurrent queries for environment
                total_queries = sum(self.active_queries[env].values())
                if total_queries >= self.max_concurrent_queries:
                    raise TooManyConcurrentQueriesError(
                        f"Too many concurrent queries on {env} environment "
                        f"({total_queries}/{self.max_concurrent_queries}). Please try again later."
                    )
                
                # Check per-user limit
                user_queries = self.active_queries[env].get(user, 0)
                if user_queries >= self.max_concurrent_queries_per_user:
                    raise TooManyConcurrentQueriesError(
                        f"Too many concurrent queries for user '{user}' "
                        f"({user_queries}/{self.max_concurrent_queries_per_user}). Please try again later."
                    )
                
                # Acquire slot
                self.active_queries[env][user] = user_queries + 1
                acquired = True
                
                logger.info(
                    "query_slot_acquired",
                    env=env,
                    user=user,
                    user_queries=user_queries + 1,
                    total_queries=total_queries + 1
                )
            
            # Yield control to caller
            yield
            
        finally:
            # Release slot
            if acquired:
                with self.lock:
                    self.active_queries[env][user] -= 1
                    if self.active_queries[env][user] == 0:
                        del self.active_queries[env][user]
                    
                    logger.info("query_slot_released", env=env, user=user)
    
    def get_active_count(self, env: str) -> int:
        """Get number of active queries for environment."""
        with self.lock:
            if env not in self.active_queries:
                return 0
            return sum(self.active_queries[env].values())
    
    def get_user_active_count(self, env: str, user: str) -> int:
        """Get number of active queries for specific user in environment."""
        with self.lock:
            if env not in self.active_queries:
                return 0
            return self.active_queries[env].get(user, 0)


class TooManyConcurrentQueriesError(Exception):
    """Raised when concurrent query limits are exceeded."""
    pass
