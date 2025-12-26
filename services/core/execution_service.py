"""
Core Execution Service with Comprehensive Security Enforcement.
Handles 'query_readonly' and 'explain' with multi-layer protection.
"""
import structlog
from typing import Dict, Any, Optional
from services.infrastructure.db_connection_service import DbConnectionService
from services.analysis.sql_analyzer import SqlAnalyzer
from services.analysis.review_service import ReviewService
from services.security.query_cost_checker import QueryCostChecker, QueryTooExpensiveError
from services.security.concurrency_throttler import ConcurrencyThrottler, TooManyConcurrentQueriesError
from services.security.nolock_injector import NolockInjector, NolockInjectionError
from services.security.resource_control_injector import ResourceControlInjector
from services.common.exceptions import DatabaseError
from config.configuration import get_config
import time
import sqlglot
from sqlglot import exp

logger = structlog.get_logger()

class ExecutionService:
    """Handles read-only SQL execution with comprehensive security."""
    
    def __init__(self):
        self.db = DbConnectionService()
        self.analyzer = SqlAnalyzer()
        self.config = get_config()
        
        # Initialize security modules
        self.concurrency_throttler = ConcurrencyThrottler(
            max_concurrent_queries=self.config.safety.max_concurrent_queries,
            max_concurrent_queries_per_user=self.config.safety.max_concurrent_queries_per_user
        )
        self.nolock_injector = NolockInjector()
        self.resource_control_injector = ResourceControlInjector()
        
        # Initialize review service (pass self for execution plan access)
        self.review_service = ReviewService(sql_analyzer=self.analyzer, execution_service=self)


    def execute_readonly(
        self, 
        query: str, 
        env: Optional[str] = None, 
        database: Optional[str] = None,
        user: str = "anonymous",
        page_size: Optional[int] = None,
        page: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a SELECT query with comprehensive security enforcement.
        
        Security Layers:
        1. Concurrency throttling
        2. AST validation (read-only check)
        3. Environment-specific limits
        4. Query cost checking
        5. NOLOCK hint injection (production only)
        6. Execution monitoring
        
        Args:
            query: SQL query to execute
            env: Target environment (Int, Stg, Prd)
            database: Target database
            user: User identifier for throttling
            page_size: Optional number of rows per page (max 1000). If None, returns all rows up to max_rows.
            page: Optional page number (1-based). Required if page_size is provided.
            
        Returns:
            Dict with success status, data, and metadata
        """
        start_time = time.time()
        target_env = env or self.config.environment
        best_practice_warnings = []  # Initialize to ensure it's always defined
        
        # Validate pagination parameters
        if page_size is not None or page is not None:
            if page_size is None or page is None:
                return {
                    "success": False,
                    "error": "Both page_size and page must be provided together, or both omitted."
                }
            if page_size < 1 or page_size > 1000:
                return {
                    "success": False,
                    "error": f"page_size must be between 1 and 1000, got {page_size}"
                }
            if page < 1:
                return {
                    "success": False,
                    "error": f"page must be >= 1, got {page}"
                }
        
        try:
            # Layer 1: Concurrency Throttling
            with self.concurrency_throttler.acquire(target_env, user):
                
                # Layer 2: AST Validation (Read-Only Check)
                is_safe, error = self.analyzer.validate_readonly(query)
                if not is_safe:
                    logger.warning("readonly_validation_failed", error=error, env=target_env)
                    return {
                        "success": False,
                        "error": f"Security Violation: {error}",
                        "risk_score": 100
                    }
                
                # Apply pagination to query if requested
                if page_size is not None and page is not None:
                    query = self._apply_pagination(query, page_size, page)
                
                # Layer 2.1: Allowed Databases Check
                allowed_databases = self.config.safety.allowed_databases
                if allowed_databases:  # Empty list means allow all
                    # Determine target database
                    target_database = database
                    
                    # If no database parameter provided, get from connection config
                    if not target_database and target_env in self.config.database.connection_components:
                        target_database = self.config.database.connection_components[target_env].database
                    
                    if target_database:
                        # Normalize for comparison (case-insensitive)
                        target_db_lower = target_database.lower()
                        allowed_dbs_lower = [db.lower() for db in allowed_databases]
                        
                        if target_db_lower not in allowed_dbs_lower:
                            logger.warning("database_not_allowed", database=target_database, env=target_env)
                            return {
                                "success": False,
                                "error": f"Database '{target_database}' is not in the allowed list. Allowed databases: {', '.join(allowed_databases)}",
                                "risk_score": 100
                            }
                
                # Layer 2.5: Full SQL Review (like review_sql_script)
                # This performs comprehensive analysis including best practices, execution plan, and metadata
                review_result = None
                blocking_violations = []
                best_practice_warnings = []
                
                try:
                    review_result = self.review_service.review(query, env=env, database=database)
                    
                    # Check for blocking violations
                    # Block if: REJECTED status, or CRITICAL/HIGH severity security/performance issues
                    # Allow LOW severity best practice violations (warnings only)
                    review_status = review_result.get("summary", {}).get("status", "APPROVED")
                    review_risk = review_result.get("summary", {}).get("risk_score", 0)
                    
                    # Check for blocking violations: CRITICAL or HIGH severity security/performance issues
                    # Note: We only block on actual severity, not on risk score alone
                    # (risk score can be high due to many LOW severity issues, which shouldn't block)
                    for issue in review_result.get("issues", []):
                        # Handle both dict and Finding object
                        if hasattr(issue, "severity"):
                            # It's a Finding object
                            severity = issue.severity
                            category = issue.category
                            code = issue.code
                            title = issue.title
                            description = issue.description
                            recommendation = getattr(issue, "recommendation", "")
                        else:
                            # It's a dict
                            severity = issue.get("severity", "LOW")
                            category = issue.get("category", "")
                            code = issue.get("code", "")
                            title = issue.get("title", "")
                            description = issue.get("description", "")
                            recommendation = issue.get("recommendation", "")
                        
                        # Block on CRITICAL or HIGH severity security/performance issues
                        # But allow LOW/MEDIUM severity best practice violations (warnings only)
                        if severity in ["CRITICAL", "HIGH"] and category != "BEST_PRACTICE":
                            blocking_violations.append({
                                "code": code,
                                "severity": severity,
                                "category": category,
                                "title": title,
                                "description": description
                            })
                        elif category == "BEST_PRACTICE":
                            # Collect best practice warnings (non-blocking)
                            best_practice_warnings.append({
                                "code": code,
                                "severity": severity,
                                "title": title,
                                "description": description,
                                "recommendation": recommendation
                            })
                    
                    # Also block if status is REJECTED due to syntax errors or critical issues
                    # (but only if we found actual blocking violations, not just high risk score)
                    if review_status == "REJECTED" and review_result.get("summary", {}).get("top_severity") == "CRITICAL":
                        if not blocking_violations:
                            # No specific violations found but status is REJECTED with CRITICAL severity
                            # This usually means syntax error or critical security issue
                            blocking_violations.append({
                                "reason": "Query rejected by security review",
                                "status": review_status,
                                "risk_score": review_risk,
                                "verdict": review_result.get("summary", {}).get("verdict", "Unknown"),
                                "top_severity": review_result.get("summary", {}).get("top_severity", "UNKNOWN")
                            })
                    
                    # Block execution if there are blocking violations
                    if blocking_violations:
                        logger.warning(
                            "query_blocked_by_review",
                            env=target_env,
                            violations_count=len(blocking_violations),
                            risk_score=review_risk
                        )
                        return {
                            "success": False,
                            "error": "Query blocked due to security or performance violations detected in review",
                            "blocking_violations": blocking_violations,
                            "review_summary": {
                                "status": review_status,
                                "risk_score": review_risk,
                                "verdict": review_result.get("summary", {}).get("verdict", "Unknown")
                            },
                            "best_practice_warnings": best_practice_warnings
                        }
                    
                    if best_practice_warnings:
                        logger.info("best_practice_warnings", env=target_env, count=len(best_practice_warnings))
                        
                except Exception as e:
                    # If review fails, log but don't block execution (fail-safe)
                    logger.error("review_check_failed", env=target_env, error=str(e))
                    # Continue with execution but note the review failure
                
                # Layer 3: Get Environment-Specific Limits
                max_rows = self.config.safety.get_env_setting(target_env, "max_rows", self.config.safety.max_rows)
                max_time = self.config.safety.get_env_setting(target_env, "max_execution_time_seconds", self.config.safety.max_execution_time_seconds)
                enable_nolock = self.config.safety.get_env_setting(target_env, "enable_nolock_hint", False)
                
                # Layer 4: Query Cost Checking (if enabled)
                if self.config.safety.enable_cost_check:
                    cost_threshold = self.config.safety.get_env_setting(target_env, "query_cost_threshold", self.config.safety.max_query_cost)
                    
                    # Get execution plan first
                    plan_result = self.get_execution_plan(query, env, database)
                    if plan_result.get("success") and plan_result.get("plan_xml"):
                        cost_checker = QueryCostChecker(threshold=cost_threshold)
                        is_allowed, estimated_cost = cost_checker.check_query_cost(plan_result["plan_xml"], query)
                        
                        if not is_allowed:
                            logger.warning(
                                "query_cost_exceeded",
                                env=target_env,
                                estimated_cost=estimated_cost,
                                threshold=cost_threshold
                            )
                            return {
                                "success": False,
                                "error": f"Query cost ({estimated_cost:.2f}) exceeds threshold ({cost_threshold}) for {target_env} environment",
                                "estimated_cost": estimated_cost,
                                "threshold": cost_threshold
                            }
                
                # Layer 5: NOLOCK Hint Injection (Production Only)
                final_query = query
                if self.nolock_injector.should_inject(target_env, enable_nolock):
                    final_query = self.nolock_injector.inject_nolock_hints(query)
                    logger.info("nolock_hints_injected", env=target_env)
                
                # Layer 5.5: Resource Control Hint Injection (CPU and Memory Limits at SQL Server Level)
                enable_resource_hints = self.config.safety.get_env_setting(target_env, "enable_resource_hints", self.config.safety.enable_resource_hints)
                if enable_resource_hints:
                    maxdop = self.config.safety.get_env_setting(target_env, "maxdop", self.config.safety.maxdop)
                    max_grant_percent = self.config.safety.get_env_setting(target_env, "max_grant_percent", self.config.safety.max_grant_percent)
                    
                    if self.resource_control_injector.should_inject(target_env, enable_resource_hints):
                        final_query = self.resource_control_injector.inject_resource_hints(
                            final_query, 
                            target_env,
                            maxdop=maxdop,
                            max_grant_percent=max_grant_percent
                        )
                        logger.info(
                            "resource_hints_injected",
                            env=target_env,
                            maxdop=maxdop,
                            max_grant_percent=max_grant_percent
                        )
                
                # Layer 6: Execute Query with Monitoring and Payload Size Validation
                def fetch_strategy(cursor, connection):
                    columns = [column[0] for column in cursor.description]
                    rows = []
                    
                    # Configurable limit for payload size (convert MB to bytes)
                    max_payload_mb = self.config.safety.max_payload_size_mb
                    max_payload_bytes = max_payload_mb * 1024 * 1024
                    current_payload_size = 0
                    
                    # Use fetchmany to control memory usage better than fetchall
                    batch_size = 100
                    while True:
                        results = cursor.fetchmany(batch_size)
                        if not results:
                            break
                            
                        for row in results:
                            row_dict = {}
                            row_size_estimate = 0
                            
                            for i, value in enumerate(row):
                                col_name = columns[i]
                                # Truncate large text
                                if isinstance(value, str):
                                    row_size_estimate += len(value.encode('utf-8')) # Approximate byte size
                                    if len(value) > 1000:
                                        value = value[:1000] + "...(truncated)"
                                else:
                                    row_size_estimate += 16 # Rough estimate for other types
                                    
                                row_dict[col_name] = value
                            
                            current_payload_size += row_size_estimate
                            current_payload_size += row_size_estimate
                            if current_payload_size > max_payload_bytes:
                                logger.warning("payload_limit_exceeded", 
                                             env=target_env, 
                                             current_size=current_payload_size, 
                                             limit=max_payload_bytes)
                                raise DatabaseError(f"Query result too large (exceeded {max_payload_bytes/1024/1024:.1f}MB limit). Please refine your filters.")
                                
                            rows.append(row_dict)
                            
                            if len(rows) >= max_rows:
                                break
                        
                        if len(rows) >= max_rows:
                            break
                    
                    return rows

                result = self.db.execute_query(
                    final_query, 
                    env=target_env, 
                    db=database, 
                    fetch_method=fetch_strategy,
                    command_timeout=max_time  # Critical: Enforce query timeout
                )
                
                execution_time = (time.time() - start_time) * 1000  # ms
                
                # Log metrics
                logger.info(
                    "query_executed",
                    env=target_env,
                    execution_time_ms=execution_time,
                    row_count=len(result),
                    user=user
                )
                
                # Include review summary if available
                review_summary = None
                if review_result:
                    review_summary = {
                        "status": review_result.get("summary", {}).get("status", "APPROVED"),
                        "risk_score": review_result.get("summary", {}).get("risk_score", 0),
                        "verdict": review_result.get("summary", {}).get("verdict", "Query passed review")
                    }
                
                # Build response with pagination metadata if applicable
                response = {
                    "success": True,
                    "data": result,
                    "row_count": len(result),
                    "execution_time_ms": round(execution_time, 2),
                    "environment": target_env,
                    "limits_applied": {
                        "max_rows": max_rows,
                        "max_time_seconds": max_time,
                        "nolock_enabled": enable_nolock
                    },
                    "review_summary": review_summary,
                    "best_practice_warnings": best_practice_warnings if best_practice_warnings else []
                }
                
                # Add pagination metadata if pagination was applied
                if page_size is not None and page is not None:
                    response["pagination"] = {
                        "page": page,
                        "page_size": page_size,
                        "rows_returned": len(result),
                        "offset": (page - 1) * page_size
                    }
                
                return response
                
        except TooManyConcurrentQueriesError as e:
            logger.warning("concurrency_limit_exceeded", env=target_env, user=user, error=str(e))
            return {
                "success": False,
                "error": str(e),
                "retry_after_seconds": 5
            }
        except NolockInjectionError as e:
            # Critical protection for Production
            logger.error("nolock_error_blocking_execution", env=target_env, error=str(e))
            return {
                "success": False,
                "error": f"Security enforcement failed: {str(e)}. Query blocked on {target_env} to prevent locking.",
                "risk_score": 100
            }
        except Exception as e:
            logger.error("query_execution_error", env=target_env, error=str(e))
            return {
                "success": False,
                "error": f"Execution error: {str(e)}"
            }

    def _apply_pagination(self, query: str, page_size: int, page: int) -> str:
        """
        Apply SQL Server OFFSET/FETCH pagination to a SELECT query.
        
        Args:
            query: SQL SELECT query
            page_size: Number of rows per page
            page: Page number (1-based)
            
        Returns:
            Modified query with OFFSET/FETCH NEXT clauses
        """
        try:
            parsed = sqlglot.parse(query, read="tsql")
            if not parsed:
                return query  # Return original if parsing fails
            
            # Filter out None (comments/empty)
            parsed = [p for p in parsed if p]
            if not parsed or not isinstance(parsed[0], exp.Select):
                return query  # Return original if not a SELECT
            
            select_stmt = parsed[0]
            
            # Check if query already has OFFSET/FETCH
            if select_stmt.find(exp.Offset) or select_stmt.find(exp.Fetch):
                # Query already has pagination - return as-is (user's pagination takes precedence)
                logger.warning("query_already_has_pagination", query=query[:100])
                return query
            
            # Calculate offset (0-based)
            offset = (page - 1) * page_size
            
            # Check if ORDER BY exists (required for OFFSET/FETCH in SQL Server)
            has_order_by = bool(select_stmt.find(exp.Order))
            
            if not has_order_by:
                # SQL Server requires ORDER BY for OFFSET/FETCH
                # We'll add ORDER BY (SELECT NULL) as a fallback
                # This is a common pattern for "no specific ordering"
                order_expr = exp.Order(expressions=[exp.Literal.number(1)])
                select_stmt.set("order", order_expr)
                logger.info("added_dummy_order_by_for_pagination")
            
            # Add OFFSET using sqlglot's expression builder
            offset_expr = exp.Offset(expression=exp.Literal.number(offset))
            select_stmt.set("offset", offset_expr)
            
            # Add FETCH NEXT using sqlglot's expression builder
            fetch_expr = exp.Fetch(
                kind="NEXT",
                expression=exp.Literal.number(page_size)
            )
            select_stmt.set("fetch", fetch_expr)
            
            # Convert back to SQL
            return select_stmt.sql(dialect="tsql")
            
        except Exception as e:
            logger.error("pagination_application_failed", error=str(e), query=query[:100])
            # Return original query if pagination fails
            return query

    def get_execution_plan(self, query: str, env: Optional[str] = None, database: Optional[str] = None) -> Dict[str, Any]:
        """
        Get SQL Server execution plan XML.
        
        Args:
            query: SQL query
            env: Target environment
            database: Target database
            
        Returns:
            Dict with success status and plan XML
        """
        target_env = env or self.config.environment
        
        try:
            # Wrap query to get execution plan
            plan_query = f"SET SHOWPLAN_XML ON; {query}; SET SHOWPLAN_XML OFF;"
            
            def fetch_plan(cursor, connection):
                # Execution plan is returned as result set
                plan_xml = ""
                for row in cursor.fetchall():
                    if row[0]:
                        plan_xml += str(row[0])
                return plan_xml
            
            plan_xml = self.db.execute_query(plan_query, env=target_env, db=database, fetch_method=fetch_plan)
            
            return {
                "success": True,
                "plan_xml": plan_xml,
                "environment": target_env
            }
            
        except Exception as e:
            logger.error("execution_plan_error", env=target_env, error=str(e))
            return {
                "success": False,
                "error": f"Failed to get execution plan: {str(e)}"
            }
