"""
Query Cost Checker for SQL Server.
Analyzes execution plan cost and blocks expensive queries.
"""
import xml.etree.ElementTree as ET
from typing import Optional
import structlog

logger = structlog.get_logger()


class QueryCostChecker:
    """Checks query cost from execution plan and enforces thresholds."""
    
    def __init__(self, threshold: float = 50.0):
        """
        Initialize query cost checker.
        
        Args:
            threshold: Maximum allowed estimated query cost
        """
        self.threshold = threshold
    
    def check_query_cost(self, plan_xml: str, query: str) -> tuple[bool, float]:
        """
        Check if query cost exceeds threshold.
        
        Args:
            plan_xml: SQL Server execution plan XML
            query: Original SQL query (for logging)
            
        Returns:
            Tuple of (is_allowed, estimated_cost)
            
        Raises:
            QueryTooExpensiveError: If cost exceeds threshold
        """
        try:
            cost = self._extract_cost_from_plan(plan_xml)
            
            if cost > self.threshold:
                logger.warning(
                    "query_cost_exceeded",
                    estimated_cost=cost,
                    threshold=self.threshold,
                    query_preview=query[:100]
                )
                return (False, cost)
            
            logger.info(
                "query_cost_check_passed",
                estimated_cost=cost,
                threshold=self.threshold
            )
            return (True, cost)
            
        except Exception as e:
            logger.error("query_cost_check_error", error=str(e))
            # If we can't check cost, allow the query (fail open)
            return (True, 0.0)
    
    def _extract_cost_from_plan(self, plan_xml: str) -> float:
        """
        Extract estimated total subtree cost from execution plan.
        
        Args:
            plan_xml: SQL Server execution plan XML
            
        Returns:
            Estimated cost (float)
        """
        if not plan_xml or not plan_xml.strip():
            return 0.0
        
        try:
            root = ET.fromstring(plan_xml)
            namespace = {'p': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
            
            # Find the root RelOp element (highest cost)
            # StatementSubTreeCost is at the StmtSimple level
            stmt = root.find('.//p:StmtSimple', namespace)
            if stmt is not None:
                cost_str = stmt.get('StatementSubTreeCost', '0')
                return float(cost_str)
            
            # Fallback: find highest EstimatedTotalSubtreeCost
            max_cost = 0.0
            for rel_op in root.findall('.//p:RelOp', namespace):
                cost_str = rel_op.get('EstimatedTotalSubtreeCost', '0')
                try:
                    cost = float(cost_str)
                    max_cost = max(max_cost, cost)
                except ValueError:
                    continue
            
            return max_cost
            
        except ET.ParseError:
            logger.warning("invalid_execution_plan_xml")
            return 0.0
        except Exception as e:
            logger.error("cost_extraction_error", error=str(e))
            return 0.0


class QueryTooExpensiveError(Exception):
    """Raised when query cost exceeds threshold."""
    pass
