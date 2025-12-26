"""
Centralized SQL Review Service.
Orchestrates all analyzers to provide comprehensive SQL script review.
This service is used by both review_sql_script and execute_readonly to ensure consistency.
"""
from typing import Dict, Any, Optional, TYPE_CHECKING
import structlog
from services.analysis.sql_analyzer import SqlAnalyzer
from services.analysis.models import ReviewResult, Finding

if TYPE_CHECKING:
    from services.core.execution_service import ExecutionService

logger = structlog.get_logger()


class ReviewService:
    """
    Centralized service for comprehensive SQL review.
    Runs all analyzers in a consistent manner.
    """
    
    def __init__(self, sql_analyzer: Optional[SqlAnalyzer] = None, 
                 execution_service: Optional['ExecutionService'] = None):
        """
        Initialize the review service.
        
        Args:
            sql_analyzer: Optional SqlAnalyzer instance (creates new if not provided)
            execution_service: Optional ExecutionService instance (creates new if not provided)
        """
        self.sql_analyzer = sql_analyzer or SqlAnalyzer()
        self.execution_service = execution_service
    
    def review(self, sql: str, env: Optional[str] = None, 
               database: Optional[str] = None) -> Dict[str, Any]:
        """
        Perform comprehensive SQL review using all available analyzers.
        
        This method orchestrates:
        1. AST Analysis (SqlAnalyzer) - Security, best practices, syntax
        2. Execution Plan Analysis (ExecutionPlanAnalyzer) - Performance insights
        3. Metadata Analysis (MetadataAnalyzer) - Schema and statistics validation
        
        Args:
            sql: SQL script to review
            env: Target environment (Int, Stg, Prd)
            database: Target database name
            
        Returns:
            Dict containing review result with all findings
        """
        # 1. AST Analysis (always runs)
        result = self.sql_analyzer.analyze(sql)
        
        # If syntax error, return immediately
        if result.summary.status == "REJECTED" and result.summary.top_severity == "CRITICAL":
            return result.model_dump()
        
        # 2. Execution Plan Analysis
        self._add_execution_plan_findings(result, sql, env, database)
        
        # 3. Metadata Analysis
        self._add_metadata_findings(result, env, database)
        
        # 4. Recalculate Risk Score and Update Status
        self._finalize_review(result)
        
        return result.model_dump()
    
    def _add_execution_plan_findings(self, result: ReviewResult, sql: str, 
                                     env: Optional[str] = None, 
                                     database: Optional[str] = None) -> None:
        """
        Add execution plan analysis findings to the review result.
        
        Args:
            result: ReviewResult to update
            sql: SQL query
            env: Target environment
            database: Target database
        """
        if not self.execution_service:
            logger.warning("execution_service_not_provided", 
                         message="Execution plan analysis skipped")
            return
        
        try:
            plan_result = self.execution_service.get_execution_plan(sql, env, database)
            
            if plan_result.get("success") and plan_result.get("plan_xml"):
                from services.analysis.execution_plan_analyzer import ExecutionPlanAnalyzer
                
                plan_analyzer = ExecutionPlanAnalyzer()
                plan_findings_strs = plan_analyzer.analyze_plan(plan_result["plan_xml"])
                
                for f_str in plan_findings_strs:
                    # Determine severity based on content
                    severity = "MEDIUM"
                    if "Scan" in f_str or "Lookup" in f_str:
                        severity = "HIGH"
                    
                    result.issues.append(Finding(
                        code="PLAN001",
                        severity=severity,
                        category="PERFORMANCE",
                        title="Execution Plan Insight",
                        description=f_str,
                        recommendation="Optimize query based on plan warning (e.g., add missing index).",
                    ))
                
                result.performance_insights.execution_plan_available = True
                
        except Exception as e:
            # Plan retrieval failed, log but don't fail the review
            logger.warning("execution_plan_analysis_failed", 
                         error=str(e), env=env)
    
    def _add_metadata_findings(self, result: ReviewResult, 
                              env: Optional[str] = None, 
                              database: Optional[str] = None) -> None:
        """
        Add metadata analysis findings to the review result.
        
        Args:
            result: ReviewResult to update
            env: Target environment
            database: Target database
        """
        try:
            from services.analysis.metadata_analyzer import MetadataAnalyzer
            
            metadata_analyzer = MetadataAnalyzer()
            meta_findings_strs = metadata_analyzer.analyze_metadata(env=env, database=database)
            
            for f_str in meta_findings_strs:
                result.issues.append(Finding(
                    code="META001",
                    severity="MEDIUM",
                    category="RELIABILITY",
                    title="Metadata Issue",
                    description=f_str,
                    recommendation="Check database schema and statistics.",
                ))
                
        except Exception as e:
            # Metadata analysis failed, log but don't fail the review
            logger.warning("metadata_analysis_failed", 
                         error=str(e), env=env)
    
    def _finalize_review(self, result: ReviewResult) -> None:
        """
        Recalculate risk score and update status based on all findings.
        
        Args:
            result: ReviewResult to finalize
        """
        # Recalculate Risk Score based on execution plan and metadata findings
        total_risk = result.summary.risk_score
        for issue in result.issues:
            if issue.code.startswith("PLAN") or issue.code.startswith("META"):
                if issue.severity == "HIGH":
                    total_risk += 15
                elif issue.severity == "MEDIUM":
                    total_risk += 5
        
        result.summary.risk_score = min(100, total_risk)
        
        # Update Status if Risk increased
        if result.summary.risk_score >= 80:
            result.summary.status = "REJECTED"
            result.summary.verdict = "Significant risks detected in plan/metadata."
        elif result.summary.risk_score >= 30:
            result.summary.status = "WARNING"
            result.summary.verdict = "Script contains potential issues."

