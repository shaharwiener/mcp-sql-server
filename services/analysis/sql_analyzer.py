"""
Safe SQL Analysis Service using sqlglot.
Performs static analysis, validation, and risk scoring.
"""
import sqlglot
from sqlglot import exp
from typing import List, Dict, Any, Tuple, Set, Optional
import structlog
from config.configuration import get_config
from services.analysis.best_practices import BestPracticesEngine
from services.analysis.models import (
    ReviewResult, ReviewSummary, SafetyChecks, Finding, 
    PerformanceInsights, SchemaContext
)

logger = structlog.get_logger()

class SqlAnalyzer:
    def __init__(self):
        self.config = get_config()
        self.dialect = "tsql"
        self.bp_engine = BestPracticesEngine()

    def analyze(self, sql: str) -> ReviewResult:
        """
        Deep analysis of a SQL script returning structured findings.
        """
        try:
            parsed = sqlglot.parse(sql, read=self.dialect)
        except Exception as e:
            logger.error("parse_error", error=str(e))
            # Return a "Rejected" result due to syntax error
            return self._create_syntax_error_result(str(e))

        risk_score = 0
        findings: List[Finding] = []
        statements_summary = []
        referenced_tables = set()
        
        has_write_ops = False
        has_ddl = False
        
        for expression in parsed:
            # 1. Classification
            stmt_type = expression.key
            statements_summary.append(stmt_type)
            
            # 2. Extract Tables
            for table in expression.find_all(exp.Table):
                curr_table = table.name
                if table.db:
                    curr_table = f"{table.db}.{curr_table}"
                referenced_tables.add(curr_table)
            
            # 3. Dangerous Pattern Detection
            
            # Write Operations
            if isinstance(expression, (exp.Delete, exp.Update, exp.Insert, exp.Merge)):
                risk_score += 100
                has_write_ops = True
                findings.append(Finding(
                    code="SEC001",
                    severity="CRITICAL",
                    category="SECURITY",
                    title="Write Operation Detected",
                    description=f"The script contains a {stmt_type} statement which modifies data.",
                    recommendation="Ensure this write operation is intended and authorized for the target environment.",
                    snippet=expression.sql()[:100] + "..."
                ))
                
                # Check for WHERE clause in Update/Delete
                if isinstance(expression, (exp.Delete, exp.Update)):
                    if not expression.find(exp.Where):
                        weight = self.config.safety.risk_weights.no_where_clause
                        risk_score += weight
                        findings.append(Finding(
                            code="SEC002",
                            severity="CRITICAL",
                            category="SECURITY",
                            title=f"Missing WHERE Clause in {stmt_type}",
                            description=f"Executing {stmt_type} without a WHERE clause will affect ALL rows in the table.",
                            recommendation="Add a WHERE clause to restrict the scope of the operation.",
                            snippet=expression.sql()[:100] + "..."
                        ))

            # DDL
            ddl_classes = [exp.Create, exp.Drop]
            # Check if AlterTable and AlterColumn exist (version compatibility)
            if hasattr(exp, 'AlterTable'):
                ddl_classes.append(exp.AlterTable)
            if hasattr(exp, 'AlterColumn'):
                ddl_classes.append(exp.AlterColumn)
            if isinstance(expression, tuple(ddl_classes)):
                 weight = self.config.safety.risk_weights.ddl_statement
                 risk_score += weight
                 has_ddl = True
                 findings.append(Finding(
                    code="SEC003",
                    severity="HIGH",
                    category="SECURITY",
                    title="DDL Statement Detected", 
                    description=f"The script contains a {stmt_type} statement which modifies the schema.",
                    recommendation="DDL changes should be managed via migration tools, not ad-hoc scripts.",
                    snippet=expression.sql()[:100] + "..."
                 ))
                 
            # Dynamic SQL (EXEC/EXECUTE)
            if isinstance(expression, exp.Command):
                weight = self.config.safety.risk_weights.dynamic_sql
                risk_score += weight
                findings.append(Finding(
                    code="SEC004",
                    severity="HIGH",
                    category="SECURITY",
                    title="Dynamic SQL Execution",
                    description="Dynamic SQL (EXEC/EXECUTE) allows arbitrary code execution and is hard to analyze.",
                    recommendation="Replace dynamic SQL with static SQL or parameterized queries where possible.",
                    snippet=expression.sql()[:100] + "..."
                ))

            # Linked Server Detection
            if not self.config.safety.allow_linked_servers:
                sql_upper = expression.sql().upper()
                # Check for linked server patterns
                has_linked_server = False
                linked_server_patterns = [
                    "OPENQUERY",
                    "OPENDATASOURCE",
                    "OPENROWSET",
                    "FOUR_PART_NAME"  # server.database.schema.table
                ]
                
                for pattern in linked_server_patterns:
                    if pattern in sql_upper:
                        has_linked_server = True
                        break
                
                # Also check for four-part names (server.database.schema.table)
                # sqlglot represents these as Table with db and catalog
                for table in expression.find_all(exp.Table):
                    # Four-part name: catalog.db.schema.table (catalog is the linked server)
                    if table.catalog:
                        has_linked_server = True
                        break
                    # Three-part name could also be linked server if it's not the current server
                    # But we can't easily detect that without connection context
                
                if has_linked_server:
                    risk_score += 100
                    findings.append(Finding(
                        code="SEC005",
                        severity="CRITICAL",
                        category="SECURITY",
                        title="Linked Server Access Detected",
                        description="Query attempts to access linked servers, which is disabled for security reasons.",
                        recommendation="Linked server access is not allowed. Use direct database connections instead.",
                        snippet=expression.sql()[:100] + "..."
                    ))

            # Cross Joins
            for join in expression.find_all(exp.Join):
                if join.kind == "CROSS":
                    weight = self.config.safety.risk_weights.cross_join
                    risk_score += weight
                    findings.append(Finding(
                        code="PERF001",
                        severity="MEDIUM",
                        category="PERFORMANCE",
                        title="Cross Join Detected",
                        description="Cross joins generate a Cartesian product of rows, which can be performance-intensive.",
                        recommendation="Use an INNER JOIN with a specific ON condition instead.",
                        snippet=join.sql()[:100] + "..."
                    ))

            # Best Practices Checks
            # Note: bp_engine currently returns strings. We'll wrap them generic Findings for now.
            # Ideally BP engine would return Finding objects too.
            bp_violations = self.bp_engine.check_rules(expression)
            for violation in bp_violations:
                risk_score += 5
                # Extract code from violation string (e.g., "BP001: Description")
                code = "BP000"
                description = violation
                
                if ":" in violation:
                    parts = violation.split(":", 1)
                    if parts[0].strip().startswith("BP"):
                        code = parts[0].strip()
                        description = parts[1].strip()

                findings.append(Finding(
                    code=code,
                    severity="LOW",
                    category="BEST_PRACTICE",
                    title="Best Practice Violation",
                    description=description,
                    recommendation="Review the SQL Best Practices guide.",
                    snippet=expression.sql()[:50] + "..."
                ))

        # Cap Risk Score
        risk_score = min(risk_score, 100)
        
        # Determine Status
        status = "APPROVED"
        if risk_score >= 80:
            status = "REJECTED"
        elif risk_score >= 30:
            status = "WARNING"
            
        verdict = "Script is safe execute."
        if status == "REJECTED":
            verdict = "Script poses critical risks and should NOT be executed."
        elif status == "WARNING":
            verdict = "Script contains potential issues. Review findings before critical execution."

        # Compile Result
        return ReviewResult(
            summary=ReviewSummary(
                status=status,
                risk_score=risk_score,
                verdict=verdict,
                top_severity=self._get_top_severity(findings)
            ),
            safety_checks=SafetyChecks(
                is_readonly=not (has_write_ops or has_ddl),
                has_write_ops=has_write_ops,
                has_ddl=has_ddl
            ),
            issues=findings,
            performance_insights=PerformanceInsights(
                execution_plan_available=False, # Filled later by caller
                estimated_cost=None
            ),
            schema_context=SchemaContext(
                valid_objects=list(referenced_tables),
                invalid_objects=[] # Filled later by metadata analyzer
            )
        )

    def _create_syntax_error_result(self, error_msg: str) -> ReviewResult:
        return ReviewResult(
            summary=ReviewSummary(
                status="REJECTED",
                risk_score=100,
                verdict="Syntax Error prevented analysis.",
                top_severity="CRITICAL"
            ),
            safety_checks=SafetyChecks(
                is_readonly=False, has_write_ops=False, has_ddl=False
            ),
            issues=[Finding(
                code="SYN001",
                severity="CRITICAL",
                category="MAINTAINABILITY",
                title="SQL Syntax Error",
                description=error_msg,
                recommendation="Fix the syntax error to allow further analysis."
            )],
            performance_insights=PerformanceInsights(execution_plan_available=False),
            schema_context=SchemaContext()
        )

    def _get_top_severity(self, findings: List[Finding]) -> str:
        priority = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
        max_p = 0
        top = "LOW"
        for f in findings:
            p = priority.get(f.severity, 1)
            if p > max_p:
                max_p = p
                top = f.severity
        return top

    def validate_readonly(self, sql: str) -> Tuple[bool, str]:
        """
        Strict validation for query_readonly tool.
        Must be a single SELECT statement. No batches.
        """
        try:
            parsed = sqlglot.parse(sql, read=self.dialect)
            # Filter out None (comments/empty)
            parsed = [p for p in parsed if p]
            
            if not parsed:
                return False, "Empty query."
                
            if len(parsed) > 1:
                return False, "Multi-statement batches are not allowed in read-only mode."
                
            stmt = parsed[0]
            
            # Must be SELECT
            if not isinstance(stmt, exp.Select):
                return False, f"Only SELECT statements are allowed. Found: {stmt.key}"
                
            # Check for allowed constructs
            if stmt.find(exp.Into):
                return False, "SELECT INTO is not allowed (write operation)."
                
            return True, ""
            
        except Exception as e:  # pragma: no cover
            return False, f"Parsing error: {e}"
