"""
Comprehensive Best Practices Rule Engine.
Implements all programmatically checkable rules from sql_best_practices.md.
"""
from sqlglot import exp
from typing import List, Dict, Any
from pathlib import Path
from config.configuration import get_config

class BestPracticesEngine:
    def __init__(self):
        self.config = get_config()
        
    def check_rules(self, expression: exp.Expression) -> List[str]:
        """
        Check all AST-based best practice rules.
        Returns list of violation messages with unique codes (BP###).
        """
        violations = []
        
        # BEGINNER TIPS (Checkable via AST)
        if self.config.best_practices.enforce_no_select_star:
            violations.extend(self._check_select_star(expression))  # BP001
        
        if self.config.best_practices.enforce_schema_prefix:
            violations.extend(self._check_schema_prefix(expression))  # BP002
        
        violations.extend(self._check_cross_joins(expression))  # BP003
        violations.extend(self._check_functions_in_where(expression))  # BP004
        violations.extend(self._check_or_in_where(expression))  # BP005
        violations.extend(self._check_distinct_usage(expression))  # BP006
        violations.extend(self._check_in_vs_exists(expression))  # BP007
        violations.extend(self._check_cursors(expression))  # BP008
        violations.extend(self._check_scalar_functions_in_select(expression))  # BP009
        violations.extend(self._check_large_in_lists(expression))  # BP010
        violations.extend(self._check_union_usage(expression))  # BP011
        violations.extend(self._check_implicit_conversions(expression))  # BP012
        
        # ADDITIONAL CHECKABLE RULES
        violations.extend(self._check_set_nocount(expression))  # BP013
        violations.extend(self._check_set_xact_abort(expression))  # BP014
        violations.extend(self._check_try_catch(expression))  # BP015
        violations.extend(self._check_proper_joins(expression))  # BP016
        violations.extend(self._check_temp_tables_vs_variables(expression))  # BP017
        violations.extend(self._check_dynamic_sql(expression))  # BP018
        violations.extend(self._check_transaction_usage(expression))  # BP019
        violations.extend(self._check_subquery_optimization(expression))  # BP020
        violations.extend(self._check_top_usage(expression))  # BP021
        violations.extend(self._check_stored_procedure_prefix(expression))  # BP022
        
        return list(set(violations))  # Deduplicate
    
    def _check_select_star(self, expression: exp.Expression) -> List[str]:
        """Avoid SELECT * - specify columns explicitly."""
        violations = []
        for star in expression.find_all(exp.Star):
            # Ignore COUNT(*)
            if isinstance(star.parent, exp.Count):
                continue
            violations.append("BP001: Avoid 'SELECT *'. Specify columns explicitly for better performance and maintainability.")
        return violations
    
    def _check_schema_prefix(self, expression: exp.Expression) -> List[str]:
        """Use schema prefix for all tables."""
        violations = []
        for table in expression.find_all(exp.Table):
            if not table.db:
                violations.append(f"BP002: Table '{table.name}' missing schema prefix. Use 'schema.table' format.")
        return violations
    
    def _check_cross_joins(self, expression: exp.Expression) -> List[str]:
        """Avoid cross joins - they can explode result sets."""
        violations = []
        for join in expression.find_all(exp.Join):
            # Check for explicit CROSS JOIN or implicit comma join (kind is None/empty)
            # Note: INNER JOINs without ON are also technically cross products, but usually parsing handles that.
            if join.kind == "CROSS":
                 violations.append("BP003: Cross join detected. Ensure this is intentional as it can severely impact performance.")
            elif not join.kind and not join.args.get("on"):
                 # Comma join (e.g. FROM A, B) usually has no kind and no ON clause
                 violations.append("BP003: Implicit cross join detected (comma-separated tables). Use explicit JOIN syntax.")
        
        return violations
    
    def _check_functions_in_where(self, expression: exp.Expression) -> List[str]:
        """Avoid functions wrapping columns in WHERE - prevents index usage."""
        violations = []
        for where in expression.find_all(exp.Where):
            for func in where.find_all(exp.Func):
                # Check if a Column is inside the function
                if func.find(exp.Column):
                    violations.append(f"BP004: Function '{func.sql()}' wraps column in WHERE clause. This prevents index usage. Consider rewriting.")
        return violations
    
    def _check_or_in_where(self, expression: exp.Expression) -> List[str]:
        """Avoid OR in WHERE clause - can prevent index usage."""
        violations = []
        for where in expression.find_all(exp.Where):
            for or_expr in where.find_all(exp.Or):
                violations.append("BP005: OR condition in WHERE clause detected. Consider using UNION ALL for better index usage.")
        return violations
    
    def _check_distinct_usage(self, expression: exp.Expression) -> List[str]:
        """Minimize use of DISTINCT - adds processing overhead."""
        violations = []
        for distinct in expression.find_all(exp.Distinct):
            violations.append("BP006: DISTINCT detected. Ensure it's necessary as it adds processing overhead. Consider fixing duplicates at source.")
        return violations
    
    def _check_in_vs_exists(self, expression: exp.Expression) -> List[str]:
        """Recommend EXISTS over IN for subqueries."""
        violations = []
        for in_expr in expression.find_all(exp.In):
            # Check if the IN contains a subquery (Select or Subquery)
            if in_expr.find(exp.Subquery) or in_expr.find(exp.Select):
                violations.append("BP007: IN with subquery detected. Consider using EXISTS for better performance.")
        return violations
    
    def _check_cursors(self, expression: exp.Expression) -> List[str]:
        """Avoid cursors - use set-based operations."""
        violations = []
        # Check for DECLARE CURSOR statements
        sql_text = expression.sql().upper()
        if "DECLARE" in sql_text and "CURSOR" in sql_text:
            violations.append("BP008: Cursor detected. Cursors process rows one-by-one and are slow. Use set-based operations instead.")
        return violations
    
    def _check_scalar_functions_in_select(self, expression: exp.Expression) -> List[str]:
        """Avoid scalar functions in SELECT - performance bottleneck."""
        violations = []
        for select in expression.find_all(exp.Select):
            for func in select.expressions:
                # Handle scalar functions (including schema-qualified via Dot)
                is_scalar = False
                if isinstance(func, exp.Func) and not isinstance(func, (exp.AggFunc, exp.Anonymous)):
                     is_scalar = True
                elif isinstance(func, exp.Dot):
                     # Check right side (e.g. dbo.MyFunc)
                     if isinstance(func.expression, (exp.Func, exp.Anonymous)):
                          is_scalar = True
                
                if is_scalar:
                    violations.append(f"BP009: Scalar function '{func.sql()}' in SELECT. Consider alternatives like CROSS APPLY for better performance.")
        return violations
    
    def _check_large_in_lists(self, expression: exp.Expression) -> List[str]:
        """Avoid large IN lists - use temp tables or joins."""
        violations = []
        for in_expr in expression.find_all(exp.In):
            # Check if IN has a tuple with many values
            if isinstance(in_expr.args.get("expressions"), list):
                values = in_expr.args.get("expressions")
                if len(values) > 100:
                    violations.append(f"BP010: Large IN list detected ({len(values)} values). Consider using temp table or JOIN for better performance.")
        return violations
    
    def _check_union_usage(self, expression: exp.Expression) -> List[str]:
        """Recommend UNION ALL over UNION when duplicates don't matter."""
        violations = []
        for union in expression.find_all(exp.Union):
            if not union.args.get("distinct") is False:  # UNION without ALL
                violations.append("BP011: UNION detected. If duplicates are acceptable, use UNION ALL to avoid deduplication overhead.")
        return violations
    
    def _check_implicit_conversions(self, expression: exp.Expression) -> List[str]:
        """Warn about potential implicit conversions."""
        violations = []
        # This is heuristic - check for string literals compared to numeric columns
        for predicate in expression.find_all(exp.EQ):
            left = predicate.left
            right = predicate.right
            
            # Check if comparing column to literal of different type
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                # If literal is quoted but column might be numeric, warn
                if right.is_string:
                    violations.append(f"BP012: Potential implicit conversion detected. Ensure data types match to avoid index scan.")
        return violations
    
    def _check_set_nocount(self, expression: exp.Expression) -> List[str]:
        """Check for SET NOCOUNT ON in stored procedures."""
        violations = []
        sql_text = expression.sql().upper()
        if "CREATE PROCEDURE" in sql_text or "CREATE PROC" in sql_text:
            if "SET NOCOUNT ON" not in sql_text:
                violations.append("BP013: Stored procedure missing 'SET NOCOUNT ON'. This reduces network traffic.")
        return violations
    
    def _check_set_xact_abort(self, expression: exp.Expression) -> List[str]:
        """Check for SET XACT_ABORT ON in transactions."""
        violations = []
        sql_text = expression.sql().upper()
        if "BEGIN TRAN" in sql_text or "BEGIN TRANSACTION" in sql_text:
            if "SET XACT_ABORT ON" not in sql_text:
                violations.append("BP014: Transaction missing 'SET XACT_ABORT ON'. This ensures automatic rollback on errors.")
        return violations
    
    def _check_try_catch(self, expression: exp.Expression) -> List[str]:
        """Check for TRY...CATCH error handling."""
        violations = []
        sql_text = expression.sql().upper()
        if ("BEGIN TRAN" in sql_text or "CREATE PROCEDURE" in sql_text):
            if "TRY" not in sql_text or "CATCH" not in sql_text:
                violations.append("BP015: Consider using TRY...CATCH blocks for error handling in procedures and transactions.")
        return violations
    
    def _check_proper_joins(self, expression: exp.Expression) -> List[str]:
        """Prefer INNER JOIN over OUTER JOIN when possible."""
        violations = []
        for join in expression.find_all(exp.Join):
            kind = (join.kind or "").upper()
            side = (join.args.get("side") or "").upper()
            if kind in ["LEFT", "RIGHT", "FULL"] or side in ["LEFT", "RIGHT", "FULL"]:
                violations.append(f"BP016: {kind or side} OUTER JOIN detected. Prefer INNER JOIN when possible for better performance.")
        return violations
    
    def _check_temp_tables_vs_variables(self, expression: exp.Expression) -> List[str]:
        """Warn about table variables for large datasets."""
        violations = []
        sql_text = expression.sql().upper()
        if "DECLARE @" in sql_text and "TABLE" in sql_text:
            violations.append("BP017: Table variable detected. For large datasets, use temp tables (#temp) which support indexing.")
        return violations
    
    def _check_dynamic_sql(self, expression: exp.Expression) -> List[str]:
        """Warn about dynamic SQL usage."""
        violations = []
        for command in expression.find_all(exp.Command):
            sql = command.sql().upper()
            if "EXEC" in sql or "EXECUTE" in sql:
                violations.append("BP018: Dynamic SQL detected. Ensure inputs are parameterized to prevent SQL injection.")
        return violations
    
    def _check_transaction_usage(self, expression: exp.Expression) -> List[str]:
        """Check for proper transaction usage."""
        violations = []
        sql_text = expression.sql().upper()
        if "BEGIN TRAN" in sql_text:
            if "COMMIT" not in sql_text and "ROLLBACK" not in sql_text:
                violations.append("BP019: Transaction started but no COMMIT or ROLLBACK found. Ensure transactions are properly closed.")
        return violations
    
    def _check_subquery_optimization(self, expression: exp.Expression) -> List[str]:
        """Suggest optimizing subqueries."""
        violations = []
        # Count Select statements that are NOT the root expression
        # This catches subqueries throughout (in WHERE, FROM, etc)
        # We start count at -1 since finding all includes self if expression is Select
        all_selects = list(expression.find_all(exp.Select))
        
        if len(all_selects) > 3: # Main + 2 subs = 3. So > 3 implies >2 subs.
            violations.append(f"BP020: Multiple subqueries detected. Consider converting to JOINs or CTEs for better performance.")
        return violations
    
    def _check_top_usage(self, expression: exp.Expression) -> List[str]:
        """Check for TOP/LIMIT to limit result sets."""
        violations = []
        for select in expression.find_all(exp.Select):
            if not select.args.get("limit") and not select.find(exp.Limit):
                if not any(isinstance(expr, exp.Count) for expr in select.expressions):
                    violations.append("BP021: SELECT without TOP/LIMIT. Consider limiting result sets to reduce server load.")
        return violations
    
    def _check_stored_procedure_prefix(self, expression: exp.Expression) -> List[str]:
        """Check for proper stored procedure naming (usp_ not sp_)."""
        violations = []
        sql_text = expression.sql().upper()
        if "CREATE PROCEDURE SP_" in sql_text or "CREATE PROC SP_" in sql_text:
            violations.append("BP022: Stored procedure uses 'sp_' prefix. Use 'usp_' for user-defined procedures ('sp_' is for system procedures).")
        return violations
    
    def get_all_practices_documentation(self) -> Dict[str, Any]:
        """
        Load and return all DBA best practices from sql_best_practices.json.
        This is for documentation/reference purposes via dedicated API.
        """
        import json
        
        try:
            json_path = Path(__file__).parent.parent.parent / "config" / "sql_best_practices.json"
            if not json_path.exists():
                return {}
                
            with open(json_path, "r") as f:
                practices_data = json.load(f)
                
            return practices_data
                    
        except Exception:
            return {}

