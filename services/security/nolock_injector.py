"""
NOLOCK Hint Injector for SQL Server.
Automatically adds WITH (NOLOCK) hints to prevent read blocking on production.
"""
import sqlglot
from sqlglot import exp
import structlog

logger = structlog.get_logger()


class NolockInjector:
    """Injects NOLOCK hints into SQL queries for production."""
    
    def should_inject(self, env: str, enable_nolock_hint: bool) -> bool:
        """
        Determine if NOLOCK hints should be injected.
        
        Args:
            env: Environment (Int, Stg, Prd)
            enable_nolock_hint: Configuration flag
            
        Returns:
            True if hints should be injected
        """
        return enable_nolock_hint and env == "Prd"
    
    def inject_nolock_hints(self, query: str) -> str:
        """
        Add WITH (NOLOCK) hints to all tables in query.
        
        Args:
            query: Original SQL query
            
        Returns:
            Modified query with NOLOCK hints
        """
        try:
            # Parse SQL
            parsed = sqlglot.parse_one(query, dialect="tsql")
            
            # Find all table references
            for table in parsed.find_all(exp.Table):
                # Check if table already has hints
                existing_hints = table.args.get("hints")
                
                if existing_hints:
                    # Check if NOLOCK already present
                    has_nolock = False
                    for hint_node in existing_hints:
                        # hint_node is WithTableHint
                        for expr in hint_node.expressions:
                            if isinstance(expr, exp.Var) and expr.name.upper() == "NOLOCK":
                                has_nolock = True
                                break
                        if has_nolock:
                            break
                    
                    if has_nolock:
                        continue
                
                # Add NOLOCK hint
                nolock_var = exp.Var(this="NOLOCK")
                if existing_hints:
                    # Existing is a list of WithTableHint probably
                    # We just need to ensure one of them has NOLOCK or add a new WithTableHint?
                    # T-SQL usually allows multiple hints in one WITH clause e.g. WITH (NOLOCK, INDEX(1))
                    # But sqlglot might split them.
                    # Simplest is to append a new WithTableHint or add to existing?
                    # Let's try appending a new WithTableHint for safety if strict T-SQL allows multiple WITH? 
                    # Actually T-SQL: FROM Table T WITH (NOLOCK) WITH (INDEX(0)) is INVALID.
                    # Must be FROM Table T WITH (NOLOCK, INDEX(0)).
                    
                    # So we should find the existing WithTableHint and append to its expressions.
                    
                    target_hint_node = existing_hints[0] # Assuming at least one
                    target_hint_node.expressions.append(nolock_var)
                else:
                    nolock_hint = exp.WithTableHint(expressions=[nolock_var])
                    table.set("hints", [nolock_hint])
            
            # Generate modified SQL
            modified_query = parsed.sql(dialect="tsql")
            
            logger.info("nolock_hints_injected", original_length=len(query), modified_length=len(modified_query))
            return modified_query
            
        except Exception as e:
            logger.error("nolock_injection_failed", error=str(e), query_preview=query[:100])
            
            # Critical: In Production, if we can't inject NOLOCK, we must not execute the query
            # to avoid blocking issues. Fail hard.
            # However, since we don't have 'env' here in the method signature easily without passing it,
            # we should look at how it's called. 
            # Wait, best practice is to handle this logic where 'should_inject' is decided or pass env here?
            # The method signature is inject_nolock_hints(self, query).
            # I will modify the caller in execution_service to handle the error or 
            # allow this method to return the original query only for non-Prd if I could check env.
            # But simpler: caller executes this ONLY if env=Prd and should_inject is True.
            # So if this fails, we should technically raise.
            # But let's check safety. If parser fails, query might be complex.
            # Returning original query risks blocking. 
            # I will raise a specific exception that execution_service catches.
            raise NolockInjectionError(f"Failed to inject NOLOCK hints: {e}")

class NolockInjectionError(Exception):
    """Raised when NOLOCK injection fails."""
    pass
    

