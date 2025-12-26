"""
Resource Control Hint Injector for SQL Server.
Injects query hints (MAXDOP, MAX_GRANT_PERCENT) to control CPU and memory usage.
"""
import structlog
import re

logger = structlog.get_logger()


class ResourceControlInjector:
    """Injects query hints to control CPU and memory usage."""
    
    def should_inject(self, env: str, enable_resource_hints: bool) -> bool:
        """
        Determine if resource control hints should be injected.
        
        Args:
            env: Environment (Int, Stg, Prd)
            enable_resource_hints: Configuration flag
            
        Returns:
            True if hints should be injected
        """
        return enable_resource_hints
    
    def inject_resource_hints(
        self, 
        query: str, 
        env: str,
        maxdop: int = 1,
        max_grant_percent: int = 10
    ) -> str:
        """
        Inject resource control hints (MAXDOP, MAX_GRANT_PERCENT) into query.
        
        Args:
            query: Original SQL query
            env: Environment (Int, Stg, Prd)
            maxdop: Maximum degree of parallelism
            max_grant_percent: Maximum memory grant percentage
            
        Returns:
            Modified query with resource hints
        """
        try:
            # Check if query already has OPTION clause
            query_upper = query.upper().strip()
            
            # Remove trailing semicolon if present
            query_clean = query.rstrip(';').strip()
            
            # Check if OPTION clause already exists
            # Use regex to find OPTION clause (case-insensitive)
            option_pattern = r'\bOPTION\s*\([^)]*\)\s*;?\s*$'
            has_option = bool(re.search(option_pattern, query_upper, re.IGNORECASE))
            
            if has_option:
                # Query already has OPTION clause - we'll append to it
                # Extract existing OPTION content
                option_match = re.search(option_pattern, query_upper, re.IGNORECASE)
                if option_match:
                    # Remove existing OPTION clause and append new one with merged hints
                    query_without_option = re.sub(option_pattern, '', query_clean, flags=re.IGNORECASE).strip()
                    
                    # Extract existing hints from OPTION clause
                    existing_option = option_match.group(0)
                    existing_hints = []
                    
                    # Parse existing hints (simple extraction)
                    option_content = re.search(r'OPTION\s*\((.*?)\)', existing_option, re.IGNORECASE)
                    if option_content:
                        existing_hints_str = option_content.group(1)
                        # Split by comma and clean
                        existing_hints = [h.strip() for h in existing_hints_str.split(',')]
                    
                    # Build new hints list
                    hints = []
                    
                    # Check if MAXDOP already exists
                    has_maxdop = any('MAXDOP' in h.upper() for h in existing_hints)
                    if not has_maxdop:
                        hints.append(f"MAXDOP {maxdop}")
                    
                    # Check if MAX_GRANT_PERCENT already exists
                    has_max_grant = any('MAX_GRANT_PERCENT' in h.upper() for h in existing_hints)
                    if not has_max_grant:
                        hints.append(f"MAX_GRANT_PERCENT = {max_grant_percent}")
                    
                    if hints:
                        # Merge with existing hints
                        all_hints = existing_hints + hints
                        hints_str = ", ".join(all_hints)
                        modified_query = query_without_option + f" OPTION ({hints_str})"
                    else:
                        # No new hints to add, return original
                        modified_query = query
                else:
                    # Couldn't parse existing OPTION, append new OPTION clause
                    hints = [f"MAXDOP {maxdop}", f"MAX_GRANT_PERCENT = {max_grant_percent}"]
                    hints_str = ", ".join(hints)
                    modified_query = query_clean + f" OPTION ({hints_str})"
            else:
                # No OPTION clause exists, append new one
                hints = [f"MAXDOP {maxdop}", f"MAX_GRANT_PERCENT = {max_grant_percent}"]
                hints_str = ", ".join(hints)
                modified_query = query_clean + f" OPTION ({hints_str})"
            
            logger.info(
                "resource_hints_injected",
                env=env,
                maxdop=maxdop,
                max_grant_percent=max_grant_percent,
                original_length=len(query),
                modified_length=len(modified_query)
            )
            
            return modified_query
            
        except Exception as e:
            logger.error("resource_hint_injection_failed", error=str(e), query_preview=query[:100])
            # Fail-safe: return original query
            return query


class ResourceControlInjectionError(Exception):
    """Raised when resource control injection fails critically."""
    pass

