"""
Token-Optimized Schema Service.
"""
from typing import Dict, Any, List, Optional
from services.infrastructure.db_connection_service import DbConnectionService
from config.configuration import get_config

class SchemaService:
    def __init__(self):
        self.db = DbConnectionService()
        self.config = get_config()

    def get_summary(self, env: Optional[str] = None, search_term: Optional[str] = None) -> Dict[str, Any]:
        """
        Return a concise schema summary (tables, columns, types) optimized for LLM context.
        """
        # We query sys tables for speed and detail
        sql = """
        SELECT 
            s.name as schema_name,
            t.name as table_name,
            c.name as column_name,
            ty.name as type_name,
            c.max_length
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.columns c ON t.object_id = c.object_id
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name, c.column_id
        """
        
        # If search provided, filter in python or SQL? SQL is better but we built a static query.
        # Let's filter in python for flexibility or append WHERE if needed.
        if search_term:
            # Safe parameterization needed if we inject. 
            # For simplicity, let's fetch all (metadata is usually small enough < 1MB) 
            # and filter, unless it's huge.
            pass

        try:
            results = self.db.execute_query(sql, env=env, fetch_method=lambda c, _: c.fetchall())
            
            schema_map = {}
            for row in results or []:
                schema_val, table_val, col_val, type_val, len_val = row
                
                full_table = f"{schema_val}.{table_val}"
                if search_term and search_term.lower() not in full_table.lower():
                    continue

                if full_table not in schema_map:
                    schema_map[full_table] = []
                
                # Concise format: "col_name (type)"
                schema_map[full_table].append(f"{col_val} ({type_val})")

            # Format as compact string list
            summary = []
            for table, cols in schema_map.items():
                if len(summary) > 50 and search_term: 
                    # Cap results if searching
                   break
                
                col_str = ", ".join(cols)
                if len(col_str) > 500:
                    col_str = col_str[:500] + "..."
                summary.append(f"TABLE {table}: {col_str}")

            return {
                "success": True,
                "summary": summary,
                "count": len(summary)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
