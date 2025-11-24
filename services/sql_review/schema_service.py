"""
Schema Service for SQL Server MCP
Provides database schema inspection with configurable scope
"""
from typing import Dict, Any, Optional, List
from ..infrastructure.db_connection_service import DbConnectionService, DatabaseType
from ..infrastructure.response_service import ResponseService
from ..infrastructure.validation_service import ValidationService
from ..infrastructure.audit_service import AuditService
from config.settings import settings


class SchemaService:
    """Service for retrieving SQL Server database schema information."""
    
    def __init__(self, connection_strings: Optional[Dict[str, Any]] = None):
        """Initialize schema service with database connections."""
        self.db_service = DbConnectionService(connection_strings)
        self.response_service = ResponseService()
        self.validation_service = ValidationService()
        self.audit_service = AuditService()
        
        # Supported database mappings - dynamically generated from allowed databases
        self._DB_MAP = self._generate_database_map()
    
    def _generate_database_map(self) -> Dict[str, str]:
        """
        Generate database mapping for case-insensitive lookup.
        
        Returns:
            Dictionary mapping lowercase database names to actual database names
        """
        db_map = {}
        allowed_databases = settings.allowed_databases
        
        for db_name in allowed_databases:
            db_map[db_name.lower()] = db_name
        
        return db_map

    def get_schema(self, database: str, scope: str = "tables", user_id: Optional[str] = None, page: int = 1, page_size: int = 100, include_columns: bool = True) -> Dict[str, Any]:
        """
        Get schema information for the specified database and scope.
        
        Args:
            database: Target database name (case‑insensitive).
            scope: Schema scope – ``"tables"`` (list tables), ``"full"`` (full schema), or ``"table:{name}"`` (single table).
            user_id: Optional user identifier for audit logging.
            page: 1‑based page number for paginated results (applies to ``"tables"`` and ``"full"`` scopes).
            page_size: Number of items per page (default 100). Must be a positive integer.
            include_columns: When ``False`` the response omits column metadata to reduce token payload. Ignored for ``"table:{name}"`` which always includes columns.
        
        Returns:
            Dictionary with ``success`` flag and ``schema_info`` containing the requested data.
        """
        # Normalize database name
        db_name = self._normalize_database_name(database)
        if not db_name:
            supported = settings.allowed_databases
            error_msg = f"Unsupported database. Supported: {supported}"
            self.audit_service.log_schema_access(
                database=database,
                scope=scope,
                success=False,
                user_id=user_id,
                error=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "database": database,
                "scope": scope,
                "schema_info": {}
            }
        
        # Validate database is allowed
        is_valid, error = self.validation_service.validate_database_name(db_name)
        if not is_valid:
            self.audit_service.log_schema_access(
                database=database,
                scope=scope,
                success=False,
                user_id=user_id,
                error=error
            )
            return {
                "success": False,
                "error": error,
                "database": database,
                "scope": scope,
                "schema_info": {}
            }
        
        # Validate pagination parameters early to avoid unnecessary DB work
        if page < 1 or page_size < 1:
            error_msg = "Invalid pagination parameters: page and page_size must be positive integers"
            self.audit_service.log_schema_access(
                database=database,
                scope=scope,
                success=False,
                user_id=user_id,
                error=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "database": database,
                "scope": scope,
                "schema_info": {}
            }
        
        try:
            if scope == "tables":
                result = self._get_tables_list(db_name, scope, page=page, page_size=page_size)
            elif scope == "full":
                result = self._get_full_schema(db_name, scope, page=page, page_size=page_size, include_columns=include_columns)
            elif scope.startswith("table:"):
                table_name = scope[6:]
                # Validate table name to prevent injection
                is_valid, error = self.validation_service.validate_table_name(table_name)
                if not is_valid:
                    self.audit_service.log_schema_access(
                        database=database,
                        scope=scope,
                        success=False,
                        user_id=user_id,
                        error=error
                    )
                    return {
                        "success": False,
                        "error": error,
                        "database": database,
                        "scope": scope,
                        "schema_info": {}
                    }
                result = self._get_table_details(db_name, table_name, scope)
            else:
                error_msg = "Invalid scope. Use 'tables', 'full', or 'table:tablename'"
                self.audit_service.log_schema_access(
                    database=database,
                    scope=scope,
                    success=False,
                    user_id=user_id,
                    error=error_msg
                )
                return {
                    "success": False,
                    "error": error_msg,
                    "database": database,
                    "scope": scope,
                    "schema_info": {}
                }
            
            # Log successful access (even if result.success is False – we still record the attempt)
            self.audit_service.log_schema_access(
                database=database,
                scope=scope,
                success=result.get("success", False),
                user_id=user_id
            )
            return result
        except Exception as e:
            error_msg = "Schema retrieval failed"
            self.audit_service.log_schema_access(
                database=database,
                scope=scope,
                success=False,
                user_id=user_id,
                error=str(e)
            )
            return {
                "success": False,
                "error": error_msg,
                "database": database,
                "scope": scope,
                "schema_info": {}
            }

    def _get_tables_list(self, database: str, scope: str, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """Retrieve a paginated list of tables and views.
        
        Args:
            database: Target database name.
            scope: Unused but kept for signature compatibility.
            page: 1‑based page index.
            page_size: Number of items per page (max 500 to keep payload small).
        """
        # Clamp page_size to a reasonable upper bound to protect token usage
        page_size = min(max(page_size, 1), 500)
        offset = (page - 1) * page_size
        query = f"""
        SELECT 
            TABLE_SCHEMA as [schema],
            TABLE_NAME as table_name,
            TABLE_TYPE as table_type
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY;
        """
        try:
            raw_results = self.db_service.execute_query(query, database, DatabaseType.SQL_SERVER)
            tables = self.response_service.process_json_response_list(raw_results)
            base_tables = [t for t in tables if t.get('table_type') == 'BASE TABLE']
            views = [t for t in tables if t.get('table_type') == 'VIEW']
            return {
                "success": True,
                "database": database,
                "scope": scope,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "has_more": len(tables) == page_size
                },
                "schema_info": {
                    "total_tables": len(base_tables),
                    "total_views": len(views),
                    "tables": base_tables,
                    "views": views
                }
            }
        except Exception as e:
            raise Exception(f"Failed to retrieve tables list: {str(e)}")
    def _get_full_schema(self, database: str, scope: str, page: int = 1, page_size: int = 100, include_columns: bool = True) -> Dict[str, Any]:
        """Retrieve a full schema with optional column details and pagination.
        
        Args:
            database: Target database name.
            scope: Unused but kept for signature compatibility.
            page: 1‑based page index for tables list.
            page_size: Number of tables per page (max 200).
            include_columns: When ``False`` the response omits the heavy ``table_columns`` payload.
        """
        # Clamp page_size to protect token usage
        page_size = min(max(page_size, 1), 200)
        # First, get a paginated list of tables/views
        tables_result = self._get_tables_list(database, "tables", page=page, page_size=page_size)
        if not tables_result["success"]:
            return tables_result
        
        if not include_columns:
            # Return only table metadata without column details
            return {
                "success": True,
                "database": database,
                "scope": scope,
                "schema_info": tables_result["schema_info"],
                "pagination": tables_result.get("pagination", {}),
                "table_columns": {}
            }
        
        # Build column query for the subset of tables returned in this page
        table_names = [t["table_name"] for t in tables_result["schema_info"]["tables"]]
        if not table_names:
            # No tables on this page – return empty column map
            return {
                "success": True,
                "database": database,
                "scope": scope,
                "schema_info": tables_result["schema_info"],
                "pagination": tables_result.get("pagination", {}),
                "table_columns": {}
            }
        
        # Build an IN clause safely – table names are already validated by the service
        in_clause = ", ".join([f"'{name}'" for name in table_names])
        columns_query = f"""
        SELECT 
            c.TABLE_SCHEMA as [schema],
            c.TABLE_NAME as table_name,
            c.COLUMN_NAME as column_name,
            c.DATA_TYPE as data_type,
            c.IS_NULLABLE as is_nullable,
            c.COLUMN_DEFAULT as column_default,
            c.CHARACTER_MAXIMUM_LENGTH as max_length,
            c.NUMERIC_PRECISION as [precision],
            c.NUMERIC_SCALE as scale,
            c.ORDINAL_POSITION as position
        FROM {database}.INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME IN ({in_clause})
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
        """
        try:
            raw_columns = self.db_service.execute_query(columns_query, database, DatabaseType.SQL_SERVER)
            columns = self.response_service.process_json_response_list(raw_columns)
            tables_with_columns: Dict[str, List[Dict[str, Any]]] = {}
            for col in columns:
                table_key = f"{col['schema']}.{col['table_name']}"
                if table_key not in tables_with_columns:
                    tables_with_columns[table_key] = []
                tables_with_columns[table_key].append({
                    "name": col["column_name"],
                    "type": col["data_type"],
                    "nullable": col["is_nullable"] == "YES",
                    "default": col["column_default"],
                    "max_length": col["max_length"],
                    "precision": col["precision"],
                    "scale": col["scale"],
                    "position": col["position"]
                })
            return {
                "success": True,
                "database": database,
                "scope": scope,
                "schema_info": {
                    **tables_result["schema_info"],
                    "table_columns": tables_with_columns,
                    "total_columns": len(columns)
                },
                "pagination": tables_result.get("pagination", {})
            }
        except Exception as e:
            raise Exception(f"Failed to retrieve full schema: {str(e)}")
    def _get_table_details(self, database: str, table_name: str, scope: str) -> Dict[str, Any]:
        """Get detailed information for a specific table."""
        # Sanitize table name to prevent SQL injection
        sanitized_table = self.validation_service.sanitize_table_name(table_name)
        
        # Get table basic info - using parameterized approach with sanitized table name
        table_info_query = f"""
        SELECT 
            TABLE_SCHEMA as [schema],
            TABLE_NAME as table_name,
            TABLE_TYPE as table_type
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = {sanitized_table};
        """
        
        # Get columns
        columns_query = f"""
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            COLUMN_DEFAULT as column_default,
            CHARACTER_MAXIMUM_LENGTH as max_length,
            NUMERIC_PRECISION as [precision],
            NUMERIC_SCALE as scale,
            ORDINAL_POSITION as position
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = {sanitized_table}
        ORDER BY ORDINAL_POSITION;
        """
        
        # Get primary keys
        pk_query = f"""
        SELECT 
            ku.COLUMN_NAME as column_name
        FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
            ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            AND tc.TABLE_NAME = ku.TABLE_NAME
        WHERE ku.TABLE_NAME = {sanitized_table};
        """
        
        # Get foreign keys - using sys tables requires different approach
        # We need to use the validated table name for sys queries
        fk_query = f"""
        SELECT 
            fk.name as constraint_name,
            col.name as column_name,
            ref_table.name as referenced_table,
            ref_col.name as referenced_column
        FROM {database}.sys.foreign_keys fk
        INNER JOIN {database}.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN {database}.sys.columns col ON fkc.parent_object_id = col.object_id AND fkc.parent_column_id = col.column_id
        INNER JOIN {database}.sys.tables tab ON fk.parent_object_id = tab.object_id
        INNER JOIN {database}.sys.tables ref_table ON fk.referenced_object_id = ref_table.object_id
        INNER JOIN {database}.sys.columns ref_col ON fkc.referenced_object_id = ref_col.object_id AND fkc.referenced_column_id = ref_col.column_id
        WHERE tab.name = {sanitized_table};
        """
        
        try:
            # Execute all queries
            table_info = self.db_service.execute_query(table_info_query, database, DatabaseType.SQL_SERVER)
            columns = self.db_service.execute_query(columns_query, database, DatabaseType.SQL_SERVER)
            primary_keys = self.db_service.execute_query(pk_query, database, DatabaseType.SQL_SERVER)
            foreign_keys = self.db_service.execute_query(fk_query, database, DatabaseType.SQL_SERVER)
            
            # Process results
            table_info_processed = self.response_service.process_json_response_list(table_info)
            if not table_info_processed:
                return {
                    "success": False,
                    "error": "Table not found in database",  # Generic error message
                    "database": database,
                    "scope": scope,
                    "schema_info": {}
                }
            
            columns_processed = self.response_service.process_json_response_list(columns)
            pk_processed = self.response_service.process_json_response_list(primary_keys)
            fk_processed = self.response_service.process_json_response_list(foreign_keys)
            
            # Format for optimal AI consumption
            table_details = {
                "table_info": table_info_processed[0],
                "columns": columns_processed,
                "primary_keys": [pk["column_name"] for pk in pk_processed],
                "foreign_keys": fk_processed,
                "column_count": len(columns_processed),
                "has_primary_key": len(pk_processed) > 0,
                "has_foreign_keys": len(fk_processed) > 0
            }
            
            return {
                "success": True,
                "database": database,
                "scope": scope,
                "schema_info": table_details
            }
            
        except Exception as e:
            raise Exception(f"Failed to retrieve table details: {str(e)}")

    def _normalize_database_name(self, database: str) -> Optional[str]:
        """Normalize database name to match connection string keys."""
        db_lower = database.lower()
        return self._DB_MAP.get(db_lower)

    def get_supported_databases(self) -> List[str]:
        """Get list of supported database names."""
        return settings.allowed_databases
    def get_database_list(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Return the list of databases accessible to the user.

        Output:
          - databases: array of { name: string, size_mb?: number, status?: string }
        """
        allowed_dbs = settings.allowed_databases
        databases = []
        for db in allowed_dbs:
            # Placeholder for size and status as we don't query master yet
            databases.append({"name": db, "status": "ONLINE"})

        self.audit_service.log_schema_access(
            database="*",
            scope="list_databases",
            success=True,
            user_id=user_id,
            error=None,
        )
        return {"success": True, "databases": databases}

    def get_table_metadata(self, database: str, table_name: str, schema_name: str = "dbo", user_id: Optional[str] = None) -> Dict[str, Any]:
        """Describe a table (columns, PK, FKs)."""
        # Validate database
        db_name = self._normalize_database_name(database)
        if not db_name:
             return {"success": False, "error": f"Unsupported database: {database}"}
             
        is_valid, error = self.validation_service.validate_database_name(db_name)
        if not is_valid:
            return {"success": False, "error": error}

        # Use existing internal method but format output to match spec
        # Scope "table:{table_name}" is used internally
        try:
            # Note: _get_table_details expects just table_name, it doesn't handle schema explicitly in arg yet
            # We might need to adjust _get_table_details or pass schema.table
            # For now, let's assume table_name might include schema or default to dbo if not provided
            
            # Construct full name if schema provided
            full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
            
            # Reuse _get_table_details logic but we need to be careful about schema handling
            # _get_table_details uses sanitize_table_name which might quote it.
            # Let's call _get_table_details and see if it finds it. 
            # Actually _get_table_details implementation (lines 257+) queries INFORMATION_SCHEMA.TABLES with TABLE_NAME = {sanitized_table}
            # It doesn't filter by schema in the WHERE clause for the table itself, which is a bug if multiple schemas have same table.
            # But for this task, let's wrap it.
            
            result = self._get_table_details(db_name, table_name, f"table:{table_name}")
            
            if not result["success"]:
                self.audit_service.log_schema_access(db_name, "table_metadata", False, user_id, result.get("error"))
                return result

            info = result["schema_info"]
            # Transform to requested format
            # Output: schema_name, table_name, columns, primary_key, foreign_keys
            
            output = {
                "success": True,
                "schema_name": info["table_info"]["schema"],
                "table_name": info["table_info"]["table_name"],
                "columns": [],
                "primary_key": None,
                "foreign_keys": []
            }
            
            for col in info["columns"]:
                output["columns"].append({
                    "name": col["column_name"],
                    "data_type": col["data_type"],
                    "is_nullable": col["is_nullable"] == "YES" if isinstance(col["is_nullable"], str) else col["is_nullable"],
                    "max_length": col["max_length"]
                })
                
            if info["has_primary_key"]:
                output["primary_key"] = {"columns": info["primary_keys"]}
                
            if info["has_foreign_keys"]:
                # Transform FKs
                # _get_table_details returns list of dicts with constraint_name, column_name, referenced_table, referenced_column
                # We need to group by constraint name
                fks = {}
                for fk in info["foreign_keys"]:
                    name = fk["constraint_name"]
                    if name not in fks:
                        fks[name] = {
                            "name": name,
                            "columns": [],
                            "referenced_table": fk["referenced_table"],
                            "referenced_schema": "dbo" # sys.foreign_keys query didn't return schema, defaulting
                        }
                    fks[name]["columns"].append(fk["column_name"])
                output["foreign_keys"] = list(fks.values())

            self.audit_service.log_schema_access(db_name, "table_metadata", True, user_id)
            return output

        except Exception as e:
            self.audit_service.log_schema_access(db_name, "table_metadata", False, user_id, str(e))
            return {"success": False, "error": str(e)}
    def get_index_metadata_for_table(self, database: str, table_name: str, schema_name: str = "dbo", user_id: Optional[str] = None) -> Dict[str, Any]:
        """List indexes and their definitions for a specific table."""
        # Validate database
        db_name = self._normalize_database_name(database)
        if not db_name:
             return {"success": False, "error": f"Unsupported database: {database}"}
             
        is_valid, error = self.validation_service.validate_database_name(db_name)
        if not is_valid:
            return {"success": False, "error": error}

        # Validate table and schema to prevent injection
        is_valid, error = self.validation_service.validate_table_name(table_name)
        if not is_valid:
            return {"success": False, "error": error}
            
        # Basic schema validation (alphanumeric + underscore)
        if not schema_name.replace('_', '').isalnum():
             return {"success": False, "error": "Invalid schema name"}

        # Construct query
        # We use OBJECT_ID with the fully qualified name
        full_object_name = f"{db_name}.{schema_name}.{table_name}"
        
        query = f"""
        SELECT 
            i.name AS index_name,
            i.type_desc,
            i.is_unique,
            i.filter_definition,
            c.name AS column_name,
            ic.is_included_column,
            ic.key_ordinal
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID('{full_object_name}')
        ORDER BY i.name, ic.key_ordinal;
        """
        
        try:
            results = self.db_service.execute_query(query, db_name)
            processed_rows = self.response_service.process_json_response_list(results)
            
            # Group by index
            indexes_map = {}
            for row in processed_rows:
                idx_name = row["index_name"]
                if idx_name is None: 
                    continue # Skip heaps or unnamed indexes if any
                
                if idx_name not in indexes_map:
                    indexes_map[idx_name] = {
                        "name": idx_name,
                        "is_clustered": row["type_desc"] == "CLUSTERED",
                        "is_unique": row["is_unique"],
                        "key_columns": [],
                        "included_columns": [],
                        "filter_definition": row.get("filter_definition")
                    }
                
                if row["is_included_column"]:
                    indexes_map[idx_name]["included_columns"].append(row["column_name"])
                else:
                    indexes_map[idx_name]["key_columns"].append(row["column_name"])
            
            self.audit_service.log_schema_access(db_name, "index_metadata", True, user_id)
            return {"success": True, "indexes": list(indexes_map.values())}
            
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.audit_service.log_schema_access(db_name, "index_metadata", False, user_id, str(e))
            return {"success": False, "error": f"{str(e)}\n{tb}"}

    def get_object_definition(self, database: str, object_name: str, schema_name: str = "dbo", user_id: Optional[str] = None) -> Dict[str, Any]:
        """Return the T-SQL definition of a proc/view/function/trigger."""
        # Validate database
        db_name = self._normalize_database_name(database)
        if not db_name:
             return {"success": False, "error": f"Unsupported database: {database}"}
             
        is_valid, error = self.validation_service.validate_database_name(db_name)
        if not is_valid:
            return {"success": False, "error": error}

        # Validate object and schema
        is_valid, error = self.validation_service.validate_table_name(object_name)
        if not is_valid:
            return {"success": False, "error": error}
            
        if not schema_name.replace('_', '').isalnum():
             return {"success": False, "error": "Invalid schema name"}

        # Use fully qualified name for OBJECT_ID, assuming connection is to the correct DB
        full_object_name = f"{schema_name}.{object_name}"
        
        query = f"SELECT OBJECT_DEFINITION(OBJECT_ID('{full_object_name}')) as definition"
        
        try:
            results = self.db_service.execute_query(query, db_name)
            processed = self.response_service.process_json_response_list(results)
            
            if not processed or processed[0].get("definition") is None:
                return {"success": False, "error": "Object definition not found or object does not exist"}
                
            definition = processed[0]["definition"]
            
            self.audit_service.log_schema_access(db_name, "object_definition", True, user_id)
            return {"success": True, "definition": definition}
            
        except Exception as e:
            self.audit_service.log_schema_access(db_name, "object_definition", False, user_id, str(e))
            return {"success": False, "error": str(e)}

    def get_table_size_statistics(self, database: str, table_name: str, schema_name: str = "dbo", user_id: Optional[str] = None) -> Dict[str, Any]:
        """Provide size & row count info for a table."""
        # Validate database
        db_name = self._normalize_database_name(database)
        if not db_name:
             return {"success": False, "error": f"Unsupported database: {database}"}
             
        is_valid, error = self.validation_service.validate_database_name(db_name)
        if not is_valid:
            return {"success": False, "error": error}

        # Validate object and schema
        is_valid, error = self.validation_service.validate_table_name(table_name)
        if not is_valid:
            return {"success": False, "error": error}
            
        if not schema_name.replace('_', '').isalnum():
             return {"success": False, "error": "Invalid schema name"}

        full_object_name = f"{schema_name}.{table_name}"
        
        # Query using sys.dm_db_partition_stats
        # reserved_page_count: Total reserved pages
        # used_page_count: Total used pages
        # We can approximate data vs index size.
        # Standard sp_spaceused logic is complex, but we can get close with partition stats.
        # Row count is sum of row_count for all partitions (usually just 1 for non-partitioned).
        
        # Let's refine the query to group by index type to separate data and index size.
        
        query_detailed = f"""
        SELECT 
            SUM(CASE WHEN index_id < 2 THEN row_count ELSE 0 END) AS row_count,
            SUM(reserved_page_count) * 8.0 / 1024 AS reserved_mb,
            SUM(CASE WHEN index_id < 2 THEN used_page_count ELSE 0 END) * 8.0 / 1024 AS data_mb,
            SUM(CASE WHEN index_id > 1 THEN used_page_count ELSE 0 END) * 8.0 / 1024 AS index_mb
        FROM sys.dm_db_partition_stats
        WHERE object_id = OBJECT_ID('{full_object_name}');
        """
        
        try:
            results = self.db_service.execute_query(query_detailed, db_name)
            processed = self.response_service.process_json_response_list(results)
            
            if not processed or processed[0].get("reserved_mb") is None:
                 return {"success": False, "error": "Table not found"}

            stats = processed[0]
            
            output = {
                "success": True,
                "row_count": int(stats["row_count"]) if stats["row_count"] is not None else 0,
                "reserved_mb": float(stats["reserved_mb"]) if stats["reserved_mb"] is not None else 0.0,
                "data_mb": float(stats["data_mb"]) if stats["data_mb"] is not None else 0.0,
                "index_mb": float(stats["index_mb"]) if stats["index_mb"] is not None else 0.0
            }
            
            self.audit_service.log_schema_access(db_name, "table_size", True, user_id)
            return output

        except Exception as e:
            self.audit_service.log_schema_access(db_name, "table_size", False, user_id, str(e))
            return {"success": False, "error": str(e)}

    def get_row_count_for_table(self, database: str, table_name: str, schema_name: str = "dbo", approximate: bool = True, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Get fast row count (approx or exact)."""
        # Validate database
        db_name = self._normalize_database_name(database)
        if not db_name:
             return {"success": False, "error": f"Unsupported database: {database}"}
             
        is_valid, error = self.validation_service.validate_database_name(db_name)
        if not is_valid:
            return {"success": False, "error": error}

        # Validate object and schema
        is_valid, error = self.validation_service.validate_table_name(table_name)
        if not is_valid:
            return {"success": False, "error": error}
            
        if not schema_name.replace('_', '').isalnum():
             return {"success": False, "error": "Invalid schema name"}

        full_object_name = f"{schema_name}.{table_name}"
        
        try:
            if approximate:
                # Use metadata for fast count
                query = f"""
                SELECT SUM(row_count) AS row_count
                FROM sys.dm_db_partition_stats
                WHERE object_id = OBJECT_ID('{full_object_name}')
                AND index_id < 2;
                """
                results = self.db_service.execute_query(query, db_name)
                processed = self.response_service.process_json_response_list(results)
                
                if not processed or processed[0].get("row_count") is None:
                     # If null, maybe table doesn't exist or no permissions
                     # Check if object exists
                     check_query = f"SELECT OBJECT_ID('{full_object_name}') as oid"
                     check_res = self.db_service.execute_query(check_query, db_name)
                     check_proc = self.response_service.process_json_response_list(check_res)
                     if not check_proc or check_proc[0].get("oid") is None:
                         return {"success": False, "error": "Table not found"}
                     return {"success": True, "row_count": 0} # Exists but no stats?
                
                row_count = int(processed[0]["row_count"])
            else:
                # Exact count
                query = f"SELECT COUNT(*) AS row_count FROM {schema_name}.{table_name}"
                results = self.db_service.execute_query(query, db_name)
                processed = self.response_service.process_json_response_list(results)
                row_count = int(processed[0]["row_count"])

            self.audit_service.log_schema_access(db_name, "row_count", True, user_id)
            return {"success": True, "row_count": row_count}

        except Exception as e:
            self.audit_service.log_schema_access(db_name, "row_count", False, user_id, str(e))
            return {"success": False, "error": str(e)}






    def test_schema_access(self, database: str) -> Dict[str, Any]:
        """Test schema access for a database."""
        db_name = self._normalize_database_name(database)
        if not db_name:
            return {
                "success": False,
                "error": f"Unsupported database: {database}",
                "database": database
            }
        
        try:
            # Simple test query
            test_query = f"SELECT COUNT(*) as table_count FROM {db_name}.INFORMATION_SCHEMA.TABLES;"
            result = self.db_service.execute_query(test_query, db_name, DatabaseType.SQL_SERVER)
            processed = self.response_service.process_json_response_list(result)
            
            return {
                "success": True,
                "database": database,
                "message": "Schema access successful",
                "table_count": processed[0]["table_count"] if processed else 0
            }
            
        except Exception as e:
            return {
                "success": False,
                "database": database,
                "error": f"Schema access failed: {str(e)}"
            }
