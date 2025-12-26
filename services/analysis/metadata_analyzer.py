"""
Metadata-Based Best Practices Checker.
Queries SQL Server system views to detect configuration and structural issues.
"""
from typing import List, Dict, Any
import pyodbc
from services.infrastructure.db_connection_service import DbConnectionService

class MetadataAnalyzer:
    """Analyzes database metadata for best practice violations."""
    
    def __init__(self):
        self.db_service = DbConnectionService()
    
    def analyze_metadata(self, env: str = None, database: str = None) -> List[str]:
        """
        Analyze database metadata and return list of violations.
        
        Args:
            env: Environment to check
            database: Specific database to analyze
            
        Returns:
            List of violation messages with BP codes
        """
        violations = []
        
        try:
            with self.db_service.get_connection(env=env, database=database) as conn:
                cursor = conn.cursor()
                
                # BP032: Statistics Freshness
                violations.extend(self._check_statistics_freshness(cursor))
                
                # BP033: Index Fragmentation
                violations.extend(self._check_index_fragmentation(cursor))
                
                # BP034: Missing Statistics
                violations.extend(self._check_missing_statistics(cursor))
                
                # BP035: Unused Indexes
                violations.extend(self._check_unused_indexes(cursor))
                
                # BP036: Duplicate Indexes
                violations.extend(self._check_duplicate_indexes(cursor))
                
                # BP037: Table Partitioning
                violations.extend(self._check_table_partitioning(cursor))
                
                # BP038: Columnstore Indexes
                violations.extend(self._check_columnstore_indexes(cursor))
                
                # BP039: Data Type Appropriateness
                violations.extend(self._check_data_types(cursor))
                
                # BP040: Heap Tables
                violations.extend(self._check_heap_tables(cursor))
                
                # BP041: Wide Tables
                violations.extend(self._check_wide_tables(cursor))
                
                # BP042: Foreign Key Indexes
                violations.extend(self._check_foreign_key_indexes(cursor))
                
        except Exception as e:
            # If metadata analysis fails, return empty (don't block main analysis)
            pass
        
        return list(set(violations))
    
    def _check_statistics_freshness(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for outdated statistics (BP032)."""
        violations = []
        query = """
        SELECT 
            OBJECT_SCHEMA_NAME(s.object_id) + '.' + OBJECT_NAME(s.object_id) AS table_name,
            s.name AS stats_name,
            DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) AS days_old
        FROM sys.stats s
        WHERE STATS_DATE(s.object_id, s.stats_id) < DATEADD(day, -7, GETDATE())
        AND OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP032: Statistics on '{row.table_name}' are {row.days_old} days old. Update statistics for better query plans.")
        except:
            pass
        
        return violations
    
    def _check_index_fragmentation(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for fragmented indexes (BP033)."""
        violations = []
        query = """
        SELECT 
            OBJECT_SCHEMA_NAME(ips.object_id) + '.' + OBJECT_NAME(ips.object_id) AS table_name,
            i.name AS index_name,
            ips.avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
        JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent > 30
        AND ips.page_count > 1000
        AND i.name IS NOT NULL
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP033: Index '{row.index_name}' on '{row.table_name}' is {row.avg_fragmentation_in_percent:.1f}% fragmented. Consider rebuilding.")
        except:
            pass
        
        return violations
    
    def _check_missing_statistics(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for tables without statistics (BP034)."""
        violations = []
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name
        FROM sys.tables t
        LEFT JOIN sys.stats s ON t.object_id = s.object_id
        WHERE s.stats_id IS NULL
        AND t.is_ms_shipped = 0
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP034: Table '{row.table_name}' has no statistics. Create statistics for better query optimization.")
        except:
            pass
        
        return violations
    
    def _check_unused_indexes(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for unused indexes (BP035)."""
        violations = []
        query = """
        SELECT 
            OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) AS table_name,
            i.name AS index_name
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats ius 
            ON i.object_id = ius.object_id AND i.index_id = ius.index_id
        WHERE i.type_desc != 'HEAP'
        AND i.is_primary_key = 0
        AND i.is_unique_constraint = 0
        AND ius.index_id IS NULL
        AND OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP035: Index '{row.index_name}' on '{row.table_name}' is never used. Consider dropping to reduce write overhead.")
        except:
            pass
        
        return violations
    
    def _check_duplicate_indexes(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for duplicate indexes (BP036)."""
        violations = []
        # This is complex - simplified check for indexes with same key columns
        query = """
        SELECT 
            OBJECT_SCHEMA_NAME(i1.object_id) + '.' + OBJECT_NAME(i1.object_id) AS table_name,
            i1.name AS index1,
            i2.name AS index2
        FROM sys.indexes i1
        JOIN sys.indexes i2 ON i1.object_id = i2.object_id 
            AND i1.index_id < i2.index_id
        WHERE EXISTS (
            SELECT ic1.column_id
            FROM sys.index_columns ic1
            WHERE ic1.object_id = i1.object_id AND ic1.index_id = i1.index_id
            INTERSECT
            SELECT ic2.column_id
            FROM sys.index_columns ic2
            WHERE ic2.object_id = i2.object_id AND ic2.index_id = i2.index_id
        )
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP036: Potential duplicate indexes '{row.index1}' and '{row.index2}' on '{row.table_name}'. Review and consolidate.")
        except:
            pass
        
        return violations
    
    def _check_table_partitioning(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for large tables that should be partitioned (BP037)."""
        violations = []
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
            SUM(p.rows) AS row_count
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0,1)
        AND t.is_ms_shipped = 0
        GROUP BY t.schema_id, t.name
        HAVING SUM(p.rows) > 10000000
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP037: Table '{row.table_name}' has {row.row_count:,} rows. Consider partitioning for better performance.")
        except:
            pass
        
        return violations
    
    def _check_columnstore_indexes(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for data warehouse tables without columnstore (BP038)."""
        violations = []
        # Heuristic: large tables without columnstore in DW scenarios
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
            SUM(p.rows) AS row_count
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0,1)
        AND t.is_ms_shipped = 0
        AND NOT EXISTS (
            SELECT 1 FROM sys.indexes i 
            WHERE i.object_id = t.object_id AND i.type IN (5,6)
        )
        GROUP BY t.schema_id, t.name
        HAVING SUM(p.rows) > 5000000
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP038: Large table '{row.table_name}' ({row.row_count:,} rows) lacks columnstore index. Consider for analytics workloads.")
        except:
            pass
        
        return violations
    
    def _check_data_types(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for oversized data types (BP039)."""
        violations = []
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
            c.name AS column_name,
            ty.name AS data_type,
            c.max_length
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE ty.name IN ('nvarchar', 'varchar', 'nchar', 'char')
        AND c.max_length = -1
        AND t.is_ms_shipped = 0
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP039: Column '{row.table_name}.{row.column_name}' uses MAX data type. Specify explicit size when possible.")
        except:
            pass
        
        return violations
    
    def _check_heap_tables(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for heap tables without clustered index (BP040)."""
        violations = []
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name
        FROM sys.tables t
        WHERE NOT EXISTS (
            SELECT 1 FROM sys.indexes i 
            WHERE i.object_id = t.object_id AND i.type = 1
        )
        AND t.is_ms_shipped = 0
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP040: Table '{row.table_name}' is a heap (no clustered index). Add clustered index for better performance.")
        except:
            pass
        
        return violations
    
    def _check_wide_tables(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for tables with excessive columns (BP041)."""
        violations = []
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
            COUNT(*) AS column_count
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        WHERE t.is_ms_shipped = 0
        GROUP BY t.schema_id, t.name
        HAVING COUNT(*) > 50
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP041: Table '{row.table_name}' has {row.column_count} columns. Consider normalizing or vertical partitioning.")
        except:
            pass
        
        return violations
    
    def _check_foreign_key_indexes(self, cursor: pyodbc.Cursor) -> List[str]:
        """Check for foreign keys without supporting indexes (BP042)."""
        violations = []
        query = """
        SELECT 
            OBJECT_SCHEMA_NAME(fk.parent_object_id) + '.' + OBJECT_NAME(fk.parent_object_id) AS table_name,
            fk.name AS fk_name,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        WHERE NOT EXISTS (
            SELECT 1 FROM sys.index_columns ic
            WHERE ic.object_id = fkc.parent_object_id
            AND ic.column_id = fkc.parent_column_id
            AND ic.index_column_id = 1
        )
        """
        
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                violations.append(f"BP042: Foreign key '{row.fk_name}' on '{row.table_name}.{row.column_name}' lacks supporting index. Add index for better join performance.")
        except:
            pass
        
        return violations
