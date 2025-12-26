"""
Execution Plan Analyzer for SQL Server.
Parses execution plan XML to detect performance issues and best practice violations.
"""
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional

class ExecutionPlanAnalyzer:
    """Analyzes SQL Server execution plans for performance issues."""
    
    def __init__(self):
        self.namespace = {'p': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
    
    def analyze_plan(self, plan_xml: str) -> List[str]:
        """
        Analyze execution plan XML and return list of violations.
        
        Args:
            plan_xml: SQL Server execution plan XML string
            
        Returns:
            List of violation messages with BP codes
        """
        if not plan_xml or not plan_xml.strip():
            return []
        
        violations = []
        
        try:
            root = ET.fromstring(plan_xml)
            
            # BP023: Missing Indexes
            violations.extend(self._check_missing_indexes(root))
            
            # BP024: Table Scans
            violations.extend(self._check_table_scans(root))
            
            # BP025: Index Scans vs Seeks
            violations.extend(self._check_index_scans(root))
            
            # BP026: Implicit Conversions
            violations.extend(self._check_implicit_conversions(root))
            
            # BP027: Parallelism Issues
            violations.extend(self._check_parallelism(root))
            
            # BP028: Expensive Sort Operations
            violations.extend(self._check_expensive_sorts(root))
            
            # BP029: Hash Operations
            violations.extend(self._check_hash_operations(root))
            
            # BP030: Key Lookups
            violations.extend(self._check_key_lookups(root))
            
            # BP031: Cardinality Estimation Issues
            violations.extend(self._check_cardinality_estimation(root))
            
        except ET.ParseError:
            # Invalid XML, skip analysis
            pass
        except Exception:
            # Other errors, skip analysis
            pass
        
        return list(set(violations))
    
    def _check_missing_indexes(self, root: ET.Element) -> List[str]:
        """Check for missing index recommendations in plan."""
        violations = []
        missing_indexes = root.findall('.//p:MissingIndexes/p:MissingIndexGroup', self.namespace)
        
        if missing_indexes:
            for idx_group in missing_indexes:
                impact = idx_group.get('Impact', '0')
                violations.append(f"BP023: Missing index detected (Impact: {impact}%). Consider creating recommended indexes.")
        
        return violations
    
    def _check_table_scans(self, root: ET.Element) -> List[str]:
        """Check for table scan operations."""
        violations = []
        table_scans = root.findall('.//p:RelOp[@PhysicalOp="Table Scan"]', self.namespace)
        
        for scan in table_scans:
            table_name = self._get_table_name(scan)
            violations.append(f"BP024: Table scan detected on '{table_name}'. This reads entire table. Add appropriate indexes.")
        
        return violations
    
    def _check_index_scans(self, root: ET.Element) -> List[str]:
        """Check for index scan operations (prefer seeks)."""
        violations = []
        index_scans = root.findall('.//p:RelOp[@PhysicalOp="Index Scan"]', self.namespace)
        
        for scan in index_scans:
            table_name = self._get_table_name(scan)
            violations.append(f"BP025: Index scan detected on '{table_name}'. Index seeks are more efficient. Review WHERE clause and indexes.")
        
        return violations
    
    def _check_implicit_conversions(self, root: ET.Element) -> List[str]:
        """Check for implicit conversion warnings in plan."""
        violations = []
        
        # Look for CONVERT_IMPLICIT in plan
        converts = root.findall('.//p:ScalarOperator[@ScalarString]', self.namespace)
        for conv in converts:
            scalar_str = conv.get('ScalarString', '')
            if 'CONVERT_IMPLICIT' in scalar_str:
                violations.append("BP026: Implicit conversion detected in execution plan. This prevents index usage. Ensure data types match.")
        
        return violations
    
    def _check_parallelism(self, root: ET.Element) -> List[str]:
        """Check for parallelism operators."""
        violations = []
        parallelism_ops = root.findall('.//p:RelOp[@PhysicalOp="Parallelism"]', self.namespace)
        
        if len(parallelism_ops) > 3:
            violations.append(f"BP027: Excessive parallelism detected ({len(parallelism_ops)} operators). May indicate inefficient query or MAXDOP settings.")
        
        return violations
    
    def _check_expensive_sorts(self, root: ET.Element) -> List[str]:
        """Check for expensive sort operations."""
        violations = []
        sorts = root.findall('.//p:RelOp[@PhysicalOp="Sort"]', self.namespace)
        
        for sort in sorts:
            # Check estimated cost
            cost = float(sort.get('EstimatedTotalSubtreeCost', '0'))
            if cost > 1.0:
                violations.append(f"BP028: Expensive sort operation detected (Cost: {cost:.2f}). Consider adding index to avoid sort.")
        
        return violations
    
    def _check_hash_operations(self, root: ET.Element) -> List[str]:
        """Check for hash match operations (joins, aggregates)."""
        violations = []
        hash_ops = root.findall('.//p:RelOp[contains(@PhysicalOp, "Hash")]', self.namespace)
        
        for hash_op in hash_ops:
            op_type = hash_op.get('PhysicalOp', '')
            if 'Hash Match' in op_type:
                violations.append(f"BP029: Hash match operation detected ({op_type}). Consider adding indexes to enable merge or nested loop joins.")
        
        return violations
    
    def _check_key_lookups(self, root: ET.Element) -> List[str]:
        """Check for key lookup operations (RID/Key lookups)."""
        violations = []
        key_lookups = root.findall('.//p:RelOp[@PhysicalOp="Key Lookup"]', self.namespace)
        rid_lookups = root.findall('.//p:RelOp[@PhysicalOp="RID Lookup"]', self.namespace)
        
        total_lookups = len(key_lookups) + len(rid_lookups)
        if total_lookups > 0:
            violations.append(f"BP030: Key/RID lookups detected ({total_lookups}). Consider creating covering index to include all required columns.")
        
        return violations
    
    def _check_cardinality_estimation(self, root: ET.Element) -> List[str]:
        """Check for cardinality estimation issues."""
        violations = []
        
        # Look for large discrepancies between estimated and actual rows
        rel_ops = root.findall('.//p:RelOp[@EstimateRows][@ActualRows]', self.namespace)
        
        for op in rel_ops:
            try:
                estimated = float(op.get('EstimateRows', '0'))
                actual = float(op.get('ActualRows', '0'))
                
                if estimated > 0 and actual > 0:
                    ratio = max(estimated, actual) / min(estimated, actual)
                    if ratio > 10:  # 10x difference
                        violations.append(f"BP031: Cardinality estimation issue detected (Est: {estimated:.0f}, Actual: {actual:.0f}). Update statistics.")
            except (ValueError, ZeroDivisionError):
                pass
        
        return violations
    
    def _get_table_name(self, rel_op: ET.Element) -> str:
        """Extract table name from RelOp element."""
        # Try to find table name in various locations
        index_scan = rel_op.find('.//p:IndexScan', self.namespace)
        if index_scan is not None:
            obj = index_scan.find('.//p:Object', self.namespace)
            if obj is not None:
                schema = obj.get('Schema', '')
                table = obj.get('Table', '')
                return f"{schema}.{table}" if schema else table
        
        return "Unknown"
