from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

class Finding(BaseModel):
    code: str
    severity: Literal["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    category: Literal["SECURITY", "PERFORMANCE", "RELIABILITY", "MAINTAINABILITY", "BEST_PRACTICE"]
    title: str
    description: str
    recommendation: str
    line_number: Optional[int] = None
    snippet: Optional[str] = None
    documentation_url: Optional[str] = None

class RiskBreakdown(BaseModel):
    ast_based: int
    execution_plan: int
    metadata: int

class ReviewSummary(BaseModel):
    status: Literal["APPROVED", "WARNING", "REJECTED"]
    risk_score: int
    verdict: str
    top_severity: str

class SafetyChecks(BaseModel):
    is_readonly: bool
    has_write_ops: bool
    has_ddl: bool

class PerformanceInsights(BaseModel):
    execution_plan_available: bool
    estimated_cost: Optional[float] = None
    warnings: List[Dict[str, Any]] = Field(default_factory=list)

class SchemaContext(BaseModel):
    valid_objects: List[str] = Field(default_factory=list)
    invalid_objects: List[str] = Field(default_factory=list)

class ReviewResult(BaseModel):
    summary: ReviewSummary
    safety_checks: SafetyChecks
    issues: List[Finding]
    performance_insights: PerformanceInsights
    schema_context: SchemaContext
    
    # For backward compatibility / ease of MCP serialization
    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()
