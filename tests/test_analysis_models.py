"""
Unit tests for comparison and validation of analysis models.
"""
import pytest
from services.analysis.models import (
    Finding, ReviewResult, ReviewSummary, SafetyChecks,
    PerformanceInsights, SchemaContext
)

def test_finding_model_creation():
    """Test creating a valid Finding object."""
    finding = Finding(
        code="BP001",
        severity="HIGH",
        category="PERFORMANCE",
        title="Test Finding",
        description="Description",
        recommendation="Fix it",
        snippet="SELECT *",
        line_number=10
    )
    
    assert finding.code == "BP001"
    assert finding.severity == "HIGH"
    assert finding.category == "PERFORMANCE"
    
    # Test JSON serialization
    json_data = finding.model_dump()
    assert json_data["code"] == "BP001"
    assert json_data["severity"] == "HIGH"

def test_review_result_structure():
    """Test the full structure of ReviewResult."""
    result = ReviewResult(
        summary=ReviewSummary(
            status="APPROVED",
            risk_score=10,
            verdict="Safe",
            top_severity="LOW"
        ),
        safety_checks=SafetyChecks(
            is_readonly=True,
            has_write_ops=False,
            has_ddl=False
        ),
        issues=[],
        schema_context=SchemaContext(valid_objects=["dbo.Users"]),
        performance_insights=PerformanceInsights(execution_plan_available=False)
    )
    
    assert result.summary.status == "APPROVED"
    assert result.safety_checks.is_readonly is True
    assert len(result.issues) == 0
    assert "dbo.Users" in result.schema_context.valid_objects

def test_invalid_severity():
    """Test that invalid severity raises validation error."""
    with pytest.raises(ValueError):
        Finding(
            code="BP001",
            severity="INVALID",
            category="PERFORMANCE",
            title="Title",
            description="Desc",
            recommendation="Fix it"
        )

def test_review_result_to_dict():
    """Test to_dict serialization."""
    data = {
        "summary": {"status": "APPROVED", "risk_score": 0, "verdict": "OK", "top_severity": "LOW"},
        "safety_checks": {"is_readonly": True, "has_write_ops": False, "has_ddl": False},
        "issues": [],
        "performance_insights": {"execution_plan_available": False},
        "schema_context": {}
    }
    result = ReviewResult(**data)
    assert isinstance(result.to_dict(), dict)
    assert result.to_dict()["summary"]["status"] == "APPROVED"
