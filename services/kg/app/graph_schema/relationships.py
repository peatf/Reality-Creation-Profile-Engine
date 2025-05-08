from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class RelationshipBase(BaseModel):
    # Common fields for all relationships, if any
    # element_id: Optional[str] = Field(None, description="Neo4j internal element ID for the relationship.")
    pass

class HasFeatureProperties(RelationshipBase):
    source_calculation_id: Optional[str] = Field(None, description="ID of the calculation run that generated this feature link.")
    # Other properties like 'timestamp' could be added
    class Config:
        extra = 'allow'

class InfluencesProperties(RelationshipBase):
    strength: Optional[float] = Field(None, description="Strength of the influence (e.g., 0.0 to 1.0).")
    description: Optional[str] = Field(None, description="Qualitative description of the influence.")
    context: Optional[str] = Field(None, description="Specific context under which this influence applies.")
    class Config:
        extra = 'allow'

class ConflictsWithProperties(RelationshipBase):
    severity: Optional[str] = Field(None, description="Severity of the conflict (e.g., 'low', 'medium', 'high').")
    description: Optional[str] = Field(None, description="Description of the conflict.")
    resolution_suggestion: Optional[str] = Field(None, description="Suggestion for resolving or understanding the conflict.")
    class Config:
        extra = 'allow'

class HasTypologyResultProperties(RelationshipBase):
    assessment_date: Optional[str] = Field(None, description="Date when the typology assessment was taken or result generated.")
    # No specific properties mentioned beyond what's on the TypologyResultNode itself,
    # but keeping it distinct for semantic clarity and future expansion.
    class Config:
        extra = 'allow'


# Example usage:
if __name__ == "__main__":
    has_feature_rel = HasFeatureProperties(source_calculation_id="calc_run_001")
    print("HAS_FEATURE Properties:", has_feature_rel.model_dump_json(indent=2))

    influences_rel = InfluencesProperties(
        strength=0.75,
        description="Strongly supportive influence towards creative expression.",
        context="When individual is in a flow state."
    )
    print("\nINFLUENCES Properties:", influences_rel.model_dump_json(indent=2))

    conflicts_rel = ConflictsWithProperties(
        severity="medium",
        description="Tendency for impulsiveness (Mars feature) conflicts with need for stability (Saturn feature).",
        resolution_suggestion="Practice mindfulness before taking action."
    )
    print("\nCONFLICTS_WITH Properties:", conflicts_rel.model_dump_json(indent=2))

    has_typology_rel = HasTypologyResultProperties(assessment_date="2023-10-26")
    print("\nHAS_TYPOLOGY_RESULT Properties:", has_typology_rel.model_dump_json(indent=2))