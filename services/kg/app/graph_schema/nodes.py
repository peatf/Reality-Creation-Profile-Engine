from pydantic import BaseModel, Field, Json
from typing import Optional, Dict, Any
from datetime import datetime

class NodeBase(BaseModel):
    # Common fields for all nodes, if any, could go here
    # For Neo4j, element_id is often used internally by the driver for a node's identity
    # but we'll rely on specific unique properties like user_id for Person.
    pass

class PersonNode(NodeBase):
    user_id: str = Field(..., description="Unique identifier for the person, often from an auth system.")
    profile_id: Optional[str] = Field(None, description="Associated profile ID, if any.")
    name: Optional[str] = Field(None, description="Name of the person.")
    birth_datetime_utc: Optional[datetime] = Field(None, description="Birth datetime in UTC.")
    birth_latitude: Optional[float] = Field(None, description="Birth latitude.")
    birth_longitude: Optional[float] = Field(None, description="Birth longitude.")
    # Neo4j specific:
    # element_id: Optional[str] = Field(None, description="Neo4j internal element ID, for reference after creation.")

    class Config:
        orm_mode = True # For compatibility if used with ORMs, though not directly here
        extra = 'allow' # Allow extra fields that might come from DB but not strictly in model

class AstroFeatureNode(NodeBase):
    feature_type: str = Field(..., description="Type of astrological feature (e.g., 'planet_in_sign', 'aspect').")
    name: str = Field(..., description="Name of the feature (e.g., 'Sun in Aries', 'Sun trine Moon'). This should be unique per feature_type.")
    details: Optional[Json[Dict[str, Any]]] = Field(None, description="JSON blob for other specific attributes of the feature.")
    # element_id: Optional[str] = Field(None, description="Neo4j internal element ID.")

    class Config:
        orm_mode = True
        extra = 'allow'


class HDFeatureNode(NodeBase):
    feature_type: str = Field(..., description="Type of Human Design feature (e.g., 'type', 'center', 'gate').")
    name: str = Field(..., description="Name of the feature (e.g., 'Manifestor', 'Defined Ajna', 'Gate 1'). This should be unique per feature_type.")
    details: Optional[Json[Dict[str, Any]]] = Field(None, description="JSON blob for other specific attributes of the feature.")
    # element_id: Optional[str] = Field(None, description="Neo4j internal element ID.")

    class Config:
        orm_mode = True
        extra = 'allow'

class TypologyResultNode(NodeBase):
    typology_name: str = Field(..., description="Name of the typology system (e.g., 'Myers-Briggs', 'Enneagram').")
    assessment_id: str = Field(..., description="Identifier for the specific assessment instance.")
    profile_id: Optional[str] = Field(None, description="Associated profile ID from the PersonNode.") # Made Optional
    # user_id: str # This will be linked via relationship to PersonNode
    score: Optional[str] = Field(None, description="Score or result (e.g., 'INTJ', 'Type 5').") # Using str for flexibility
    confidence: Optional[float] = Field(None, description="Confidence level of the result, if applicable.")
    details: Optional[Json[Dict[str, Any]]] = Field(None, description="JSON blob for full results or nuances.")
    # element_id: Optional[str] = Field(None, description="Neo4j internal element ID.")

    class Config:
        orm_mode = True
        extra = 'allow'

# Example of how you might use these:
if __name__ == "__main__":
    person_data = {
        "user_id": "user123",
        "profile_id": "profABC",
        "name": "Jane Doe",
        "birth_datetime_utc": datetime.now(tz=datetime.timezone.utc),
        "birth_latitude": 34.05,
        "birth_longitude": -118.25
    }
    person = PersonNode(**person_data)
    print("Person Node:", person.model_dump_json(indent=2))

    astro_feature_data = {
        "feature_type": "planet_in_sign",
        "name": "Mars in Scorpio",
        "details": {"sign_degree": 15.5, "house": "8th"}
    }
    astro_feature = AstroFeatureNode(**astro_feature_data)
    print("\nAstroFeature Node:", astro_feature.model_dump_json(indent=2))

    hd_feature_data = {
        "feature_type": "type",
        "name": "Generator",
        "details": {"strategy": "To Respond", "authority": "Sacral"}
    }
    hd_feature = HDFeatureNode(**hd_feature_data)
    print("\nHDFeature Node:", hd_feature.model_dump_json(indent=2))

    typology_result_data = {
        "typology_name": "CustomArchetype",
        "assessment_id": "assessXYZ789",
        "score": "The Explorer",
        "confidence": 0.85,
        "details": {"dominant_traits": ["Curious", "Adventurous"], "shadow_traits": ["Restless"]}
    }
    typology_result = TypologyResultNode(**typology_result_data)
    print("\nTypologyResult Node:", typology_result.model_dump_json(indent=2))