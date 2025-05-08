from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Optional

class GateActivation(BaseModel):
    gate_number: int = Field(..., json_schema_extra={'example': 1}, description="Gate number (1-64)")
    line_number: int = Field(..., json_schema_extra={'example': 3}, description="Line number (1-6)")
    planet: str = Field(..., json_schema_extra={'example': "Sun"}, description="Activating planet")
    is_conscious: bool = Field(..., json_schema_extra={'example': True}, description="True if conscious (Personality), False if unconscious (Design)")
    # Additional details like color, tone, base can be added if needed

class Channel(BaseModel):
    channel_number: str = Field(..., json_schema_extra={'example': "1-8"}, description="Channel designation (e.g., '1-8')")
    name: str = Field(..., json_schema_extra={'example': "Inspiration"}, description="Name of the channel")
    # type: str = Field(..., json_schema_extra={'example': "Generated"}, description="Type of channel (Generated, Manifested, Projected)") # This might be too detailed or vary by system

class DefinedCenter(BaseModel):
    name: str = Field(..., json_schema_extra={'example': "Sacral"}, description="Name of the defined center")
    # is_motor: bool = Field(..., json_schema_extra={'example': True}, description="True if the center is a motor")

class HumanDesignChartResponse(BaseModel):
    request_data: Dict # Will be the CalculationRequest model, defined in main.py
    type: str = Field(..., json_schema_extra={'example': "Manifesting Generator"}, description="Human Design Type")
    strategy: str = Field(..., json_schema_extra={'example': "Responding"}, description="Human Design Strategy")
    authority: str = Field(..., json_schema_extra={'example': "Sacral"}, description="Inner Authority")
    profile: str = Field(..., json_schema_extra={'example': "5/1 Heretic Investigator"}, description="Profile (e.g., '1/3', '5/1')")
    definition: str = Field(..., json_schema_extra={'example': "Single Definition"}, description="Definition (Single, Split, Triple Split, Quadruple Split, No Definition)")
    incarnation_cross: Optional[str] = Field(None, json_schema_extra={'example': "Right Angle Cross of The Sphinx (1/2 | 7/13)"}, description="Incarnation Cross (optional, can be complex to calculate/name)")
    motivation: Optional[str] = Field(None, json_schema_extra={'example': "Fear – Communalist (Left / Strategic)"}, description="Motivation Variable (full term name)")
    motivation_orientation: Optional[str] = Field(None, json_schema_extra={'example': "Left"}, description="Motivation Orientation (Left/Right)")
    perspective: Optional[str] = Field(None, json_schema_extra={'example': "Survival (Left – Focused)"}, description="Perspective Variable (full term name)")
    perspective_orientation: Optional[str] = Field(None, json_schema_extra={'example': "Left"}, description="Perspective Orientation (Left/Right)")
    g_center_access_type: Optional[str] = Field(None, json_schema_extra={'example': "Generator Defined"}, description="G Center Access Type (e.g., Generator Defined, Projector Undefined)")
    g_center_access_definition: Optional[str] = Field(None, json_schema_extra={'example': "Generators with fixed access..."}, description="Definition of the G Center Access type")
    
    conscious_sun_gate: GateActivation = Field(..., description="Conscious Sun Gate activation")
    unconscious_sun_gate: GateActivation = Field(..., description="Unconscious Sun Gate activation")
    
    defined_centers: List[DefinedCenter] = Field(..., description="List of defined centers")
    open_centers: List[str] = Field(..., json_schema_extra={'example': ["Ajna", "Throat"]}, description="List of names of open centers")
    
    channels: List[Channel] = Field(..., description="List of defined channels")
    gates: List[GateActivation] = Field(..., description="List of all gate activations (conscious and unconscious)")
    
    environment: Optional[str] = Field(None, json_schema_extra={'example': "Caves - Selective"}, description="PHS Environment (optional)")
    # Add other relevant HD data points as needed:
    # e.g., digestion, motivation, view, etc.

    model_config = ConfigDict(from_attributes=True)