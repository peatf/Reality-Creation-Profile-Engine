from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Optional

class PlanetaryPosition(BaseModel):
    name: str = Field(..., json_schema_extra={'example': "Sun"}, description="Name of the celestial body")
    longitude: float = Field(..., json_schema_extra={'example': 273.12345}, description="Ecliptic longitude in degrees")
    latitude: float = Field(..., json_schema_extra={'example': 0.00123}, description="Ecliptic latitude in degrees (for bodies not on the ecliptic)")
    speed: float = Field(..., json_schema_extra={'example': 0.985}, description="Apparent speed in degrees per day")
    sign: str = Field(..., json_schema_extra={'example': "Capricorn"}, description="Zodiac sign")
    sign_longitude: float = Field(..., json_schema_extra={'example': 3.12345}, description="Longitude within the sign (0-30 degrees)")
    house: Optional[int] = Field(None, json_schema_extra={'example': 10}, description="House placement (if calculated)")
    retrograde: bool = Field(False, json_schema_extra={'example': False}, description="True if the planet is retrograde")

class HouseCusp(BaseModel):
    house_number: int = Field(..., json_schema_extra={'example': 1}, description="House number (1-12)")
    longitude: float = Field(..., json_schema_extra={'example': 123.456}, description="Ecliptic longitude of the cusp in degrees")
    sign: str = Field(..., json_schema_extra={'example': "Leo"}, description="Zodiac sign of the cusp")
    sign_longitude: float = Field(..., json_schema_extra={'example': 3.456}, description="Longitude within the sign (0-30 degrees)")

class Aspect(BaseModel):
    planet1: str = Field(..., json_schema_extra={'example': "Sun"}, description="First planet in the aspect")
    planet2: str = Field(..., json_schema_extra={'example': "Moon"}, description="Second planet in the aspect")
    aspect_type: str = Field(..., json_schema_extra={'example': "Trine"}, description="Type of aspect (e.g., Conjunction, Sextile, Square, Trine, Opposition)")
    orb: float = Field(..., json_schema_extra={'example': 1.25}, description="Orb of the aspect in degrees")
    is_applying: bool = Field(True, json_schema_extra={'example': True}, description="True if the aspect is applying, False if separating")

class AstrologyChartResponse(BaseModel):
    request_data: Dict # Will be the CalculationRequest model, defined in main.py
    planetary_positions: List[PlanetaryPosition] = Field(..., description="List of planetary positions")
    house_cusps: List[HouseCusp] = Field(..., description="List of house cusps (e.g., Placidus)")
    aspects: List[Aspect] = Field(..., description="List of major aspects between planets")
    north_node: Optional[PlanetaryPosition] = Field(None, description="Position of the True North Node (optional)")
    # Add other relevant astrological data points as needed
    # e.g., elements_summary: Dict[str, int], modes_summary: Dict[str, int]

    model_config = ConfigDict(from_attributes=True)