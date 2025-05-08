from pydantic import BaseModel, Field

class CalculationRequest(BaseModel):
    birth_date: str = Field(..., json_schema_extra={'example': "1990-01-15"}, description="Birth date in YYYY-MM-DD format")
    birth_time: str = Field(..., json_schema_extra={'example': "14:30:00"}, description="Birth time in HH:MM:SS format (UTC as per prompt)")
    latitude: float = Field(..., json_schema_extra={'example': 34.0522}, description="Latitude in decimal degrees", ge=-90.0, le=90.0)
    longitude: float = Field(..., json_schema_extra={'example': -118.2437}, description="Longitude in decimal degrees", ge=-180.0, le=180.0)