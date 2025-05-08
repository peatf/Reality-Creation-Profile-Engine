from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, Optional
from datetime import date, time
import pytz # For timezone validation
from src.location.geocoding import get_coordinates, GeocodingError # Added for automatic geocoding
import logging # Added for logging

logger = logging.getLogger(__name__) # Added for logging

class BirthData(BaseModel):
    """
    Model for user's birth information.
    Latitude and Longitude can be auto-populated via geocoding if not provided.
    """
    birth_date: date = Field(..., description="User's date of birth (YYYY-MM-DD)")
    birth_time: time = Field(..., description="User's time of birth (HH:MM:SS)")
    city_of_birth: str = Field(..., description="City where the user was born")
    country_of_birth: str = Field(..., description="Country where the user was born (for disambiguation)")
    latitude: Optional[float] = Field(default=None, description="Latitude of the birth location (auto-populated if not provided)")
    longitude: Optional[float] = Field(default=None, description="Longitude of the birth location (auto-populated if not provided)")
    timezone: str = Field(..., description="IANA timezone string for the birth location (e.g., 'America/New_York')")

    @model_validator(mode='after')
    def populate_coordinates_and_validate_timezone(self) -> 'BirthData':
        """
        Populates latitude and longitude using geocoding if not provided,
        and validates the timezone.
        Operates on the model instance after individual field validation.
        """
        # Access fields via self
        city = self.city_of_birth
        country = self.country_of_birth
        lat = self.latitude
        lng = self.longitude
        tz_str = self.timezone

        # Populate coordinates if not provided
        if (lat is None or lng is None) and city: # Ensure city is present for geocoding
            location_query = f"{city}, {country}" if country else city
            try:
                logger.info(f"Attempting to geocode {location_query} for BirthData")
                geo_result = get_coordinates(city=city, country=country) # Pass country if available
                self.latitude = geo_result.latitude
                self.longitude = geo_result.longitude
                logger.info(f"Successfully geocoded {location_query}: lat={self.latitude}, lng={self.longitude}")
            except GeocodingError as e:
                logger.warning(f"Geocoding failed for {location_query}: {e}. Latitude/Longitude will remain as initially set (None or provided).")
                # Depending on requirements, could raise ValueError here.
                # For now, allowing None if geocoding fails.
            except Exception as e:
                logger.error(f"Unexpected error during geocoding for {location_query}: {e}")
                # Depending on requirements, could raise ValueError here.
        elif lat is None or lng is None: # City was not provided for geocoding
             logger.warning("City not provided, cannot geocode for latitude/longitude. Values will remain as initially set.")


        # Validate timezone
        if tz_str:
            try:
                pytz.timezone(tz_str)
            except pytz.exceptions.UnknownTimeZoneError:
                raise ValueError(f"Invalid timezone string: '{tz_str}'. Must be a valid IANA timezone.")
        # If tz_str is None/empty, Pydantic's Field(..., description=...) should have caught it if it's required.
        
        return self

    # @validator('timezone') # This validator is now part of the root_validator
    # def validate_timezone(cls, tz_str):
        """Validate that the timezone string is a valid IANA timezone."""
        try:
            pytz.timezone(tz_str)
        except pytz.exceptions.UnknownTimeZoneError:
            raise ValueError(f"Invalid timezone string: '{tz_str}'. Must be a valid IANA timezone.")
        return tz_str

class AssessmentResponses(BaseModel):
    """
    Model for user's assessment responses.
    Keys are question IDs, values are the selected option values.
    """
    typology: Dict[str, str] = Field(..., description="Responses for Part 1: Typology Assessment")
    mastery: Dict[str, str] = Field(..., description="Responses for Part 2: Mastery Assessment")

    @field_validator('typology')
    @classmethod
    def validate_typology_responses(cls, v: Dict[str, str]) -> Dict[str, str]:
        """Basic validation for typology responses."""
        allowed_values = {'left', 'balanced', 'right'}
        for q_id, value in v.items():
            if value not in allowed_values:
                raise ValueError(f"Invalid typology response value '{value}' for question '{q_id}'. Must be 'left', 'balanced', or 'right'.")
        # Add more specific checks if needed (e.g., ensure all required questions are answered)
        return v

    # Add validator for mastery responses if specific value checks are needed

class ProfileCreateRequest(BaseModel):
    """
    Model for the request body of the /profile/create endpoint.
    """
    birth_data: BirthData = Field(..., description="User's birth information")
    assessment_responses: AssessmentResponses = Field(..., description="User's assessment responses")