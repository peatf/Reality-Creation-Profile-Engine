# src/auth/schemas.py
import uuid
from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Dict

@dataclass(frozen=True) # Immutable user state
class AuthenticatedUser:
    id: uuid.UUID
    tier: str # e.g., "free", "premium"

class ErrorDetail(BaseModel):
    """Standard error response detail."""
    code: str = Field(..., description="Application-specific error code.")
    message: str = Field(..., description="User-friendly error message.")

class ErrorResponse(BaseModel):
    """Standard error response model."""
    detail: ErrorDetail


# --- Tier definitions ---
# Define an order or mapping for tiers for comparison logic
# Lower numbers are lower tiers.
TIER_LEVELS: Dict[str, int] = {
    "free": 0,
    "basic": 10,
    "premium": 20,
    "enterprise": 30,
    "admin": 99 # Example admin tier, significantly higher
}

def get_tier_level(tier_name: str | None) -> int:
    """
    Returns the numerical level of a tier name.
    Returns -1 if the tier name is None or not found.
    """
    if tier_name is None:
        return -1
    return TIER_LEVELS.get(tier_name.lower(), -1)
# --- Endpoint Schemas ---

class UserRegisterRequest(BaseModel):
    email: str = Field(..., description="User's email address.", example="user@example.com")
    password: str = Field(..., min_length=8, description="User's chosen password (min 8 characters).")
    # Add other fields if needed during registration, e.g., name

class UserLoginRequest(BaseModel):
    email: str = Field(..., description="User's email address.", example="user@example.com")
    password: str = Field(..., description="User's password.")

class TokenResponse(BaseModel):
    access_token: str = Field(..., description="JWT Access Token.")
    refresh_token: str = Field(..., description="JWT Refresh Token.")
    token_type: str = Field("bearer", description="Token type (always 'bearer').")

class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., description="The JWT Refresh Token to use.")

class PasswordForgotRequest(BaseModel):
    email: str = Field(..., description="Email address to send password reset instructions.", example="user@example.com")

class PasswordResetRequest(BaseModel):
    reset_token: str = Field(..., description="The password reset token received via email.")
    new_password: str = Field(..., min_length=8, description="The new password for the user (min 8 characters).")

class SuccessResponse(BaseModel):
    """Generic success response for actions like registration or password reset."""
    message: str = Field("Operation successful.", description="Confirmation message.")
    # Optionally add a code field here too if needed
    code: str = Field("OK", description="Status code.")