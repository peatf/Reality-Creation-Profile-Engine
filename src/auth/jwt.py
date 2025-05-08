# src/auth/jwt.py
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Literal, Optional
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

# --- Configuration ---
try:
    JWT_PRIVATE_KEY_PATH = Path(os.environ["JWT_PRIVATE_KEY_PATH"])
    JWT_PUBLIC_KEY_PATH = Path(os.environ["JWT_PUBLIC_KEY_PATH"])
    JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "RS256")
    JWT_ACCESS_TTL_SECONDS = int(os.environ.get("JWT_ACCESS_TTL_SECONDS", 900))  # 15 min
    JWT_REFRESH_TTL_SECONDS = int(os.environ.get("JWT_REFRESH_TTL_SECONDS", 1209600)) # 14 days
    # Optional: Issuer and Audience for stricter validation
    JWT_ISSUER = os.environ.get("JWT_ISSUER", "rcpe-api")
    JWT_AUDIENCE = os.environ.get("JWT_AUDIENCE", "rcpe-client")
except KeyError as e:
    raise EnvironmentError(f"Missing critical JWT environment variable: {e}")
except ValueError as e:
     raise EnvironmentError(f"Invalid JWT TTL value in environment variables: {e}")

# --- Load Keys ---
try:
    # Ensure keys directory exists if using relative paths in .env
    keys_dir = JWT_PRIVATE_KEY_PATH.parent
    if not keys_dir.is_dir():
         print(f"Warning: JWT key directory '{keys_dir}' not found. Assuming absolute paths or keys are elsewhere.")

    with open(JWT_PRIVATE_KEY_PATH, "rb") as key_file:
        # Assuming PEM format, no password for dev key
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None, # Add password handling if key is encrypted
            backend=default_backend()
        )
    with open(JWT_PUBLIC_KEY_PATH, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    # Generate a simple key ID based on the public key path (for rotation)
    # A more robust approach might use a hash of the key material
    CURRENT_KEY_ID = JWT_PUBLIC_KEY_PATH.stem # Use stem directly from Path object

except FileNotFoundError as e:
    # Provide more context in the error message
    raise EnvironmentError(f"JWT key file not found at path specified in environment variable: {e}")
except IsADirectoryError as e:
     raise EnvironmentError(f"JWT key path specified is a directory, not a file: {e}")
except PermissionError as e:
     raise EnvironmentError(f"Permission denied when trying to read JWT key file: {e}")
except Exception as e: # Catch broader errors during key loading/parsing
    raise EnvironmentError(f"Error loading JWT keys: {type(e).__name__} - {e}")


# --- Custom Exceptions ---
class TokenError(Exception):
    """Base class for token-related errors."""
    def __init__(self, message="Token error occurred", code="TOKEN_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)

class TokenExpired(TokenError):
    """Raised when a token's expiration time has passed."""
    def __init__(self, message="Token has expired", code="TOKEN_EXPIRED"):
        super().__init__(message, code)

class TokenInvalid(TokenError):
    """Raised when a token is invalid (bad signature, wrong format, claims etc.)."""
    def __init__(self, message="Token is invalid", code="TOKEN_INVALID"):
        super().__init__(message, code)

class TokenRevoked(TokenError):
    """Raised when a token has been revoked (e.g., refresh token)."""
    # Note: Revocation check happens *before* decode_and_validate in the flow
    def __init__(self, message="Token has been revoked", code="TOKEN_REVOKED"):
        super().__init__(message, code)


# --- Token Creation ---
def _create_token(
    payload: Dict[str, Any],
    expires_delta: timedelta,
    token_type: Literal["access", "refresh"],
    key_id: str,
) -> str:
    """Helper function to create JWT tokens."""
    to_encode = payload.copy()
    now = datetime.now(timezone.utc)
    expire = now + expires_delta

    to_encode.update({
        "exp": expire,
        "iat": now,
        "nbf": now, # Not Before
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "typ": token_type,
    })

    headers = {"kid": key_id}

    try:
        encoded_jwt = jwt.encode(
            to_encode,
            private_key,
            algorithm=JWT_ALGORITHM,
            headers=headers
        )
        return encoded_jwt
    except Exception as e:
        # Log error details
        print(f"Error encoding JWT: {type(e).__name__} - {e}")
        # Raise a more specific internal error or re-raise
        raise RuntimeError("Failed to create token due to encoding error.")


def create_access_token(*, user_id: uuid.UUID, tier: str) -> str:
    """Creates a new access token."""
    if not isinstance(user_id, uuid.UUID):
        raise TypeError("user_id must be a UUID.")
    expires_delta = timedelta(seconds=JWT_ACCESS_TTL_SECONDS)
    payload = {
        "sub": str(user_id), # Subject claim
        "tier": tier,
        # Add other relevant non-sensitive claims if needed
    }
    return _create_token(
        payload=payload,
        expires_delta=expires_delta,
        token_type="access",
        key_id=CURRENT_KEY_ID
    )

def create_refresh_token(*, user_id: uuid.UUID, tier: str, token_id: uuid.UUID) -> str:
    """Creates a new refresh token with a unique token ID (jti)."""
    if not isinstance(user_id, uuid.UUID) or not isinstance(token_id, uuid.UUID):
        raise TypeError("user_id and token_id must be UUIDs.")
    expires_delta = timedelta(seconds=JWT_REFRESH_TTL_SECONDS)
    payload = {
        "sub": str(user_id),
        "tier": tier, # Include tier for potential future use during refresh
        "jti": str(token_id), # JWT ID claim, used for revocation
    }
    return _create_token(
        payload=payload,
        expires_delta=expires_delta,
        token_type="refresh",
        key_id=CURRENT_KEY_ID
    )


# --- Token Decoding and Validation ---
def decode_and_validate(
    token: str,
    expected_type: Literal["access", "refresh"]
) -> Dict[str, Any]:
    """
    Decodes and validates a JWT token.

    Args:
        token: The JWT token string.
        expected_type: The expected token type ('access' or 'refresh').

    Returns:
        The decoded payload dictionary.

    Raises:
        TokenExpired: If the token has expired.
        TokenInvalid: If the token is invalid (bad signature, format, claims).
    """
    if not token:
        raise TokenInvalid("Token cannot be empty.")

    try:
        # Note: Key rotation would require fetching the correct public key based on 'kid'
        # For now, we only validate against the currently configured public key.
        # A real implementation might have a dictionary of public keys keyed by 'kid'.
        # headers = jwt.get_unverified_header(token)
        # key_id = headers.get("kid")
        # if key_id != CURRENT_KEY_ID: # Or check if key_id exists in a key store
        #     raise TokenInvalid("Unknown or mismatched key ID.")
        # public_key_to_use = public_key # Replace with key lookup based on key_id

        payload = jwt.decode(
            token,
            public_key, # Use public_key_to_use in a multi-key setup
            algorithms=[JWT_ALGORITHM],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
            options={
                "require": ["exp", "iat", "nbf", "iss", "aud", "sub", "typ"],
                # Leeway accounts for clock skew between servers
                "leeway": timedelta(seconds=30)
            }
        )

        # Explicitly check token type
        token_type = payload.get("typ")
        if token_type != expected_type:
            raise TokenInvalid(f"Invalid token type. Expected '{expected_type}', got '{token_type}'.", code="TOKEN_TYPE_MISMATCH")

        # For refresh tokens, ensure 'jti' is present
        if expected_type == "refresh":
             jti = payload.get("jti")
             if not jti:
                 raise TokenInvalid("Refresh token missing 'jti' claim.", code="TOKEN_MISSING_JTI")
             # Optionally validate jti format (e.g., is it a valid UUID string?)
             try:
                 uuid.UUID(jti)
             except ValueError:
                 raise TokenInvalid("Invalid 'jti' format in refresh token.", code="TOKEN_INVALID_JTI")


        # Validate essential claims format (e.g., sub should be UUID)
        sub = payload.get("sub")
        if not sub:
             raise TokenInvalid("Token missing 'sub' claim.", code="TOKEN_MISSING_SUB")
        try:
            uuid.UUID(sub)
        except ValueError:
            raise TokenInvalid("Invalid 'sub' format in token.", code="TOKEN_INVALID_SUB")


        return payload

    except jwt.ExpiredSignatureError:
        raise TokenExpired() # Use default message/code
    except jwt.InvalidAudienceError:
        raise TokenInvalid("Invalid audience.", code="TOKEN_INVALID_AUDIENCE")
    except jwt.InvalidIssuerError:
        raise TokenInvalid("Invalid issuer.", code="TOKEN_INVALID_ISSUER")
    except jwt.MissingRequiredClaimError as e:
        raise TokenInvalid(f"Missing required claim: {e}", code="TOKEN_MISSING_CLAIM")
    except jwt.DecodeError as e:
        # More specific error for decoding issues (e.g., bad base64)
        raise TokenInvalid(f"Token decoding failed: {e}", code="TOKEN_DECODE_ERROR")
    except jwt.InvalidSignatureError as e:
         raise TokenInvalid(f"Token signature verification failed: {e}", code="TOKEN_SIGNATURE_INVALID")
    except jwt.InvalidTokenError as e: # Catches various other JWT errors
        raise TokenInvalid(f"Token is invalid: {e}", code="TOKEN_GENERIC_INVALID")
    except Exception as e: # Catch unexpected errors during decoding
        # Log this unexpected error
        print(f"Unexpected error during token decoding: {type(e).__name__} - {e}")
        raise TokenInvalid("An unexpected error occurred while processing the token.", code="TOKEN_UNEXPECTED_ERROR")