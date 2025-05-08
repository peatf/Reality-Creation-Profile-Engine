# src/middleware/auth.py
import uuid
import logging
from typing import Callable, Awaitable, Sequence, Set

from fastapi import Request, Response, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

from src.auth.jwt import decode_and_validate, TokenError, TokenExpired, TokenInvalid
from src.auth.schemas import AuthenticatedUser, ErrorResponse, ErrorDetail, get_tier_level

_log = logging.getLogger(__name__)

# --- Error Definitions ---
AUTH_ERROR_DETAIL_MISSING = ErrorDetail(code="AUTH_001", message="Authentication credentials were not provided.")
AUTH_ERROR_DETAIL_INVALID = ErrorDetail(code="AUTH_001", message="Invalid authentication credentials.") # Re-use code for simplicity or use AUTH_002
AUTH_ERROR_DETAIL_EXPIRED = ErrorDetail(code="AUTH_002", message="Token has expired.")
AUTH_ERROR_DETAIL_FORBIDDEN = ErrorDetail(code="AUTH_003", message="Insufficient permissions for this resource.")
AUTH_ERROR_DETAIL_INTERNAL = ErrorDetail(code="AUTH_999", message="An internal error occurred during authentication.")

# --- Bearer Scheme ---
# auto_error=False means it returns None if no header, instead of raising HTTPException
bearer_scheme = HTTPBearer(auto_error=False, description="JWT Access Token for authentication.")

# --- Authentication Middleware ---
class AuthenticationMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware to handle JWT authentication.

    - Extracts Bearer token from Authorization header.
    - Validates the access token using src.auth.jwt.decode_and_validate.
    - Attaches AuthenticatedUser(id, tier) to request.state.user on success.
    - Returns 401 JSON response on token missing, invalid, or expired.
    - Skips authentication for specified excluded paths and OPTIONS requests.
    """
    def __init__(
        self,
        app,
        excluded_paths: Sequence[str] | Set[str] | None = None,
    ):
        super().__init__(app)
        self.excluded_paths = set(excluded_paths) if excluded_paths else set()
        # Automatically exclude OpenAPI/Docs paths
        self.excluded_paths.update({"/docs", "/openapi.json", "/redoc"})
        _log.info(f"Auth Middleware initialized. Excluded paths: {sorted(list(self.excluded_paths))}")

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Ensure request.state exists
        if not hasattr(request, "state"):
             # This should be handled by Starlette/FastAPI, but defensively check
             _log.error("Request object missing 'state' attribute.")
             return JSONResponse(
                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                 content=ErrorResponse(detail=AUTH_ERROR_DETAIL_INTERNAL).model_dump(),
             )
        # Set a default empty user initially
        request.state.user = None

        # Skip authentication for excluded paths
        if request.url.path in self.excluded_paths:
            _log.debug(f"Skipping auth for excluded path: {request.url.path}")
            return await call_next(request)

        # Allow OPTIONS requests for CORS preflight without authentication
        if request.method == "OPTIONS":
             _log.debug(f"Allowing OPTIONS request for CORS (no auth check): {request.url.path}")
             return await call_next(request)

        # Extract token
        credentials: HTTPAuthorizationCredentials | None = await bearer_scheme(request)

        if not credentials:
            _log.warning(f"Auth failed: No token provided for path {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=ErrorResponse(detail=AUTH_ERROR_DETAIL_MISSING).model_dump(),
                headers={"WWW-Authenticate": "Bearer"}, # Indicate Bearer scheme is expected
            )

        token = credentials.credentials
        try:
            payload = decode_and_validate(token=token, expected_type="access")

            user_id_str = payload.get("sub")
            tier = payload.get("tier")

            if not user_id_str or not tier:
                 _log.error(f"Auth failed: Missing 'sub' or 'tier' in token payload for path {request.url.path}")
                 # Use the specific exception from jwt module if possible, otherwise raise TokenInvalid
                 raise TokenInvalid("Token payload missing required fields.", code="TOKEN_MISSING_CLAIM")

            try:
                user_id = uuid.UUID(user_id_str)
            except ValueError:
                _log.error(f"Auth failed: Invalid UUID format for 'sub' claim: {user_id_str}")
                raise TokenInvalid("Invalid user identifier format in token.", code="TOKEN_INVALID_SUB")

            # Attach user to request state
            request.state.user = AuthenticatedUser(id=user_id, tier=str(tier)) # Ensure tier is string
            _log.debug(f"Auth success: User {user_id} ({tier}) accessed {request.url.path}")

        except TokenExpired as e:
            _log.warning(f"Auth failed: Token expired for path {request.url.path}. Code: {e.code}, Msg: {e.message}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=ErrorResponse(detail=AUTH_ERROR_DETAIL_EXPIRED).model_dump(),
                headers={"WWW-Authenticate": f"Bearer error=\"invalid_token\", error_description=\"{e.message}\""},
            )
        except TokenInvalid as e:
            _log.warning(f"Auth failed: Token invalid for path {request.url.path}. Code: {e.code}, Msg: {e.message}")
            # Use the specific error code/message from the exception if available
            error_detail = ErrorDetail(code=e.code, message=e.message)
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=ErrorResponse(detail=error_detail).model_dump(),
                 headers={"WWW-Authenticate": f"Bearer error=\"invalid_token\", error_description=\"{e.message}\""},
            )
        except TokenError as e: # Catch base TokenError for unexpected token lib issues
             _log.error(f"Auth failed: Unexpected TokenError for path {request.url.path}. Code: {e.code}, Msg: {e.message}", exc_info=True)
             error_detail = ErrorDetail(code=e.code, message=e.message)
             return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED, # Still 401 as it's a token issue
                content=ErrorResponse(detail=error_detail).model_dump(),
                headers={"WWW-Authenticate": f"Bearer error=\"invalid_token\", error_description=\"{e.message}\""},
            )
        except Exception as e:
            # Catch-all for truly unexpected errors during the process
            _log.error(f"Auth failed: Unexpected internal error during token validation for path {request.url.path}. Error: {type(e).__name__} - {e}", exc_info=True)
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content=ErrorResponse(detail=AUTH_ERROR_DETAIL_INTERNAL).model_dump(),
            )

        # Proceed to the next middleware or route handler
        response = await call_next(request)
        return response


# --- Tier Requirement Dependency ---
def require_tier(min_tier: str) -> Callable[[Request], Awaitable[AuthenticatedUser]]:
    """
    Factory for creating a FastAPI dependency that checks user tier level.

    Ensures the authenticated user's tier meets the minimum requirement.
    Relies on the AuthenticationMiddleware having run first and populated
    `request.state.user`.

    Args:
        min_tier: The minimum tier name required (e.g., "premium").
                  Must be a key in `src.auth.schemas.TIER_LEVELS`.

    Returns:
        An asynchronous dependency function suitable for `Depends()`.

    Raises:
        HTTPException(403): If the user's tier is insufficient.
        HTTPException(500): If `request.state.user` is not set (middleware issue).
        ValueError: If `min_tier` is not a valid tier name during setup.
    """
    required_level = get_tier_level(min_tier)
    if required_level == -1:
        # Raise configuration error during setup if min_tier is invalid
        raise ValueError(f"Invalid minimum tier specified in require_tier: '{min_tier}'. Must be one of {list(get_tier_level.__globals__['TIER_LEVELS'].keys())}")

    async def _verify_tier(request: Request) -> AuthenticatedUser:
        """The actual dependency function injected into routes."""
        # Check if user state was populated by middleware
        user: AuthenticatedUser | None = getattr(request.state, "user", None)

        if not isinstance(user, AuthenticatedUser):
            # This should ideally not happen if middleware runs correctly
            _log.error(f"Tier check failed: request.state.user not found or invalid type on path {request.url.path}. Middleware might not have run or failed silently.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(detail=AUTH_ERROR_DETAIL_INTERNAL).model_dump(),
            )

        user_level = get_tier_level(user.tier)
        if user_level == -1:
             _log.warning(f"Tier check failed: User {user.id} has an unknown tier '{user.tier}' at {request.url.path}")
             # Treat unknown tiers as insufficient for any requirement > free (level 0)
             if required_level > 0:
                 raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=ErrorResponse(detail=AUTH_ERROR_DETAIL_FORBIDDEN).model_dump(),
                 )
             # Allow access if required level is 0 or less (e.g., "free")

        if user_level < required_level:
            _log.warning(f"Forbidden: User {user.id} (Tier: {user.tier}/{user_level}) attempted to access resource requiring Tier {min_tier}/{required_level} at {request.url.path}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=ErrorResponse(detail=AUTH_ERROR_DETAIL_FORBIDDEN).model_dump(),
            )

        _log.debug(f"Tier check passed for user {user.id} ({user.tier}) requiring >= {min_tier} at {request.url.path}")
        return user # Return the user object if check passes

    return _verify_tier
