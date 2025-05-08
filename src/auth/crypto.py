# src/auth/crypto.py
import os
from passlib.context import CryptContext
from passlib.exc import UnknownHashError # Removed ValidationError

# Recommended Argon2 settings (adjust cost factors as needed for your hardware)
pwd_context = CryptContext(
    schemes=["argon2"],
    deprecated="auto",
    # Argon2 specific options
    argon2__memory_cost=65536,  # KiB
    argon2__parallelism=4,
    argon2__time_cost=3,
)

# Load pepper from environment variable
PASSWORD_PEPPER = os.getenv("PASSWORD_PEPPER")
if not PASSWORD_PEPPER:
    # In a real app, consider a more robust way to handle missing critical config
    # Maybe raise a specific configuration error or log and exit.
    # For now, raising EnvironmentError to halt startup if not set.
    raise EnvironmentError("PASSWORD_PEPPER environment variable not set.")


def hash_password(password: str) -> str:
    """Hashes a password using Argon2 with a configured pepper."""
    if not password:
        raise ValueError("Password cannot be empty.")
    # Combine password and pepper before hashing
    password_with_pepper = f"{password}{PASSWORD_PEPPER}"
    return pwd_context.hash(password_with_pepper)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a stored hash."""
    if not plain_password or not hashed_password:
        return False
    password_with_pepper = f"{plain_password}{PASSWORD_PEPPER}"
    try:
        # Use constant-time comparison provided by passlib
        return pwd_context.verify(password_with_pepper, hashed_password)
    except UnknownHashError:
        # Handle cases where the hash uses an unknown scheme
        # Log this event for security monitoring
        print(f"Warning: Attempted verification with unknown hash format: {hashed_password[:10]}...") # Consider proper logging
        return False