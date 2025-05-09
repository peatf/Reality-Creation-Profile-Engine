# tests/auth/test_tokens.py

import os
import uuid
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# Modules to test
from src.auth import jwt as jwt_utils # Alias for the module
from src.auth import crypto
# Import functions directly for convenience, but be mindful of reloads for classes/exceptions
from src.auth.jwt import (
    create_access_token, create_refresh_token, decode_and_validate
    # TokenExpired, TokenInvalid, TokenError will be accessed via jwt_utils after reload
)
from src.auth.crypto import hash_password, verify_password

# --- Test Fixtures ---

@pytest.fixture(scope="module")
def test_keys(tmp_path_factory):
    """Generates temporary RSA keys for testing."""
    key_dir = tmp_path_factory.mktemp("test_keys")
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    pem_private = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    pem_public = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    private_key_path = key_dir / "test_private.pem"
    public_key_path = key_dir / "test_public.pem"

    private_key_path.write_bytes(pem_private)
    public_key_path.write_bytes(pem_public)

    return {"private": private_key_path, "public": public_key_path}

@pytest.fixture(autouse=True)
def mock_jwt_env_vars(monkeypatch, test_keys):
    """Mocks environment variables needed by the jwt module."""
    monkeypatch.setenv("JWT_PRIVATE_KEY_PATH", str(test_keys["private"]))
    monkeypatch.setenv("JWT_PUBLIC_KEY_PATH", str(test_keys["public"]))
    monkeypatch.setenv("JWT_ALGORITHM", "RS256")
    monkeypatch.setenv("JWT_ACCESS_TTL_SECONDS", "10") # Short TTL for testing expiry
    monkeypatch.setenv("JWT_REFRESH_TTL_SECONDS", "20")
    monkeypatch.setenv("JWT_ISSUER", "test-issuer")
    monkeypatch.setenv("JWT_AUDIENCE", "test-audience")

    # Reload the module to pick up mocked env vars
    # This is crucial because the module reads env vars at import time
    import importlib
    importlib.reload(jwt_utils)

@pytest.fixture(autouse=True)
def mock_crypto_env_vars(monkeypatch):
    """Mocks environment variables needed by the crypto module."""
    test_pepper = "test-pepper-secret-value-12345"
    monkeypatch.setenv("PASSWORD_PEPPER", test_pepper)

    # Reload the module to pick up mocked env vars
    import importlib
    importlib.reload(crypto)
    return test_pepper # Return pepper for use in tests

@pytest.fixture
def sample_user_id() -> uuid.UUID:
    return uuid.uuid4()

@pytest.fixture
def sample_tier() -> str:
    return "premium"

@pytest.fixture
def sample_token_id() -> uuid.UUID:
    return uuid.uuid4()

# --- JWT Tests ---

def test_create_access_token_success(sample_user_id, sample_tier):
    """Test successful creation of an access token."""
    token = create_access_token(user_id=sample_user_id, tier=sample_tier)
    assert isinstance(token, str)
    assert len(token.split('.')) == 3 # Basic JWT structure check

def test_create_refresh_token_success(sample_user_id, sample_tier, sample_token_id):
    """Test successful creation of a refresh token."""
    token = create_refresh_token(user_id=sample_user_id, tier=sample_tier, token_id=sample_token_id)
    assert isinstance(token, str)
    assert len(token.split('.')) == 3

def test_decode_access_token_success(sample_user_id, sample_tier):
    """Test successful decoding and validation of a valid access token."""
    token = create_access_token(user_id=sample_user_id, tier=sample_tier)
    payload = decode_and_validate(token=token, expected_type="access")

    assert payload["sub"] == str(sample_user_id)
    assert payload["tier"] == sample_tier
    assert payload["typ"] == "access"
    assert payload["iss"] == "test-issuer"
    assert payload["aud"] == "test-audience"
    assert "exp" in payload
    assert "iat" in payload
    assert "nbf" in payload
    assert "jti" not in payload # Access tokens don't have jti in this impl

def test_decode_refresh_token_success(sample_user_id, sample_tier, sample_token_id):
    """Test successful decoding and validation of a valid refresh token."""
    token = create_refresh_token(user_id=sample_user_id, tier=sample_tier, token_id=sample_token_id)
    payload = decode_and_validate(token=token, expected_type="refresh")

    assert payload["sub"] == str(sample_user_id)
    assert payload["tier"] == sample_tier
    assert payload["jti"] == str(sample_token_id)
    assert payload["typ"] == "refresh"
    assert payload["iss"] == "test-issuer"
    assert payload["aud"] == "test-audience"
    assert "exp" in payload
    assert "iat" in payload
    assert "nbf" in payload

def test_decode_token_expired(sample_user_id, sample_tier):
    """Test that decoding an expired token raises TokenExpired."""
    token = create_access_token(user_id=sample_user_id, tier=sample_tier)
    # Wait for token to expire (JWT_ACCESS_TTL_SECONDS = 10)
    time.sleep(11)
    # Access TokenExpired from the reloaded jwt_utils module
    with pytest.raises(jwt_utils.TokenExpired):
        decode_and_validate(token=token, expected_type="access")

def test_decode_token_wrong_type(sample_user_id, sample_tier, sample_token_id):
    """Test that decoding with the wrong expected type raises TokenInvalid."""
    access_token = create_access_token(user_id=sample_user_id, tier=sample_tier)
    refresh_token = create_refresh_token(user_id=sample_user_id, tier=sample_tier, token_id=sample_token_id)
 
    with pytest.raises(jwt_utils.TokenInvalid, match="Invalid token type. Expected 'refresh'"):
        decode_and_validate(token=access_token, expected_type="refresh")
 
    with pytest.raises(jwt_utils.TokenInvalid, match="Invalid token type. Expected 'access'"):
        decode_and_validate(token=refresh_token, expected_type="access")

def test_decode_token_invalid_signature(sample_user_id, sample_tier, test_keys):
    """Test that decoding a token with an invalid signature raises TokenInvalid."""
    token = create_access_token(user_id=sample_user_id, tier=sample_tier)
    header, payload, sig = token.split('.')
    # Tamper with the payload slightly
    tampered_token = f"{header}.{payload}tampered.{sig}"
 
    with pytest.raises(jwt_utils.TokenInvalid, match="Token decoding failed"): # PyJWT raises DecodeError first
        decode_and_validate(token=tampered_token, expected_type="access")
 
    # Test with a token signed by a different key
    wrong_private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    wrong_pem_private = wrong_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    now = datetime.now(timezone.utc)
    expire = now + timedelta(seconds=30)
    payload_data = {
        "sub": str(sample_user_id), "tier": sample_tier, "typ": "access",
        "exp": expire, "iat": now, "nbf": now,
        "iss": "test-issuer", "aud": "test-audience"
    }
    wrong_sig_token = jwt.encode(
        payload_data, wrong_pem_private, algorithm="RS256", headers={"kid": "test_public"}
    )
 
    with pytest.raises(jwt_utils.TokenInvalid, match="Token signature verification failed"):
        decode_and_validate(token=wrong_sig_token, expected_type="access")
 
 
def test_decode_token_missing_claim(sample_user_id, sample_tier, test_keys):
    """Test decoding a token missing a required claim raises TokenInvalid."""
    # Manually craft a token missing 'tier'
    now = datetime.now(timezone.utc)
    expire = now + timedelta(seconds=30)
    payload_data = {
        "sub": str(sample_user_id), #"tier": sample_tier, # Missing tier
        "typ": "access",
        "exp": expire, "iat": now, "nbf": now,
        "iss": "test-issuer", "aud": "test-audience"
    }
    private_key_path = Path(os.environ["JWT_PRIVATE_KEY_PATH"])
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    token = jwt.encode(
        payload_data, private_key, algorithm="RS256", headers={"kid": "test_public"}
    )

    # PyJWT's decode checks required claims specified in options
    # The custom exception mapping in jwt_utils.decode_and_validate for MissingRequiredClaimError
    # should result in TokenInvalid with code TOKEN_MISSING_STANDARD_CLAIM
    with pytest.raises(jwt_utils.TokenInvalid) as exc_info:
        decode_and_validate(token=token, expected_type="access")
    assert exc_info.value.code == "TOKEN_MISSING_STANDARD_CLAIM" # Check specific code

 
def test_decode_refresh_token_missing_jti(sample_user_id, sample_tier, test_keys):
    """Test decoding a refresh token missing 'jti' raises TokenInvalid."""
    now = datetime.now(timezone.utc)
    expire = now + timedelta(seconds=30)
    payload_data = {
        "sub": str(sample_user_id), "tier": sample_tier,
        "typ": "refresh", #"jti": str(uuid.uuid4()), # Missing jti
        "exp": expire, "iat": now, "nbf": now,
        "iss": "test-issuer", "aud": "test-audience"
    }
    private_key_path = Path(os.environ["JWT_PRIVATE_KEY_PATH"])
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    token = jwt.encode(
        payload_data, private_key, algorithm="RS256", headers={"kid": "test_public"}
    )
    # This should now hit the custom TOKEN_MISSING_JTI
    with pytest.raises(jwt_utils.TokenInvalid) as exc_info:
        decode_and_validate(token=token, expected_type="refresh")
    assert exc_info.value.code == "TOKEN_MISSING_JTI"

 
def test_decode_token_invalid_sub_format(sample_tier, test_keys):
    """Test decoding a token with invalid 'sub' format raises TokenInvalid."""
    now = datetime.now(timezone.utc)
    expire = now + timedelta(seconds=30)
    payload_data = {
        "sub": "not-a-uuid", "tier": sample_tier, "typ": "access",
        "exp": expire, "iat": now, "nbf": now,
        "iss": "test-issuer", "aud": "test-audience"
    }
    private_key_path = Path(os.environ["JWT_PRIVATE_KEY_PATH"])
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    token = jwt.encode(
        payload_data, private_key, algorithm="RS256", headers={"kid": "test_public"}
    )
 
    with pytest.raises(jwt_utils.TokenInvalid) as exc_info:
        decode_and_validate(token=token, expected_type="access")
    assert exc_info.value.code == "TOKEN_INVALID_SUB"

 
 
# --- Crypto Tests ---

def test_hash_password_success():
    """Test successful password hashing."""
    password = "mysecretpassword"
    hashed = hash_password(password)
    assert isinstance(hashed, str)
    assert hashed != password
    # Check if the hash seems to be in Argon2 format
    assert hashed.startswith("$argon2")

def test_hash_empty_password():
    """Test hashing an empty password raises ValueError."""
    with pytest.raises(ValueError, match="Password cannot be empty."):
        hash_password("")

def test_verify_password_success(mock_crypto_env_vars):
    """Test successful password verification."""
    password = "mysecretpassword"
    hashed = hash_password(password)
    assert verify_password(password, hashed) is True

def test_verify_password_failure(mock_crypto_env_vars):
    """Test password verification failure for incorrect password."""
    password = "mysecretpassword"
    wrong_password = "wrongpassword"
    hashed = hash_password(password)
    assert verify_password(wrong_password, hashed) is False

def test_verify_password_empty_inputs():
    """Test verification with empty inputs returns False."""
    password = "mysecretpassword"
    hashed = hash_password(password)
    assert verify_password("", hashed) is False
    assert verify_password(password, "") is False
    assert verify_password("", "") is False

def test_verify_password_invalid_hash():
    """Test verification with an invalid hash format returns False."""
    password = "mysecretpassword"
    invalid_hash = "not-a-valid-hash-format"
    assert verify_password(password, invalid_hash) is False

def test_pepper_is_used(mock_crypto_env_vars):
    """Verify that the pepper influences the hash and verification."""
    password = "mysecretpassword"
    pepper = mock_crypto_env_vars

    # Hash with the mocked pepper
    hashed_with_pepper = hash_password(password)

    # Manually hash without pepper (using passlib context directly)
    hashed_without_pepper = crypto.pwd_context.hash(password)

    assert hashed_with_pepper != hashed_without_pepper

    # Verification should fail if pepper is missing during verification attempt
    # (Simulate by verifying plain password against hash expecting pepper)
    assert crypto.pwd_context.verify(password, hashed_with_pepper) is False

    # Verification should succeed when correct pepper is included
    assert crypto.pwd_context.verify(f"{password}{pepper}", hashed_with_pepper) is True
    assert verify_password(password, hashed_with_pepper) is True # Using our helper