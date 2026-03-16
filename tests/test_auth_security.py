"""
Tests for analytics/api/auth/security.py

Covers password hashing/verification and JWT creation/decoding,
including edge cases like expired tokens and tampered payloads.
"""

import os
import time
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from jose import jwt

# Ensure the env var is available before the module-level read in security.py
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-do-not-use-in-prod")

from auth.security import (
    JWT_ALGORITHM,
    JWT_SECRET_KEY,
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)


# ===================================================================
# Password hashing
# ===================================================================

class TestHashPassword:
    def test_returns_bcrypt_hash(self):
        hashed = hash_password("my-secret")
        # bcrypt hashes always start with $2b$
        assert hashed.startswith("$2b$")

    def test_different_calls_produce_different_hashes(self):
        h1 = hash_password("same-password")
        h2 = hash_password("same-password")
        assert h1 != h2, "bcrypt should use a random salt each time"

    def test_hash_is_not_plaintext(self):
        plain = "my-secret"
        hashed = hash_password(plain)
        assert plain not in hashed


class TestVerifyPassword:
    def test_correct_password_returns_true(self):
        hashed = hash_password("correct-password")
        assert verify_password("correct-password", hashed) is True

    def test_wrong_password_returns_false(self):
        hashed = hash_password("correct-password")
        assert verify_password("wrong-password", hashed) is False

    def test_empty_password_returns_false(self):
        hashed = hash_password("non-empty")
        assert verify_password("", hashed) is False

    def test_unicode_password(self):
        hashed = hash_password("contraseña-segura-ñ")
        assert verify_password("contraseña-segura-ñ", hashed) is True
        assert verify_password("contrasena-segura-n", hashed) is False


# ===================================================================
# JWT creation
# ===================================================================

class TestCreateAccessToken:
    def test_returns_string(self):
        token = create_access_token("user-123")
        assert isinstance(token, str)

    def test_token_has_three_parts(self):
        token = create_access_token("user-123")
        assert len(token.split(".")) == 3

    def test_payload_contains_sub(self):
        token = create_access_token("user-abc")
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert payload["sub"] == "user-abc"

    def test_payload_contains_exp(self):
        token = create_access_token("user-123")
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        assert "exp" in payload
        # exp should be roughly 30 days from now (within 60 seconds tolerance)
        expected_exp = datetime.utcnow() + timedelta(days=30)
        actual_exp = datetime.utcfromtimestamp(payload["exp"])
        assert abs((expected_exp - actual_exp).total_seconds()) < 60

    def test_different_users_get_different_tokens(self):
        t1 = create_access_token("user-1")
        t2 = create_access_token("user-2")
        assert t1 != t2


# ===================================================================
# JWT decoding
# ===================================================================

class TestDecodeAccessToken:
    def test_valid_token_returns_user_id(self):
        token = create_access_token("user-xyz")
        assert decode_access_token(token) == "user-xyz"

    def test_expired_token_raises_value_error(self):
        # Manually craft an expired token
        payload = {
            "sub": "user-expired",
            "exp": datetime.utcnow() - timedelta(seconds=10),
        }
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        with pytest.raises(ValueError, match="Invalid token"):
            decode_access_token(token)

    def test_token_with_wrong_secret_raises_value_error(self):
        payload = {
            "sub": "user-wrong-secret",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, "wrong-secret-key", algorithm=JWT_ALGORITHM)
        with pytest.raises(ValueError, match="Invalid token"):
            decode_access_token(token)

    def test_token_without_sub_raises_value_error(self):
        payload = {"exp": datetime.utcnow() + timedelta(hours=1)}
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        with pytest.raises(ValueError, match="missing 'sub' claim"):
            decode_access_token(token)

    def test_garbage_token_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid token"):
            decode_access_token("not.a.valid.token")

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError):
            decode_access_token("")

    def test_token_with_none_sub_raises_value_error(self):
        payload = {
            "sub": None,
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        with pytest.raises(ValueError, match="missing 'sub' claim"):
            decode_access_token(token)

    def test_token_with_different_algorithm_raises_value_error(self):
        """Token signed with HS384 should fail when we only accept HS256."""
        payload = {
            "sub": "user-alg",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm="HS384")
        with pytest.raises(ValueError, match="Invalid token"):
            decode_access_token(token)
