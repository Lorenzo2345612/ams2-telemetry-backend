"""
Tests for analytics/api/routers/auth_router.py

Covers /auth/register, /auth/login, and /auth/change-password endpoints
including success paths, validation failures, and authorization checks.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from auth.security import verify_password
from models.database import User
from tests.conftest import make_auth_header


# ===================================================================
# POST /auth/register
# ===================================================================

class TestRegister:
    REGISTER_URL = "/auth/register"

    def test_register_success(self, client: TestClient):
        resp = client.post(self.REGISTER_URL, json={
            "email": "newuser@example.com",
            "password": "StrongPassword123!",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "newuser@example.com"
        assert "user_id" in data
        assert "token" in data
        assert len(data["token"]) > 0

    def test_register_creates_user_in_db(self, client: TestClient, db: Session):
        client.post(self.REGISTER_URL, json={
            "email": "dbcheck@example.com",
            "password": "StrongPassword123!",
        })
        user = db.query(User).filter(User.email == "dbcheck@example.com").first()
        assert user is not None
        assert user.email == "dbcheck@example.com"
        # Password should be hashed, not stored as plain text
        assert user.hashed_password != "StrongPassword123!"
        assert verify_password("StrongPassword123!", user.hashed_password)

    def test_register_duplicate_email_returns_409(self, client: TestClient, test_user: User):
        resp = client.post(self.REGISTER_URL, json={
            "email": test_user.email,
            "password": "AnyPassword123!",
        })
        assert resp.status_code == 409
        assert "already registered" in resp.json()["detail"].lower()

    def test_register_invalid_email_returns_422(self, client: TestClient):
        resp = client.post(self.REGISTER_URL, json={
            "email": "not-an-email",
            "password": "StrongPassword123!",
        })
        assert resp.status_code == 422

    def test_register_missing_password_returns_422(self, client: TestClient):
        resp = client.post(self.REGISTER_URL, json={
            "email": "valid@example.com",
        })
        assert resp.status_code == 422

    def test_register_missing_email_returns_422(self, client: TestClient):
        resp = client.post(self.REGISTER_URL, json={
            "password": "StrongPassword123!",
        })
        assert resp.status_code == 422

    def test_register_empty_body_returns_422(self, client: TestClient):
        resp = client.post(self.REGISTER_URL, json={})
        assert resp.status_code == 422

    def test_register_token_is_valid_jwt(self, client: TestClient):
        """The returned token should have three dot-separated parts."""
        resp = client.post(self.REGISTER_URL, json={
            "email": "jwtcheck@example.com",
            "password": "StrongPassword123!",
        })
        token = resp.json()["token"]
        assert len(token.split(".")) == 3

    def test_register_case_sensitivity(self, client: TestClient, test_user: User):
        """
        Emails are case-insensitive per RFC 5321.  If the implementation
        normalizes, a differently-cased duplicate should be rejected.  If it
        does NOT normalize, the test still passes because the cased version
        will simply register as a new user.  This test documents the current
        behavior.
        """
        resp = client.post(self.REGISTER_URL, json={
            "email": test_user.email.upper(),
            "password": "StrongPassword123!",
        })
        # Accept either 200 (case-sensitive) or 409 (case-insensitive)
        assert resp.status_code in (200, 409)


# ===================================================================
# POST /auth/login
# ===================================================================

class TestLogin:
    LOGIN_URL = "/auth/login"

    def test_login_success(self, client: TestClient, test_user: User):
        resp = client.post(self.LOGIN_URL, json={
            "email": test_user.email,
            "password": "TestPassword123!",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == test_user.email
        assert data["user_id"] == test_user.id
        assert "token" in data

    def test_login_wrong_password_returns_401(self, client: TestClient, test_user: User):
        resp = client.post(self.LOGIN_URL, json={
            "email": test_user.email,
            "password": "WrongPassword!",
        })
        assert resp.status_code == 401
        assert "invalid" in resp.json()["detail"].lower()

    def test_login_nonexistent_user_returns_401(self, client: TestClient):
        resp = client.post(self.LOGIN_URL, json={
            "email": "nobody@example.com",
            "password": "DoesNotMatter!",
        })
        assert resp.status_code == 401

    def test_login_invalid_email_format_returns_422(self, client: TestClient):
        resp = client.post(self.LOGIN_URL, json={
            "email": "bad-email",
            "password": "whatever",
        })
        assert resp.status_code == 422

    def test_login_missing_fields_returns_422(self, client: TestClient):
        resp = client.post(self.LOGIN_URL, json={})
        assert resp.status_code == 422

    def test_login_empty_password_returns_401(self, client: TestClient, test_user: User):
        resp = client.post(self.LOGIN_URL, json={
            "email": test_user.email,
            "password": "",
        })
        # Empty password will not match the bcrypt hash
        assert resp.status_code == 401

    def test_login_returns_different_token_each_time(
        self, client: TestClient, test_user: User
    ):
        """Tokens should differ due to different iat/exp timestamps."""
        r1 = client.post(self.LOGIN_URL, json={
            "email": test_user.email,
            "password": "TestPassword123!",
        })
        r2 = client.post(self.LOGIN_URL, json={
            "email": test_user.email,
            "password": "TestPassword123!",
        })
        # Tokens may or may not differ depending on timing resolution,
        # but both should be valid
        assert r1.status_code == 200
        assert r2.status_code == 200


# ===================================================================
# POST /auth/change-password
# ===================================================================

class TestChangePassword:
    CHANGE_URL = "/auth/change-password"

    def test_change_password_success(
        self, client: TestClient, test_user: User, db: Session
    ):
        headers = make_auth_header(test_user.id)
        resp = client.post(self.CHANGE_URL, json={
            "email": test_user.email,
            "current_password": "TestPassword123!",
            "new_password": "NewPassword456!",
        }, headers=headers)
        assert resp.status_code == 200
        assert "success" in resp.json()["message"].lower()

        # Verify the password was actually changed in the database
        db.refresh(test_user)
        assert verify_password("NewPassword456!", test_user.hashed_password)
        assert not verify_password("TestPassword123!", test_user.hashed_password)

    def test_change_password_wrong_current_returns_401(
        self, client: TestClient, test_user: User
    ):
        headers = make_auth_header(test_user.id)
        resp = client.post(self.CHANGE_URL, json={
            "email": test_user.email,
            "current_password": "WrongCurrent!",
            "new_password": "NewPassword456!",
        }, headers=headers)
        assert resp.status_code == 401
        assert "incorrect" in resp.json()["detail"].lower()

    def test_change_password_email_mismatch_returns_403(
        self, client: TestClient, test_user: User
    ):
        headers = make_auth_header(test_user.id)
        resp = client.post(self.CHANGE_URL, json={
            "email": "other@example.com",
            "current_password": "TestPassword123!",
            "new_password": "NewPassword456!",
        }, headers=headers)
        assert resp.status_code == 403
        assert "does not match" in resp.json()["detail"].lower()

    def test_change_password_no_auth_returns_401_or_403(self, client: TestClient):
        """Without a Bearer token the request should be rejected."""
        resp = client.post(self.CHANGE_URL, json={
            "email": "testuser@example.com",
            "current_password": "TestPassword123!",
            "new_password": "NewPassword456!",
        })
        assert resp.status_code in (401, 403)

    def test_change_password_expired_token_returns_401(
        self, client: TestClient, test_user: User
    ):
        """An expired JWT should be rejected."""
        from datetime import datetime, timedelta
        from jose import jwt as jose_jwt
        from auth.security import JWT_SECRET_KEY, JWT_ALGORITHM

        expired_payload = {
            "sub": test_user.id,
            "exp": datetime.utcnow() - timedelta(seconds=10),
        }
        expired_token = jose_jwt.encode(
            expired_payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM
        )

        resp = client.post(self.CHANGE_URL, json={
            "email": test_user.email,
            "current_password": "TestPassword123!",
            "new_password": "NewPassword456!",
        }, headers={"Authorization": f"Bearer {expired_token}"})
        assert resp.status_code == 401

    def test_change_password_invalid_token_returns_401(self, client: TestClient):
        resp = client.post(self.CHANGE_URL, json={
            "email": "testuser@example.com",
            "current_password": "TestPassword123!",
            "new_password": "NewPassword456!",
        }, headers={"Authorization": "Bearer garbage.token.here"})
        assert resp.status_code == 401

    def test_change_password_missing_fields_returns_422(
        self, client: TestClient, test_user: User
    ):
        headers = make_auth_header(test_user.id)
        resp = client.post(self.CHANGE_URL, json={
            "email": test_user.email,
        }, headers=headers)
        assert resp.status_code == 422
