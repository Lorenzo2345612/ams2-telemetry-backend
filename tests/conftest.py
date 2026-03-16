"""
Shared pytest fixtures for the AMS2 telemetry auth test suite.

Provides:
- In-memory SQLite database and session
- FastAPI TestClient with dependency overrides
- Helper factories for creating users and auth tokens
"""

import os
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Set JWT_SECRET_KEY before any application code is imported, so
# analytics/api/auth/security.py can read it at module level.
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-do-not-use-in-prod")

from auth.security import create_access_token, hash_password
from database.db_config import get_db
from main import app
from models.database import Base, User


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def engine():
    """Create a single in-memory SQLite engine for the entire test session."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def db(engine) -> Session:
    """
    Provide a transactional database session for each test.

    Each test runs inside a transaction that is rolled back at the end, so
    tests do not interfere with each other.
    """
    connection = engine.connect()
    transaction = connection.begin()
    TestingSessionLocal = sessionmaker(bind=connection)
    session = TestingSessionLocal()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# FastAPI client fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def client(db: Session) -> TestClient:
    """
    TestClient that injects the test database session into all endpoints.
    No auth header — used for unauthenticated requests.
    """

    def _override_get_db():
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture()
def auth_client(client: TestClient, test_user: User) -> TestClient:
    """
    TestClient with a valid Bearer token for ``test_user``.
    """
    token = create_access_token(test_user.id)
    client.headers["Authorization"] = f"Bearer {token}"
    return client


# ---------------------------------------------------------------------------
# User / token helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def test_user(db: Session) -> User:
    """Create and return a persisted test user."""
    user = User(
        id=str(uuid.uuid4()),
        email="testuser@example.com",
        hashed_password=hash_password("TestPassword123!"),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@pytest.fixture()
def second_user(db: Session) -> User:
    """Create and return a second persisted test user for isolation tests."""
    user = User(
        id=str(uuid.uuid4()),
        email="seconduser@example.com",
        hashed_password=hash_password("SecondPassword456!"),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def make_auth_header(user_id: str) -> dict:
    """Return an Authorization header dict for *user_id*."""
    token = create_access_token(user_id)
    return {"Authorization": f"Bearer {token}"}
