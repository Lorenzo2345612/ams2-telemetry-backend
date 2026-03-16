"""
Tests for auth enforcement on analytics/api/routers/race_router.py

Validates that:
1. All race endpoints return 401/403 when no auth token is provided.
2. User A cannot access user B's races (user isolation / ownership checks).
3. Endpoints that are expected to become protected actually reject
   unauthenticated requests once auth is wired in.

NOTE: Several race endpoints depend on S3 / Redis / RQ which are not
available in the test environment.  For those, we only verify the auth
gate (401 without token) rather than full end-to-end behavior.  Tests
that create races use direct DB inserts so they don't hit S3.
"""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from auth.security import create_access_token
from models.database import Lap, Race, RaceStatus, User
from tests.conftest import make_auth_header


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_race(db: Session, user_id: str | None = None, **kwargs) -> Race:
    """Insert a Race directly into the test DB."""
    race = Race(
        race_id=kwargs.get("race_id", str(uuid.uuid4())),
        status=kwargs.get("status", RaceStatus.READY),
        raw_data_path=kwargs.get("raw_data_path", "races/test/raw.deflate"),
        vehicle_name=kwargs.get("vehicle_name", "TestCar"),
        class_name=kwargs.get("class_name", "GT3"),
        user_id=user_id,
    )
    db.add(race)
    db.commit()
    db.refresh(race)
    return race


def _create_lap(db: Session, race_id: str, lap_number: int) -> Lap:
    lap = Lap(
        lap_uuid=str(uuid.uuid4()),
        race_id=race_id,
        lap_number=lap_number,
        processed_data_path=f"races/{race_id}/lap_{lap_number}.npy",
    )
    db.add(lap)
    db.commit()
    db.refresh(lap)
    return lap


# ===================================================================
# 1. All race endpoints reject unauthenticated requests
# ===================================================================
#
# After auth is wired into race_router.py, every endpoint should
# return 401 or 403 when no Bearer token is supplied.  Until then,
# some endpoints will still return 200.  The tests below document
# the *expected* post-auth behavior so they serve as a regression
# gate for the auth integration.
# ===================================================================

class TestRaceEndpointsRequireAuth:
    """
    Each test hits an endpoint WITHOUT an Authorization header and
    asserts that the response is 401 or 403.

    If any of these tests fail with a 200, it means the endpoint
    has not yet been protected with ``Depends(get_current_user)``.
    """

    def test_upload_requires_auth(self, client: TestClient):
        resp = client.post("/race/upload", json={"data": "dGVzdA=="})
        assert resp.status_code in (401, 403), (
            f"POST /race/upload should require auth, got {resp.status_code}"
        )

    def test_list_requires_auth(self, client: TestClient):
        resp = client.get("/race/list")
        assert resp.status_code in (401, 403), (
            f"GET /race/list should require auth, got {resp.status_code}"
        )

    def test_list_ids_requires_auth(self, client: TestClient):
        resp = client.get("/race/list_ids")
        assert resp.status_code in (401, 403), (
            f"GET /race/list_ids should require auth, got {resp.status_code}"
        )

    def test_status_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.get(f"/race/{race.race_id}/status")
        assert resp.status_code in (401, 403)

    def test_download_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.get(f"/race/{race.race_id}/download")
        assert resp.status_code in (401, 403)

    def test_download_raw_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.get(f"/race/{race.race_id}/download/raw")
        assert resp.status_code in (401, 403)

    def test_delete_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.delete(f"/race/{race.race_id}")
        assert resp.status_code in (401, 403)

    def test_compare_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.get(f"/race/{race.race_id}/compare/1/2")
        assert resp.status_code in (401, 403)

    def test_fuel_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.get(f"/race/{race.race_id}/fuel/1")
        assert resp.status_code in (401, 403)

    def test_fuel_compare_requires_auth(self, client: TestClient, db: Session, test_user: User):
        race = _create_race(db, user_id=test_user.id)
        resp = client.get(f"/race/{race.race_id}/fuel/compare/1/2")
        assert resp.status_code in (401, 403)

    def test_last_lap_requires_auth(self, client: TestClient):
        resp = client.post("/race/last-lap", json={"data": "dGVzdA=="})
        assert resp.status_code in (401, 403)

    def test_last_lap_audio_requires_auth(self, client: TestClient):
        resp = client.get("/race/last-lap/audio")
        assert resp.status_code in (401, 403)


# ===================================================================
# 2. User isolation — user A cannot see user B's races
# ===================================================================

class TestUserIsolation:
    """
    Verifies that list endpoints only return races belonging to the
    authenticated user, and that per-race endpoints return 404 (not
    403) when the race belongs to another user.

    Returning 404 instead of 403 is intentional: it prevents
    information leakage about the existence of other users' resources.
    """

    def test_list_returns_only_own_races(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_a = _create_race(db, user_id=test_user.id, vehicle_name="CarA")
        race_b = _create_race(db, user_id=second_user.id, vehicle_name="CarB")

        headers = make_auth_header(test_user.id)
        resp = client.get("/race/list", headers=headers)

        # If auth is not yet wired into /race/list, this might return all
        # races.  Once auth is implemented, only test_user's race should appear.
        if resp.status_code == 200:
            race_ids = [r["race_id"] for r in resp.json()]
            assert race_a.race_id in race_ids
            assert race_b.race_id not in race_ids, (
                "User A should NOT see user B's race in the list"
            )

    def test_list_ids_returns_only_own_races(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_a = _create_race(db, user_id=test_user.id)
        race_b = _create_race(db, user_id=second_user.id)

        headers = make_auth_header(test_user.id)
        resp = client.get("/race/list_ids", headers=headers)

        if resp.status_code == 200:
            ids = resp.json()
            assert race_a.race_id in ids
            assert race_b.race_id not in ids

    def test_status_returns_404_for_other_users_race(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_b = _create_race(db, user_id=second_user.id)
        headers = make_auth_header(test_user.id)
        resp = client.get(f"/race/{race_b.race_id}/status", headers=headers)
        assert resp.status_code == 404, (
            "Should return 404, not expose that the race exists"
        )

    def test_download_returns_404_for_other_users_race(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_b = _create_race(db, user_id=second_user.id)
        headers = make_auth_header(test_user.id)
        resp = client.get(f"/race/{race_b.race_id}/download", headers=headers)
        assert resp.status_code == 404

    def test_delete_returns_404_for_other_users_race(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_b = _create_race(db, user_id=second_user.id)
        headers = make_auth_header(test_user.id)
        resp = client.delete(f"/race/{race_b.race_id}", headers=headers)
        assert resp.status_code == 404

    def test_compare_returns_404_for_other_users_race(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_b = _create_race(db, user_id=second_user.id)
        _create_lap(db, race_b.race_id, 1)
        _create_lap(db, race_b.race_id, 2)
        headers = make_auth_header(test_user.id)
        resp = client.get(
            f"/race/{race_b.race_id}/compare/1/2", headers=headers
        )
        assert resp.status_code == 404

    def test_fuel_returns_404_for_other_users_race(
        self, client: TestClient, db: Session, test_user: User, second_user: User
    ):
        race_b = _create_race(db, user_id=second_user.id)
        headers = make_auth_header(test_user.id)
        resp = client.get(f"/race/{race_b.race_id}/fuel/1", headers=headers)
        assert resp.status_code == 404


# ===================================================================
# 3. Owner CAN access own races (positive auth tests)
# ===================================================================

class TestOwnerAccess:
    """
    Verify that an authenticated user CAN access their own races.
    These tests only exercise the auth/ownership gate — they may fail
    on downstream S3/storage calls, which is fine for auth testing.
    We check the status code is NOT 401/403/404 (i.e., auth passed).
    """

    def test_owner_can_get_status(
        self, client: TestClient, db: Session, test_user: User
    ):
        race = _create_race(db, user_id=test_user.id)
        headers = make_auth_header(test_user.id)
        resp = client.get(f"/race/{race.race_id}/status", headers=headers)
        # Should be 200 (success) not 401/403/404
        assert resp.status_code == 200

    def test_owner_can_delete_own_race(
        self, client: TestClient, db: Session, test_user: User
    ):
        race = _create_race(db, user_id=test_user.id)
        headers = make_auth_header(test_user.id)
        resp = client.delete(f"/race/{race.race_id}", headers=headers)
        # Should succeed (200) or internal error (500 from S3), not auth error
        assert resp.status_code not in (401, 403, 404)

    def test_nonexistent_race_returns_404_for_authenticated_user(
        self, client: TestClient, test_user: User
    ):
        headers = make_auth_header(test_user.id)
        fake_id = str(uuid.uuid4())
        resp = client.get(f"/race/{fake_id}/status", headers=headers)
        assert resp.status_code == 404


# ===================================================================
# 4. Token edge cases on race endpoints
# ===================================================================

class TestRaceTokenEdgeCases:

    def test_malformed_bearer_token(self, client: TestClient):
        resp = client.get("/race/list", headers={
            "Authorization": "Bearer not-a-jwt"
        })
        assert resp.status_code == 401

    def test_bearer_scheme_missing(self, client: TestClient):
        """Authorization header with wrong scheme should fail."""
        token = create_access_token("user-123")
        resp = client.get("/race/list", headers={
            "Authorization": f"Basic {token}"
        })
        assert resp.status_code in (401, 403)

    def test_token_for_deleted_user(self, client: TestClient):
        """Token referencing a non-existent user_id should return 401."""
        fake_user_id = str(uuid.uuid4())
        token = create_access_token(fake_user_id)
        resp = client.get("/race/list", headers={
            "Authorization": f"Bearer {token}"
        })
        assert resp.status_code == 401
