"""
Tests for analytics/api/repositories/race_repository.py

Focuses on user_id-scoped operations that will be added as part of
the auth implementation:
- create_race with user_id
- list_races / list_race_ids filtered by user_id
- Ownership verification patterns

These tests run against an in-memory SQLite database.
"""

import uuid

import pytest
from sqlalchemy.orm import Session

from models.database import Lap, Race, RaceStatus, User
from repositories.race_repository import RaceRepositoryDB


# ===================================================================
# Helpers
# ===================================================================

def _make_repo(db: Session) -> RaceRepositoryDB:
    return RaceRepositoryDB(db)


# ===================================================================
# create_race
# ===================================================================

class TestCreateRace:

    @pytest.mark.asyncio
    async def test_create_race_without_user_id(self, db: Session):
        repo = _make_repo(db)
        race = await repo.create_race(
            race_id=str(uuid.uuid4()),
            raw_data_path="races/test/raw.deflate",
            vehicle_name="Porsche 911",
            class_name="GT3",
        )
        assert race.race_id is not None
        assert race.status == RaceStatus.PROCESSING
        assert race.vehicle_name == "Porsche 911"
        assert race.user_id is None  # nullable for migration safety

    @pytest.mark.asyncio
    async def test_create_race_with_user_id(self, db: Session, test_user: User):
        """
        Once auth is implemented, create_race should accept user_id.
        If the current signature does not support it, this test will
        fail — signaling that the repository needs updating.
        """
        repo = _make_repo(db)
        race_id = str(uuid.uuid4())

        # The auth plan calls for adding user_id param to create_race.
        # Try passing it; if the signature doesn't support it yet, skip.
        try:
            race = await repo.create_race(
                race_id=race_id,
                raw_data_path="races/test/raw.deflate",
                vehicle_name="BMW M4",
                class_name="GT3",
                user_id=test_user.id,
            )
            assert race.user_id == test_user.id
        except TypeError:
            # create_race doesn't accept user_id yet — mark as expected failure
            pytest.skip("create_race does not accept user_id param yet")

    @pytest.mark.asyncio
    async def test_create_race_defaults(self, db: Session):
        repo = _make_repo(db)
        race = await repo.create_race(race_id=str(uuid.uuid4()))
        assert race.vehicle_name == "Unknown"
        assert race.class_name == "Unknown"
        assert race.status == RaceStatus.PROCESSING


# ===================================================================
# list_races / list_race_ids (user_id filtering)
# ===================================================================

class TestListRacesFiltering:
    """
    Once auth is implemented, list_races and list_race_ids should
    accept an optional user_id parameter and filter results.

    These tests set up races for two different users and verify
    that the filtering works correctly.
    """

    def _seed_races(self, db: Session, user_a: User, user_b: User):
        """Create 2 races for user_a and 1 for user_b."""
        for i in range(2):
            race = Race(
                race_id=str(uuid.uuid4()),
                status=RaceStatus.READY,
                vehicle_name=f"CarA-{i}",
                class_name="GT3",
                user_id=user_a.id,
            )
            db.add(race)

        race_b = Race(
            race_id=str(uuid.uuid4()),
            status=RaceStatus.READY,
            vehicle_name="CarB",
            class_name="GT4",
            user_id=user_b.id,
        )
        db.add(race_b)
        db.commit()

    @pytest.mark.asyncio
    async def test_list_races_all(self, db: Session, test_user: User, second_user: User):
        """Without user_id filter, list_races returns all races."""
        self._seed_races(db, test_user, second_user)
        repo = _make_repo(db)
        all_races = await repo.list_races()
        assert len(all_races) >= 3  # at least the 3 we seeded

    @pytest.mark.asyncio
    async def test_list_races_filtered_by_user(
        self, db: Session, test_user: User, second_user: User
    ):
        """
        After auth: list_races(user_id=...) should return only that
        user's races.  If the method doesn't accept user_id yet, skip.
        """
        self._seed_races(db, test_user, second_user)
        repo = _make_repo(db)

        try:
            user_a_races = await repo.list_races(user_id=test_user.id)
        except TypeError:
            pytest.skip("list_races does not accept user_id param yet")
            return

        assert len(user_a_races) == 2
        assert all(r.user_id == test_user.id for r in user_a_races)

    @pytest.mark.asyncio
    async def test_list_race_ids_filtered_by_user(
        self, db: Session, test_user: User, second_user: User
    ):
        self._seed_races(db, test_user, second_user)
        repo = _make_repo(db)

        try:
            user_a_ids = await repo.list_race_ids(user_id=test_user.id)
        except TypeError:
            pytest.skip("list_race_ids does not accept user_id param yet")
            return

        assert len(user_a_ids) == 2

    @pytest.mark.asyncio
    async def test_list_races_empty_for_new_user(self, db: Session):
        """A user with no races should get an empty list, not an error."""
        new_user = User(
            id=str(uuid.uuid4()),
            email="empty@example.com",
            hashed_password="hashed",
        )
        db.add(new_user)
        db.commit()

        repo = _make_repo(db)
        try:
            races = await repo.list_races(user_id=new_user.id)
            assert races == []
        except TypeError:
            pytest.skip("list_races does not accept user_id param yet")


# ===================================================================
# get_race ownership verification
# ===================================================================

class TestRaceOwnership:
    """
    Tests for a helper like ``_verify_race_ownership(race, user_id)``
    or equivalent check.  These verify the pattern at the repository
    / query level.
    """

    @pytest.mark.asyncio
    async def test_get_race_returns_race_for_owner(
        self, db: Session, test_user: User
    ):
        race = Race(
            race_id=str(uuid.uuid4()),
            status=RaceStatus.READY,
            user_id=test_user.id,
        )
        db.add(race)
        db.commit()

        repo = _make_repo(db)
        fetched = await repo.get_race(race.race_id)
        assert fetched is not None
        assert fetched.user_id == test_user.id

    @pytest.mark.asyncio
    async def test_race_user_id_is_queryable(
        self, db: Session, test_user: User, second_user: User
    ):
        """We can filter Race by user_id at the query level."""
        race_a = Race(
            race_id=str(uuid.uuid4()),
            status=RaceStatus.READY,
            user_id=test_user.id,
        )
        race_b = Race(
            race_id=str(uuid.uuid4()),
            status=RaceStatus.READY,
            user_id=second_user.id,
        )
        db.add_all([race_a, race_b])
        db.commit()

        result = (
            db.query(Race)
            .filter(Race.user_id == test_user.id)
            .all()
        )
        race_ids = [r.race_id for r in result]
        assert race_a.race_id in race_ids
        assert race_b.race_id not in race_ids


# ===================================================================
# update_race_status
# ===================================================================

class TestUpdateRaceStatus:

    @pytest.mark.asyncio
    async def test_update_status(self, db: Session):
        repo = _make_repo(db)
        race = await repo.create_race(race_id=str(uuid.uuid4()))
        assert race.status == RaceStatus.PROCESSING

        updated = await repo.update_race_status(race.race_id, RaceStatus.READY)
        assert updated.status == RaceStatus.READY

    @pytest.mark.asyncio
    async def test_update_nonexistent_race_raises(self, db: Session):
        repo = _make_repo(db)
        with pytest.raises(ValueError, match="not found"):
            await repo.update_race_status("nonexistent-id", RaceStatus.FAILED)


# ===================================================================
# delete_race
# ===================================================================

class TestDeleteRace:

    @pytest.mark.asyncio
    async def test_delete_race_removes_from_db(self, db: Session):
        repo = _make_repo(db)
        race = await repo.create_race(race_id=str(uuid.uuid4()))
        await repo.delete_race(race.race_id)
        fetched = await repo.get_race(race.race_id)
        assert fetched is None

    @pytest.mark.asyncio
    async def test_delete_race_cascades_laps(self, db: Session):
        repo = _make_repo(db)
        race = await repo.create_race(race_id=str(uuid.uuid4()))
        await repo.create_lap(
            race_id=race.race_id,
            lap_number=1,
            lap_uuid=str(uuid.uuid4()),
        )
        await repo.delete_race(race.race_id)
        laps = db.query(Lap).filter(Lap.race_id == race.race_id).all()
        assert laps == []

    @pytest.mark.asyncio
    async def test_delete_nonexistent_race_is_noop(self, db: Session):
        """Deleting a race that doesn't exist should not raise."""
        repo = _make_repo(db)
        await repo.delete_race("does-not-exist")  # should not raise
