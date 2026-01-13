from abc import ABC, abstractmethod
from models.race_udps import RaceUDPs
from models.database import Race, Lap, RaceStatus
from sqlalchemy.orm import Session
import json
import uuid
import os
from typing import List, Optional
import numpy as np

class RaceRepository(ABC):
    @abstractmethod
    async def save_race_data(self, data: list[dict]):
        pass

    @abstractmethod
    async def list_race_ids(self) -> List[str]:
        pass

    @abstractmethod
    async def create_race(self, race_id: str, raw_data_path: Optional[str] = None) -> Race:
        pass

    @abstractmethod
    async def update_race_status(self, race_id: str, status: RaceStatus) -> Race:
        pass

    @abstractmethod
    async def get_race(self, race_id: str) -> Optional[Race]:
        pass

    @abstractmethod
    async def create_lap(self, race_id: str, lap_number: int, lap_uuid: str, raw_data_path: Optional[str] = None, processed_data_path: Optional[str] = None) -> Lap:
        pass

    @abstractmethod
    async def get_race_status(self, race_id: str) -> Optional[Race]:
        """Get race without loading laps relationship."""
        pass

    @abstractmethod
    async def get_lap_by_number(self, race_id: str, lap_number: int) -> Optional[Lap]:
        """Get a specific lap by race_id and lap_number."""
        pass

    @abstractmethod
    async def get_laps_by_numbers(self, race_id: str, lap_numbers: List[int]) -> List[Lap]:
        """Get multiple laps by race_id and lap_numbers."""
        pass

class RaceRepositoryDB(RaceRepository):
    """SQLAlchemy-based race repository for PostgreSQL."""

    def __init__(self, db: Session):
        self.db = db
        self.path = "race_data"  # Base path for file storage

    async def create_race(self, race_id: str, raw_data_path: Optional[str] = None) -> Race:
        """Create a new race record in Processing status."""
        race = Race(
            race_id=race_id,
            status=RaceStatus.PROCESSING,
            raw_data_path=raw_data_path
        )
        self.db.add(race)
        self.db.commit()
        self.db.refresh(race)
        return race

    async def update_race_status(self, race_id: str, status: RaceStatus) -> Race:
        """Update the status of a race."""
        race = self.db.query(Race).filter(Race.race_id == race_id).first()
        if not race:
            raise ValueError(f"Race {race_id} not found")

        race.status = status
        self.db.commit()
        self.db.refresh(race)
        return race

    async def get_race(self, race_id: str) -> Optional[Race]:
        """Get a race by ID."""
        return self.db.query(Race).filter(Race.race_id == race_id).first()

    async def get_race_status(self, race_id: str) -> Optional[Race]:
        """Get race without loading laps relationship (only status info)."""
        return self.db.query(Race).filter(Race.race_id == race_id).first()

    async def get_lap_by_number(self, race_id: str, lap_number: int) -> Optional[Lap]:
        """Get a specific lap by race_id and lap_number."""
        return self.db.query(Lap).filter(
            Lap.race_id == race_id,
            Lap.lap_number == lap_number
        ).first()

    async def get_laps_by_numbers(self, race_id: str, lap_numbers: List[int]) -> List[Lap]:
        """Get multiple laps by race_id and lap_numbers."""
        return self.db.query(Lap).filter(
            Lap.race_id == race_id,
            Lap.lap_number.in_(lap_numbers)
        ).all()

    async def create_lap(self, race_id: str, lap_number: int, lap_uuid: str, raw_data_path: Optional[str] = None, processed_data_path: Optional[str] = None) -> Lap:
        """Create a new lap record."""
        lap = Lap(
            lap_uuid=lap_uuid,
            race_id=race_id,
            lap_number=lap_number,
            raw_data_path=raw_data_path,
            processed_data_path=processed_data_path
        )
        self.db.add(lap)
        self.db.commit()
        self.db.refresh(lap)
        return lap

    async def save_race_data(self, data: list[dict]):
        """Save race data (laps) to files and database."""
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        race_id = str(uuid.uuid4())

        for lap in data:
            # Save data as numpy array for faster loading
            np_data = np.array(lap)
            processed_path = os.path.join(self.path, f"race_{race_id}_lap_{lap['lap_number']}.npy")
            np.save(processed_path, np_data)

            # Create lap record in database
            await self.create_lap(
                race_id=race_id,
                lap_number=lap['lap_number'],
                processed_data_path=processed_path
            )

        return race_id

    async def list_race_ids(self) -> List[str]:
        """List all race IDs from database."""
        races = self.db.query(Race.race_id).all()
        return [race.race_id for race in races]

class RaceRepositoryMock(RaceRepository):
    path = "mock_race_data"

    async def create_race(self, race_id: str, raw_data_path: Optional[str] = None) -> Race:
        # Mock implementation - just return a race object
        return Race(race_id=race_id, status=RaceStatus.PROCESSING, raw_data_path=raw_data_path)

    async def update_race_status(self, race_id: str, status: RaceStatus) -> Race:
        # Mock implementation
        return Race(race_id=race_id, status=status)

    async def get_race(self, race_id: str) -> Optional[Race]:
        # Mock implementation
        return Race(race_id=race_id, status=RaceStatus.READY)

    async def get_race_status(self, race_id: str) -> Optional[Race]:
        # Mock implementation
        return Race(race_id=race_id, status=RaceStatus.READY)

    async def get_lap_by_number(self, race_id: str, lap_number: int) -> Optional[Lap]:
        # Mock implementation
        return Lap(lap_uuid=str(uuid.uuid4()), race_id=race_id, lap_number=lap_number)

    async def get_laps_by_numbers(self, race_id: str, lap_numbers: List[int]) -> List[Lap]:
        # Mock implementation
        return [Lap(lap_uuid=str(uuid.uuid4()), race_id=race_id, lap_number=ln) for ln in lap_numbers]

    async def create_lap(self, race_id: str, lap_number: int, lap_uuid: str, raw_data_path: Optional[str] = None, processed_data_path: Optional[str] = None) -> Lap:
        # Mock implementation
        return Lap(lap_uuid=lap_uuid, race_id=race_id, lap_number=lap_number, raw_data_path=raw_data_path, processed_data_path=processed_data_path)

    async def save_race_data(self, data: list[dict]):
        # Save data in a json file for testing purposes
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        race_id = str(uuid.uuid4())

        for lap in data:
            # Save data as numpy array for faster loading
            np_data = np.array(lap)
            np.save(os.path.join(self.path, f"race_{race_id}_lap_{lap['lap_number']}.npy"), np_data)
        return race_id

    async def list_race_ids(self) -> List[str]:
        # Return a mock list of race IDs
        if not os.path.exists(self.path):
            return []
        files = os.listdir(self.path)
        race_ids = [f.split("_")[1].split(".")[0] for f in files if f.startswith("race_")]
        return race_ids


