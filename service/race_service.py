from abc import ABC, abstractmethod
from models.race_udps import RaceUDPs
from models.database import RaceStatus
from repositories.telemetry_resampler_repository import TelemetryResamplerRepository
from repositories.race_repository import RaceRepository
from repositories.telemetry_parser import TelemetryParser
from service.file_storage_service import FileStorageService
from typing import List, Optional
import uuid
import io
import numpy as np

class RaceService(ABC):
    def __init__(self, repository: RaceRepository, parser: TelemetryParser, resampler: TelemetryResamplerRepository, storage_service: FileStorageService):
        self.repository = repository
        self.parser = parser
        self.resampler = resampler
        self.storage_service = storage_service

    @abstractmethod
    async def upload_race_data(self, data: RaceUDPs, race_id: Optional[str] = None, raw_data_path: Optional[str] = None):
        pass

    @abstractmethod
    async def list_race_ids(self) -> List[str]:
        pass

class RaceServiceImpl(RaceService):
    async def upload_race_data(self, data: RaceUDPs, race_id: Optional[str] = None, raw_data_path: Optional[str] = None):
        """
        Upload race data with database tracking and S3 storage.

        Args:
            data: RaceUDPs containing the raw telemetry bytes
            race_id: Optional pre-generated race_id (will be created if not provided)
            raw_data_path: Optional path to the raw compressed data file

        Returns:
            race_id: The ID of the uploaded race
        """
        # Generate race_id if not provided
        if not race_id:
            race_id = str(uuid.uuid4())

        try:
            # Create race record in Processing status
            await self.repository.create_race(race_id=race_id, raw_data_path=raw_data_path)

            # Parse and resample telemetry data
            parsed_data = await self.parser.parse(data.file)
            resampled_data = self.resampler.resample_telemetry_data(parsed_data)

            # Save lap data to S3 and create lap records
            for lap in resampled_data:
                lap_number = lap['lap_number']
                lap_uuid = str(uuid.uuid4())

                # Convert lap data to numpy array and save to bytes
                np_data = np.array(lap)
                bytes_io = io.BytesIO()
                np.save(bytes_io, np_data)
                np_bytes = bytes_io.getvalue()

                # Upload to S3 with lap UUID as key
                s3_key = f"races/{race_id}/laps/{lap_uuid}.npy"
                processed_path = await self.storage_service.save_file(
                    file_bytes=np_bytes,
                    extension=".npy",
                    file_key=s3_key
                )

                # Create lap record in database
                await self.repository.create_lap(
                    race_id=race_id,
                    lap_number=lap_number,
                    lap_uuid=lap_uuid,
                    processed_data_path=processed_path
                )

            # Update race status to Ready
            await self.repository.update_race_status(race_id, RaceStatus.READY)

            return race_id

        except Exception as e:
            # Update race status to Failed
            try:
                await self.repository.update_race_status(race_id, RaceStatus.FAILED)
            except:
                pass  # Race might not exist yet
            raise e

    async def list_race_ids(self) -> List[str]:
        return await self.repository.list_race_ids()

