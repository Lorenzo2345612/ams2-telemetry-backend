"""
RQ Worker tasks for processing race telemetry data.

These tasks run in background workers and handle the heavy processing
of race data including decompression, parsing, resampling, and storage.
"""

import zlib
import uuid
import io
import numpy as np
import asyncio
from models.race_udps import RaceUDPs
from models.database import RaceStatus
from repositories.telemetry_parser import AMS2TelemetryParser
from repositories.telemetry_resampler_repository import AMS2TelemetryResamplerRepository
from repositories.race_repository import RaceRepositoryDB
from service.file_storage_service import S3FileStorageService
from database.db_config import SessionLocal
import os


async def _process_race_data_async(race_id: str, raw_data_s3_path: str):
    """
    Background task to process race telemetry data.

    This task:
    1. Downloads compressed data from S3
    2. Decompresses the data
    3. Parses telemetry data
    4. Resamples the data
    5. Saves each lap as numpy array to S3
    6. Creates lap records in database
    7. Updates race status to Ready or Failed

    Args:
        race_id: The UUID of the race
        raw_data_s3_path: S3 path to the compressed raw data file
    """
    print(f"[Worker] Starting processing for race {race_id} inside async function")
    db = SessionLocal()

    try:
        print(f"[Worker] Initializing services")
        # Initialize services
        storage_service = S3FileStorageService(
            bucket_name=os.getenv("S3_BUCKET_NAME", "ams2-telemetry"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )

        print(f"[Worker] Initializing repositories and parsers")

        repository = RaceRepositoryDB(db)
        parser = AMS2TelemetryParser()
        resampler = AMS2TelemetryResamplerRepository()

        print(f"[Worker] Processing race {race_id}")

        # Download compressed data from S3
        print(f"[Worker] Downloading from S3: {raw_data_s3_path}")
        compressed_data = await storage_service.get_file(raw_data_s3_path)

        # Decompress with zlib
        print(f"[Worker] Decompressing data ({len(compressed_data)} bytes)")
        decompressed_data = zlib.decompress(compressed_data)
        print(f"[Worker] Decompressed to {len(decompressed_data)} bytes")

        # Parse telemetry data
        print(f"[Worker] Parsing telemetry data")
        race_udps = RaceUDPs(file=decompressed_data)
        parsed_data = await parser.parse(race_udps.file)

        # Resample data
        print(f"[Worker] Resampling telemetry data")
        resampled_data = resampler.resample_telemetry_data(parsed_data)
        print(f"[Worker] Found {len(resampled_data)} laps")

        # Save each lap to S3 and create database records
        for lap in resampled_data:
            lap_number = lap['lap_number']
            lap_uuid = str(uuid.uuid4())

            print(f"[Worker] Processing lap {lap_number} (UUID: {lap_uuid})")

            # Convert lap data to numpy array and save to bytes
            np_data = np.array(lap)
            bytes_io = io.BytesIO()
            np.save(bytes_io, np_data)
            np_bytes = bytes_io.getvalue()

            # Upload to S3 with lap UUID as key
            s3_key = f"races/{race_id}/laps/{lap_uuid}.npy"
            processed_path = await storage_service.save_file(
                file_bytes=np_bytes,
                extension=".npy",
                file_key=s3_key
            )

            # Create lap record in database
            await repository.create_lap(
                race_id=race_id,
                lap_number=lap_number,
                lap_uuid=lap_uuid,
                processed_data_path=processed_path
            )

            print(f"[Worker] Lap {lap_number} saved to {processed_path}")

        # Update race status to Ready
        await repository.update_race_status(race_id, RaceStatus.READY)
        print(f"[Worker] Race {race_id} processing completed successfully")

        return {
            "race_id": race_id,
            "status": "Ready",
            "laps_processed": len(resampled_data)
        }

    except Exception as e:
        # Update race status to Failed
        print(f"[Worker] Error processing race {race_id}: {str(e)}")
        try:
            repository = RaceRepositoryDB(db)
            await repository.update_race_status(race_id, RaceStatus.FAILED)
        except Exception as db_error:
            print(f"[Worker] Failed to update race status: {str(db_error)}")

        raise e

    finally:
        db.close()


def process_race_data(race_id: str, raw_data_s3_path: str):
    """
    Synchronous wrapper for RQ to call the async processing function.

    Args:
        race_id: The UUID of the race
        raw_data_s3_path: S3 path to the compressed raw data file
    """
    print(f"[Worker] Starting processing for race {race_id}")
    return asyncio.run(_process_race_data_async(race_id, raw_data_s3_path))
