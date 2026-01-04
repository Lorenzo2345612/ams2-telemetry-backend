from functools import lru_cache
from repositories.telemetry_resampler_repository import AMS2TelemetryResamplerRepository
from repositories.race_repository import RaceRepositoryDB, RaceRepositoryMock
from repositories.telemetry_parser import AMS2TelemetryParser
from service.race_service import RaceServiceImpl
from service.file_storage_service import MockFileStorageService, S3FileStorageService, FileStorageService
from sqlalchemy.orm import Session
import os

def get_race_service(db: Session) -> RaceServiceImpl:
    """
    Get race service with database-backed repository and S3 storage.

    Args:
        db: SQLAlchemy database session (injected via FastAPI Depends)

    Returns:
        RaceServiceImpl instance
    """
    resampler = AMS2TelemetryResamplerRepository()
    repository = RaceRepositoryDB(db)
    telemetry_parser = AMS2TelemetryParser()
    storage_service = get_lap_storage_service()
    service = RaceServiceImpl(repository, telemetry_parser, resampler, storage_service)
    return service

@lru_cache()
def get_file_storage_service() -> FileStorageService:
    """
    Get S3 file storage service for raw race data (compressed files).
    """
    return S3FileStorageService(
        bucket_name=os.getenv("S3_BUCKET_NAME", "ams2-telemetry"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

@lru_cache()
def get_lap_storage_service() -> FileStorageService:
    """
    Get S3 storage service for processed lap data (numpy arrays).
    """
    return S3FileStorageService(
        bucket_name=os.getenv("S3_BUCKET_NAME", "ams2-telemetry"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )