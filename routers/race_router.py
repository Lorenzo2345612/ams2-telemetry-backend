from fastapi import APIRouter, Depends
from models.race_udps import RaceRequest
from models.database import RaceStatus
from dependencies.race_dependencies import get_file_storage_service
from database.db_config import get_db
from repositories.race_repository import RaceRepositoryDB
from rq_config.redis_config import get_race_queue
from workers.race_worker import process_race_data
from sqlalchemy.orm import Session
from typing import List
from base64 import b64decode
from fastapi import HTTPException
import uuid

router = APIRouter(prefix="/race", tags=["race"])

@router.post("/upload")
async def upload_race_data(request: RaceRequest, db: Session = Depends(get_db)):
    """
    Upload race telemetry data.

    Flow:
    1. Decode base64 compressed data
    2. Save compressed file to S3
    3. Create Race record with 'Processing' status
    4. Enqueue background job to process the data
    5. Return immediately with race_id and Processing status

    The background worker will:
    - Download and decompress the data
    - Parse and resample telemetry
    - Save lap data to S3
    - Create Lap records
    - Update Race status to 'Ready' or 'Failed'
    """
    race_id = str(uuid.uuid4())

    try:
        file_service = get_file_storage_service()
        repository = RaceRepositoryDB(db)
        race_queue = get_race_queue()

        # Decode from base64
        compressed_data = b64decode(request.data)

        print(f"[API] Received: {len(compressed_data)} bytes for race {race_id}")

        # Save compressed file to S3
        s3_key = f"races/{race_id}/raw_data.deflate"
        raw_data_path = await file_service.save_file(
            file_bytes=compressed_data,
            extension=".deflate",
            file_key=s3_key
        )

        print(f"[API] Saved compressed data to: {raw_data_path}")

        # Create Race record in Processing status
        await repository.create_race(race_id=race_id, raw_data_path=raw_data_path)

        print(f"[API] Created race record with Processing status")

        # Enqueue background job for processing
        job = race_queue.enqueue(
            process_race_data,
            race_id=race_id,
            raw_data_s3_path=raw_data_path,
            job_timeout='30m'  # 30 minute timeout for large races
        )

        print(f"[API] Enqueued job {job.id} for race {race_id}")

        return {
            "race_id": race_id,
            "status": "Processing",
            "job_id": job.id,
            "message": "Race data uploaded successfully. Processing in background."
        }

    except Exception as e:
        print(f"[API] Error: {str(e)}")
        # Try to update race status to Failed if it was created
        try:
            repository = RaceRepositoryDB(db)
            await repository.update_race_status(race_id, RaceStatus.FAILED)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list_ids")
async def list_race_ids(db: Session = Depends(get_db)) -> List[str]:
    """List all race IDs from the database."""
    repository = RaceRepositoryDB(db)
    return await repository.list_race_ids()

@router.get("/{race_id}/status")
async def get_race_status(race_id: str, db: Session = Depends(get_db)):
    """
    Get the current status of a race.

    Returns:
        Race information including status (Processing, Ready, or Failed)
    """
    repository = RaceRepositoryDB(db)
    race = await repository.get_race(race_id)

    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    return {
        "race_id": race.race_id,
        "status": race.status.value,
        "created_at": race.created_at.isoformat(),
        "updated_at": race.updated_at.isoformat(),
        "raw_data_path": race.raw_data_path,
        "laps_count": len(race.laps) if race.laps else 0
    }