from fastapi import APIRouter, Depends
from fastapi.responses import Response
from models.race_udps import RaceRequest
from models.database import RaceStatus
from models.lap_comparison import LapComparisonResponse
from models.fuel_analysis import SingleLapFuelResponse, FuelComparisonResponse
from dependencies.race_dependencies import get_file_storage_service
from database.db_config import get_db
from repositories.race_repository import RaceRepositoryDB
from rq_config.redis_config import get_race_queue
from workers.race_worker import process_race_data
from service.lap_comparison_service import LapComparisonService
from service.fuel_analysis_service import FuelAnalysisService
from sqlalchemy.orm import Session
from typing import List
from base64 import b64decode, b64encode
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


@router.get("/list")
async def list_races(db: Session = Depends(get_db)) -> List[dict]:
    """
    List all races with their details.

    Returns:
        List of race objects with id, status, created_at, laps_count
    """
    repository = RaceRepositoryDB(db)
    races = await repository.list_races()

    return [
        {
            "race_id": race.race_id,
            "status": race.status.value,
            "created_at": race.created_at.isoformat(),
            "updated_at": race.updated_at.isoformat(),
            "laps_count": len(race.laps) if race.laps else 0,
            "raw_data_path": race.raw_data_path,
        }
        for race in races
    ]


@router.get("/{race_id}/download")
async def download_race_data(race_id: str, db: Session = Depends(get_db)):
    """
    Download the raw compressed data for a race.

    Returns:
        Base64 encoded raw data with metadata
    """
    repository = RaceRepositoryDB(db)
    storage_service = get_file_storage_service()

    # Get race to ensure it exists
    race = await repository.get_race_status(race_id)
    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    if not race.raw_data_path:
        raise HTTPException(status_code=404, detail=f"No raw data available for race {race_id}")

    try:
        # Download raw data from S3
        raw_bytes = await storage_service.get_file(race.raw_data_path)

        # Return as base64 encoded string with metadata
        return {
            "race_id": race_id,
            "status": race.status.value,
            "size_bytes": len(raw_bytes),
            "data": b64encode(raw_bytes).decode('utf-8'),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error downloading race data: {str(e)}"
        )


@router.get("/{race_id}/download/raw")
async def download_race_data_raw(race_id: str, db: Session = Depends(get_db)):
    """
    Download the raw compressed data for a race as binary file.

    Returns:
        Binary file response with the raw deflate data
    """
    repository = RaceRepositoryDB(db)
    storage_service = get_file_storage_service()

    # Get race to ensure it exists
    race = await repository.get_race_status(race_id)
    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    if not race.raw_data_path:
        raise HTTPException(status_code=404, detail=f"No raw data available for race {race_id}")

    try:
        # Download raw data from S3
        raw_bytes = await storage_service.get_file(race.raw_data_path)

        # Return as binary file download
        return Response(
            content=raw_bytes,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename=race_{race_id}.deflate"
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error downloading race data: {str(e)}"
        )


@router.delete("/{race_id}")
async def delete_race(race_id: str, db: Session = Depends(get_db)):
    """
    Delete a race and all associated data.

    Returns:
        Confirmation message
    """
    repository = RaceRepositoryDB(db)

    # Get race to ensure it exists
    race = await repository.get_race_status(race_id)
    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    try:
        await repository.delete_race(race_id)
        return {"message": f"Race {race_id} deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting race: {str(e)}"
        )


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

@router.get("/{race_id}/compare/{lap_1_number}/{lap_2_number}", response_model=LapComparisonResponse)
async def compare_laps(
    race_id: str,
    lap_1_number: int,
    lap_2_number: int,
    db: Session = Depends(get_db)
) -> LapComparisonResponse:
    """
    Compare two laps from the same race and return delta time analysis.

    Args:
        race_id: The UUID of the race
        lap_1_number: Reference lap number
        lap_2_number: Comparison lap number

    Returns:
        Complete lap comparison data including:
        - Summary statistics (lap times, delta min/max, speeds)
        - Delta time series
        - Speed, throttle, brake, and steering comparisons
    """
    repository = RaceRepositoryDB(db)
    storage_service = get_file_storage_service()

    # Get race status without loading all laps
    race = await repository.get_race_status(race_id)
    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    if race.status != RaceStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Race is not ready for comparison. Current status: {race.status.value}"
        )

    # Get only the specific laps needed
    laps = await repository.get_laps_by_numbers(race_id, [lap_1_number, lap_2_number])
    lap_1 = next((lap for lap in laps if lap.lap_number == lap_1_number), None)
    lap_2 = next((lap for lap in laps if lap.lap_number == lap_2_number), None)

    if not lap_1:
        raise HTTPException(
            status_code=404,
            detail=f"Lap {lap_1_number} not found in race {race_id}"
        )

    if not lap_2:
        raise HTTPException(
            status_code=404,
            detail=f"Lap {lap_2_number} not found in race {race_id}"
        )

    # Create comparison service and compare laps
    comparison_service = LapComparisonService(storage_service)

    try:
        comparison_data = await comparison_service.compare_laps(
            lap_1_s3_path=lap_1.processed_data_path,
            lap_2_s3_path=lap_2.processed_data_path
        )
        return comparison_data
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error comparing laps: {str(e)}"
        )

@router.get("/{race_id}/fuel/{lap_number}", response_model=SingleLapFuelResponse)
async def analyze_lap_fuel(
    race_id: str,
    lap_number: int,
    db: Session = Depends(get_db)
) -> SingleLapFuelResponse:
    """
    Analyze fuel consumption for a single lap.

    Args:
        race_id: The UUID of the race
        lap_number: The lap number to analyze

    Returns:
        Complete fuel analysis including:
        - Summary (fuel used, consumption rates, estimated laps remaining)
        - Fuel remaining curve over lap distance
    """
    repository = RaceRepositoryDB(db)
    storage_service = get_file_storage_service()

    # Get race status without loading all laps
    race = await repository.get_race_status(race_id)
    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    if race.status != RaceStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Race is not ready for analysis. Current status: {race.status.value}"
        )

    # Get only the specific lap needed
    lap = await repository.get_lap_by_number(race_id, lap_number)

    if not lap:
        raise HTTPException(
            status_code=404,
            detail=f"Lap {lap_number} not found in race {race_id}"
        )

    # Create fuel analysis service and analyze
    fuel_service = FuelAnalysisService(storage_service)

    try:
        analysis_data = await fuel_service.analyze_single_lap(
            lap_s3_path=lap.processed_data_path
        )
        return analysis_data
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing fuel: {str(e)}"
        )

@router.get("/{race_id}/fuel/compare/{lap_1_number}/{lap_2_number}", response_model=FuelComparisonResponse)
async def compare_lap_fuel(
    race_id: str,
    lap_1_number: int,
    lap_2_number: int,
    db: Session = Depends(get_db)
) -> FuelComparisonResponse:
    """
    Compare fuel consumption between two laps.

    Args:
        race_id: The UUID of the race
        lap_1_number: Reference lap number
        lap_2_number: Comparison lap number

    Returns:
        Complete fuel comparison including:
        - Summary (fuel used per lap, consumption rates, deltas, more efficient lap)
        - Fuel consumption delta series over distance
        - Fuel remaining curves for both laps
    """
    repository = RaceRepositoryDB(db)
    storage_service = get_file_storage_service()

    # Get race status without loading all laps
    race = await repository.get_race_status(race_id)
    if not race:
        raise HTTPException(status_code=404, detail=f"Race {race_id} not found")

    if race.status != RaceStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Race is not ready for comparison. Current status: {race.status.value}"
        )

    # Get only the specific laps needed
    laps = await repository.get_laps_by_numbers(race_id, [lap_1_number, lap_2_number])
    lap_1 = next((lap for lap in laps if lap.lap_number == lap_1_number), None)
    lap_2 = next((lap for lap in laps if lap.lap_number == lap_2_number), None)

    if not lap_1:
        raise HTTPException(
            status_code=404,
            detail=f"Lap {lap_1_number} not found in race {race_id}"
        )

    if not lap_2:
        raise HTTPException(
            status_code=404,
            detail=f"Lap {lap_2_number} not found in race {race_id}"
        )

    # Create fuel analysis service and compare
    fuel_service = FuelAnalysisService(storage_service)

    try:
        comparison_data = await fuel_service.compare_fuel(
            lap_1_s3_path=lap_1.processed_data_path,
            lap_2_s3_path=lap_2.processed_data_path
        )
        return comparison_data
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error comparing fuel: {str(e)}"
        )