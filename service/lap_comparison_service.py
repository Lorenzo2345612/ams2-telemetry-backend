"""
Service for comparing two laps and calculating delta time analysis.
"""

import numpy as np
from scipy.interpolate import interp1d
from typing import List, Tuple
from service.file_storage_service import FileStorageService
from models.lap_comparison import (
    LapComparisonResponse,
    LapSummary,
    TelemetryTimeSeries,
    DeltaTimeSeries,
)
import io


class LapComparisonService:
    """Service for comparing two laps and generating delta time analysis data."""

    def __init__(self, storage_service: FileStorageService):
        self.storage_service = storage_service

    async def load_lap_from_s3(self, s3_path: str) -> Tuple[List[dict], float]:
        """
        Load lap data from S3 storage.

        Args:
            s3_path: S3 path to the .npy file

        Returns:
            Tuple of (lap_data, lap_time)
        """
        # Download file from S3
        npy_bytes = await self.storage_service.get_file(s3_path)

        # Load numpy data
        bytes_io = io.BytesIO(npy_bytes)
        lap_data = np.load(bytes_io, allow_pickle=True).item()

        return lap_data["data"], lap_data.get("lap_time", 0)

    def calculate_delta_time(
        self, lap_1_data: List[dict], lap_2_data: List[dict]
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate delta time using lap_distance from the game.

        Args:
            lap_1_data: Reference lap data
            lap_2_data: Comparison lap data

        Returns:
            Tuple of (common_distances, delta_time)
        """
        # Extract lap_distance and time
        lap_1_distance = np.array([p["lap_distance"] for p in lap_1_data])
        lap_2_distance = np.array([p["lap_distance"] for p in lap_2_data])

        lap_1_time = np.array([p["current_time"] for p in lap_1_data])
        lap_2_time = np.array([p["current_time"] for p in lap_2_data])

        # Create interpolation functions
        interp_lap_1 = interp1d(
            lap_1_distance, lap_1_time, kind="linear", fill_value="extrapolate"
        )
        interp_lap_2 = interp1d(
            lap_2_distance, lap_2_time, kind="linear", fill_value="extrapolate"
        )

        # Common distance grid
        max_distance = min(lap_1_distance.max(), lap_2_distance.max())
        common_distances = np.linspace(0, max_distance, 1000)

        # Interpolate times
        time_lap_1_at_common = interp_lap_1(common_distances)
        time_lap_2_at_common = interp_lap_2(common_distances)

        # Delta time
        delta_time = time_lap_2_at_common - time_lap_1_at_common

        return common_distances, delta_time

    def interpolate_telemetry(
        self, lap_data: List[dict], common_distances: np.ndarray, field: str
    ) -> np.ndarray:
        """
        Interpolate a telemetry field to common distance grid.

        Args:
            lap_data: Lap data
            common_distances: Common distance grid
            field: Field name to interpolate (e.g., 'speed', 'throttle')

        Returns:
            Interpolated values
        """
        distances = np.array([p["lap_distance"] for p in lap_data])
        values = np.array([p[field] for p in lap_data])

        interp_func = interp1d(
            distances, values, kind="linear", fill_value="extrapolate"
        )

        return interp_func(common_distances)

    async def compare_laps(
        self, lap_1_s3_path: str, lap_2_s3_path: str
    ) -> LapComparisonResponse:
        """
        Compare two laps and return all comparison data.

        Args:
            lap_1_s3_path: S3 path to reference lap
            lap_2_s3_path: S3 path to comparison lap

        Returns:
            LapComparisonResponse with all comparison data
        """
        # Load laps from S3
        lap_1_data, lap_1_total_time = await self.load_lap_from_s3(lap_1_s3_path)
        lap_2_data, lap_2_total_time = await self.load_lap_from_s3(lap_2_s3_path)

        # Calculate delta time
        common_distances, delta_time = self.calculate_delta_time(lap_1_data, lap_2_data)

        # Interpolate telemetry fields
        speed_1 = self.interpolate_telemetry(lap_1_data, common_distances, "speed") * 3.6  # Convert to km/h
        speed_2 = self.interpolate_telemetry(lap_2_data, common_distances, "speed") * 3.6

        throttle_1 = self.interpolate_telemetry(lap_1_data, common_distances, "throttle")
        throttle_2 = self.interpolate_telemetry(lap_2_data, common_distances, "throttle")

        brake_1 = self.interpolate_telemetry(lap_1_data, common_distances, "brake")
        brake_2 = self.interpolate_telemetry(lap_2_data, common_distances, "brake")

        steering_1 = self.interpolate_telemetry(lap_1_data, common_distances, "steering")
        steering_2 = self.interpolate_telemetry(lap_2_data, common_distances, "steering")

        # Build response using Pydantic models with from methods
        return LapComparisonResponse(
            summary=LapSummary.from_data(
                lap_1_time=lap_1_total_time,
                lap_2_time=lap_2_total_time,
                common_distances=common_distances,
                delta_time=delta_time,
                lap_1_data=lap_1_data,
                lap_2_data=lap_2_data,
            ),
            delta_time=DeltaTimeSeries.from_arrays(
                distance=common_distances,
                delta=delta_time,
            ),
            speed=TelemetryTimeSeries.from_arrays(
                distance=common_distances,
                lap_1_values=speed_1,
                lap_2_values=speed_2,
            ),
            throttle=TelemetryTimeSeries.from_arrays(
                distance=common_distances,
                lap_1_values=throttle_1,
                lap_2_values=throttle_2,
            ),
            brake=TelemetryTimeSeries.from_arrays(
                distance=common_distances,
                lap_1_values=brake_1,
                lap_2_values=brake_2,
            ),
            steering=TelemetryTimeSeries.from_arrays(
                distance=common_distances,
                lap_1_values=steering_1,
                lap_2_values=steering_2,
            ),
        )
