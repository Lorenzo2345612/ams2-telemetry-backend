"""
Service for fuel consumption analysis on single and multiple laps.
"""

import io
import numpy as np
from scipy.interpolate import interp1d
from typing import List, Tuple
from service.file_storage_service import FileStorageService
from models.fuel_analysis import (
    SingleLapFuelResponse,
    FuelSummary,
    FuelCurve,
    FuelSpeedScatter,
    FuelThrottleScatter,
    FuelComparisonResponse,
    FuelComparisonSummary,
    FuelDeltaSeries,
    FuelComparisonCurves,
)


class FuelAnalysisService:
    """Service for analyzing fuel consumption in lap telemetry data."""

    def __init__(self, storage_service: FileStorageService):
        self.storage_service = storage_service

    async def load_lap_from_s3(self, s3_path: str) -> Tuple[List[dict], float, int]:
        """
        Load lap data from S3 storage.

        Args:
            s3_path: S3 path to the .npy file

        Returns:
            Tuple of (lap_data, lap_time, lap_number)
        """
        # Download file from S3
        npy_bytes = await self.storage_service.get_file(s3_path)

        # Load numpy data
        bytes_io = io.BytesIO(npy_bytes)
        lap_data = np.load(bytes_io, allow_pickle=True).item()

        return (
            lap_data["data"],
            lap_data.get("lap_time", 0),
            lap_data.get("lap_number", 0)
        )

    def interpolate_fuel_to_common_distances(
        self,
        lap_data: List[dict],
        common_distances: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Interpolate fuel data to common distance grid.

        Args:
            lap_data: Lap frame data
            common_distances: Common distance grid

        Returns:
            Tuple of (fuel_liters, fuel_percentage) arrays
        """
        distances = np.array([p["lap_distance"] for p in lap_data])
        fuel_liters = np.array([p["fuel_liters"] for p in lap_data])
        fuel_percentage = np.array([p["fuel_level_percentage"] for p in lap_data])

        interp_liters = interp1d(
            distances, fuel_liters, kind="linear", fill_value="extrapolate"
        )
        interp_percentage = interp1d(
            distances, fuel_percentage, kind="linear", fill_value="extrapolate"
        )

        return (
            interp_liters(common_distances),
            interp_percentage(common_distances),
        )

    def calculate_fuel_delta(
        self,
        lap_1_data: List[dict],
        lap_2_data: List[dict],
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Calculate fuel consumption delta using distance-based alignment.

        Args:
            lap_1_data: Reference lap data
            lap_2_data: Comparison lap data

        Returns:
            Tuple of (common_distances, fuel_delta, lap_1_fuel, lap_2_fuel)
        """
        # Get distance ranges
        lap_1_distance = np.array([p["lap_distance"] for p in lap_1_data])
        lap_2_distance = np.array([p["lap_distance"] for p in lap_2_data])

        # Common distance grid
        max_distance = min(lap_1_distance.max(), lap_2_distance.max())
        common_distances = np.linspace(0, max_distance, 1000)

        # Interpolate fuel for both laps
        lap_1_fuel, _ = self.interpolate_fuel_to_common_distances(lap_1_data, common_distances)
        lap_2_fuel, _ = self.interpolate_fuel_to_common_distances(lap_2_data, common_distances)

        # Normalize fuel to consumption from start (fuel used at each point)
        lap_1_start = lap_1_data[0]["fuel_liters"]
        lap_2_start = lap_2_data[0]["fuel_liters"]

        lap_1_consumed = lap_1_start - lap_1_fuel
        lap_2_consumed = lap_2_start - lap_2_fuel

        # Delta in fuel consumed (positive = lap2 consumed more)
        fuel_delta = lap_2_consumed - lap_1_consumed

        return common_distances, fuel_delta, lap_1_fuel, lap_2_fuel

    def calculate_fuel_vs_speed(
        self,
        lap_data: List[dict],
        n_segments: int = 100,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Calculate fuel consumption per segment at different speeds.
        Compresses lap data into segments for clearer fuel consumption visualization.

        Args:
            lap_data: Lap frame data
            n_segments: Number of segments to divide the lap into

        Returns:
            Tuple of (speed, fuel_consumed, throttle, gear) arrays
        """
        n_frames = len(lap_data)

        # Extract arrays
        speed = np.array([p["speed"] for p in lap_data], dtype=np.float32)
        fuel = np.array([p["fuel_liters"] for p in lap_data], dtype=np.float32)
        throttle = np.array([p["throttle"] for p in lap_data], dtype=np.float32)
        gear = np.array([p["gear"] for p in lap_data], dtype=np.int32)

        # Divide into segments
        segment_size = max(1, n_frames // n_segments)
        actual_segments = n_frames // segment_size

        # Reshape arrays for segment aggregation
        truncated_len = actual_segments * segment_size

        speed_segments = speed[:truncated_len].reshape(actual_segments, segment_size)
        fuel_segments = fuel[:truncated_len].reshape(actual_segments, segment_size)
        throttle_segments = throttle[:truncated_len].reshape(actual_segments, segment_size)
        gear_segments = gear[:truncated_len].reshape(actual_segments, segment_size)

        # Calculate per-segment metrics
        avg_speed = np.mean(speed_segments, axis=1)
        fuel_consumed = fuel_segments[:, 0] - fuel_segments[:, -1]  # Fuel at start - fuel at end of segment
        avg_throttle = np.mean(throttle_segments, axis=1)

        # Most common gear per segment (mode)
        mode_gear = np.array([
            np.bincount(seg).argmax() for seg in gear_segments
        ], dtype=np.int32)

        # Filter out segments with zero or negative fuel consumption (noise)
        valid_mask = fuel_consumed > 0
        avg_speed = avg_speed[valid_mask]
        fuel_consumed = fuel_consumed[valid_mask]
        avg_throttle = avg_throttle[valid_mask]
        mode_gear = mode_gear[valid_mask]

        return avg_speed, fuel_consumed, avg_throttle, mode_gear

    async def analyze_single_lap(self, lap_s3_path: str) -> SingleLapFuelResponse:
        """
        Analyze fuel consumption for a single lap.

        Args:
            lap_s3_path: S3 path to lap data

        Returns:
            SingleLapFuelResponse with fuel analysis
        """
        # Load lap data
        lap_data, lap_time, lap_number = await self.load_lap_from_s3(lap_s3_path)

        # Create common distance grid for smooth curve
        max_distance = lap_data[-1]["lap_distance"]
        common_distances = np.linspace(0, max_distance, 500)

        # Interpolate fuel data
        fuel_liters, fuel_percentage = self.interpolate_fuel_to_common_distances(
            lap_data, common_distances
        )

        # Calculate fuel vs speed scatter data
        speed, fuel_consumed, throttle, gear = self.calculate_fuel_vs_speed(lap_data)

        # Build response
        return SingleLapFuelResponse(
            summary=FuelSummary.from_data(
                lap_number=lap_number,
                lap_time=lap_time,
                lap_data=lap_data,
            ),
            fuel_curve=FuelCurve.from_arrays(
                distance=common_distances,
                fuel_liters=fuel_liters,
                fuel_percentage=fuel_percentage,
            ),
            fuel_speed_scatter=FuelSpeedScatter.from_arrays(
                speed=speed,
                fuel_consumed=fuel_consumed,
                throttle=throttle,
                gear=gear,
            ),
            fuel_throttle_scatter=FuelThrottleScatter.from_arrays(
                throttle=throttle,
                fuel_consumed=fuel_consumed,
                speed=speed,
                gear=gear,
            ),
        )

    async def compare_fuel(
        self,
        lap_1_s3_path: str,
        lap_2_s3_path: str,
    ) -> FuelComparisonResponse:
        """
        Compare fuel consumption between two laps.

        Args:
            lap_1_s3_path: S3 path to reference lap
            lap_2_s3_path: S3 path to comparison lap

        Returns:
            FuelComparisonResponse with comparison data
        """
        # Load both laps
        lap_1_data, lap_1_time, lap_1_number = await self.load_lap_from_s3(lap_1_s3_path)
        lap_2_data, lap_2_time, lap_2_number = await self.load_lap_from_s3(lap_2_s3_path)

        # Calculate fuel delta
        common_distances, fuel_delta, lap_1_fuel, lap_2_fuel = self.calculate_fuel_delta(
            lap_1_data, lap_2_data
        )

        # Build response
        return FuelComparisonResponse(
            summary=FuelComparisonSummary.from_data(
                lap_1_number=lap_1_number,
                lap_2_number=lap_2_number,
                lap_1_time=lap_1_time,
                lap_2_time=lap_2_time,
                lap_1_data=lap_1_data,
                lap_2_data=lap_2_data,
            ),
            fuel_delta=FuelDeltaSeries.from_arrays(
                distance=common_distances,
                delta=fuel_delta,
            ),
            fuel_curves=FuelComparisonCurves.from_arrays(
                distance=common_distances,
                lap_1_fuel=lap_1_fuel,
                lap_2_fuel=lap_2_fuel,
            ),
        )
