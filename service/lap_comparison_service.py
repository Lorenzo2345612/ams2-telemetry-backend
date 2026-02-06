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
    SegmentAnalysis,
    DeltaTrackMap,
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

    def find_top_segments(
        self,
        distances: np.ndarray,
        delta_time: np.ndarray,
        window_size: int = 50,
        top_n: int = 5,
    ) -> Tuple[List[tuple], List[tuple]]:
        """
        Find top non-overlapping segments where time is gained or lost.

        Uses numpy sliding window to calculate time change per segment,
        then greedily selects top non-overlapping segments.

        Args:
            distances: Distance array (common grid)
            delta_time: Delta time array (lap2 - lap1)
            window_size: Size of the sliding window in samples
            top_n: Number of top segments to return

        Returns:
            Tuple of (time_loss_segments, time_gain_segments)
            Each segment is (start_distance, end_distance, time_delta)
        """
        if len(delta_time) < window_size:
            return [], []

        # Calculate delta change per window using numpy sliding window
        windows = np.lib.stride_tricks.sliding_window_view(delta_time, window_size)
        delta_change = windows[:, -1] - windows[:, 0]

        n_windows = len(delta_change)

        def find_top_non_overlapping(ascending: bool) -> List[tuple]:
            """Find top non-overlapping segments based on delta change."""
            sorted_indices = np.argsort(delta_change)
            if not ascending:
                sorted_indices = sorted_indices[::-1]

            selected = []
            used_mask = np.zeros(n_windows, dtype=bool)

            for idx in sorted_indices:
                if used_mask[idx]:
                    continue

                start_idx = idx
                end_idx = idx + window_size - 1

                start_dist = float(distances[start_idx])
                end_dist = float(distances[end_idx])
                time_delta = float(delta_change[idx])

                selected.append((start_dist, end_dist, time_delta))

                # Mark overlapping windows as used
                overlap_start = max(0, idx - window_size + 1)
                overlap_end = min(n_windows, idx + window_size)
                used_mask[overlap_start:overlap_end] = True

                if len(selected) >= top_n:
                    break

            return selected

        # Time loss: largest positive delta change (lap 2 losing time)
        time_loss_segments = find_top_non_overlapping(ascending=False)

        # Time gain: largest negative delta change (lap 2 gaining time)
        time_gain_segments = find_top_non_overlapping(ascending=True)

        return time_loss_segments, time_gain_segments

    def calculate_delta_track_map(
        self,
        lap_data: List[dict],
        common_distances: np.ndarray,
        delta_time: np.ndarray,
        time_loss_segments: List[tuple],
        time_gain_segments: List[tuple],
        segment_size: int = 20,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Calculate track map with color values based on delta segments.

        Args:
            lap_data: Reference lap data (for position)
            common_distances: Common distance grid
            delta_time: Delta time array
            time_loss_segments: List of (start_dist, end_dist, time_delta) tuples
            time_gain_segments: List of (start_dist, end_dist, time_delta) tuples
            segment_size: Number of points to average per segment

        Returns:
            Tuple of (pos_x, pos_z, color_value) arrays
        """
        # Extract positions from lap data
        pos_x = np.array([p["pos_x"] for p in lap_data], dtype=np.float32)
        pos_z = np.array([p["pos_z"] for p in lap_data], dtype=np.float32)
        distances = np.array([p["lap_distance"] for p in lap_data])

        # Downsample for visualization
        n_points = len(pos_x)
        actual_segments = n_points // segment_size
        truncated_len = actual_segments * segment_size

        pos_x_segments = pos_x[:truncated_len].reshape(actual_segments, segment_size)
        pos_z_segments = pos_z[:truncated_len].reshape(actual_segments, segment_size)
        dist_segments = distances[:truncated_len].reshape(actual_segments, segment_size)

        # Take middle point of each segment
        mid_idx = segment_size // 2
        avg_pos_x = pos_x_segments[:, mid_idx]
        avg_pos_z = pos_z_segments[:, mid_idx]
        avg_dist = dist_segments[:, mid_idx]

        # Initialize color values as neutral (0)
        color_values = np.zeros(actual_segments, dtype=np.float32)

        # Mark time loss segments (positive = red = 1)
        for start_dist, end_dist, time_delta in time_loss_segments:
            mask = (avg_dist >= start_dist) & (avg_dist <= end_dist)
            # Normalize intensity based on time delta magnitude
            color_values[mask] = 1.0

        # Mark time gain segments (negative = green = -1)
        for start_dist, end_dist, time_delta in time_gain_segments:
            mask = (avg_dist >= start_dist) & (avg_dist <= end_dist)
            color_values[mask] = -1.0

        return avg_pos_x, avg_pos_z, color_values

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

        # Find top segments for time gain/loss analysis
        time_loss_segments, time_gain_segments = self.find_top_segments(
            common_distances, delta_time
        )

        # Calculate track map with delta coloring
        track_pos_x, track_pos_z, track_color = self.calculate_delta_track_map(
            lap_1_data,
            common_distances,
            delta_time,
            time_loss_segments,
            time_gain_segments,
        )

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
            segment_analysis=SegmentAnalysis.from_arrays(
                time_loss_segments=time_loss_segments,
                time_gain_segments=time_gain_segments,
            ),
            delta_track_map=DeltaTrackMap.from_arrays(
                pos_x=track_pos_x,
                pos_z=track_pos_z,
                color_value=track_color,
            ),
        )
