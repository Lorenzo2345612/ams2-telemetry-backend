"""
Pydantic models for lap comparison data.
"""

from pydantic import BaseModel, Field
from typing import List
import numpy as np


class LapSummary(BaseModel):
    """Summary statistics for a lap comparison."""

    lap_1_time: float = Field(..., description="Total time for lap 1 in seconds")
    lap_2_time: float = Field(..., description="Total time for lap 2 in seconds")
    delta_final: float = Field(..., description="Final delta time (lap2 - lap1) in seconds")
    delta_min: float = Field(..., description="Minimum delta time in seconds (where lap2 was fastest)")
    delta_max: float = Field(..., description="Maximum delta time in seconds (where lap2 was slowest)")
    delta_min_position: float = Field(..., description="Track distance where minimum delta occurred")
    delta_max_position: float = Field(..., description="Track distance where maximum delta occurred")
    max_speed_lap_1: float = Field(..., description="Maximum speed in lap 1 (km/h)")
    max_speed_lap_2: float = Field(..., description="Maximum speed in lap 2 (km/h)")

    @classmethod
    def from_data(
        cls,
        lap_1_time: float,
        lap_2_time: float,
        common_distances: np.ndarray,
        delta_time: np.ndarray,
        lap_1_data: List[dict],
        lap_2_data: List[dict],
    ) -> "LapSummary":
        """
        Create LapSummary from raw comparison data.

        Args:
            lap_1_time: Total time for lap 1
            lap_2_time: Total time for lap 2
            common_distances: Common distance grid
            delta_time: Delta time array
            lap_1_data: Raw lap 1 data
            lap_2_data: Raw lap 2 data

        Returns:
            LapSummary instance
        """
        delta_min_idx = np.argmin(delta_time)
        delta_max_idx = np.argmax(delta_time)

        lap_1_speeds = np.array([p["speed"] * 3.6 for p in lap_1_data])
        lap_2_speeds = np.array([p["speed"] * 3.6 for p in lap_2_data])

        return cls(
            lap_1_time=float(lap_1_time),
            lap_2_time=float(lap_2_time),
            delta_final=float(delta_time[-1]),
            delta_min=float(delta_time[delta_min_idx]),
            delta_max=float(delta_time[delta_max_idx]),
            delta_min_position=float(common_distances[delta_min_idx]),
            delta_max_position=float(common_distances[delta_max_idx]),
            max_speed_lap_1=float(np.max(lap_1_speeds)),
            max_speed_lap_2=float(np.max(lap_2_speeds)),
        )


class TelemetryTimeSeries(BaseModel):
    """Time series data for a telemetry channel."""

    distance: List[float] = Field(..., description="Distance along track (meters)")
    lap_1: List[float] = Field(..., description="Lap 1 values")
    lap_2: List[float] = Field(..., description="Lap 2 values")

    @classmethod
    def from_arrays(
        cls,
        distance: np.ndarray,
        lap_1_values: np.ndarray,
        lap_2_values: np.ndarray,
    ) -> "TelemetryTimeSeries":
        """
        Create TelemetryTimeSeries from numpy arrays.

        Args:
            distance: Distance array
            lap_1_values: Lap 1 values
            lap_2_values: Lap 2 values

        Returns:
            TelemetryTimeSeries instance
        """
        return cls(
            distance=distance.tolist(),
            lap_1=lap_1_values.tolist(),
            lap_2=lap_2_values.tolist(),
        )


class DeltaTimeSeries(BaseModel):
    """Delta time series data."""

    distance: List[float] = Field(..., description="Distance along track (meters)")
    delta: List[float] = Field(..., description="Delta time in seconds (positive = lap2 slower)")

    @classmethod
    def from_arrays(
        cls,
        distance: np.ndarray,
        delta: np.ndarray,
    ) -> "DeltaTimeSeries":
        """
        Create DeltaTimeSeries from numpy arrays.

        Args:
            distance: Distance array
            delta: Delta time array

        Returns:
            DeltaTimeSeries instance
        """
        return cls(
            distance=distance.tolist(),
            delta=delta.tolist(),
        )


class Segment(BaseModel):
    """A track segment with time delta information."""

    start_distance: float = Field(..., description="Start distance of the segment (meters)")
    end_distance: float = Field(..., description="End distance of the segment (meters)")
    time_delta: float = Field(..., description="Time gained or lost in this segment (seconds)")


class SegmentAnalysis(BaseModel):
    """Analysis of top segments where time is gained or lost."""

    time_loss_segments: List[Segment] = Field(
        ..., description="Top 5 non-overlapping segments where lap 2 loses the most time"
    )
    time_gain_segments: List[Segment] = Field(
        ..., description="Top 5 non-overlapping segments where lap 2 gains the most time"
    )

    @classmethod
    def from_arrays(
        cls,
        time_loss_segments: List[tuple],
        time_gain_segments: List[tuple],
    ) -> "SegmentAnalysis":
        """
        Create SegmentAnalysis from segment tuples.

        Args:
            time_loss_segments: List of (start_dist, end_dist, time_delta) tuples
            time_gain_segments: List of (start_dist, end_dist, time_delta) tuples

        Returns:
            SegmentAnalysis instance
        """
        return cls(
            time_loss_segments=[
                Segment(start_distance=s[0], end_distance=s[1], time_delta=s[2])
                for s in time_loss_segments
            ],
            time_gain_segments=[
                Segment(start_distance=s[0], end_distance=s[1], time_delta=s[2])
                for s in time_gain_segments
            ],
        )


class DeltaTrackMap(BaseModel):
    """Track map with delta time derivative coloring."""

    pos_x: List[float] = Field(..., description="X position on track")
    pos_z: List[float] = Field(..., description="Z position on track")
    color_value: List[float] = Field(
        ..., description="Color value: -1 (green/gaining) to 1 (red/losing), 0 = neutral"
    )

    @classmethod
    def from_arrays(
        cls,
        pos_x: np.ndarray,
        pos_z: np.ndarray,
        color_value: np.ndarray,
    ) -> "DeltaTrackMap":
        """
        Create DeltaTrackMap from numpy arrays.

        Args:
            pos_x: X position array
            pos_z: Z position array
            color_value: Color values (-1 to 1)

        Returns:
            DeltaTrackMap instance
        """
        return cls(
            pos_x=pos_x.tolist(),
            pos_z=pos_z.tolist(),
            color_value=color_value.tolist(),
        )


class LapComparisonResponse(BaseModel):
    """Complete lap comparison response."""

    summary: LapSummary = Field(..., description="Summary statistics")
    delta_time: DeltaTimeSeries = Field(..., description="Delta time data")
    speed: TelemetryTimeSeries = Field(..., description="Speed comparison (km/h)")
    throttle: TelemetryTimeSeries = Field(..., description="Throttle comparison (0-1)")
    brake: TelemetryTimeSeries = Field(..., description="Brake comparison (0-1)")
    steering: TelemetryTimeSeries = Field(..., description="Steering comparison (-1 to 1)")
    segment_analysis: SegmentAnalysis = Field(..., description="Top segments for time gain/loss")
    delta_track_map: DeltaTrackMap = Field(..., description="Track map with delta coloring")

    class Config:
        json_schema_extra = {
            "example": {
                "summary": {
                    "lap_1_time": 85.234,
                    "lap_2_time": 85.891,
                    "delta_final": 0.657,
                    "delta_min": -0.234,
                    "delta_max": 1.123,
                    "delta_min_position": 1234.5,
                    "delta_max_position": 2345.6,
                    "max_speed_lap_1": 287.4,
                    "max_speed_lap_2": 285.2,
                },
                "delta_time": {
                    "distance": [0, 100, 200],
                    "delta": [0, 0.1, 0.2],
                },
                "speed": {
                    "distance": [0, 100, 200],
                    "lap_1": [120, 180, 250],
                    "lap_2": [118, 175, 245],
                },
                "throttle": {
                    "distance": [0, 100, 200],
                    "lap_1": [0.8, 1.0, 1.0],
                    "lap_2": [0.75, 0.95, 1.0],
                },
                "brake": {
                    "distance": [0, 100, 200],
                    "lap_1": [0, 0, 0],
                    "lap_2": [0.1, 0, 0],
                },
                "steering": {
                    "distance": [0, 100, 200],
                    "lap_1": [0.2, 0.5, -0.3],
                    "lap_2": [0.25, 0.48, -0.35],
                },
            }
        }
