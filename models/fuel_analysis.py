"""
Pydantic models for fuel analysis data.
"""

from pydantic import BaseModel, Field
from typing import List
import numpy as np


class FuelSummary(BaseModel):
    """Summary statistics for single lap fuel analysis."""

    lap_number: int = Field(..., description="Lap number")
    lap_time: float = Field(..., description="Total lap time in seconds")
    fuel_capacity: float = Field(..., description="Tank capacity in liters")
    fuel_start: float = Field(..., description="Fuel at lap start in liters")
    fuel_end: float = Field(..., description="Fuel at lap end in liters")
    fuel_used: float = Field(..., description="Total fuel consumed during lap in liters")
    consumption_rate_per_km: float = Field(..., description="Fuel consumption per kilometer (L/km)")
    lap_distance_km: float = Field(..., description="Total lap distance in kilometers")
    estimated_laps_remaining: float = Field(..., description="Estimated laps remaining at this consumption rate")

    @classmethod
    def from_data(
        cls,
        lap_number: int,
        lap_time: float,
        lap_data: List[dict],
    ) -> "FuelSummary":
        """
        Create FuelSummary from raw lap data.

        Args:
            lap_number: The lap number
            lap_time: Total lap time in seconds
            lap_data: Raw lap frame data

        Returns:
            FuelSummary instance
        """
        fuel_capacity = lap_data[0].get("fuel_capacity", 100.0)
        fuel_start = lap_data[0]["fuel_liters"]
        fuel_end = lap_data[-1]["fuel_liters"]
        fuel_used = fuel_start - fuel_end

        # Get lap distance in km
        lap_distance_m = lap_data[-1]["lap_distance"]
        lap_distance_km = lap_distance_m / 1000.0

        # Calculate consumption rate
        consumption_rate_per_km = fuel_used / lap_distance_km if lap_distance_km > 0 else 0

        # Estimate remaining laps
        estimated_laps_remaining = fuel_end / fuel_used if fuel_used > 0 else float('inf')

        return cls(
            lap_number=lap_number,
            lap_time=float(lap_time),
            fuel_capacity=float(fuel_capacity),
            fuel_start=float(fuel_start),
            fuel_end=float(fuel_end),
            fuel_used=float(fuel_used),
            consumption_rate_per_km=float(consumption_rate_per_km),
            lap_distance_km=float(lap_distance_km),
            estimated_laps_remaining=float(estimated_laps_remaining),
        )


class FuelCurve(BaseModel):
    """Fuel remaining curve over lap distance."""

    distance: List[float] = Field(..., description="Distance along track (meters)")
    fuel_liters: List[float] = Field(..., description="Fuel remaining in liters")
    fuel_percentage: List[float] = Field(..., description="Fuel remaining as percentage (0-1)")

    @classmethod
    def from_arrays(
        cls,
        distance: np.ndarray,
        fuel_liters: np.ndarray,
        fuel_percentage: np.ndarray,
    ) -> "FuelCurve":
        """Create FuelCurve from numpy arrays."""
        return cls(
            distance=distance.tolist(),
            fuel_liters=fuel_liters.tolist(),
            fuel_percentage=fuel_percentage.tolist(),
        )


class SingleLapFuelResponse(BaseModel):
    """Complete single lap fuel analysis response."""

    summary: FuelSummary = Field(..., description="Fuel consumption summary")
    fuel_curve: FuelCurve = Field(..., description="Fuel remaining over distance")


class FuelComparisonSummary(BaseModel):
    """Summary statistics for fuel comparison between two laps."""

    lap_1_number: int = Field(..., description="Lap 1 number")
    lap_2_number: int = Field(..., description="Lap 2 number")
    lap_1_time: float = Field(..., description="Lap 1 time in seconds")
    lap_2_time: float = Field(..., description="Lap 2 time in seconds")
    lap_1_fuel_used: float = Field(..., description="Fuel used in lap 1 (liters)")
    lap_2_fuel_used: float = Field(..., description="Fuel used in lap 2 (liters)")
    fuel_delta: float = Field(..., description="Fuel difference (lap2 - lap1), negative means lap2 used less")
    lap_1_consumption_rate: float = Field(..., description="Lap 1 consumption rate (L/km)")
    lap_2_consumption_rate: float = Field(..., description="Lap 2 consumption rate (L/km)")
    consumption_rate_delta: float = Field(..., description="Consumption rate difference (L/km)")
    more_efficient_lap: int = Field(..., description="Lap number with lower fuel consumption")

    @classmethod
    def from_data(
        cls,
        lap_1_number: int,
        lap_2_number: int,
        lap_1_time: float,
        lap_2_time: float,
        lap_1_data: List[dict],
        lap_2_data: List[dict],
    ) -> "FuelComparisonSummary":
        """Create FuelComparisonSummary from raw lap data."""
        # Lap 1 calculations
        lap_1_fuel_start = lap_1_data[0]["fuel_liters"]
        lap_1_fuel_end = lap_1_data[-1]["fuel_liters"]
        lap_1_fuel_used = lap_1_fuel_start - lap_1_fuel_end
        lap_1_distance_km = lap_1_data[-1]["lap_distance"] / 1000.0
        lap_1_consumption_rate = lap_1_fuel_used / lap_1_distance_km if lap_1_distance_km > 0 else 0

        # Lap 2 calculations
        lap_2_fuel_start = lap_2_data[0]["fuel_liters"]
        lap_2_fuel_end = lap_2_data[-1]["fuel_liters"]
        lap_2_fuel_used = lap_2_fuel_start - lap_2_fuel_end
        lap_2_distance_km = lap_2_data[-1]["lap_distance"] / 1000.0
        lap_2_consumption_rate = lap_2_fuel_used / lap_2_distance_km if lap_2_distance_km > 0 else 0

        # Deltas
        fuel_delta = lap_2_fuel_used - lap_1_fuel_used
        consumption_rate_delta = lap_2_consumption_rate - lap_1_consumption_rate

        # Determine more efficient lap
        more_efficient_lap = lap_1_number if lap_1_fuel_used <= lap_2_fuel_used else lap_2_number

        return cls(
            lap_1_number=lap_1_number,
            lap_2_number=lap_2_number,
            lap_1_time=float(lap_1_time),
            lap_2_time=float(lap_2_time),
            lap_1_fuel_used=float(lap_1_fuel_used),
            lap_2_fuel_used=float(lap_2_fuel_used),
            fuel_delta=float(fuel_delta),
            lap_1_consumption_rate=float(lap_1_consumption_rate),
            lap_2_consumption_rate=float(lap_2_consumption_rate),
            consumption_rate_delta=float(consumption_rate_delta),
            more_efficient_lap=more_efficient_lap,
        )


class FuelDeltaSeries(BaseModel):
    """Delta fuel consumption series over distance."""

    distance: List[float] = Field(..., description="Distance along track (meters)")
    delta: List[float] = Field(..., description="Fuel delta (lap2 - lap1) in liters, negative = lap2 more efficient")

    @classmethod
    def from_arrays(
        cls,
        distance: np.ndarray,
        delta: np.ndarray,
    ) -> "FuelDeltaSeries":
        """Create FuelDeltaSeries from numpy arrays."""
        return cls(
            distance=distance.tolist(),
            delta=delta.tolist(),
        )


class FuelComparisonCurves(BaseModel):
    """Fuel curves for both laps at common distance points."""

    distance: List[float] = Field(..., description="Distance along track (meters)")
    lap_1_fuel: List[float] = Field(..., description="Lap 1 fuel remaining (liters)")
    lap_2_fuel: List[float] = Field(..., description="Lap 2 fuel remaining (liters)")

    @classmethod
    def from_arrays(
        cls,
        distance: np.ndarray,
        lap_1_fuel: np.ndarray,
        lap_2_fuel: np.ndarray,
    ) -> "FuelComparisonCurves":
        """Create FuelComparisonCurves from numpy arrays."""
        return cls(
            distance=distance.tolist(),
            lap_1_fuel=lap_1_fuel.tolist(),
            lap_2_fuel=lap_2_fuel.tolist(),
        )


class FuelComparisonResponse(BaseModel):
    """Complete fuel comparison response."""

    summary: FuelComparisonSummary = Field(..., description="Fuel comparison summary")
    fuel_delta: FuelDeltaSeries = Field(..., description="Fuel consumption delta over distance")
    fuel_curves: FuelComparisonCurves = Field(..., description="Fuel remaining curves for both laps")
