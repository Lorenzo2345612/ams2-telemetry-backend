from abc import ABC, abstractmethod
import numpy as np
from scipy.interpolate import interp1d

CONTINUOUS_FEATURES = [
    "pos_x",
    "pos_z",
    "speed",
    "rpm",
    "throttle",
    "brake",
    "steering",
    "yaw",
]

DISCRETE_FEATURES = ["gear"]


class TelemetryResamplerRepository(ABC):
    @abstractmethod
    def resample_telemetry_data(self, data: list) -> list:
        """Resample telemetry data to the specified interval.

        Args:
            data: The original telemetry data.
            interval: The desired resampling interval.

        Returns:
            Resampled telemetry data.
        """
        pass


class AMS2TelemetryResamplerRepository(TelemetryResamplerRepository):
    def resample_telemetry_data(self, data: list) -> list:
        # Get the target frames from the avg of all laps divided by 2 (50% reduction)
        target_frames = sum(len(lap["data"]) for lap in data) // len(data) // 2

        result = []

        for lap_data in data:
            resampled = self.resample_lap(lap_data, target_frames)
            
            result.append(resampled)

        return result

    def resample_lap(self, lap_json: dict, target_frames: int) -> dict:
        """
        Resamplea una vuelta a un número fijo de frames.
        """
        lap_number = lap_json["lap_number"]
        data = lap_json["data"]
        
        # Separar timings y telemetry
        timings, telemetry = self.separate_data_by_type(data)
        
        if len(telemetry) < 2:
            raise ValueError(f"Vuelta {lap_number} inválida: menos de 2 frames de telemetría")
        
        if len(timings) < 2:
            raise ValueError(f"Vuelta {lap_number} inválida: menos de 2 frames de timing")
        
        # Calcular lap_time desde timings
        lap_time = max([t.get("current_time", 0) for t in timings])
        
        # Ordenar por tick_count para telemetry
        telemetry = sorted(telemetry, key=lambda x: x.get("tick_count", 0))
        n_telem = len(telemetry)
        
        # Eje temporal para telemetría
        t_telem = np.linspace(0.0, 1.0, n_telem)
        t_resampled = np.linspace(0.0, 1.0, target_frames)
        
        interpolators = {}
        
        # Interpolar features continuas de telemetría
        for feature in CONTINUOUS_FEATURES:
            raw = np.array([f.get(feature, 0.0) for f in telemetry], dtype=np.float32)
            clean = self.sanitize_signal(raw)
            
            interpolators[feature] = interp1d(
                t_telem,
                clean,
                kind="linear",
                bounds_error=False,
                fill_value=(clean[0], clean[-1]),
                assume_sorted=True,
            )
        
        # Interpolar gear (discreto)
        gear_raw = np.array([f.get("gear", 0) for f in telemetry], dtype=np.int32)
        gear_raw = np.nan_to_num(gear_raw, nan=0, posinf=0, neginf=0)
        
        gear_interp = interp1d(
            t_telem,
            gear_raw,
            kind="nearest",
            bounds_error=False,
            fill_value=(gear_raw[0], gear_raw[-1]),
            assume_sorted=True,
        )
        
        # Interpolar lap_distance de timings
        timings = sorted(timings, key=lambda x: x.get("lap_distance", 0))
        n_timings = len(timings)
        
        t_timings = np.linspace(0.0, 1.0, n_timings)
        lap_distances = np.array([t.get("lap_distance", 0) for t in timings], dtype=np.float32)
        
        distance_interp = interp1d(
            t_timings,
            lap_distances,
            kind="linear",
            bounds_error=False,
            fill_value=(lap_distances[0], lap_distances[-1]),
            assume_sorted=True,
        )
        
        # Construir frames resampleados
        resampled_data = []
        
        for t in t_resampled:
            frame = {
                "time": float(t * lap_time),
                "lap_distance": float(distance_interp(t))
            }
            
            # Agregar features de telemetría
            for feature, fn in interpolators.items():
                val = float(fn(t))
                frame[feature] = float(np.nan_to_num(val, nan=0.0, posinf=0.0, neginf=0.0))
            
            frame["gear"] = int(gear_interp(t))
            resampled_data.append(frame)
        
        return {
            "lap_number": lap_number,
            "lap_time": lap_time,
            "frames": target_frames,
            "original_telemetry_points": n_telem,
            "original_timing_points": n_timings,
            "data": resampled_data,
        }
    
    # -----------------------------
    # LIMPIEZA DE SEÑALES
    # -----------------------------
    def sanitize_signal(self, values: np.ndarray) -> np.ndarray:
        """
        Garantiza que la señal:
        - no tenga NaN
        - no tenga +/-Infinity
        - sea interpolable
        """
        values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)

        if np.all(values == 0):
            return values

        # forward fill
        for i in range(1, len(values)):
            if not np.isfinite(values[i]):
                values[i] = values[i - 1]

        # backward fill
        for i in range(len(values) - 2, -1, -1):
            if not np.isfinite(values[i]):
                values[i] = values[i + 1]

        return values


    # -----------------------------
    # ELIMINAR DUPLICADOS DE TIMINGS
    # -----------------------------
    def remove_timing_duplicates(self, timings: list) -> list:
        """
        Elimina timings duplicados basándose en lap_distance y current_lap.
        Mantiene el primer registro de cada combinación única.
        """
        seen = set()
        unique_timings = []
        
        for timing in timings:
            key = (timing.get("current_lap"), timing.get("lap_distance"))
            if key not in seen:
                seen.add(key)
                unique_timings.append(timing)
        
        return unique_timings


    # -----------------------------
    # SEPARAR DATOS POR TIPO
    # -----------------------------
    def separate_data_by_type(self, data: list) -> tuple:
        """
        Separa los datos en timings y telemetry.
        """
        timings = [d for d in data if d.get("type") == "timings"]
        telemetry = [d for d in data if d.get("type") == "telemetry"]
        
        # Eliminar duplicados de timings
        timings = self.remove_timing_duplicates(timings)
        
        return timings, telemetry
