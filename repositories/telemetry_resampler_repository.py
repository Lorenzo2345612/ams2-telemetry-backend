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
    'fuel_capacity',
    'fuel_level_percentage',
    'fuel_liters'
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
        Resamplea una vuelta a un número fijo de frames usando distancia como eje común.
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
        
        # Obtener la distancia total de la vuelta (desde cualquiera de las fuentes)
        max_distance_timings = max([t.get("lap_distance", 0) for t in timings])
        max_distance_telemetry = max([t.get("lap_distance", 0) for t in telemetry])
        total_lap_distance = max(max_distance_timings, max_distance_telemetry)
        
        # Crear el eje de distancia resampleado (común para ambos)
        distance_resampled = np.linspace(0.0, total_lap_distance, target_frames)
        
        # ========== INTERPOLAR TELEMETRÍA ==========
        telemetry = sorted(telemetry, key=lambda x: x.get("lap_distance", 0))
        distances_telem = np.array([t.get("lap_distance", 0) for t in telemetry], dtype=np.float32)
        
        interpolators_telem = {}
        
        # Interpolar features continuas de telemetría
        for feature in CONTINUOUS_FEATURES:
            raw = np.array([f.get(feature, 0.0) for f in telemetry], dtype=np.float32)
            clean = self.sanitize_signal(raw)
            
            interpolators_telem[feature] = interp1d(
                distances_telem,
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
            distances_telem,
            gear_raw,
            kind="nearest",
            bounds_error=False,
            fill_value=(gear_raw[0], gear_raw[-1]),
            assume_sorted=True,
        )
        
        # ========== INTERPOLAR TIMINGS ==========
        timings = sorted(timings, key=lambda x: x.get("lap_distance", 0))
        distances_timing = np.array([t.get("lap_distance", 0) for t in timings], dtype=np.float32)
        
        # Interpolar current_time desde timings
        current_times = np.array([t.get("current_time", 0) for t in timings], dtype=np.float32)
        
        time_interp = interp1d(
            distances_timing,
            current_times,
            kind="linear",
            bounds_error=False,
            fill_value=(current_times[0], current_times[-1]),
            assume_sorted=True,
        )
        
        # ========== CONSTRUIR FRAMES RESAMPLEADOS ==========
        resampled_data = []
        
        for dist in distance_resampled:
            frame = {
                "lap_distance": float(dist),
                "time": float(time_interp(dist))
            }
            
            # Agregar features de telemetría
            for feature, fn in interpolators_telem.items():
                val = float(fn(dist))
                frame[feature] = float(np.nan_to_num(val, nan=0.0, posinf=0.0, neginf=0.0))
            
            frame["gear"] = int(gear_interp(dist))
            resampled_data.append(frame)
        
        return {
            "lap_number": lap_number,
            "lap_time": lap_time,
            "frames": target_frames,
            "original_telemetry_points": len(telemetry),
            "original_timing_points": len(timings),
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
    # SEPARAR DATOS POR TIPO
    # -----------------------------
    def separate_data_by_type(self, data: list) -> tuple:
        """
        Separa los datos en timings y telemetry.
        Elimina datos del inicio que pertenecen a la vuelta anterior (lap_distance negativo).
        """
        timings = [d for d in data if d.get("type") == "timings"]
        telemetry = [d for d in data if d.get("type") == "telemetry"]
        
        # Contar cuántos timings tienen lap_distance negativo al inicio
        invalid_count = 0
        for timing in timings:
            if timing.get("lap_distance", 0) < 0:
                invalid_count += 1
            else:
                break  # Dejar de contar cuando encontramos el primer válido
        
        # Eliminar los primeros n timings inválidos
        if invalid_count > 0:
            timings = timings[invalid_count:]
            # Eliminar también los primeros n frames de telemetría
            telemetry = telemetry[invalid_count:]
        
        return timings, telemetry
