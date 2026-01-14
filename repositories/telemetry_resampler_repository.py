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
        Resamplea una vuelta a un número fijo de frames.
        - Timings se interpolan por lap_distance
        - Telemetry se interpola por tick_count
        """
        lap_number = lap_json["lap_number"]
        data = lap_json["data"]

        # Separar timings y telemetry
        timings, telemetry = self.separate_data_by_type(data)

        if len(telemetry) < 2:
            raise ValueError(f"Vuelta {lap_number} inválida: menos de 2 frames de telemetría")

        if len(timings) < 2:
            raise ValueError(f"Vuelta {lap_number} inválida: menos de 2 frames de timing")

        # ========== INTERPOLAR TIMINGS POR LAP_DISTANCE ==========
        timings = sorted(timings, key=lambda x: x.get("lap_distance", 0))
        distances_timing = np.array([t.get("lap_distance", 0) for t in timings], dtype=np.float32)
        current_times = np.array([t.get("current_time", 0) for t in timings], dtype=np.float32)

        # Eliminar duplicados
        distances_timing, unique_idx = np.unique(distances_timing, return_index=True)
        current_times = current_times[unique_idx]

        # Eje de distancia resampleado
        total_lap_distance = distances_timing[-1]
        distance_resampled = np.linspace(0.0, total_lap_distance, target_frames)

        # Interpolar current_time vectorizado
        current_time_resampled = np.interp(distance_resampled, distances_timing, current_times)

        # ========== INTERPOLAR TELEMETRÍA POR TICK_COUNT ==========
        telemetry = sorted(telemetry, key=lambda x: x.get("tick_count", 0))
        ticks = np.array([t.get("tick_count", 0) for t in telemetry], dtype=np.float32)

        # Extraer todas las features en un dict de arrays
        telem_arrays = {}
        for feature in CONTINUOUS_FEATURES:
            raw = np.array([t.get(feature, 0.0) for t in telemetry], dtype=np.float32)
            telem_arrays[feature] = self.sanitize_signal(raw)

        gear_raw = np.array([t.get("gear", 0) for t in telemetry], dtype=np.int32)
        gear_raw = np.nan_to_num(gear_raw, nan=0, posinf=0, neginf=0)

        # Eliminar duplicados
        ticks_unique, unique_idx = np.unique(ticks, return_index=True)
        for feature in CONTINUOUS_FEATURES:
            telem_arrays[feature] = telem_arrays[feature][unique_idx]
        gear_raw = gear_raw[unique_idx]

        # Eje de ticks resampleado
        ticks_resampled = np.linspace(ticks_unique[0], ticks_unique[-1], target_frames)

        # Interpolar features continuas vectorizado
        telem_resampled = {}
        for feature in CONTINUOUS_FEATURES:
            interpolated = np.interp(ticks_resampled, ticks_unique, telem_arrays[feature])
            telem_resampled[feature] = np.nan_to_num(interpolated, nan=0.0, posinf=0.0, neginf=0.0)

        # Interpolar gear (nearest neighbor usando searchsorted)
        indices = np.searchsorted(ticks_unique, ticks_resampled, side='left')
        indices = np.clip(indices, 0, len(gear_raw) - 1)
        gear_resampled = gear_raw[indices]

        # ========== UNIR EN LISTA DE DICTS ==========
        resampled_data = [
            {
                "lap_distance": float(distance_resampled[i]),
                "current_time": float(current_time_resampled[i]),
                **{feature: float(telem_resampled[feature][i]) for feature in CONTINUOUS_FEATURES},
                "gear": int(gear_resampled[i])
            }
            for i in range(target_frames)
        ]

        lap_time = float(current_times[-1])

        return {
            "lap_number": lap_number,
            "lap_time": lap_time,
            "frames": target_frames,
            "original_telemetry_points": len(telemetry),
            "original_timing_points": len(timings),
            "data": resampled_data,
        }
    # -----------------------------
    # ELIMINAR DUPLICADOS EN DISTANCIA
    # -----------------------------
    def remove_duplicate_distances(self, distances: np.ndarray, data_list: list) -> tuple:
        """
        Elimina puntos con distancias duplicadas, manteniendo el último valor para cada distancia única.
        Retorna las distancias únicas y la lista de datos filtrada.
        """
        if len(distances) == 0:
            return distances, data_list

        # Encontrar índices de valores únicos (mantiene el último de cada duplicado)
        _, unique_indices = np.unique(distances[::-1], return_index=True)
        unique_indices = len(distances) - 1 - unique_indices
        unique_indices = np.sort(unique_indices)

        unique_distances = distances[unique_indices]
        filtered_data = [data_list[i] for i in unique_indices]

        return unique_distances, filtered_data

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
