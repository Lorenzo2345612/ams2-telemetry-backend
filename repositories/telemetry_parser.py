from abc import ABC, abstractmethod
import struct
from collections import defaultdict
from dataclasses import dataclass

class TelemetryParser(ABC):
    @abstractmethod
    async def parse(self, data: bytes) -> list[dict]:
        pass

@dataclass
class TimingsData:
    current_lap: int
    current_time: float
    tick_count: int 

@dataclass
class TelemetryData:
    throttle: float
    brake: float
    steering: float
    speed: float
    rpm: int
    gear: int
    pos_x: float
    pos_y: float
    pos_z: float
    yaw: float
    tick_count: int

@dataclass
class LapData:
    timings: TimingsData
    telemetry: list[TelemetryData]


class AMS2TelemetryParser(TelemetryParser):
    async def parse(self, data: bytes) -> list[dict]:
        # Each 4 bytes starts a new packet, the first 4 bytes are the length of the next packet
        packets = []
        offset = 0
        while offset + 4 <= len(data):
            packet_length = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            if offset + packet_length > len(data):
                break
            packet_data = data[offset:offset + packet_length]
            packets.append(packet_data)
            offset += packet_length
        
        parsed_data = defaultdict(list)
        for packet in packets:
            packet_type = self.parse_packet_type(packet)
            if packet_type == 0:
                telemetry = self.parse_telemetry(packet)
                if telemetry:
                    telemetry['type'] = 'telemetry'
                    parsed_data['packages'].append(telemetry)

            elif packet_type == 3:
                timings = self.parse_timings(packet)
                if timings:
                    timings['type'] = 'timings'
                    parsed_data['packages'].append(timings)
        
        return self.merge_telemetry_and_timings(parsed_data)
    
    def merge_telemetry_and_timings(self, parsed_data):
        """
        Combina telemetría y timings en una sola línea de tiempo ordenada
        """
        combined = parsed_data['packages']

        # Merge in laps
        laps = []
        current_lap = 0

        for entry in combined:
            if entry['type'] == 'timings':
                if entry['current_lap'] > current_lap:
                    current_lap = entry['current_lap']
                    laps.append({
                        'lap_number': current_lap,
                        'data': []
                    })
            if laps:
                laps[-1]['data'].append(entry)
        
        return laps
    
    def parse_packet_type(self, data):
        """Obtiene el tipo de paquete"""
        if len(data) < 12:
            return None
        return struct.unpack_from('B', data, 10)[0]
    
    def parse_telemetry(self, data):
        """
        Parsea paquete tipo 0 (eCarPhysics) - CON TICK_COUNT
        """
        if len(data) < 556:
            return None

        try:
            offset = 12  # Después del header base

            # sViewedParticipantIndex
            offset += 1

            # Inputs sin filtrar (5 bytes)
            throttle_raw = struct.unpack_from('B', data, offset)[0] / 255.0
            offset += 1
            brake_raw = struct.unpack_from('B', data, offset)[0] / 255.0
            offset += 1
            steering_raw = struct.unpack_from('b', data, offset)[0] / 127.0
            offset += 1
            offset += 1  # clutch_raw

            # Flags y temperaturas (15 bytes)
            offset += 1  # car_flags
            offset += 2  # oil_temp
            offset += 2  # oil_pressure
            offset += 2  # water_temp
            offset += 2  # water_pressure
            offset += 2  # fuel_pressure
            offset += 1  # fuel_capacity
            
            brake = struct.unpack_from('B', data, offset)[0] / 255.0
            offset += 1
            throttle = struct.unpack_from('B', data, offset)[0] / 255.0
            offset += 1
            offset += 1  # clutch

            # Datos del motor
            fuel_level = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            speed = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            rpm = struct.unpack_from('<H', data, offset)[0]
            offset += 2
            max_rpm = struct.unpack_from('<H', data, offset)[0]
            offset += 2

            steering = struct.unpack_from('b', data, offset)[0] / 127.0
            offset += 1
            
            gear_data = struct.unpack_from('B', data, offset)[0]
            gear = gear_data & 0x0F
            offset += 1

            offset += 1  # boost
            offset += 1  # crash_state
            offset += 4  # odometer

            # Orientación [yaw, pitch, roll]
            yaw = struct.unpack_from('<f', data, offset)[0]
            offset += 12

            # Velocidades (para referencia, pero NO las usamos para posición)
            offset += 12  # local_velocity
            offset += 12  # world_velocity
            offset += 12  # angular_velocity
            offset += 12  # local_acceleration
            offset += 12  # world_acceleration
            offset += 12  # extents_centre

            # Saltamos datos de neumáticos y suspensión
            offset += 4   # tyre_flags[4]
            offset += 4   # terrain[4]
            offset += 16  # tyre_y[4]
            offset += 16  # tyre_rps[4]
            offset += 4   # tyre_temp[4]
            offset += 16  # tyre_height_above_ground[4]
            offset += 4   # tyre_wear[4]
            offset += 4   # brake_damage[4]
            offset += 4   # suspension_damage[4]
            offset += 8   # brake_temp_celsius[4]
            offset += 8   # tyre_tread_temp[4]
            offset += 8   # tyre_layer_temp[4]
            offset += 8   # tyre_carcass_temp[4]
            offset += 8   # tyre_rim_temp[4]
            offset += 8   # tyre_internal_air_temp[4]
            offset += 8   # tyre_temp_left[4]
            offset += 8   # tyre_temp_center[4]
            offset += 8   # tyre_temp_right[4]
            offset += 16  # wheel_local_position_y[4]
            offset += 16  # ride_height[4]
            offset += 16  # suspension_travel[4]
            offset += 16  # suspension_velocity[4]
            offset += 8   # suspension_ride_height[4]
            offset += 8   # air_pressure[4]
            offset += 4   # engine_speed
            offset += 4   # engine_torque
            offset += 2   # wings[2]
            offset += 1   # hand_brake
            offset += 1   # aero_damage
            offset += 1   # engine_damage
            offset += 4   # joypad
            offset += 1   # d_pad
            offset += 160 # tyre_compound[4][40]
            offset += 4   # turbo_boost_pressure

            # Posición absoluta - sFullPosition[3]
            pos_x = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            pos_y = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            pos_z = struct.unpack_from('<f', data, offset)[0]
            offset += 4

            offset += 1  # brake_bias

            # Offset 555 (tamaño total 559 bytes)
            tick_count = struct.unpack_from('<I', data, offset)[0]

            return {
                'throttle': throttle,
                'brake': brake,
                'steering': steering,
                'speed': speed,
                'rpm': rpm,
                'max_rpm': max_rpm,
                'gear': gear,
                'fuel_level': fuel_level,
                'yaw': yaw,
                'pos_x': pos_x,
                'pos_y': pos_y,
                'pos_z': pos_z,
                'tick_count': tick_count
            }

        except Exception as e:
            print(f"Error parseando telemetría: {e}")
            import traceback
            traceback.print_exc()
            return None

    def parse_timings(self, data):
        """
        Parsea paquete tipo 3 (eTimings) - CON TICK_COUNT
        """
        if len(data) < 50:
            return None

        try:
            offset = 12  # Después del header base
            
            # sNumParticipants
            num_participants = struct.unpack_from('b', data, offset)[0]
            offset += 1
            
            # sParticipantsChangedTimestamp
            offset += 4
            
            # sEventTimeRemaining
            offset += 4
            
            # sSplitTimeAhead, sSplitTimeBehind, sSplitTime
            offset += 4
            offset += 4
            offset += 4

            # Array de participantes - leemos el primero (jugador)
            # sWorldPosition[3] - int16 * 3
            offset += 6
            # sOrientation[3] - int16 * 3
            offset += 6
            # sCurrentLapDistance - uint16
            lap_distance = struct.unpack_from('<H', data, offset)[0]
            offset += 2
            # sRacePosition - uint8
            offset += 1
            # sSector - uint8
            offset += 1
            # sHighestFlag - uint8
            offset += 1
            # sPitModeSchedule - uint8
            offset += 1
            # sCarIndex - uint16
            offset += 2
            # sRaceState - uint8
            offset += 1
            # sCurrentLap - uint8
            current_lap = struct.unpack_from('B', data, offset)[0]
            offset += 1
            # sCurrentTime - float
            current_time = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            # sCurrentSectorTime - float
            offset += 4
            # sMPParticipantIndex - uint16
            offset += 2

            # Ahora tenemos que saltar el resto de participantes para llegar al TickCount
            # Cada participante ocupa 32 bytes
            # Ya leímos el primero, saltamos los demás (num_participants - 1) * 32
            if num_participants > 1:
                offset += (num_participants - 1) * 32

            # sLocalParticipantIndex - uint16
            offset += 2

            # 🔥 TICK_COUNT - uint32 al final del paquete de timings
            tick_count = struct.unpack_from('<I', data, offset)[0]

            return {
                'current_lap': current_lap,
                'current_time': current_time,
                'lap_distance': lap_distance,  # También lo incluimos
                'tick_count': tick_count,
            }

        except Exception as e:
            print(f"Error parseando timings: {e}")
            import traceback
            traceback.print_exc()
            return None