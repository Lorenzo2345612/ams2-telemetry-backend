from abc import ABC, abstractmethod
import struct
from collections import defaultdict

class TelemetryParser(ABC):
    @abstractmethod
    async def parse(self, data: bytes) -> list[dict]:
        pass


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
        
        ESTRATEGIA:
        - Solo telemetría tiene tick_count confiable
        - Ordenamos todo por tick_count de telemetría
        - Insertamos timings en posición cercana basándonos en lap_distance
        """
        packages = parsed_data['packages']
        
        # Separar telemetría y timings
        telemetry_list = [p for p in packages if p['type'] == 'telemetry']
        timings_list = [p for p in packages if p['type'] == 'timings']
        
        # Ordenar telemetría por tick_count
        telemetry_list.sort(key=lambda x: x['tick_count'])
        
        # Crear línea de tiempo inicial con telemetría
        timeline = telemetry_list.copy()
        
        # Insertar cada timing cerca de la telemetría más cercana
        for timing in timings_list:
            best_idx = 0
            min_distance = float('inf')
            
            # Buscar la telemetría más cercana por lap_distance
            for idx, tel in enumerate(timeline):
                if tel['type'] == 'telemetry':
                    # Comparar distancia en vuelta
                    tel_distance = tel.get('lap_distance', 0)
                    tim_distance = timing.get('lap_distance', 0)
                    distance_diff = abs(tel_distance - tim_distance)
                    
                    if distance_diff < min_distance:
                        min_distance = distance_diff
                        best_idx = idx
            
            # Insertar timing después de la telemetría más cercana
            timeline.insert(best_idx + 1, timing)
        
        # Agrupar por vueltas
        laps = []
        current_lap = 0

        for entry in timeline:
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
        Parsea paquete tipo 0 (eCarPhysics) - CON COMBUSTIBLE Y TICK_COUNT
        """
        if len(data) < 556:
            return None

        try:
            offset = 12

            offset += 1  # viewed_participant
            offset += 1  # throttle_raw
            offset += 1  # brake_raw
            offset += 1  # steering_raw
            offset += 1  # clutch_raw
            offset += 1  # car_flags
            offset += 2  # oil_temp
            offset += 2  # oil_pressure
            offset += 2  # water_temp
            offset += 2  # water_pressure
            offset += 2  # fuel_pressure
            
            fuel_capacity = struct.unpack_from('B', data, offset)[0]
            offset += 1
            
            brake = struct.unpack_from('B', data, offset)[0] / 255.0
            offset += 1
            throttle = struct.unpack_from('B', data, offset)[0] / 255.0
            offset += 1
            offset += 1  # clutch

            fuel_level_percentage = struct.unpack_from('<f', data, offset)[0]
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

            yaw = struct.unpack_from('<f', data, offset)[0]
            offset += 12  # orientation completa
            offset += 12  # local_velocity
            offset += 12  # world_velocity
            offset += 12  # angular_velocity
            offset += 12  # local_acceleration
            offset += 12  # world_acceleration
            offset += 12  # extents_centre

            # Saltar neumáticos y suspensión
            offset += 4 + 4 + 16 + 16 + 4 + 16 + 4 + 4 + 4 + 8
            offset += 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8
            offset += 16 + 16 + 16 + 16 + 8 + 8
            offset += 4 + 4 + 2 + 1 + 1 + 1 + 4 + 1 + 160 + 4

            # Posición absoluta
            pos_x = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            pos_y = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            pos_z = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            offset += 1  # brake_bias

            # 🔥 TICK_COUNT - Solo existe en telemetría
            tick_count = struct.unpack_from('<I', data, offset)[0]

            fuel_liters = fuel_level_percentage * fuel_capacity

            return {
                'throttle': throttle,
                'brake': brake,
                'steering': steering,
                'speed': speed,
                'rpm': rpm,
                'max_rpm': max_rpm,
                'gear': gear,
                'yaw': yaw,
                'pos_x': pos_x,
                'pos_y': pos_y,
                'pos_z': pos_z,
                'tick_count': tick_count,
                'fuel_capacity': fuel_capacity,
                'fuel_level_percentage': fuel_level_percentage,
                'fuel_liters': round(fuel_liters, 2),
            }

        except Exception as e:
            print(f"Error parseando telemetría: {e}")
            import traceback
            traceback.print_exc()
            return None

    def parse_timings(self, data):
        """
        Parsea paquete tipo 3 (eTimings)
        ⚠️ ESTE PAQUETE NO TIENE TICK_COUNT
        Tamaño: ~128 bytes (variable según número de participantes)
        """
        if len(data) < 50:
            return None

        try:
            offset = 12
            
            # sNumParticipants
            num_participants = struct.unpack_from('b', data, offset)[0]
            offset += 1
            
            # sParticipantsChangedTimestamp (podemos usarlo como identificador)
            participants_timestamp = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            
            # sEventTimeRemaining
            offset += 4
            
            # Splits
            offset += 4 + 4 + 4

            # Primer participante (jugador)
            offset += 6  # world_position[3]
            offset += 6  # orientation[3]
            
            lap_distance = struct.unpack_from('<H', data, offset)[0]
            offset += 2
            offset += 1  # race_position
            offset += 1  # sector
            offset += 1  # highest_flag
            offset += 1  # pit_mode_schedule
            offset += 2  # car_index
            offset += 1  # race_state
            
            current_lap = struct.unpack_from('B', data, offset)[0]
            offset += 1
            current_time = struct.unpack_from('<f', data, offset)[0]

            # ⚠️ NO INTENTAR LEER TICK_COUNT - NO EXISTE EN ESTE PAQUETE
            
            return {
                'current_lap': current_lap,
                'current_time': current_time,
                'lap_distance': lap_distance,
                # Usar participants_timestamp como identificador auxiliar
                'timestamp': participants_timestamp,
            }

        except Exception as e:
            print(f"Error parseando timings: {e}")
            import traceback
            traceback.print_exc()
            return None