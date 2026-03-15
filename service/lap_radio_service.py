"""
Service for lap radio analysis and TTS generation.

Compares the last completed lap vs the fastest lap to find time loss zones,
generates a radio-style message, calls piper-tts to synthesize audio, and
stores everything in Redis.
"""
import json
import os
from typing import Optional

import httpx

PIPER_TTS_URL = os.getenv("PIPER_TTS_URL", "http://localhost:8000")
TTS_VOICE = os.getenv("TTS_VOICE", "en_US")

BUCKET_SIZE = 100        # meters between analysis buckets
TOP_K_ZONES = 3          # max time-loss zones to report
LOSS_THRESHOLD = 0.10    # minimum seconds lost in a bucket to be reported

REDIS_KEY_LAST_LAP   = "telemetry:last_lap"
REDIS_KEY_FASTEST_LAP = "telemetry:fastest_lap"
REDIS_KEY_LAST_AUDIO  = "telemetry:last_lap_audio"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_lap_time(lap_data: dict) -> float:
    """Return the maximum current_time found in the timings entries of a lap."""
    timings = [e for e in lap_data.get("data", []) if e.get("type") == "timings"]
    if not timings:
        return 0.0
    return max(t.get("current_time", 0.0) for t in timings)


def _build_dist_time_map(lap_data: dict) -> tuple[dict[int, float], list[int]]:
    """Build a {distance_int: time_float} mapping and sorted distance keys."""
    timings = [
        e for e in lap_data.get("data", [])
        if e.get("type") == "timings" and e.get("lap_distance", 0) > 0
    ]
    dist_map: dict[int, float] = {}
    for t in timings:
        d = int(t["lap_distance"])
        dist_map[d] = t.get("current_time", 0.0)
    sorted_keys = sorted(dist_map.keys())
    return dist_map, sorted_keys


def _interpolate(dist_map: dict[int, float], sorted_keys: list[int], target: int) -> Optional[float]:
    """Linearly interpolate time at *target* distance."""
    if not sorted_keys:
        return None
    if target <= sorted_keys[0]:
        return dist_map[sorted_keys[0]]
    if target >= sorted_keys[-1]:
        return dist_map[sorted_keys[-1]]
    for i in range(len(sorted_keys) - 1):
        d0, d1 = sorted_keys[i], sorted_keys[i + 1]
        if d0 <= target <= d1:
            t0, t1 = dist_map[d0], dist_map[d1]
            ratio = (target - d0) / (d1 - d0) if d1 != d0 else 0.0
            return t0 + ratio * (t1 - t0)
    return None


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyze_time_loss(current_lap: dict, fastest_lap: dict) -> list[dict]:
    """
    Find the top-K distance zones where the driver is losing time vs the
    fastest lap.

    Returns a list of dicts: [{distance: int, time_loss: float}, ...]
    sorted by time_loss descending.
    """
    curr_map, curr_keys = _build_dist_time_map(current_lap)
    fast_map, fast_keys = _build_dist_time_map(fastest_lap)

    if not curr_keys or not fast_keys:
        return []

    max_dist = max(curr_keys[-1], fast_keys[-1])
    zones: list[dict] = []
    prev_delta = 0.0

    for bucket_start in range(0, int(max_dist) + 1, BUCKET_SIZE):
        sample_dist = bucket_start + BUCKET_SIZE // 2

        curr_t = _interpolate(curr_map, curr_keys, sample_dist)
        fast_t = _interpolate(fast_map, fast_keys, sample_dist)

        if curr_t is not None and fast_t is not None:
            delta = curr_t - fast_t
            loss_in_bucket = delta - prev_delta
            prev_delta = delta
            if loss_in_bucket > LOSS_THRESHOLD:
                zones.append({
                    "distance": sample_dist,
                    "time_loss": round(loss_in_bucket, 3),
                })

    zones.sort(key=lambda x: x["time_loss"], reverse=True)
    return zones[:TOP_K_ZONES]


def generate_radio_message(
    time_loss_zones: list[dict],
    lap_time: float,
    is_fastest: bool,
) -> str:
    """Build a short radio-style message for the completed lap."""
    minutes = int(lap_time // 60)
    seconds = lap_time % 60
    lap_str = f"{minutes}:{seconds:06.3f}"

    if is_fastest:
        return f"Fastest lap! {lap_str}. Great job, keep it up!"

    if not time_loss_zones:
        return f"Lap time {lap_str}. Clean lap, keep pushing."

    parts = [
        f"around {z['distance']} meters losing {z['time_loss']:.1f} seconds"
        for z in time_loss_zones[:2]
    ]
    return f"Lap time {lap_str}. You are losing time {', and '.join(parts)}."


# ---------------------------------------------------------------------------
# TTS
# ---------------------------------------------------------------------------

async def generate_tts_audio(text: str, voice: str = TTS_VOICE) -> Optional[str]:
    """Call the piper-tts service and return the audio URL, or None on failure."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{PIPER_TTS_URL}/tts",
                json={"text": text, "voice": voice, "radio_effect": True},
            )
            if response.status_code == 200:
                return response.json().get("url")
            print(f"[LapRadio] TTS error {response.status_code}: {response.text}")
    except Exception as exc:
        print(f"[LapRadio] TTS exception: {exc}")
    return None


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def store_last_lap(redis_conn, lap_data: dict) -> None:
    redis_conn.set(REDIS_KEY_LAST_LAP, json.dumps(lap_data))


def get_fastest_lap(redis_conn) -> Optional[dict]:
    raw = redis_conn.get(REDIS_KEY_FASTEST_LAP)
    return json.loads(raw) if raw else None


def update_fastest_lap_if_needed(redis_conn, lap_data: dict) -> bool:
    """Replace the stored fastest lap if *lap_data* is faster. Returns True when updated."""
    current_time = _get_lap_time(lap_data)
    if current_time <= 0:
        return False

    fastest = get_fastest_lap(redis_conn)
    if fastest is None or current_time < _get_lap_time(fastest):
        redis_conn.set(REDIS_KEY_FASTEST_LAP, json.dumps(lap_data))
        return True
    return False


def store_last_audio_url(redis_conn, url: str) -> None:
    redis_conn.set(REDIS_KEY_LAST_AUDIO, url)


def get_last_audio_url(redis_conn) -> Optional[str]:
    raw = redis_conn.get(REDIS_KEY_LAST_AUDIO)
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else raw
