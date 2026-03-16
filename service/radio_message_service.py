"""
Radio-style message generation for lap telemetry.

Produces varied, natural-sounding engineer radio messages using
driving terminology for time references (e.g. "two tens" instead of "0.2s").
"""
import random


# ---------------------------------------------------------------------------
# Time formatting in driving terms
# ---------------------------------------------------------------------------

def _format_time_driving(seconds: float) -> str:
    """
    Convert a time delta into natural racing engineer language.

    Examples:
        0.05  -> "half a tenth"
        0.1   -> "a tenth"
        0.2   -> "two tenths"
        0.35  -> "three and a half tenths"
        1.0   -> "a second"
        1.5   -> "a second and a half"
        2.3   -> "two seconds and three tenths"
        0.015 -> "one hundredth"
    """
    if seconds < 0.025:
        return "a few hundredths"
    if seconds < 0.075:
        return "half a tenth"
    if seconds < 0.15:
        return "a tenth"

    whole_seconds = int(seconds)
    remainder = seconds - whole_seconds

    tenths = round(remainder * 10)

    # Pure tenths (under 1 second)
    if whole_seconds == 0:
        if tenths == 1:
            return "a tenth"
        if tenths == 5:
            return "half a second"
        return f"{_number_word(tenths)} tenths"

    # Whole seconds
    sec_word = "a second" if whole_seconds == 1 else f"{_number_word(whole_seconds)} seconds"

    if tenths == 0:
        return sec_word
    if tenths == 5:
        return f"{sec_word} and a half"
    tenths_word = "a tenth" if tenths == 1 else f"{_number_word(tenths)} tenths"
    return f"{sec_word} and {tenths_word}"


def _number_word(n: int) -> str:
    """Convert small integers to words."""
    words = {
        1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
        6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
    }
    return words.get(n, str(n))


# ---------------------------------------------------------------------------
# Lap time formatting
# ---------------------------------------------------------------------------

def _format_lap_time(lap_time: float) -> str:
    """Format lap time as M:SS.mmm for radio callout."""
    minutes = int(lap_time // 60)
    seconds = lap_time % 60
    return f"{minutes}:{seconds:06.3f}"


# ---------------------------------------------------------------------------
# Zone description helpers
# ---------------------------------------------------------------------------

def _describe_zone(distance: int) -> str:
    """Produce a natural reference to a track zone by distance."""
    templates = [
        f"around the {distance} meter mark",
        f"at {distance} meters",
        f"around {distance} meters in",
    ]
    return random.choice(templates)


def _describe_loss(zone: dict) -> str:
    """Describe a single time-loss zone naturally."""
    dist = zone["distance"]
    loss = zone["time_loss"]
    time_str = _format_time_driving(loss)
    place = _describe_zone(dist)

    templates = [
        f"{place}, losing {time_str}",
        f"{place} you're dropping {time_str}",
        f"you're giving away {time_str} {place}",
        f"losing {time_str} {place}",
    ]
    return random.choice(templates)


# ---------------------------------------------------------------------------
# Message templates
# ---------------------------------------------------------------------------

_FASTEST_TEMPLATES = [
    "Fastest lap! {lap}. Great job, keep it up!",
    "Purple! {lap}. That's the fastest, brilliant stuff.",
    "Fastest lap, {lap}! That's the one, keep this pace.",
    "{lap}, fastest lap! Nice driving, stay on it.",
    "That's P1 on the timing board, {lap}. Keep pushing.",
    "Fastest lap! {lap}. You're in the zone, keep it clean.",
]

_CLEAN_TEMPLATES = [
    "Lap time {lap}. Clean lap, keep pushing.",
    "{lap}. Solid lap, no time lost, keep it consistent.",
    "That's a {lap}. Looking tidy, stay focused.",
    "{lap}. Good lap, nothing to report, keep your rhythm.",
    "Lap time {lap}. All clean, maintain this pace.",
    "{lap}, clean run. Keep your head down.",
]

_LOSS_INTRO_TEMPLATES = [
    "Lap time {lap}. ",
    "{lap}. ",
    "That's a {lap}. ",
    "Okay {lap}. ",
]

_LOSS_BODY_ONE_ZONE = [
    "Watch out {zone}.",
    "Main thing is {zone}.",
    "You're leaving time on the table, {zone}.",
    "Focus area: {zone}.",
    "Biggest loss is {zone}.",
]

_LOSS_BODY_TWO_ZONES = [
    "{z1}, and also {z2}.",
    "{z1}. Also {z2}.",
    "Two areas to work on: {z1}, and {z2}.",
    "{z1}. Second thing, {z2}.",
    "Main losses are {z1}, and {z2}.",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_radio_message(
    time_loss_zones: list[dict],
    lap_time: float,
    is_fastest: bool,
) -> str:
    """Build a varied, natural radio-style message for the completed lap."""
    lap_str = _format_lap_time(lap_time)

    if is_fastest:
        return random.choice(_FASTEST_TEMPLATES).format(lap=lap_str)

    if not time_loss_zones:
        return random.choice(_CLEAN_TEMPLATES).format(lap=lap_str)

    intro = random.choice(_LOSS_INTRO_TEMPLATES).format(lap=lap_str)

    if len(time_loss_zones) == 1:
        zone_desc = _describe_loss(time_loss_zones[0])
        body = random.choice(_LOSS_BODY_ONE_ZONE).format(zone=zone_desc)
    else:
        z1 = _describe_loss(time_loss_zones[0])
        z2 = _describe_loss(time_loss_zones[1])
        body = random.choice(_LOSS_BODY_TWO_ZONES).format(z1=z1, z2=z2)

    return intro + body
