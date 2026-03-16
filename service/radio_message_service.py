"""
Radio-style message generation for lap telemetry.

Produces varied, authentic race-engineer radio messages using proper
motorsport terminology: tenths, hundredths, braking zones, traction,
apex, entry/exit — mimicking real F1 pit wall communication style.
"""
import random


# ---------------------------------------------------------------------------
# Number words
# ---------------------------------------------------------------------------

_NUM_WORDS = {
    0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
    6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
    11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen",
    15: "fifteen", 16: "sixteen", 17: "seventeen", 18: "eighteen",
    19: "nineteen", 20: "twenty",
    30: "thirty", 40: "forty", 50: "fifty", 60: "sixty",
}


def _number_word(n: int) -> str:
    if n in _NUM_WORDS:
        return _NUM_WORDS[n]
    if 21 <= n <= 59:
        tens, ones = divmod(n, 10)
        return f"{_NUM_WORDS[tens * 10]} {_NUM_WORDS[ones]}" if ones else _NUM_WORDS[tens * 10]
    return str(n)


# ---------------------------------------------------------------------------
# Lap time formatting — spoken style like real engineers
# ---------------------------------------------------------------------------

def _format_lap_time(lap_time: float) -> str:
    """
    Format lap time the way a real race engineer says it on the radio.

    Real engineers drop "minutes" and "seconds" — they just say the numbers
    with tenths: "one twenty-three four" for 1:23.4, or just "twenty-three
    four" when the minute is obvious.

    We keep the numeric format for clarity since TTS will read it, but use
    the condensed radio style: "1:23.4" (tenths only, no thousandths).
    """
    minutes = int(lap_time // 60)
    seconds = lap_time % 60
    whole_sec = int(seconds)
    tenths = int(round((seconds - whole_sec) * 10)) % 10
    return f"{minutes}:{whole_sec:02d}.{tenths}"


def _format_lap_time_spoken(lap_time: float) -> str:
    """
    Fully spoken version: "one twenty three four" — no colons, no dots.
    Used in some message templates for a more natural TTS output.
    """
    minutes = int(lap_time // 60)
    seconds = lap_time % 60
    whole_sec = int(seconds)
    tenths = int(round((seconds - whole_sec) * 10)) % 10
    min_word = _number_word(minutes)
    sec_word = _number_word(whole_sec)
    tenth_word = _number_word(tenths)
    return f"{min_word} {sec_word} {tenth_word}"


# ---------------------------------------------------------------------------
# Time delta formatting — proper motorsport language
# ---------------------------------------------------------------------------

def _format_time_driving(seconds: float) -> str:
    """
    Convert a time delta into authentic race engineer language.

    Real engineers say "tenths" and "hundredths", never decimals.
    "Five tenths", not "zero point five". "A couple of hundredths",
    not "0.02 seconds".

    Examples:
        0.02  -> "a couple of hundredths"
        0.05  -> "half a tenth"
        0.1   -> "a tenth"
        0.2   -> "two tenths"
        0.5   -> "half a second"
        1.0   -> "a second"
        1.5   -> "a second and a half"
        2.3   -> "two seconds and three tenths"
    """
    if seconds < 0.025:
        return "a couple of hundredths"
    if seconds < 0.075:
        return "half a tenth"
    if seconds < 0.15:
        return "a tenth"

    whole = int(seconds)
    tenths = round((seconds - whole) * 10)

    if whole == 0:
        if tenths == 1:
            return "a tenth"
        if tenths == 5:
            return "half a second"
        return f"{_number_word(tenths)} tenths"

    sec_str = "a second" if whole == 1 else f"{_number_word(whole)} seconds"
    if tenths == 0:
        return sec_str
    if tenths == 5:
        return f"{sec_str} and a half"
    t_str = "a tenth" if tenths == 1 else f"{_number_word(tenths)} tenths"
    return f"{sec_str} and {t_str}"


# ---------------------------------------------------------------------------
# Zone / track position descriptions
# ---------------------------------------------------------------------------

_ZONE_TEMPLATES = [
    "around the {d} meter mark",
    "at {d} meters",
    "around {d} meters in",
    "{d} meters into the lap",
    "around meter {d}",
]


def _describe_zone(distance: int) -> str:
    return random.choice(_ZONE_TEMPLATES).format(d=distance)


# ---------------------------------------------------------------------------
# Loss description — how the engineer calls out time lost
# ---------------------------------------------------------------------------

_LOSS_PHRASES = [
    "{place}, losing {time}",
    "{place}, you're dropping {time}",
    "you're giving up {time} {place}",
    "losing {time} {place}",
    "{place}, down {time}",
    "{place}, you're leaving {time} on the table",
    "you're {time} off the pace {place}",
    "{time} lost {place}",
    "dropping {time} {place}, we can find time there",
    "{place}, that's costing you {time}",
    "{place}, giving away {time}",
    "{place}, we're hemorrhaging {time}",
    "you're bleeding {time} {place}",
]


def _describe_loss(zone: dict) -> str:
    time_str = _format_time_driving(zone["time_loss"])
    place = _describe_zone(zone["distance"])
    return random.choice(_LOSS_PHRASES).format(place=place, time=time_str)


# ---------------------------------------------------------------------------
# Message template pools
# ---------------------------------------------------------------------------

_FASTEST_TEMPLATES = [
    # Short and punchy — the engineer is excited
    "Purple! {lap}. That's the fastest, brilliant stuff.",
    "Fastest lap! {lap}. That's the one, stay on it.",
    "{lap}, fastest lap! Nice driving, keep your head down.",
    "That's P1 on the timing board, {lap}. Keep pushing.",
    "Fastest lap, {lap}! You're in the zone, keep it clean.",
    "{lap}. New fastest. Great pace, maintain it.",
    "Purple, purple! {lap}. That's a mega lap, well done.",
    "Copy, fastest lap. {lap}. The pace is there, keep it consistent.",
    "{lap}! That's the quickest we've seen. Brilliant driving.",
    "Fastest! {spoken}. That's what we needed, stay focused.",
    "New personal best, {lap}. Strong lap, keep your head down.",
    "{lap}. You've gone purple. Lovely stuff, keep pushing.",
    "That's the benchmark. {lap}. Great job out there.",
    "Copy {lap}, fastest of the session. Keep this rhythm.",
    "Purple! {spoken}. That's the pace, brilliant.",
]

_CLEAN_TEMPLATES = [
    # Calm, steady — nothing to report
    "{lap}. Clean lap, keep pushing.",
    "{lap}. Solid run, nothing to report.",
    "Copy, {lap}. Looking tidy, stay focused.",
    "{lap}. Good pace, keep it consistent.",
    "{lap}. All clean, maintain this rhythm.",
    "{lap}. Tidy lap, keep your head down.",
    "Lap time {lap}. No time lost, good job.",
    "{spoken}. All looking good this end, keep pushing.",
    "{lap}. Good lap, just maintain the pace.",
    "{lap}. Happy with that one, keep your head down.",
    "Copy {lap}. Nice and clean, stay on it.",
    "{lap}. That's a solid one. No issues, keep it up.",
    "{lap}. Good rhythm, stay focused.",
    "Okay {lap}. Clean run through, nothing to work on.",
    "{lap}. Smooth lap, keep that consistency.",
    "{spoken}. All fine, stay in the zone.",
    "{lap}. Tidy work, keep this up.",
]

_LOSS_INTRO = [
    "Lap time {lap}. ",
    "{lap}. ",
    "That's a {lap}. ",
    "Okay, {lap}. ",
    "Copy, {lap}. ",
    "{spoken}. ",
    "Right, {lap}. ",
]

_LOSS_BODY_ONE = [
    "Main loss is {z1}.",
    "Watch {z1}.",
    "We can find time, {z1}.",
    "Focus on {z1}. We can improve there.",
    "Biggest thing is {z1}.",
    "Work on {z1}, that's where the time is.",
    "{z1}. Tighten that up and we're in good shape.",
    "{z1}. Focus there next lap.",
    "Just the one area, {z1}.",
    "{z1}. That's where the lap time is.",
    "Main area to work on: {z1}.",
    "{z1}. Clean that up and we're right on the pace.",
]

_LOSS_BODY_TWO = [
    "{z1}, and also {z2}.",
    "{z1}. Second thing, {z2}.",
    "Two areas to work on. {z1}, and {z2}.",
    "Main losses: {z1}, and {z2}.",
    "{z1}. Plus {z2}.",
    "{z1}. And then {z2}. Work on those two.",
    "Couple of things: {z1}, and {z2}.",
    "{z1}. We're also {z2}.",
    "Two zones costing you, {z1}, and {z2}.",
    "{z1}, and separately {z2}. Clean those up.",
    "Okay two things. {z1}, and {z2}.",
    "{z1}. On top of that, {z2}.",
]

# Occasional encouragement suffix — appended randomly ~40% of the time
_ENCOURAGEMENT_SUFFIX = [
    " Keep your head down.",
    " Stay focused.",
    " Keep pushing.",
    " We can find that time.",
    " Stay on it.",
    " You've got the pace.",
    " Tighten it up.",
    " The pace is there.",
    " Stay in the zone.",
    " Let's get it next lap.",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_radio_message(
    time_loss_zones: list[dict],
    lap_time: float,
    is_fastest: bool,
) -> str:
    """Build a varied, authentic race-engineer radio message for the completed lap."""
    lap_str = _format_lap_time(lap_time)
    spoken = _format_lap_time_spoken(lap_time)

    if is_fastest:
        return random.choice(_FASTEST_TEMPLATES).format(lap=lap_str, spoken=spoken)

    if not time_loss_zones:
        return random.choice(_CLEAN_TEMPLATES).format(lap=lap_str, spoken=spoken)

    intro = random.choice(_LOSS_INTRO).format(lap=lap_str, spoken=spoken)

    if len(time_loss_zones) == 1:
        z1 = _describe_loss(time_loss_zones[0])
        body = random.choice(_LOSS_BODY_ONE).format(z1=z1)
    else:
        z1 = _describe_loss(time_loss_zones[0])
        z2 = _describe_loss(time_loss_zones[1])
        body = random.choice(_LOSS_BODY_TWO).format(z1=z1, z2=z2)

    msg = intro + body

    if random.random() < 0.4:
        msg += random.choice(_ENCOURAGEMENT_SUFFIX)

    return msg
