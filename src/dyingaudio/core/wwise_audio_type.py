from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable


UNKNOWN_AUDIO_TYPE = "unknown"
SOUND_AUDIO_TYPE = "sound"
SOUND_SFX_AUDIO_TYPE = "sound_sfx"
SOUND_VOICE_AUDIO_TYPE = "sound_voice"
MUSIC_TRACK_AUDIO_TYPE = "music_track"

CONFIDENCE_CONFIRMED = "confirmed"
CONFIDENCE_INFERRED = "inferred"
CONFIDENCE_UNKNOWN = "unknown"

SOUND_OBJECT_TYPES = frozenset({2})
MUSIC_OBJECT_TYPES = frozenset({10, 11, 12, 13})
GENERIC_CONTAINER_OBJECT_TYPES = frozenset({3, 4, 5, 7})

_VOICE_CONTEXT_TOKENS = frozenset(
    {
        "dialog",
        "dialogue",
        "dub",
        "line",
        "lines",
        "locale",
        "localized",
        "localization",
        "narration",
        "narrator",
        "speech",
        "subtitle",
        "subtitles",
        "voice",
        "voices",
        "vo",
        "vocals",
    }
)
_GENERIC_LANGUAGE_TOKENS = frozenset({"", "default", "none", "null", "sfx", "sound"})


@dataclass(frozen=True, slots=True)
class AudioTypeResolution:
    audio_type: str = UNKNOWN_AUDIO_TYPE
    confidence: str = CONFIDENCE_UNKNOWN
    note: str = ""

    @property
    def label(self) -> str:
        return audio_type_label(self.audio_type, self.confidence)


def audio_type_label(audio_type: str, confidence: str = CONFIDENCE_CONFIRMED) -> str:
    base = {
        SOUND_SFX_AUDIO_TYPE: "Sound SFX",
        SOUND_VOICE_AUDIO_TYPE: "Sound Voice",
        MUSIC_TRACK_AUDIO_TYPE: "Music Track",
        SOUND_AUDIO_TYPE: "Sound",
        UNKNOWN_AUDIO_TYPE: "Unknown",
    }.get(audio_type, "Unknown")
    if base == "Unknown" or confidence != CONFIDENCE_INFERRED:
        return base
    return f"{base} (inferred)"


def normalize_object_types(object_types: Iterable[int | str | None]) -> tuple[int, ...]:
    normalized: set[int] = set()
    for value in object_types:
        if value is None:
            continue
        try:
            normalized.add(int(value))
        except (TypeError, ValueError):
            continue
    return tuple(sorted(normalized))


def has_voice_context(*values: str | None) -> bool:
    for value in values:
        if not value:
            continue
        tokens = {token for token in re.split(r"[^a-z0-9]+", value.casefold()) if token}
        if tokens & _VOICE_CONTEXT_TOKENS:
            return True
    return False


def language_suggests_voice(language_name: str | None) -> bool:
    if not language_name:
        return False
    normalized = language_name.casefold().strip()
    if normalized in _GENERIC_LANGUAGE_TOKENS:
        return False
    return True


def infer_audio_type(
    *,
    object_types: Iterable[int | str | None] = (),
    archive_set: str = "",
    archive_name: str = "",
    bank_name: str = "",
    event_name: str = "",
    source_pack: str = "",
    language_name: str = "",
) -> AudioTypeResolution:
    normalized_types = set(normalize_object_types(object_types))
    voice_context = language_suggests_voice(language_name) or has_voice_context(
        archive_set,
        archive_name,
        bank_name,
        event_name,
        source_pack,
        language_name,
    )

    if 11 in normalized_types:
        return AudioTypeResolution(
            audio_type=MUSIC_TRACK_AUDIO_TYPE,
            confidence=CONFIDENCE_CONFIRMED,
            note="Resolved from a Wwise Music Track object.",
        )
    if normalized_types & (MUSIC_OBJECT_TYPES - {11}):
        return AudioTypeResolution(
            audio_type=MUSIC_TRACK_AUDIO_TYPE,
            confidence=CONFIDENCE_INFERRED,
            note="Resolved from a music container object without a direct Music Track source.",
        )
    if 2 in normalized_types:
        if voice_context:
            return AudioTypeResolution(
                audio_type=SOUND_VOICE_AUDIO_TYPE,
                confidence=CONFIDENCE_INFERRED,
                note="Resolved from a Wwise Sound object in a localized or speech-like context.",
            )
        return AudioTypeResolution(
            audio_type=SOUND_AUDIO_TYPE,
            confidence=CONFIDENCE_CONFIRMED,
            note="Resolved from a Wwise Sound object.",
        )
    if normalized_types & GENERIC_CONTAINER_OBJECT_TYPES:
        if voice_context:
            return AudioTypeResolution(
                audio_type=SOUND_VOICE_AUDIO_TYPE,
                confidence=CONFIDENCE_INFERRED,
                note="Resolved through a container in a localized or speech-like context.",
            )
        return AudioTypeResolution(
            audio_type=SOUND_AUDIO_TYPE,
            confidence=CONFIDENCE_INFERRED,
            note="Resolved through a container without a direct Sound or Music Track object.",
        )
    if voice_context:
        return AudioTypeResolution(
            audio_type=SOUND_VOICE_AUDIO_TYPE,
            confidence=CONFIDENCE_INFERRED,
            note="Inferred from localized or speech-like pack context.",
        )
    return AudioTypeResolution(
        audio_type=UNKNOWN_AUDIO_TYPE,
        confidence=CONFIDENCE_UNKNOWN,
        note="No reliable Wwise object or localization context was available.",
    )