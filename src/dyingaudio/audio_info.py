from __future__ import annotations

import wave
from dataclasses import dataclass
from pathlib import Path

from dyingaudio.core.media_tools import discover_media_tools, ffprobe_audio, vgmstream_probe_audio


@dataclass(slots=True)
class AudioMetadata:
    duration_seconds: float = 0.0
    duration_ms: int = 0
    sample_count_48k: int = 0
    detected_sample_rate: int = 0
    channel_count: int = 0
    notes: str = ""


def _metadata_from_duration(
    duration_seconds: float,
    detected_sample_rate: int,
    channel_count: int = 0,
    notes: str = "",
) -> AudioMetadata:
    return AudioMetadata(
        duration_seconds=duration_seconds,
        duration_ms=int(round(duration_seconds * 1000.0)),
        sample_count_48k=int(round(duration_seconds * 48000.0)),
        detected_sample_rate=detected_sample_rate,
        channel_count=channel_count,
        notes=notes,
    )


def _probe_wav(path: Path) -> AudioMetadata:
    with wave.open(str(path), "rb") as handle:
        frame_rate = handle.getframerate()
        frame_count = handle.getnframes()
        channel_count = handle.getnchannels()

    if frame_rate <= 0:
        raise ValueError(f"Invalid WAV frame rate in '{path}'.")

    return _metadata_from_duration(frame_count / float(frame_rate), frame_rate, channel_count, "WAV metadata loaded.")


def _probe_ogg_vorbis(path: Path) -> AudioMetadata:
    data = path.read_bytes()
    offset = 0
    packet = bytearray()
    sample_rate = 0
    channel_count = 0
    final_granule = 0

    while offset < len(data):
        if data[offset:offset + 4] != b"OggS":
            raise ValueError(f"Unsupported OGG layout in '{path}'.")

        segment_count = data[offset + 26]
        segment_table_start = offset + 27
        segment_table_end = segment_table_start + segment_count
        lacing_values = data[segment_table_start:segment_table_end]
        payload_start = segment_table_end
        payload_size = sum(lacing_values)
        payload_end = payload_start + payload_size
        granule_position = int.from_bytes(data[offset + 6:offset + 14], "little", signed=False)
        if granule_position > 0:
            final_granule = granule_position

        payload = memoryview(data)[payload_start:payload_end]
        payload_offset = 0
        for lacing in lacing_values:
            packet.extend(payload[payload_offset:payload_offset + lacing])
            payload_offset += lacing
            if lacing < 255:
                if not sample_rate and len(packet) >= 16 and packet[0] == 1 and packet[1:7] == b"vorbis":
                    channel_count = int(packet[11])
                    sample_rate = int.from_bytes(packet[12:16], "little", signed=False)
                packet.clear()

        offset = payload_end

    if sample_rate <= 0 or final_granule <= 0:
        raise ValueError(f"Could not determine OGG duration for '{path}'.")

    return _metadata_from_duration(final_granule / float(sample_rate), sample_rate, channel_count, "OGG Vorbis metadata loaded.")


def probe_audio_metadata(path: str | Path) -> AudioMetadata:
    resolved = Path(path)
    suffix = resolved.suffix.lower()

    if suffix == ".wav":
        return _probe_wav(resolved)
    if suffix == ".ogg":
        return _probe_ogg_vorbis(resolved)

    tools = discover_media_tools()
    if suffix == ".wem":
        vgmstream_result = vgmstream_probe_audio(resolved, tools)
        if vgmstream_result is not None:
            duration_seconds, detected_sample_rate, channel_count, sample_count, notes = vgmstream_result
            metadata = _metadata_from_duration(duration_seconds, detected_sample_rate, channel_count, notes)
            metadata.sample_count_48k = sample_count
            return metadata

    ffprobe_result = ffprobe_audio(resolved, tools)
    if ffprobe_result is not None:
        duration_seconds, detected_sample_rate, channel_count, notes = ffprobe_result
        return _metadata_from_duration(duration_seconds, detected_sample_rate, channel_count, notes)

    return AudioMetadata(notes=f"No metadata parser for '{suffix or 'unknown'}' files.")
