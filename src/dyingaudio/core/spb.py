from __future__ import annotations

import math
import re
import struct
import tempfile
import wave
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from dyingaudio.core.localized_text import load_text_catalog
from dyingaudio.core.media_tools import (
    MediaTools,
    _decode_vgmstream_to_wav,
    decode_audio_to_wav,
    discover_media_tools,
)
from dyingaudio.models import AudioEntry


SPB_VERSION = 1
SPB_ENDIAN_FLAG = 1
SPB_HEADER_SIZE = 20
SPB_TABLE_ROW_SIZE = 80
SPB_ENTRY_NAME_SIZE = 68
SPB_PAYLOAD_VERSION = 4
SPB_FRAME_STEP_SECONDS = 0.05
SPB_TRACK_PADDING = b"\x00" * 5
SPB_TRACK_LABELS = ("open", "W", "ShCh", "PBM", "FV", "wide", "tBack", "tRoof", "tTeeth")
SPB_TRACK_MAX_WEIGHTS = {
    "open": 0.55,
    "W": 0.80,
    "ShCh": 0.70,
    "PBM": 0.88,
    "FV": 0.75,
    "wide": 0.80,
    "tBack": 0.75,
    "tRoof": 0.80,
    "tTeeth": 0.88,
}


ProgressCallback = Callable[[str, float | None, float | None], None]


@dataclass(slots=True)
class SpeechTrack:
    label: str
    curve: list[int]
    max_weight: float = 1.0


@dataclass(slots=True)
class SpeechEntry:
    name: str
    frame_count: int
    tracks: list[SpeechTrack] = field(default_factory=list)
    frame_step: float = SPB_FRAME_STEP_SECONDS
    payload_version: int = SPB_PAYLOAD_VERSION


@dataclass(slots=True)
class SpeechBank:
    entries: list[SpeechEntry] = field(default_factory=list)
    version: int = SPB_VERSION
    endian_flag: int = SPB_ENDIAN_FLAG
    reserved: int = 0


@dataclass(slots=True)
class SpeechBuildOptions:
    text_source: str | Path | Iterable[str | Path] | None = None
    auto_text_root: str | Path | None = None
    media_tools: MediaTools | None = None
    log: Callable[[str], None] | None = None
    silence_gate: float = 0.08


@dataclass(slots=True)
class SpeechBuildResult:
    spb_path: Path
    bank: SpeechBank
    matched_text_count: int = 0
    audio_only_count: int = 0
    text_only_count: int = 0
    skipped_entries: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_entries)

    def summary(self) -> str:
        return (
            f"SPB entries: {len(self.bank.entries)}; text+audio: {self.matched_text_count}; "
            f"audio-only: {self.audio_only_count}; text-only: {self.text_only_count}; skipped: {self.skipped_count}"
        )


def _decode_curve(encoded: bytes, frame_count: int) -> list[int]:
    curve: list[int] = []
    index = 0
    while index < len(encoded):
        value = encoded[index]
        index += 1
        if value == 0xFF:
            if index + 2 > len(encoded):
                raise ValueError("Truncated SPB zero-run curve.")
            zero_count = struct.unpack_from("<H", encoded, index)[0]
            index += 2
            curve.extend([0] * zero_count)
        else:
            curve.append(value)
    if len(curve) < frame_count:
        curve.extend([0] * (frame_count - len(curve)))
    return curve[:frame_count]


def _encode_curve(curve: list[int]) -> bytes:
    output = bytearray()
    index = 0
    while index < len(curve):
        value = max(0, min(254, int(curve[index])))
        if value == 0:
            run_end = index + 1
            while run_end < len(curve) and int(curve[run_end]) == 0:
                run_end += 1
            run_length = run_end - index
            if run_length >= 3:
                remaining = run_length
                while remaining:
                    chunk = min(remaining, 0xFFFF)
                    output.append(0xFF)
                    output.extend(struct.pack("<H", chunk))
                    remaining -= chunk
            else:
                output.extend(b"\x00" * run_length)
            index = run_end
            continue
        output.append(value)
        index += 1
    return bytes(output)


def _parse_payload(data: bytes, entry_name: str) -> SpeechEntry:
    if len(data) < 16:
        raise ValueError(f"SPB entry '{entry_name}' has a truncated payload.")
    payload_version, frame_count, frame_step, track_count = struct.unpack_from("<IIfI", data, 0)
    offset = 16
    tracks: list[SpeechTrack] = []
    for _track_index in range(track_count):
        if offset >= len(data):
            raise ValueError(f"SPB entry '{entry_name}' has a truncated track list.")
        label_len = data[offset]
        offset += 1
        if offset + label_len + 9 > len(data):
            raise ValueError(f"SPB entry '{entry_name}' has a truncated track header.")
        label = data[offset : offset + label_len].decode("ascii", errors="replace")
        offset += label_len
        offset += 5
        max_weight, encoded_size = struct.unpack_from("<fI", data, offset)
        offset += 8
        if offset + encoded_size > len(data):
            raise ValueError(f"SPB entry '{entry_name}' has a truncated track curve.")
        encoded_curve = data[offset : offset + encoded_size]
        offset += encoded_size
        tracks.append(SpeechTrack(label=label, curve=_decode_curve(encoded_curve, frame_count), max_weight=max_weight))
    return SpeechEntry(
        name=entry_name,
        frame_count=frame_count,
        tracks=tracks,
        frame_step=frame_step,
        payload_version=payload_version,
    )


def parse_spb(path: str | Path) -> SpeechBank:
    data = Path(path).expanduser().resolve().read_bytes()
    if len(data) < SPB_HEADER_SIZE:
        raise ValueError("SPB file is smaller than the 20-byte header.")

    version, endian_flag, entry_count, _payload_size, reserved = struct.unpack_from("<IIIII", data, 0)
    table_end = SPB_HEADER_SIZE + entry_count * SPB_TABLE_ROW_SIZE
    if table_end > len(data):
        raise ValueError("SPB table extends past the end of the file.")

    entries: list[SpeechEntry] = []
    for index in range(entry_count):
        row_offset = SPB_HEADER_SIZE + index * SPB_TABLE_ROW_SIZE
        payload_offset, payload_size, _row_reserved = struct.unpack_from("<III", data, row_offset)
        raw_name = data[row_offset + 12 : row_offset + SPB_TABLE_ROW_SIZE]
        name = raw_name.split(b"\x00", 1)[0].decode("ascii", errors="replace")
        if payload_offset + payload_size > len(data):
            raise ValueError(f"SPB entry '{name or index}' payload extends past the end of the file.")
        entries.append(_parse_payload(data[payload_offset : payload_offset + payload_size], name))

    return SpeechBank(entries=entries, version=version, endian_flag=endian_flag, reserved=reserved)


def _encode_entry_name(name: str) -> bytes:
    raw_name = name.strip().casefold().encode("ascii", errors="replace")
    if len(raw_name) > SPB_ENTRY_NAME_SIZE:
        raise ValueError(f"SPB entry name is longer than {SPB_ENTRY_NAME_SIZE} bytes: {name!r}")
    return raw_name.ljust(SPB_ENTRY_NAME_SIZE, b"\x00")


def _pack_payload(entry: SpeechEntry) -> bytes:
    parts = [struct.pack("<IIfI", entry.payload_version, entry.frame_count, entry.frame_step, len(entry.tracks))]
    for track in entry.tracks:
        label = track.label.encode("ascii")
        if len(label) > 255:
            raise ValueError(f"SPB track label is too long: {track.label!r}")
        if len(track.curve) != entry.frame_count:
            raise ValueError(f"SPB track '{track.label}' curve length does not match entry '{entry.name}' frame count.")
        encoded_curve = _encode_curve(track.curve)
        parts.append(bytes([len(label)]))
        parts.append(label)
        parts.append(SPB_TRACK_PADDING)
        parts.append(struct.pack("<fI", float(track.max_weight), len(encoded_curve)))
        parts.append(encoded_curve)
    return b"".join(parts)


def write_spb(bank: SpeechBank, path: str | Path) -> Path:
    destination = Path(path).expanduser().resolve()
    if destination.suffix.lower() != ".spb":
        destination = destination.with_suffix(".spb")
    destination.parent.mkdir(parents=True, exist_ok=True)

    payloads = [_pack_payload(entry) for entry in bank.entries]
    payload_blob = b"".join(payloads)
    payload_offset = SPB_HEADER_SIZE + len(bank.entries) * SPB_TABLE_ROW_SIZE
    rows = bytearray()
    for entry, payload in zip(bank.entries, payloads, strict=True):
        rows.extend(struct.pack("<III", payload_offset, len(payload), 0))
        rows.extend(_encode_entry_name(entry.name))
        payload_offset += len(payload)

    header = struct.pack("<IIIII", bank.version, bank.endian_flag, len(bank.entries), len(payload_blob), bank.reserved)
    destination.write_bytes(header + bytes(rows) + payload_blob)
    return destination


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * percentile))))
    return ordered[index]


def _sample_value(data: bytes, offset: int, sample_width: int) -> float:
    if sample_width == 1:
        return (data[offset] - 128) / 128.0
    if sample_width == 2:
        return struct.unpack_from("<h", data, offset)[0] / 32768.0
    if sample_width == 3:
        value = int.from_bytes(data[offset : offset + 3], "little", signed=False)
        if value & 0x800000:
            value -= 0x1000000
        return value / 8388608.0
    if sample_width == 4:
        return struct.unpack_from("<i", data, offset)[0] / 2147483648.0
    return 0.0


def _read_wav_envelope(path: Path, *, duration_ms: int = 0, silence_gate: float = 0.08) -> tuple[list[float], int]:
    with wave.open(str(path), "rb") as handle:
        frame_rate = handle.getframerate()
        channel_count = handle.getnchannels()
        sample_width = handle.getsampwidth()
        total_frames = handle.getnframes()
        if frame_rate <= 0 or channel_count <= 0 or sample_width <= 0:
            raise ValueError(f"Invalid WAV metadata in '{path}'.")

        actual_duration_ms = int(round((total_frames / float(frame_rate)) * 1000.0))
        target_duration_ms = duration_ms or actual_duration_ms
        output_frame_count = max(1, int(math.ceil((target_duration_ms / 1000.0) / SPB_FRAME_STEP_SECONDS)))
        wav_frames_per_spb_frame = max(1, int(round(frame_rate * SPB_FRAME_STEP_SECONDS)))

        rms_values: list[float] = []
        for _index in range(output_frame_count):
            wav_frames = min(wav_frames_per_spb_frame, max(0, total_frames - handle.tell()))
            if wav_frames <= 0:
                rms_values.append(0.0)
                continue
            data = handle.readframes(wav_frames)
            step = sample_width
            sample_count = len(data) // step
            if sample_count <= 0:
                rms_values.append(0.0)
                continue
            square_sum = 0.0
            for offset in range(0, sample_count * step, step):
                value = _sample_value(data, offset, sample_width)
                square_sum += value * value
            rms_values.append(math.sqrt(square_sum / sample_count))

    baseline = _percentile(rms_values, 0.95)
    if baseline <= 0.0:
        return [0.0] * len(rms_values), target_duration_ms
    envelope = [min(1.0, value / baseline) for value in rms_values]
    return [value if value >= silence_gate else 0.0 for value in envelope], target_duration_ms


def _entry_duration_ms(entry: AudioEntry) -> int:
    if entry.duration_ms > 0:
        return entry.duration_ms
    if entry.sample_count > 0:
        return int(round((entry.sample_count / 48000.0) * 1000.0))
    return 0


def _audio_candidates(entry: AudioEntry) -> list[Path]:
    candidates: list[Path] = []
    for candidate in (entry.resolved_source_path(), entry.resolved_fsb_path()):
        if candidate is None:
            continue
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        key = str(resolved).casefold()
        if resolved.exists() and key not in {str(path).casefold() for path in candidates}:
            candidates.append(resolved)
    return candidates


def _decode_entry_audio(
    entry: AudioEntry,
    temp_root: Path,
    tools: MediaTools,
    log: Callable[[str], None],
    warnings: list[str],
) -> Path | None:
    for source_path in _audio_candidates(entry):
        try:
            if source_path.suffix.lower() == ".wav":
                return source_path
            output_path = temp_root / f"{entry.entry_name}_{source_path.stem}.wav"
            if source_path.suffix.lower() in {".fsb", ".wem"} and tools.vgmstream_path is not None:
                return _decode_vgmstream_to_wav(source_path, output_path, tools, log, label=f"'{source_path.name}'")
            return decode_audio_to_wav(source_path, output_path, log=log, tools=tools)
        except Exception as exc:
            warnings.append(f"{entry.entry_name}: could not decode {source_path.name} for SPB timing ({exc}).")
    return None


def _tokenize_visemes(text: str) -> list[str]:
    labels: list[str] = []
    for match in re.finditer(r"[A-Za-z']+", text):
        word = match.group(0).upper()
        index = 0
        while index < len(word):
            chunk = word[index:]
            if chunk.startswith("TH"):
                labels.append("tTeeth")
                index += 2
            elif chunk.startswith(("SH", "CH")):
                labels.append("ShCh")
                index += 2
            elif chunk.startswith(("OO", "OU", "OW")):
                labels.append("W")
                index += 2
            elif chunk.startswith(("AI", "AY", "AE", "EH")):
                labels.append("wide")
                index += 2
            elif word[index] in {"P", "B", "M"}:
                labels.append("PBM")
                index += 1
            elif word[index] in {"F", "V"}:
                labels.append("FV")
                index += 1
            elif word[index] in {"S", "Z", "J"}:
                labels.append("ShCh")
                index += 1
            elif word[index] in {"W", "U", "O"}:
                labels.append("W")
                index += 1
            elif word[index] in {"A", "E"}:
                labels.append("wide")
                index += 1
            elif word[index] in {"I", "Y"}:
                labels.append("open")
                index += 1
            elif word[index] in {"T", "D", "N"}:
                labels.append("tRoof")
                index += 1
            elif word[index] in {"K", "G", "R"}:
                labels.append("tBack")
                index += 1
            elif word[index] == "L":
                labels.append("tTeeth")
                index += 1
            else:
                index += 1
    return labels


def _curve_value(weight: float) -> int:
    return max(0, min(254, int(round(weight * 254.0))))


def _add_pulse(curves: dict[str, list[float]], label: str, frame_index: int, strength: float) -> None:
    curve = curves[label]
    for offset, scale in ((-1, 0.45), (0, 1.0), (1, 0.45)):
        target = frame_index + offset
        if 0 <= target < len(curve):
            curve[target] = max(curve[target], min(1.0, strength * scale))


def _build_speech_entry(
    entry_name: str,
    *,
    text: str | None,
    duration_ms: int,
    envelope: list[float] | None,
) -> SpeechEntry:
    frame_count = max(1, int(math.ceil((duration_ms / 1000.0) / SPB_FRAME_STEP_SECONDS)))
    if envelope is None:
        shaped_envelope = [0.65] * frame_count
    else:
        shaped_envelope = list(envelope[:frame_count])
        if len(shaped_envelope) < frame_count:
            shaped_envelope.extend([0.0] * (frame_count - len(shaped_envelope)))

    curves = {label: [0.0] * frame_count for label in SPB_TRACK_LABELS}
    tokens = _tokenize_visemes(text or "")
    if not tokens:
        for index, value in enumerate(shaped_envelope):
            curves["open"][index] = max(curves["open"][index], value * 0.75)
            curves["wide"][index] = max(curves["wide"][index], value * 0.45)
    else:
        speech_frames = [index for index, value in enumerate(shaped_envelope) if value > 0.0]
        if not speech_frames:
            speech_frames = list(range(frame_count))
            shaped_envelope = [0.65] * frame_count
        for index, value in enumerate(shaped_envelope):
            if value > 0.0:
                curves["open"][index] = max(curves["open"][index], value * 0.35)
        for token_index, label in enumerate(tokens):
            frame_position = int(round((token_index / max(len(tokens) - 1, 1)) * (len(speech_frames) - 1)))
            frame_index = speech_frames[min(len(speech_frames) - 1, max(0, frame_position))]
            base_strength = max(shaped_envelope[frame_index], 0.35)
            if label == "open":
                base_strength *= 0.90
            elif label == "PBM":
                base_strength *= 0.95
                curves["open"][frame_index] = min(curves["open"][frame_index], 0.10)
            else:
                base_strength *= 0.80
            _add_pulse(curves, label, frame_index, base_strength)

    tracks: list[SpeechTrack] = []
    for label in SPB_TRACK_LABELS:
        curve = [_curve_value(value) for value in curves[label]]
        if any(curve):
            tracks.append(SpeechTrack(label=label, curve=curve, max_weight=SPB_TRACK_MAX_WEIGHTS[label]))
    return SpeechEntry(name=entry_name, frame_count=frame_count, tracks=tracks)


def build_spb_file(
    entries: list[AudioEntry],
    output_path: str | Path,
    options: SpeechBuildOptions | None = None,
    progress: ProgressCallback | None = None,
) -> SpeechBuildResult:
    options = options or SpeechBuildOptions()
    destination = Path(output_path).expanduser().resolve()
    if destination.suffix.lower() != ".spb":
        destination = destination.with_suffix(".spb")
    log = options.log or (lambda _message: None)
    tools = options.media_tools or discover_media_tools()
    text_catalog = load_text_catalog(options.text_source, auto_root=options.auto_text_root or destination.parent)

    speech_entries: list[SpeechEntry] = []
    warnings: list[str] = []
    matched_text_count = 0
    audio_only_count = 0
    text_only_count = 0
    skipped_entries: list[str] = []
    total = max(len(entries), 1)

    with tempfile.TemporaryDirectory(prefix="dyingaudio_spb_") as temp_dir:
        temp_root = Path(temp_dir)
        for index, entry in enumerate(entries):
            if progress is not None:
                progress(f"Generating speech data for {entry.entry_name} ({index + 1}/{len(entries)})", index, total)

            transcript = text_catalog.get(entry.entry_name.casefold())
            duration_ms = _entry_duration_ms(entry)
            audio_path = _decode_entry_audio(entry, temp_root, tools, log, warnings)
            envelope: list[float] | None = None
            if audio_path is not None:
                try:
                    envelope, audio_duration_ms = _read_wav_envelope(
                        audio_path,
                        duration_ms=duration_ms,
                        silence_gate=options.silence_gate,
                    )
                    duration_ms = duration_ms or audio_duration_ms
                except Exception as exc:
                    warnings.append(f"{entry.entry_name}: could not read WAV envelope for SPB timing ({exc}).")
                    audio_path = None

            if duration_ms <= 0:
                skipped_entries.append(entry.entry_name)
                warnings.append(f"{entry.entry_name}: skipped SPB entry because no duration was available.")
                continue

            if transcript and audio_path is not None:
                matched_text_count += 1
            elif transcript:
                text_only_count += 1
            elif audio_path is not None:
                audio_only_count += 1
            else:
                skipped_entries.append(entry.entry_name)
                warnings.append(f"{entry.entry_name}: skipped SPB entry because no audio or transcript was available.")
                continue

            speech_entries.append(
                _build_speech_entry(
                    entry.entry_name,
                    text=transcript,
                    duration_ms=duration_ms,
                    envelope=envelope,
                )
            )

    if progress is not None:
        progress("Writing SPB file...", total, total)
    bank = SpeechBank(entries=speech_entries)
    spb_path = write_spb(bank, destination)
    result = SpeechBuildResult(
        spb_path=spb_path,
        bank=bank,
        matched_text_count=matched_text_count,
        audio_only_count=audio_only_count,
        text_only_count=text_only_count,
        skipped_entries=skipped_entries,
        warnings=warnings,
    )
    log(result.summary())
    for warning in warnings:
        log(f"SPB warning: {warning}")
    return result
