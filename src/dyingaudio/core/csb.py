from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from dyingaudio.models import AudioEntry


HEADER_PADDING_SIZE = 64
HEADER_SIZE = 76
ENTRY_NAME_SIZE = 64
ENTRY_SIZE = 88
COMPACT_TABLE_OFFSET = HEADER_PADDING_SIZE + 8
EXTENDED_TABLE_OFFSET = HEADER_SIZE
WORKSHOP_MAGIC = 0x08000008
LINKER_MAGIC = 0x00000000
MUSIC_MAGIC = 0x00000001
RETAIL_MAGIC = 0x00000002
STREAMED_MAGIC = 0x00000005
MAPS_MAGIC = 0x00000006
COMPACT_LAYOUT = "compact_no_magic"
EXTENDED_LAYOUT = "extended_magic"
SUPPORTED_MAGICS = {
    LINKER_MAGIC,
    MUSIC_MAGIC,
    RETAIL_MAGIC,
    STREAMED_MAGIC,
    MAPS_MAGIC,
    WORKSHOP_MAGIC,
}

ProgressCallback = Callable[[str, float | None, float | None], None]


@dataclass(slots=True)
class ParsedCsb:
    path: Path
    size: int
    header_size: int
    entry_count: int
    magic: int | None
    layout: str
    table_offset: int
    entries: list[AudioEntry]
    raw_bytes: bytes


def _read_u32(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset:offset + 4], "little", signed=False)


def _expected_fsb_size(data: bytes) -> int | None:
    if len(data) < 0x3C or data[:4] != b"FSB5":
        return None

    sample_headers_size = _read_u32(data, 12)
    name_table_size = _read_u32(data, 16)
    sample_data_size = _read_u32(data, 20)
    expected_size = 0x3C + sample_headers_size + name_table_size + sample_data_size
    if expected_size <= 0 or expected_size > len(data):
        return None
    return expected_size


def normalize_fsb_payload(data: bytes) -> tuple[bytes, int]:
    expected_size = _expected_fsb_size(data)
    if expected_size is None:
        return data, 0
    trailing_bytes = len(data) - expected_size
    if trailing_bytes <= 0:
        return data, 0
    return data[:expected_size], trailing_bytes


def _is_probable_entry_name(data: bytes) -> bool:
    if not data:
        return False
    saw_character = False
    for value in data:
        if value == 0:
            break
        if value < 32 or value > 126:
            return False
        saw_character = True
    return saw_character


def _entry_bounds_are_plausible(data: bytes, table_offset: int, entry_count: int) -> bool:
    if entry_count == 0:
        return True

    base = table_offset
    if base + ENTRY_SIZE > len(data):
        return False

    name_bytes = data[base:base + ENTRY_NAME_SIZE]
    if not _is_probable_entry_name(name_bytes):
        return False

    offset = _read_u32(data, base + 64)
    size = _read_u32(data, base + 68)
    if offset < HEADER_SIZE:
        return False
    if size <= 0:
        return False
    if offset + size > len(data):
        return False
    return True


def _looks_like_ascii_prefix(data: bytes) -> bool:
    if not data:
        return False
    for value in data:
        if value == 0:
            return False
        if value < 32 or value > 126:
            return False
    return True


def _detect_layout(data: bytes, entry_count: int) -> tuple[str, int | None, int]:
    magic_candidate = _read_u32(data, HEADER_PADDING_SIZE + 8)
    extended_table_end = EXTENDED_TABLE_OFFSET + (entry_count * ENTRY_SIZE)
    compact_table_end = COMPACT_TABLE_OFFSET + (entry_count * ENTRY_SIZE)

    if magic_candidate in SUPPORTED_MAGICS and extended_table_end <= len(data):
        return EXTENDED_LAYOUT, magic_candidate, EXTENDED_TABLE_OFFSET

    if compact_table_end <= len(data):
        if _entry_bounds_are_plausible(data, COMPACT_TABLE_OFFSET, entry_count):
            if _looks_like_ascii_prefix(data[COMPACT_TABLE_OFFSET:COMPACT_TABLE_OFFSET + 4]):
                return COMPACT_LAYOUT, None, COMPACT_TABLE_OFFSET

    if extended_table_end <= len(data) and _entry_bounds_are_plausible(data, EXTENDED_TABLE_OFFSET, entry_count):
        return EXTENDED_LAYOUT, magic_candidate, EXTENDED_TABLE_OFFSET

    if compact_table_end <= len(data):
        if _entry_bounds_are_plausible(data, COMPACT_TABLE_OFFSET, entry_count):
            return COMPACT_LAYOUT, None, COMPACT_TABLE_OFFSET

    raise ValueError(
        f"Unsupported CSB layout or magic '{magic_candidate:08X}'."
    )


def parse_csb(path: str | Path) -> ParsedCsb:
    resolved = Path(path).resolve()
    data = resolved.read_bytes()

    if len(data) < HEADER_SIZE:
        raise ValueError(f"'{resolved}' is too small to be a CSB file.")

    header_size = _read_u32(data, HEADER_PADDING_SIZE)
    entry_count = _read_u32(data, HEADER_PADDING_SIZE + 4)

    if header_size != HEADER_SIZE:
        raise ValueError(f"Unsupported CSB header size {header_size} in '{resolved}'.")

    layout, magic, table_offset = _detect_layout(data, entry_count)
    table_end = table_offset + (entry_count * ENTRY_SIZE)
    if table_end > len(data):
        raise ValueError(f"CSB entry table overruns the file in '{resolved}'.")

    entries: list[AudioEntry] = []
    for index in range(entry_count):
        base = table_offset + (index * ENTRY_SIZE)
        name = data[base:base + ENTRY_NAME_SIZE].split(b"\x00", 1)[0].decode("ascii", errors="ignore")
        offset = _read_u32(data, base + 64)
        size = _read_u32(data, base + 68)
        entry_type = _read_u32(data, base + 72)
        sample_count = _read_u32(data, base + 76)
        duration_ms = _read_u32(data, base + 80)
        reserved = _read_u32(data, base + 84)

        if offset + size > len(data):
            raise ValueError(f"Entry '{name}' overruns the payload in '{resolved}'.")

        entries.append(
            AudioEntry(
                entry_name=name,
                source_mode="fsb",
                entry_type=entry_type,
                sample_count=sample_count,
                duration_ms=duration_ms,
                reserved=reserved,
                notes=f"Offset={offset} Size={size}",
            )
        )

    return ParsedCsb(
        path=resolved,
        size=len(data),
        header_size=header_size,
        entry_count=entry_count,
        magic=magic,
        layout=layout,
        table_offset=table_offset,
        entries=entries,
        raw_bytes=data,
    )


def extract_csb(
    path: str | Path,
    output_dir: str | Path,
    progress: ProgressCallback | None = None,
) -> list[AudioEntry]:
    parsed = parse_csb(path)
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)

    extracted_entries: list[AudioEntry] = []
    total = max(len(parsed.entries), 1)
    for index, entry in enumerate(parsed.entries):
        if progress is not None:
            progress(f"Extracting {entry.entry_name} ({index + 1}/{len(parsed.entries)})", index, total)
        base = parsed.table_offset + (index * ENTRY_SIZE)
        offset = _read_u32(parsed.raw_bytes, base + 64)
        size = _read_u32(parsed.raw_bytes, base + 68)
        fsb_path = destination / f"{entry.entry_name}.fsb"
        payload, trailing_bytes = normalize_fsb_payload(parsed.raw_bytes[offset:offset + size])
        fsb_path.write_bytes(payload)

        extracted_entries.append(
            AudioEntry(
                entry_name=entry.entry_name,
                source_path=str(fsb_path),
                source_mode="fsb",
                fsb_path=str(fsb_path),
                entry_type=entry.entry_type,
                sample_count=entry.sample_count,
                duration_ms=entry.duration_ms,
                reserved=entry.reserved,
                notes="Extracted from CSB."
                if trailing_bytes <= 0
                else f"Extracted from CSB. Trimmed {trailing_bytes} trailing byte(s) from embedded FSB.",
            )
        )

    if progress is not None:
        progress(f"Extracted {len(extracted_entries)} FSB file(s).", total, total)
    return extracted_entries


def pack_csb(
    entries: list[AudioEntry],
    output_path: str | Path,
    magic: int | None = WORKSHOP_MAGIC,
    progress: ProgressCallback | None = None,
) -> Path:
    destination = Path(output_path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    current_offset = HEADER_SIZE + (len(entries) * ENTRY_SIZE)
    table_offset = EXTENDED_TABLE_OFFSET if magic is not None else COMPACT_TABLE_OFFSET
    payloads: list[bytes] = []
    rows: list[tuple[AudioEntry, int, int]] = []
    total = max(len(entries), 1)

    for index, entry in enumerate(entries):
        if progress is not None:
            progress(f"Packing {entry.entry_name} ({index + 1}/{len(entries)})", index, total)
        fsb_path = entry.resolved_fsb_path()
        if fsb_path is None:
            raise ValueError(f"Entry '{entry.entry_name}' does not have an FSB source path.")
        if not fsb_path.exists():
            raise FileNotFoundError(f"Missing FSB file for entry '{entry.entry_name}': {fsb_path}")

        payload, _trailing_bytes = normalize_fsb_payload(fsb_path.read_bytes())
        payloads.append(payload)
        rows.append((entry, current_offset, len(payload)))
        current_offset += len(payload)

    with destination.open("wb") as handle:
        handle.write(b"\x00" * HEADER_PADDING_SIZE)
        handle.write(HEADER_SIZE.to_bytes(4, "little"))
        handle.write(len(entries).to_bytes(4, "little"))
        if magic is not None:
            handle.write(magic.to_bytes(4, "little"))

        for entry, offset, size in rows:
            name_bytes = entry.entry_name.encode("ascii", errors="ignore")[:ENTRY_NAME_SIZE]
            handle.write(name_bytes + (b"\x00" * (ENTRY_NAME_SIZE - len(name_bytes))))
            handle.write(int(offset).to_bytes(4, "little"))
            handle.write(int(size).to_bytes(4, "little"))
            handle.write(int(entry.entry_type).to_bytes(4, "little"))
            handle.write(int(entry.sample_count).to_bytes(4, "little"))
            handle.write(int(entry.duration_ms).to_bytes(4, "little"))
            handle.write(int(entry.reserved).to_bytes(4, "little"))

        if magic is None:
            current_position = handle.tell()
            if current_position < current_offset:
                handle.write(b"\x00" * (current_offset - current_position))

        for payload in payloads:
            handle.write(payload)

    if progress is not None:
        progress(f"Wrote {destination.name}.", total, total)
    return destination
