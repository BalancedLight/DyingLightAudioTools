from __future__ import annotations

import os
import hashlib
import json
import shutil
import struct
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Callable

from dyingaudio.audio_info import probe_audio_metadata
from dyingaudio.background import TaskCancelled
from dyingaudio.core.media_tools import decode_audio_to_wav, discover_media_tools
from dyingaudio.core.wwise_audio_type import (
    CONFIDENCE_UNKNOWN,
    UNKNOWN_AUDIO_TYPE,
    infer_audio_type,
    normalize_object_types,
)
from dyingaudio.core.wwise_named_tree import MediaInfo, parse_hirc_objects, resolve_object_media_details


AKPK_SOURCE_TYPE = "Wwise PCK (AKPK)"
AKPK_MAGIC = b"AKPK"
AKPK_SOUND_ENTRY_SIZE = 20
AKPK_EXTERNAL_ENTRY_SIZE = 24
STARTER_OBJECT_TYPES = frozenset({2, 5, 7, 11, 13})
PACK_ROWS_SCHEMA_VERSION = 2

ProgressCallback = Callable[[str, float | None, float | None], None]
LogCallback = Callable[[str], None]


@dataclass(slots=True)
class PckSectorEntry:
    file_id: int
    block_size: int
    size: int
    raw_offset: int
    language_id: int
    entry_offset: int = 0
    id_size: int = 4


@dataclass(slots=True)
class DirectMediaRef:
    source_pack: str
    absolute_path: Path
    file_id: int
    offset: int
    size: int
    language_id: int = 0
    language_name: str = ""


@dataclass(slots=True)
class PckOriginRef:
    source_pack: str
    source_kind: str
    file_id: int | None = None
    bank_id: int | None = None
    bank_name: str = ""
    object_id: int | None = None
    object_type: int | None = None
    note: str = ""


@dataclass(slots=True)
class PckUnresolvedItem:
    source_pack: str
    bank_id: int | None
    bank_name: str
    object_id: int | None
    object_type: int | None
    note: str


@dataclass(slots=True)
class PckAudioRow:
    row_key: str
    display_name: str
    file_id: int
    playable_offset: int
    size: int
    source_pack: str
    row_kind: str
    cached_path: Path
    duration_ms: int = 0
    sample_count_48k: int = 0
    resolved_object_types: tuple[int, ...] = ()
    audio_type: str = UNKNOWN_AUDIO_TYPE
    audio_type_confidence: str = CONFIDENCE_UNKNOWN
    audio_type_note: str = ""
    language_name: str = ""
    origins: list[PckOriginRef] = field(default_factory=list)


@dataclass(slots=True)
class PckPackDescriptor:
    absolute_path: Path
    relative_path: str
    display_name: str
    fingerprint: str
    file_size: int
    mtime_ns: int
    header_size: int
    bank_count: int
    sound_count: int
    external_count: int
    kind_summary: str
    languages: dict[int, str] = field(default_factory=dict)

    @property
    def basename(self) -> str:
        return self.absolute_path.name


@dataclass(slots=True)
class PckPackRows:
    descriptor: PckPackDescriptor
    rows: list[PckAudioRow]
    unresolved: list[PckUnresolvedItem]
    summary_text: str
    metadata_path: Path


@dataclass(slots=True)
class PckAudioReplacementResult:
    source_pack: str
    pack_path: Path
    backup_path: Path
    row_kind: str
    file_id: int
    replacement_path: Path
    replacement_size: int
    new_offset: int


@dataclass(slots=True)
class PckWorkspaceIndex:
    source_type: str
    root: Path
    cache_root: Path
    fingerprint: str
    workspace_root: Path
    metadata_path: Path
    packs: list[PckPackDescriptor]
    direct_media_lookup: dict[int, list[DirectMediaRef]]


@dataclass(slots=True)
class ParsedPckHeader:
    path: Path
    file_size: int
    header_size: int
    languages: dict[int, str]
    bank_entries: list[PckSectorEntry]
    sound_entries: list[PckSectorEntry]
    external_entries: list[PckSectorEntry]


@dataclass(slots=True)
class ParsedBankEntry:
    bank_id: int | None
    bank_name: str
    hirc_object_types: dict[int, int]
    didx_entries: list[tuple[int, int, int]]
    data_offset: int | None
    hirc_offset: int | None


@dataclass(slots=True)
class ParsedBankChunk:
    chunk_id: bytes
    payload: bytes
    payload_offset: int


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise TaskCancelled()


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _sha1_text(*parts: object, length: int = 16) -> str:
    payload = "\0".join(str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:length]


def _relative_path(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _pack_fingerprint(path: Path) -> tuple[int, int, str]:
    stat = path.stat()
    fingerprint = _sha1_text(path.resolve(), stat.st_size, stat.st_mtime_ns)
    return stat.st_size, stat.st_mtime_ns, fingerprint


def _materialized_media_path(index: PckWorkspaceIndex, source_pack: str, offset: int, size: int) -> Path:
    source_stem = Path(source_pack).stem or "media"
    source_folder = _sha1_text(source_pack, length=8)
    media_key = _sha1_text(index.fingerprint, source_pack, offset, size, length=24)
    return index.workspace_root / "media" / source_folder / f"{media_key}_{source_stem}.wem"


def _write_slice(source_path: Path, offset: int, size: int, destination_path: Path) -> Path:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    if destination_path.exists():
        return destination_path
    with source_path.open("rb") as source_handle, destination_path.open("wb") as destination_handle:
        source_handle.seek(offset)
        remaining = size
        while remaining > 0:
            chunk = source_handle.read(min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeError(f"Unexpected EOF while copying '{source_path.name}' at offset {offset}.")
            destination_handle.write(chunk)
            remaining -= len(chunk)
    return destination_path


def _load_utf16le_string(blob: bytes, offset: int) -> str:
    cursor = offset
    pieces = bytearray()
    while cursor + 1 < len(blob):
        code_unit = blob[cursor:cursor + 2]
        if code_unit == b"\x00\x00":
            break
        pieces.extend(code_unit)
        cursor += 2
    return pieces.decode("utf-16-le", errors="ignore").strip()


def _parse_language_sector(blob: bytes, start: int, size: int) -> dict[int, str]:
    if size <= 4:
        return {}
    sector = blob[start:start + size]
    count = struct.unpack_from("<I", sector, 0)[0]
    languages: dict[int, str] = {}
    for index in range(count):
        entry_offset = 4 + (index * 8)
        if entry_offset + 8 > len(sector):
            break
        string_offset, language_id = struct.unpack_from("<II", sector, entry_offset)
        if string_offset >= len(sector):
            continue
        language_name = _load_utf16le_string(sector, string_offset)
        languages[language_id] = language_name or f"language_{language_id}"
    return languages


def _parse_standard_sector(blob: bytes, start: int, size: int) -> list[PckSectorEntry]:
    if size <= 4:
        return []
    sector = blob[start:start + size]
    count = struct.unpack_from("<I", sector, 0)[0]
    entries: list[PckSectorEntry] = []
    for index in range(count):
        entry_offset = 4 + (index * AKPK_SOUND_ENTRY_SIZE)
        if entry_offset + AKPK_SOUND_ENTRY_SIZE > len(sector):
            break
        file_id, block_size, entry_size, raw_offset, language_id = struct.unpack_from("<IIIII", sector, entry_offset)
        entries.append(
            PckSectorEntry(
                file_id=file_id,
                block_size=block_size,
                size=entry_size,
                raw_offset=raw_offset,
                language_id=language_id,
                entry_offset=8 + start + entry_offset,
                id_size=4,
            )
        )
    return entries


def _parse_external_sector(blob: bytes, start: int, size: int) -> list[PckSectorEntry]:
    if size <= 4:
        return []
    sector = blob[start:start + size]
    count = struct.unpack_from("<I", sector, 0)[0]
    entries: list[PckSectorEntry] = []
    for index in range(count):
        entry_offset = 4 + (index * AKPK_EXTERNAL_ENTRY_SIZE)
        if entry_offset + AKPK_EXTERNAL_ENTRY_SIZE > len(sector):
            break
        file_id, block_size, entry_size, raw_offset, language_id = struct.unpack_from("<QIIII", sector, entry_offset)
        entries.append(
            PckSectorEntry(
                file_id=file_id,
                block_size=block_size,
                size=entry_size,
                raw_offset=raw_offset,
                language_id=language_id,
                entry_offset=8 + start + entry_offset,
                id_size=8,
            )
        )
    return entries


def parse_pck_header(path: str | Path) -> ParsedPckHeader:
    resolved = Path(path).resolve()
    with resolved.open("rb") as handle:
        magic = handle.read(4)
        if magic != AKPK_MAGIC:
            raise ValueError(f"'{resolved}' is not an AKPK pack.")
        header_size_data = handle.read(4)
        if len(header_size_data) != 4:
            raise ValueError(f"'{resolved}' is missing an AKPK header size.")
        header_size = struct.unpack("<I", header_size_data)[0]
        header = handle.read(header_size)
        if len(header) != header_size:
            raise ValueError(f"'{resolved}' has an incomplete AKPK header.")

    if len(header) < 20:
        raise ValueError(f"'{resolved}' has an invalid AKPK header.")

    _flag, language_size, bank_size, sound_size, external_size = struct.unpack_from("<IIIII", header, 0)
    language_start = 20
    bank_start = language_start + language_size
    sound_start = bank_start + bank_size
    external_start = sound_start + sound_size
    if external_start + external_size > len(header):
        raise ValueError(f"'{resolved}' has AKPK sector sizes that exceed the header length.")

    return ParsedPckHeader(
        path=resolved,
        file_size=resolved.stat().st_size,
        header_size=header_size,
        languages=_parse_language_sector(header, language_start, language_size),
        bank_entries=_parse_standard_sector(header, bank_start, bank_size),
        sound_entries=_parse_standard_sector(header, sound_start, sound_size),
        external_entries=_parse_external_sector(header, external_start, external_size),
    )


def _kind_summary(header: ParsedPckHeader) -> str:
    parts: list[str] = []
    if header.bank_entries:
        parts.append("bank")
    if header.sound_entries:
        parts.append("sound")
    if header.external_entries:
        parts.append("external")
    return ", ".join(parts) or "empty"


def _apply_display_names(descriptors: list[PckPackDescriptor]) -> None:
    by_basename: dict[str, list[PckPackDescriptor]] = {}
    for descriptor in descriptors:
        by_basename.setdefault(descriptor.basename.lower(), []).append(descriptor)
    for descriptor in descriptors:
        duplicates = by_basename.get(descriptor.basename.lower(), [])
        if len(duplicates) <= 1:
            descriptor.display_name = descriptor.basename
            continue
        parent = Path(descriptor.relative_path).parent.as_posix()
        descriptor.display_name = f"{descriptor.basename} ({parent or '.'})"


def _workspace_metadata_path(workspace_root: Path) -> Path:
    return workspace_root / "workspace.json"


def workspace_details_text(index: PckWorkspaceIndex) -> str:
    bank_packs = sum(1 for descriptor in index.packs if descriptor.bank_count)
    sound_packs = sum(1 for descriptor in index.packs if descriptor.sound_count)
    external_packs = sum(1 for descriptor in index.packs if descriptor.external_count)
    return "\n".join(
        [
            f"Source type: {index.source_type}",
            f"Root: {index.root}",
            f"Cache root: {index.cache_root}",
            f"Workspace root: {index.workspace_root}",
            f"Fingerprint: {index.fingerprint}",
            f"Packs indexed: {len(index.packs)}",
            f"Sound packs: {sound_packs}",
            f"Bank packs: {bank_packs}",
            f"External packs: {external_packs}",
            f"Direct media IDs: {len(index.direct_media_lookup)}",
        ]
    )


def scan_pck_root(
    source_type: str,
    root: str | Path,
    cache_root: str | Path,
    log: LogCallback,
    *,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> PckWorkspaceIndex:
    if source_type != AKPK_SOURCE_TYPE:
        raise ValueError(f"Unsupported source type '{source_type}'.")

    root_path = Path(root).expanduser().resolve()
    if not root_path.exists():
        raise FileNotFoundError(f"Pack root does not exist: {root_path}")

    cache_root_path = Path(cache_root).expanduser().resolve()
    pack_paths = sorted((path for path in root_path.rglob("*.pck") if path.is_file()), key=lambda path: str(path).lower())
    total = max(len(pack_paths), 1)
    descriptors: list[PckPackDescriptor] = []
    direct_lookup: dict[int, list[DirectMediaRef]] = {}

    for index, pack_path in enumerate(pack_paths):
        _raise_if_cancelled(cancel_event)
        if progress is not None:
            progress(f"Scanning {pack_path.name}...", index + 1, total)
        header = parse_pck_header(pack_path)
        file_size, mtime_ns, fingerprint = _pack_fingerprint(pack_path)
        descriptor = PckPackDescriptor(
            absolute_path=pack_path,
            relative_path=_relative_path(root_path, pack_path),
            display_name=pack_path.name,
            fingerprint=fingerprint,
            file_size=file_size,
            mtime_ns=mtime_ns,
            header_size=header.header_size,
            bank_count=len(header.bank_entries),
            sound_count=len(header.sound_entries),
            external_count=len(header.external_entries),
            kind_summary=_kind_summary(header),
            languages=dict(header.languages),
        )
        descriptors.append(descriptor)
        for entry in header.sound_entries:
            direct_lookup.setdefault(entry.file_id, []).append(
                DirectMediaRef(
                    source_pack=descriptor.relative_path,
                    absolute_path=pack_path,
                    file_id=entry.file_id,
                    offset=entry.raw_offset,
                    size=entry.size,
                    language_id=entry.language_id,
                    language_name=descriptor.languages.get(entry.language_id, ""),
                )
            )

    _apply_display_names(descriptors)
    fingerprint = _sha1_text(
        source_type,
        root_path,
        *(f"{descriptor.relative_path}|{descriptor.fingerprint}" for descriptor in descriptors),
    )
    workspace_root = cache_root_path / "other" / "akpk" / fingerprint
    workspace_root.mkdir(parents=True, exist_ok=True)
    metadata_path = _workspace_metadata_path(workspace_root)
    index_payload = {
        "source_type": source_type,
        "root": str(root_path),
        "cache_root": str(cache_root_path),
        "fingerprint": fingerprint,
        "workspace_root": str(workspace_root),
        "packs": descriptors,
        "direct_media_lookup": direct_lookup,
    }
    metadata_path.write_text(json.dumps(_json_ready(index_payload), indent=2), encoding="utf-8")
    index = PckWorkspaceIndex(
        source_type=source_type,
        root=root_path,
        cache_root=cache_root_path,
        fingerprint=fingerprint,
        workspace_root=workspace_root,
        metadata_path=metadata_path,
        packs=descriptors,
        direct_media_lookup=direct_lookup,
    )
    log(f"Indexed {len(descriptors)} pack(s) under {root_path}.")
    return index


def export_pck_media_rows(
    rows: list[PckAudioRow],
    destination_root: str | Path,
    *,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> list[Path]:
    destination_root_path = Path(destination_root).resolve()
    destination_root_path.mkdir(parents=True, exist_ok=True)
    tools = discover_media_tools()
    used_names: set[str] = set()
    exported: list[Path] = []
    total = max(len(rows), 1)
    for index, row in enumerate(rows):
        _raise_if_cancelled(cancel_event)
        destination = destination_root_path / _pck_export_name(row, used_names)
        decode_audio_to_wav(row.cached_path, destination, tools=tools)
        exported.append(destination)
        if progress is not None:
            progress(f"Exporting {destination.name}", index + 1, total)
    return exported


def _sanitize_export_stem(value: str) -> str:
    safe = "".join(character if character.isalnum() or character in {"_", "-", "."} else "_" for character in value)
    return safe.strip("._") or "audio"


def _dedupe_export_name(base_stem: str, used_names: set[str]) -> str:
    stem = _sanitize_export_stem(base_stem)
    candidate = f"{stem}.wav"
    counter = 2
    while candidate.casefold() in used_names:
        candidate = f"{stem}_{counter}.wav"
        counter += 1
    used_names.add(candidate.casefold())
    return candidate


def _pck_export_name(row: PckAudioRow, used_names: set[str]) -> str:
    pack_stem = Path(row.source_pack).stem or "pack"
    row_stem = Path(row.display_name).stem or f"media_{row.file_id}"
    return _dedupe_export_name(f"{pack_stem}__{row_stem}", used_names)


def _descriptor_from_payload(payload: dict[str, Any]) -> PckPackDescriptor:
    return PckPackDescriptor(
        absolute_path=Path(payload["absolute_path"]).resolve(),
        relative_path=str(payload["relative_path"]),
        display_name=str(payload.get("display_name", Path(payload["absolute_path"]).name)),
        fingerprint=str(payload["fingerprint"]),
        file_size=int(payload["file_size"]),
        mtime_ns=int(payload["mtime_ns"]),
        header_size=int(payload["header_size"]),
        bank_count=int(payload.get("bank_count", 0)),
        sound_count=int(payload.get("sound_count", 0)),
        external_count=int(payload.get("external_count", 0)),
        kind_summary=str(payload.get("kind_summary", "")),
        languages={int(key): str(value) for key, value in dict(payload.get("languages", {})).items()},
    )


def _origin_from_payload(payload: dict[str, Any]) -> PckOriginRef:
    return PckOriginRef(
        source_pack=str(payload["source_pack"]),
        source_kind=str(payload["source_kind"]),
        file_id=int(payload["file_id"]) if payload.get("file_id") is not None else None,
        bank_id=int(payload["bank_id"]) if payload.get("bank_id") is not None else None,
        bank_name=str(payload.get("bank_name", "")),
        object_id=int(payload["object_id"]) if payload.get("object_id") is not None else None,
        object_type=int(payload["object_type"]) if payload.get("object_type") is not None else None,
        note=str(payload.get("note", "")),
    )


def _row_from_payload(payload: dict[str, Any]) -> PckAudioRow:
    return PckAudioRow(
        row_key=str(payload["row_key"]),
        display_name=str(payload["display_name"]),
        file_id=int(payload["file_id"]),
        playable_offset=int(payload["playable_offset"]),
        size=int(payload["size"]),
        source_pack=str(payload["source_pack"]),
        row_kind=str(payload["row_kind"]),
        cached_path=Path(payload["cached_path"]).resolve(),
        duration_ms=int(payload.get("duration_ms", 0)),
        sample_count_48k=int(payload.get("sample_count_48k", 0)),
        resolved_object_types=normalize_object_types(payload.get("resolved_object_types", [])),
        audio_type=str(payload.get("audio_type", UNKNOWN_AUDIO_TYPE) or UNKNOWN_AUDIO_TYPE),
        audio_type_confidence=str(payload.get("audio_type_confidence", CONFIDENCE_UNKNOWN) or CONFIDENCE_UNKNOWN),
        audio_type_note=str(payload.get("audio_type_note", "") or ""),
        language_name=str(payload.get("language_name", "") or ""),
        origins=[_origin_from_payload(origin) for origin in payload.get("origins", [])],
    )


def _unresolved_from_payload(payload: dict[str, Any]) -> PckUnresolvedItem:
    return PckUnresolvedItem(
        source_pack=str(payload["source_pack"]),
        bank_id=int(payload["bank_id"]) if payload.get("bank_id") is not None else None,
        bank_name=str(payload.get("bank_name", "")),
        object_id=int(payload["object_id"]) if payload.get("object_id") is not None else None,
        object_type=int(payload["object_type"]) if payload.get("object_type") is not None else None,
        note=str(payload.get("note", "")),
    )


def load_workspace_index(root: str | Path) -> PckWorkspaceIndex:
    metadata_path = Path(root).resolve()
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    return PckWorkspaceIndex(
        source_type=str(payload["source_type"]),
        root=Path(payload["root"]).resolve(),
        cache_root=Path(payload["cache_root"]).resolve(),
        fingerprint=str(payload["fingerprint"]),
        workspace_root=Path(payload["workspace_root"]).resolve(),
        metadata_path=metadata_path,
        packs=[_descriptor_from_payload(item) for item in payload.get("packs", [])],
        direct_media_lookup={
            int(key): [
                DirectMediaRef(
                    source_pack=str(ref["source_pack"]),
                    absolute_path=Path(ref["absolute_path"]).resolve(),
                    file_id=int(ref["file_id"]),
                    offset=int(ref["offset"]),
                    size=int(ref["size"]),
                    language_id=int(ref.get("language_id", 0)),
                    language_name=str(ref.get("language_name", "") or ""),
                )
                for ref in refs
            ]
            for key, refs in dict(payload.get("direct_media_lookup", {})).items()
        },
    )


def _ensure_pack_backup(path: Path) -> Path:
    backup_path = Path(f"{path}.bak")
    if not backup_path.exists():
        shutil.copy2(path, backup_path)
    return backup_path


def _descriptor_lookup(index: PckWorkspaceIndex) -> dict[str, PckPackDescriptor]:
    return {descriptor.relative_path: descriptor for descriptor in index.packs}


def _candidate_direct_media_refs(
    index: PckWorkspaceIndex,
    selected_pack: str,
    media_id: int,
) -> list[DirectMediaRef]:
    refs = index.direct_media_lookup.get(media_id, [])
    if not refs:
        return []
    same_pack_refs = [ref for ref in refs if ref.source_pack == selected_pack]
    if same_pack_refs:
        return same_pack_refs
    return refs


def _find_matching_top_level_entry(header: ParsedPckHeader, row: PckAudioRow) -> PckSectorEntry | None:
    for entry in header.sound_entries:
        if entry.file_id == row.file_id and entry.raw_offset == row.playable_offset and entry.size == row.size:
            return entry
    for entry in header.external_entries:
        if entry.file_id == row.file_id and entry.raw_offset == row.playable_offset and entry.size == row.size:
            return entry
    return None


def _read_pack_slice(path: Path, offset: int, size: int) -> bytes:
    with path.open("rb") as handle:
        handle.seek(offset)
        payload = handle.read(size)
    if len(payload) != size:
        raise RuntimeError(f"Could not read the full payload from '{path.name}' at offset {offset}.")
    return payload


def _all_pack_entries(header: ParsedPckHeader) -> list[PckSectorEntry]:
    return [*header.bank_entries, *header.sound_entries, *header.external_entries]


def _pack_entries_match(current_entries: list[PckSectorEntry], backup_entries: list[PckSectorEntry]) -> bool:
    if len(current_entries) != len(backup_entries):
        return False
    for current, backup in zip(current_entries, backup_entries):
        if (
            current.entry_offset != backup.entry_offset
            or current.id_size != backup.id_size
            or current.file_id != backup.file_id
        ):
            return False
    return True


def _preferred_entry_offsets(path: Path, header: ParsedPckHeader) -> dict[int, int]:
    backup_path = Path(f"{path}.bak")
    if not backup_path.exists():
        return {}
    try:
        backup_header = parse_pck_header(backup_path)
    except Exception:
        return {}

    if not (
        _pack_entries_match(header.bank_entries, backup_header.bank_entries)
        and _pack_entries_match(header.sound_entries, backup_header.sound_entries)
        and _pack_entries_match(header.external_entries, backup_header.external_entries)
    ):
        return {}

    preferred_offsets: dict[int, int] = {}
    for current_group, backup_group in (
        (header.bank_entries, backup_header.bank_entries),
        (header.sound_entries, backup_header.sound_entries),
        (header.external_entries, backup_header.external_entries),
    ):
        for current, backup in zip(current_group, backup_group):
            preferred_offsets[current.entry_offset] = backup.raw_offset
    return preferred_offsets


def _set_entry_location_in_blob(blob: bytearray, entry: PckSectorEntry, size: int, raw_offset: int) -> None:
    size_field_offset = entry.entry_offset + entry.id_size + 4
    raw_offset_field = size_field_offset + 4
    blob[size_field_offset:size_field_offset + 4] = struct.pack("<I", size)
    blob[raw_offset_field:raw_offset_field + 4] = struct.pack("<I", raw_offset)


def _rewrite_pack_payloads(
    path: Path,
    header: ParsedPckHeader,
    replacement_payloads: dict[int, bytes],
) -> dict[int, int]:
    header_prefix_size = 8 + header.header_size
    with path.open("rb") as handle:
        header_blob = bytearray(handle.read(header_prefix_size))
    if len(header_blob) != header_prefix_size:
        raise RuntimeError(f"Could not read the AKPK header from '{path.name}'.")

    preferred_offsets = _preferred_entry_offsets(path, header)
    payloads: dict[int, bytes] = {}
    entries = _all_pack_entries(header)
    for entry in entries:
        payloads[entry.entry_offset] = replacement_payloads.get(entry.entry_offset, _read_pack_slice(path, entry.raw_offset, entry.size))

    ordered_entries = sorted(
        entries,
        key=lambda entry: (preferred_offsets.get(entry.entry_offset, entry.raw_offset), entry.entry_offset),
    )

    next_offset = header_prefix_size
    new_offsets: dict[int, int] = {}
    body_parts: list[bytes] = []
    for entry in ordered_entries:
        payload = payloads[entry.entry_offset]
        _set_entry_location_in_blob(header_blob, entry, len(payload), next_offset)
        new_offsets[entry.entry_offset] = next_offset
        body_parts.append(payload)
        next_offset += len(payload)

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir=path.parent, prefix=f"{path.name}.", suffix=".tmp") as handle:
            temp_path = Path(handle.name)
            handle.write(header_blob)
            for payload in body_parts:
                handle.write(payload)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)

    return new_offsets


def _parse_bank_chunks(bank_bytes: bytes) -> tuple[list[ParsedBankChunk], bytes]:
    chunks: list[ParsedBankChunk] = []
    position = 0
    while position + 8 <= len(bank_bytes):
        chunk_id = bank_bytes[position:position + 4]
        chunk_length = struct.unpack_from("<I", bank_bytes, position + 4)[0]
        payload_offset = position + 8
        next_position = payload_offset + chunk_length
        if next_position > len(bank_bytes):
            break
        chunks.append(
            ParsedBankChunk(
                chunk_id=chunk_id,
                payload=bank_bytes[payload_offset:next_position],
                payload_offset=payload_offset,
            )
        )
        position = next_position
    return chunks, bank_bytes[position:]


def _build_bank_blob_with_replacement(bank_bytes: bytes, bank_entry: PckSectorEntry, row: PckAudioRow, replacement_bytes: bytes) -> bytes:
    chunks, trailing_bytes = _parse_bank_chunks(bank_bytes)
    didx_index: int | None = None
    data_index: int | None = None
    didx_entries: list[tuple[int, int, int]] = []
    target_index: int | None = None

    for index, chunk in enumerate(chunks):
        if chunk.chunk_id == b"DIDX":
            didx_index = index
            for entry_index in range(len(chunk.payload) // 12):
                media_id, relative_offset, media_size = struct.unpack_from("<III", chunk.payload, entry_index * 12)
                didx_entries.append((media_id, relative_offset, media_size))
        elif chunk.chunk_id == b"DATA":
            data_index = index

    if didx_index is None or data_index is None:
        raise RuntimeError(f"Bank entry {row.file_id} does not contain DIDX/DATA media.")

    data_chunk = chunks[data_index]
    for index, (media_id, relative_offset, media_size) in enumerate(didx_entries):
        absolute_offset = bank_entry.raw_offset + data_chunk.payload_offset + relative_offset
        if media_id == row.file_id and absolute_offset == row.playable_offset and media_size == row.size:
            target_index = index
            break

    if target_index is None:
        raise RuntimeError(
            f"Could not find embedded media ID {row.file_id} at offset {row.playable_offset} inside '{row.source_pack}'."
        )

    rebuilt_data = bytearray()
    rebuilt_didx = bytearray()
    for index, (media_id, relative_offset, media_size) in enumerate(didx_entries):
        if index == target_index:
            payload = replacement_bytes
        else:
            payload = data_chunk.payload[relative_offset:relative_offset + media_size]
        rebuilt_didx.extend(struct.pack("<III", media_id, len(rebuilt_data), len(payload)))
        rebuilt_data.extend(payload)

    rebuilt_chunks = list(chunks)
    rebuilt_chunks[didx_index] = ParsedBankChunk(chunk_id=b"DIDX", payload=bytes(rebuilt_didx), payload_offset=0)
    rebuilt_chunks[data_index] = ParsedBankChunk(chunk_id=b"DATA", payload=bytes(rebuilt_data), payload_offset=0)

    bank_blob = bytearray()
    for chunk in rebuilt_chunks:
        bank_blob.extend(chunk.chunk_id)
        bank_blob.extend(struct.pack("<I", len(chunk.payload)))
        bank_blob.extend(chunk.payload)
    bank_blob.extend(trailing_bytes)
    return bytes(bank_blob)


def _pack_rows_root(index: PckWorkspaceIndex, descriptor: PckPackDescriptor) -> Path:
    return index.workspace_root / "packs" / descriptor.fingerprint


def _pack_rows_metadata_path(index: PckWorkspaceIndex, descriptor: PckPackDescriptor) -> Path:
    return _pack_rows_root(index, descriptor) / "rows.json"


def _parse_bank_entry(path: Path, entry: PckSectorEntry) -> ParsedBankEntry:
    end_offset = entry.raw_offset + entry.size
    bank_id: int | None = None
    bank_name = f"bank_{entry.file_id:08X}"
    didx_entries: list[tuple[int, int, int]] = []
    data_offset: int | None = None
    hirc_offset: int | None = None
    hirc_object_types: dict[int, int] = {}

    with path.open("rb") as handle:
        position = entry.raw_offset
        while position + 8 <= end_offset:
            handle.seek(position)
            chunk_id = handle.read(4)
            if len(chunk_id) != 4:
                break
            chunk_length_data = handle.read(4)
            if len(chunk_length_data) != 4:
                break
            chunk_length = struct.unpack("<I", chunk_length_data)[0]
            payload_offset = position + 8
            next_position = payload_offset + chunk_length
            if next_position > end_offset:
                break

            if chunk_id == b"BKHD":
                handle.seek(payload_offset)
                version = struct.unpack("<I", handle.read(4))[0]
                if chunk_length >= 8:
                    bank_id = struct.unpack("<I", handle.read(4))[0]
                    bank_name = f"bank_{bank_id:08X}"
                else:
                    bank_name = f"bank_v{version}_{entry.file_id:08X}"
            elif chunk_id == b"DIDX":
                handle.seek(payload_offset)
                for _index in range(chunk_length // 12):
                    media_id, relative_offset, media_size = struct.unpack("<III", handle.read(12))
                    didx_entries.append((media_id, relative_offset, media_size))
            elif chunk_id == b"DATA":
                data_offset = payload_offset
            elif chunk_id == b"HIRC":
                hirc_offset = payload_offset
                objects = parse_hirc_objects(path, payload_offset)
                hirc_object_types = {object_id: object_info.object_type for object_id, object_info in objects.items()}

            position = next_position

    return ParsedBankEntry(
        bank_id=bank_id,
        bank_name=bank_name,
        hirc_object_types=hirc_object_types,
        didx_entries=didx_entries,
        data_offset=data_offset,
        hirc_offset=hirc_offset,
    )


def _source_media_info(media_id: int, offset: int, size: int, source: Path, source_pack: str) -> MediaInfo:
    return MediaInfo(
        archive=source_pack,
        media_id=media_id,
        offset=offset,
        size=size,
        source=source,
        exists=True,
    )


def _display_name(row_kind: str, file_id: int) -> str:
    safe_kind = row_kind.replace("_", "-")
    return f"{safe_kind}_{file_id}.wem"


def _row_key_for(selected_pack: str, source_pack: str, offset: int, size: int) -> str:
    return _sha1_text(selected_pack, source_pack, offset, size, length=24)


def _refresh_row_audio_type(row: PckAudioRow) -> None:
    resolution = infer_audio_type(
        object_types=row.resolved_object_types or [origin.object_type for origin in row.origins],
        source_pack=row.source_pack,
        language_name=row.language_name,
    )
    row.audio_type = resolution.audio_type
    row.audio_type_confidence = resolution.confidence
    row.audio_type_note = resolution.note


def _apply_row_context(
    row: PckAudioRow,
    origin: PckOriginRef,
    *,
    resolved_object_types: tuple[int, ...] = (),
    language_name: str = "",
) -> None:
    row.origins.append(origin)
    merged_types = set(row.resolved_object_types)
    merged_types.update(normalize_object_types(resolved_object_types))
    if origin.object_type is not None:
        merged_types.add(origin.object_type)
    row.resolved_object_types = tuple(sorted(merged_types))
    if language_name:
        if not row.language_name:
            row.language_name = language_name
        elif row.language_name != language_name:
            row.language_name = "multiple"
    _refresh_row_audio_type(row)


def _append_row(
    rows_by_key: dict[str, PckAudioRow],
    *,
    selected_pack: str,
    source_pack: str,
    file_id: int,
    playable_offset: int,
    size: int,
    row_kind: str,
    cached_path: Path,
    origin: PckOriginRef,
    resolved_object_types: tuple[int, ...] = (),
    language_name: str = "",
) -> PckAudioRow:
    row_key = _row_key_for(selected_pack, source_pack, playable_offset, size)
    row = rows_by_key.get(row_key)
    if row is None:
        row = PckAudioRow(
            row_key=row_key,
            display_name=_display_name(row_kind, file_id),
            file_id=file_id,
            playable_offset=playable_offset,
            size=size,
            source_pack=source_pack,
            row_kind=row_kind,
            cached_path=cached_path,
        )
        rows_by_key[row_key] = row
    _apply_row_context(row, origin, resolved_object_types=resolved_object_types, language_name=language_name)
    return row


def _materialize_entry(index: PckWorkspaceIndex, source_pack: str, source_path: Path, offset: int, size: int) -> Path:
    destination_path = _materialized_media_path(index, source_pack, offset, size)
    return _write_slice(source_path, offset, size, destination_path)


def _build_pack_rows_payload(pack_rows: PckPackRows) -> dict[str, Any]:
    return {
        "schema_version": PACK_ROWS_SCHEMA_VERSION,
        "descriptor": pack_rows.descriptor,
        "rows": pack_rows.rows,
        "unresolved": pack_rows.unresolved,
        "summary_text": pack_rows.summary_text,
        "metadata_path": str(pack_rows.metadata_path),
    }


def _probe_rows(
    rows: list[PckAudioRow],
    *,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    unique_paths: list[Path] = []
    seen: set[str] = set()
    for row in rows:
        key = str(row.cached_path.resolve()).casefold()
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(row.cached_path)

    total = max(len(unique_paths), 1)
    metadata_by_path: dict[str, tuple[int, int]] = {}
    max_workers = max(1, min(16, len(unique_paths), max(4, (os.cpu_count() or 1) * 2)))
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dyingaudio_pck_probe") as executor:
        futures = {executor.submit(probe_audio_metadata, path): path for path in unique_paths}
        completed = 0
        for future in as_completed(futures):
            _raise_if_cancelled(cancel_event)
            path = futures[future]
            metadata = future.result()
            metadata_by_path[str(path.resolve()).casefold()] = (metadata.duration_ms, metadata.sample_count_48k)
            completed += 1
            if progress is not None:
                progress(f"Indexing audio metadata for {path.name}...", completed, total)

    for row in rows:
        duration_ms, sample_count = metadata_by_path.get(str(row.cached_path.resolve()).casefold(), (0, 0))
        row.duration_ms = duration_ms
        row.sample_count_48k = sample_count


def _parse_bank_rows(
    index: PckWorkspaceIndex,
    descriptor: PckPackDescriptor,
    entry: PckSectorEntry,
    rows_by_key: dict[str, PckAudioRow],
    unresolved: list[PckUnresolvedItem],
    *,
    cancel_event: threading.Event | None = None,
) -> None:
    bank_info = _parse_bank_entry(descriptor.absolute_path, entry)
    local_embedded_rows: dict[int, list[PckAudioRow]] = {}
    if bank_info.data_offset is not None:
        for media_id, relative_offset, media_size in bank_info.didx_entries:
            _raise_if_cancelled(cancel_event)
            playable_offset = bank_info.data_offset + relative_offset
            cached_path = _materialize_entry(index, descriptor.relative_path, descriptor.absolute_path, playable_offset, media_size)
            origin = PckOriginRef(
                source_pack=descriptor.relative_path,
                source_kind="embedded_bank_media",
                file_id=media_id,
                bank_id=bank_info.bank_id,
                bank_name=bank_info.bank_name,
                note="Embedded DIDX/DATA media",
            )
            row = _append_row(
                rows_by_key,
                selected_pack=descriptor.relative_path,
                source_pack=descriptor.relative_path,
                file_id=media_id,
                playable_offset=playable_offset,
                size=media_size,
                row_kind="embedded_bank_media",
                cached_path=cached_path,
                origin=origin,
                language_name=descriptor.languages.get(entry.language_id, ""),
            )
            local_embedded_rows.setdefault(media_id, []).append(row)

    if bank_info.hirc_offset is None:
        if not local_embedded_rows:
            unresolved.append(
                PckUnresolvedItem(
                    source_pack=descriptor.relative_path,
                    bank_id=bank_info.bank_id,
                    bank_name=bank_info.bank_name,
                    object_id=None,
                    object_type=None,
                    note="Bank contains no DIDX/DATA or HIRC sections.",
                )
            )
        return

    objects = parse_hirc_objects(descriptor.absolute_path, bank_info.hirc_offset)
    if not objects and not local_embedded_rows:
        unresolved.append(
            PckUnresolvedItem(
                source_pack=descriptor.relative_path,
                bank_id=bank_info.bank_id,
                bank_name=bank_info.bank_name,
                object_id=None,
                object_type=None,
                note="Bank contains an empty HIRC section.",
            )
        )
        return

    helper_media_lookup: dict[int, list[MediaInfo]] = {}
    for media_id, rows in local_embedded_rows.items():
        helper_media_lookup[media_id] = [
            _source_media_info(media_id, row.playable_offset, row.size, descriptor.absolute_path, descriptor.relative_path)
            for row in rows
        ]
    for media_id, refs in index.direct_media_lookup.items():
        helper_media_lookup.setdefault(media_id, [])
        helper_media_lookup[media_id].extend(
            _source_media_info(media_id, ref.offset, ref.size, ref.absolute_path, ref.source_pack) for ref in refs
        )

    starter_ids = sorted(
        object_id for object_id, object_info in objects.items() if object_info.object_type in STARTER_OBJECT_TYPES
    )
    if not starter_ids and not local_embedded_rows:
        unresolved.append(
            PckUnresolvedItem(
                source_pack=descriptor.relative_path,
                bank_id=bank_info.bank_id,
                bank_name=bank_info.bank_name,
                object_id=None,
                object_type=None,
                note="No starter HIRC object types matched the v1 resolver.",
            )
        )
        return

    memo: dict[int, dict[int, tuple[int, ...]]] = {}
    for object_id in starter_ids:
        _raise_if_cancelled(cancel_event)
        object_info = objects[object_id]
        media_details = resolve_object_media_details(
            object_id,
            objects,
            objects,
            set(objects),
            helper_media_lookup,
            memo,
            set(),
            cancel_event,
        )
        media_ids = sorted(media_details)
        if not media_ids:
            unresolved.append(
                PckUnresolvedItem(
                    source_pack=descriptor.relative_path,
                    bank_id=bank_info.bank_id,
                    bank_name=bank_info.bank_name,
                    object_id=object_id,
                    object_type=object_info.object_type,
                    note="No media IDs resolved from the starter HIRC object.",
                )
            )
            continue

        for media_id in media_ids:
            _raise_if_cancelled(cancel_event)
            resolved_object_types = media_details.get(media_id, ())
            if media_id in local_embedded_rows:
                for row in local_embedded_rows[media_id]:
                    _apply_row_context(
                        row,
                        PckOriginRef(
                            source_pack=descriptor.relative_path,
                            source_kind="embedded_bank_media",
                            file_id=media_id,
                            bank_id=bank_info.bank_id,
                            bank_name=bank_info.bank_name,
                            object_id=object_id,
                            object_type=object_info.object_type,
                            note="Resolved from HIRC to embedded media",
                        ),
                        resolved_object_types=resolved_object_types,
                        language_name=descriptor.languages.get(entry.language_id, ""),
                    )
                continue

            matching_refs = _candidate_direct_media_refs(index, descriptor.relative_path, media_id)
            if not matching_refs:
                unresolved.append(
                    PckUnresolvedItem(
                        source_pack=descriptor.relative_path,
                        bank_id=bank_info.bank_id,
                        bank_name=bank_info.bank_name,
                        object_id=object_id,
                        object_type=object_info.object_type,
                        note=f"Resolved media ID {media_id} but no direct-sound slice matched it.",
                    )
                )
                continue

            for ref in matching_refs:
                cached_path = _materialize_entry(index, ref.source_pack, ref.absolute_path, ref.offset, ref.size)
                _append_row(
                    rows_by_key,
                    selected_pack=descriptor.relative_path,
                    source_pack=ref.source_pack,
                    file_id=media_id,
                    playable_offset=ref.offset,
                    size=ref.size,
                    row_kind="linked_bank_media",
                    cached_path=cached_path,
                    origin=PckOriginRef(
                        source_pack=descriptor.relative_path,
                        source_kind="linked_bank_media",
                        file_id=media_id,
                        bank_id=bank_info.bank_id,
                        bank_name=bank_info.bank_name,
                        object_id=object_id,
                        object_type=object_info.object_type,
                        note=f"Linked to direct media slice in {ref.source_pack}",
                    ),
                    resolved_object_types=resolved_object_types,
                    language_name=ref.language_name,
                )


def _summary_text(descriptor: PckPackDescriptor, rows: list[PckAudioRow], unresolved: list[PckUnresolvedItem]) -> str:
    kind_counts: dict[str, int] = {}
    for row in rows:
        kind_counts[row.row_kind] = kind_counts.get(row.row_kind, 0) + 1
    lines = [
        f"Pack: {descriptor.relative_path}",
        f"Kind: {descriptor.kind_summary}",
        f"Rows: {len(rows)}",
        f"Unresolved items: {len(unresolved)}",
    ]
    for row_kind in sorted(kind_counts):
        lines.append(f"{row_kind}: {kind_counts[row_kind]}")
    if unresolved:
        lines.extend(["", "Representative unresolved items:"])
        for item in unresolved[:50]:
            bank_bits = [item.bank_name or ""]
            if item.object_id is not None:
                bank_bits.append(f"object {item.object_id}")
            bank_label = " | ".join(bit for bit in bank_bits if bit)
            if bank_label:
                lines.append(f"{bank_label}: {item.note}")
            else:
                lines.append(item.note)
        if len(unresolved) > 50:
            lines.append(f"... {len(unresolved) - 50} more unresolved item(s)")
    return "\n".join(lines)


def replace_pck_audio_row(
    index: PckWorkspaceIndex,
    row: PckAudioRow,
    replacement_path: str | Path,
    log: LogCallback,
    *,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> PckAudioReplacementResult:
    _raise_if_cancelled(cancel_event)
    replacement = Path(replacement_path).expanduser().resolve()
    if not replacement.exists():
        raise FileNotFoundError(f"Replacement audio does not exist: {replacement}")

    descriptor = _descriptor_lookup(index).get(row.source_pack)
    if descriptor is None:
        raise RuntimeError(f"Could not locate the source pack '{row.source_pack}' in the current workspace.")

    replacement_bytes = replacement.read_bytes()
    if not replacement_bytes:
        raise ValueError(f"Replacement audio is empty: {replacement}")

    if progress is not None:
        progress(f"Preparing backup for {descriptor.absolute_path.name}...", 1, 3)
    backup_path = _ensure_pack_backup(descriptor.absolute_path)
    log(f"Backup ready: {backup_path}")
    _raise_if_cancelled(cancel_event)

    header = parse_pck_header(descriptor.absolute_path)
    new_offset = 0
    if row.row_kind == "embedded_bank_media":
        matching_bank_entry: PckSectorEntry | None = None
        rebuilt_bank_blob: bytes | None = None
        for bank_entry in header.bank_entries:
            _raise_if_cancelled(cancel_event)
            bank_bytes = _read_pack_slice(descriptor.absolute_path, bank_entry.raw_offset, bank_entry.size)
            try:
                rebuilt_bank_blob = _build_bank_blob_with_replacement(bank_bytes, bank_entry, row, replacement_bytes)
            except RuntimeError:
                continue
            matching_bank_entry = bank_entry
            break

        if matching_bank_entry is None or rebuilt_bank_blob is None:
            raise RuntimeError(
                f"Could not find the embedded bank payload for media ID {row.file_id} in '{descriptor.relative_path}'."
            )

        if progress is not None:
            progress(f"Rebuilding {descriptor.absolute_path.name} with updated bank data...", 2, 3)
        new_offsets = _rewrite_pack_payloads(
            descriptor.absolute_path,
            header,
            {matching_bank_entry.entry_offset: rebuilt_bank_blob},
        )
        new_offset = new_offsets[matching_bank_entry.entry_offset]
        log(
            f"Rebuilt bank media {row.file_id} in {descriptor.relative_path}; bank entry now points to offset {new_offset}."
        )
    else:
        matching_entry = _find_matching_top_level_entry(header, row)
        if matching_entry is None:
            raise RuntimeError(
                f"Could not find the top-level AKPK entry for media ID {row.file_id} in '{descriptor.relative_path}'."
            )

        if progress is not None:
            progress(f"Rebuilding {descriptor.absolute_path.name} with replacement audio...", 2, 3)
        new_offsets = _rewrite_pack_payloads(
            descriptor.absolute_path,
            header,
            {matching_entry.entry_offset: replacement_bytes},
        )
        new_offset = new_offsets[matching_entry.entry_offset]
        log(
            f"Updated {descriptor.relative_path} media {row.file_id} to offset {new_offset} ({len(replacement_bytes)} bytes)."
        )

    _raise_if_cancelled(cancel_event)
    if progress is not None:
        progress("Finalizing AKPK replacement...", 3, 3)
    return PckAudioReplacementResult(
        source_pack=descriptor.relative_path,
        pack_path=descriptor.absolute_path,
        backup_path=backup_path,
        row_kind=row.row_kind,
        file_id=row.file_id,
        replacement_path=replacement,
        replacement_size=len(replacement_bytes),
        new_offset=new_offset,
    )


def load_pck_pack_rows(
    index: PckWorkspaceIndex,
    descriptor: PckPackDescriptor,
    log: LogCallback,
    *,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> PckPackRows:
    metadata_path = _pack_rows_metadata_path(index, descriptor)
    if metadata_path.exists():
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        rows = [_row_from_payload(item) for item in payload.get("rows", [])]
        if int(payload.get("schema_version", 0)) == PACK_ROWS_SCHEMA_VERSION and all(row.cached_path.exists() for row in rows):
            pack_rows = PckPackRows(
                descriptor=_descriptor_from_payload(payload["descriptor"]),
                rows=rows,
                unresolved=[_unresolved_from_payload(item) for item in payload.get("unresolved", [])],
                summary_text=str(payload.get("summary_text", "")),
                metadata_path=metadata_path,
            )
            log(f"Loaded cached rows for {descriptor.relative_path}.")
            return pack_rows

    header = parse_pck_header(descriptor.absolute_path)
    rows_by_key: dict[str, PckAudioRow] = {}
    unresolved: list[PckUnresolvedItem] = []
    total_steps = max(len(header.sound_entries) + len(header.external_entries) + len(header.bank_entries), 1)
    current_step = 0

    for entry in header.sound_entries:
        _raise_if_cancelled(cancel_event)
        current_step += 1
        if progress is not None:
            progress(f"Loading direct audio {current_step}/{total_steps}...", current_step, total_steps)
        cached_path = _materialize_entry(index, descriptor.relative_path, descriptor.absolute_path, entry.raw_offset, entry.size)
        _append_row(
            rows_by_key,
            selected_pack=descriptor.relative_path,
            source_pack=descriptor.relative_path,
            file_id=entry.file_id,
            playable_offset=entry.raw_offset,
            size=entry.size,
            row_kind="direct_sound",
            cached_path=cached_path,
            origin=PckOriginRef(
                source_pack=descriptor.relative_path,
                source_kind="direct_sound",
                file_id=entry.file_id,
                note="Direct sound-sector slice",
            ),
            language_name=descriptor.languages.get(entry.language_id, ""),
        )

    for entry in header.external_entries:
        _raise_if_cancelled(cancel_event)
        current_step += 1
        if progress is not None:
            progress(f"Loading external audio {current_step}/{total_steps}...", current_step, total_steps)
        cached_path = _materialize_entry(index, descriptor.relative_path, descriptor.absolute_path, entry.raw_offset, entry.size)
        _append_row(
            rows_by_key,
            selected_pack=descriptor.relative_path,
            source_pack=descriptor.relative_path,
            file_id=entry.file_id,
            playable_offset=entry.raw_offset,
            size=entry.size,
            row_kind="external",
            cached_path=cached_path,
            origin=PckOriginRef(
                source_pack=descriptor.relative_path,
                source_kind="external",
                file_id=entry.file_id,
                note="External-sector slice",
            ),
            language_name=descriptor.languages.get(entry.language_id, ""),
        )

    for entry in header.bank_entries:
        _raise_if_cancelled(cancel_event)
        current_step += 1
        if progress is not None:
            progress(f"Parsing bank {current_step}/{total_steps}...", current_step, total_steps)
        _parse_bank_rows(index, descriptor, entry, rows_by_key, unresolved, cancel_event=cancel_event)

    rows = sorted(rows_by_key.values(), key=lambda row: (row.playable_offset, row.file_id, row.display_name.lower()))
    _probe_rows(rows, progress=progress, cancel_event=cancel_event)
    summary_text = _summary_text(descriptor, rows, unresolved)

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    pack_rows = PckPackRows(
        descriptor=descriptor,
        rows=rows,
        unresolved=unresolved,
        summary_text=summary_text,
        metadata_path=metadata_path,
    )
    metadata_path.write_text(json.dumps(_json_ready(_build_pack_rows_payload(pack_rows)), indent=2), encoding="utf-8")
    log(f"Loaded {len(rows)} playable row(s) from {descriptor.relative_path}.")
    return pack_rows
