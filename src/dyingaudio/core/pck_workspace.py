from __future__ import annotations

import os
import hashlib
import json
import shutil
import struct
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Callable

from dyingaudio.audio_info import probe_audio_metadata
from dyingaudio.background import TaskCancelled
from dyingaudio.core.wwise_named_tree import MediaInfo, parse_hirc_objects, resolve_object_media


AKPK_SOURCE_TYPE = "Wwise PCK (AKPK)"
AKPK_MAGIC = b"AKPK"
AKPK_SOUND_ENTRY_SIZE = 20
AKPK_EXTERNAL_ENTRY_SIZE = 24
STARTER_OBJECT_TYPES = frozenset({2, 5, 7, 11, 13})

ProgressCallback = Callable[[str, float | None, float | None], None]
LogCallback = Callable[[str], None]


@dataclass(slots=True)
class PckSectorEntry:
    file_id: int
    block_size: int
    size: int
    raw_offset: int
    language_id: int


@dataclass(slots=True)
class DirectMediaRef:
    source_pack: str
    absolute_path: Path
    file_id: int
    offset: int
    size: int


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


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise TaskCancelled()


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
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
    exported: list[Path] = []
    total = max(len(rows), 1)
    for index, row in enumerate(rows):
        _raise_if_cancelled(cancel_event)
        destination = destination_root_path / row.cached_path.name
        shutil.copy2(row.cached_path, destination)
        exported.append(destination)
        if progress is not None:
            progress(f"Exporting {row.cached_path.name}", index + 1, total)
    return exported


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
                )
                for ref in refs
            ]
            for key, refs in dict(payload.get("direct_media_lookup", {})).items()
        },
    )


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
    row.origins.append(origin)
    return row


def _materialize_entry(index: PckWorkspaceIndex, source_pack: str, source_path: Path, offset: int, size: int) -> Path:
    destination_path = _materialized_media_path(index, source_pack, offset, size)
    return _write_slice(source_path, offset, size, destination_path)


def _build_pack_rows_payload(pack_rows: PckPackRows) -> dict[str, Any]:
    return {
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

    memo: dict[int, list[int]] = {}
    for object_id in starter_ids:
        _raise_if_cancelled(cancel_event)
        object_info = objects[object_id]
        media_ids = resolve_object_media(
            object_id,
            objects,
            objects,
            set(objects),
            helper_media_lookup,
            memo,
            set(),
            cancel_event,
        )
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
            if media_id in local_embedded_rows:
                for row in local_embedded_rows[media_id]:
                    row.origins.append(
                        PckOriginRef(
                            source_pack=descriptor.relative_path,
                            source_kind="embedded_bank_media",
                            file_id=media_id,
                            bank_id=bank_info.bank_id,
                            bank_name=bank_info.bank_name,
                            object_id=object_id,
                            object_type=object_info.object_type,
                            note="Resolved from HIRC to embedded media",
                        )
                    )
                continue

            matching_refs = index.direct_media_lookup.get(media_id, [])
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
        if all(row.cached_path.exists() for row in rows):
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
