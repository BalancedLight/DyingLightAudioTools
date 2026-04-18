"""AESP archive audio replacement via append-and-update-index strategy.

Supports replacing external WEM media in sfx.aesp / streams.aesp archives
and embedded WEM media inside BNK entries in meta.aesp, as used by
Dying Light 2 and Dying Light: The Beast.  The archive is modified
in-place: replacement data is appended to the end of the file and the
corresponding index entry is updated to point to the new data.  A ``.bak``
backup is created on the first modification.

AESP binary format
==================
Header (0xB8 bytes):
  +0x00  version   u64     0x20000
  +0x08  name      128B    null-terminated ASCII
  +0x88  entry_start u64   offset to first entry (always 0xB8)
  +0x90  entry_count u64   number of entries
  +0x98  reserved  32B     zeros

Entry (152 / 0x98 bytes):
  +0x00  ascii_name  128B  media ID as decimal ASCII (or bank name), zero-padded
  +0x80  media_id    u32   same value as binary integer
  +0x84  reserved    u32   always 0
  +0x88  offset_low  u32   data offset low 32 bits
  +0x8C  offset_high u32   data offset high 32 bits
  +0x90  size_low    u32   data size low 32 bits
  +0x94  size_high   u32   data size high 32 bits
"""

from __future__ import annotations

import json
import shutil
import struct
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from dyingaudio.background import TaskCancelled

ProgressCallback = Callable[[str, float | None, float | None], None]
LogCallback = Callable[[str], None]

AESP_HEADER_SIZE = 0xB8
AESP_ENTRY_SIZE = 0x98  # 152 bytes
AESP_ENTRY_OFFSET_FIELD = 0x88  # relative to entry start
RIFF_MAGIC = b"RIFF"


@dataclass(slots=True)
class AespEntry:
    ascii_name: str
    media_id: int
    reserved: int
    data_offset: int
    data_size: int


@dataclass(slots=True)
class AespReplacementResult:
    archive_path: Path
    backup_path: Path
    media_id: int
    replacement_path: Path
    replacement_size: int
    new_offset: int


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise TaskCancelled()


def _ensure_aesp_backup(path: Path) -> Path:
    """Create a ``.bak`` copy of *path* if one does not already exist."""
    backup_path = Path(f"{path}.bak")
    if not backup_path.exists():
        shutil.copy2(path, backup_path)
    return backup_path


def restore_aesp_from_backup(archive_path: str | Path) -> Path:
    """Restore an AESP archive from its ``.bak`` backup.

    Returns the backup path that was used.  Raises ``FileNotFoundError``
    if no backup exists.
    """
    resolved = Path(archive_path).expanduser().resolve()
    backup = Path(f"{resolved}.bak")
    if not backup.exists():
        raise FileNotFoundError(f"No backup found for '{resolved.name}'.")
    shutil.copy2(backup, resolved)
    return backup


def _read_aesp_header(path: Path) -> tuple[int, int]:
    """Return ``(entry_start, entry_count)`` from the AESP header."""
    with path.open("rb") as handle:
        handle.seek(0x88)
        entry_start = struct.unpack("<Q", handle.read(8))[0]
        entry_count = struct.unpack("<Q", handle.read(8))[0]
    return entry_start, entry_count


def _parse_entry_at(handle, offset: int) -> AespEntry:
    """Parse a single AESP index entry at *offset*."""
    handle.seek(offset)
    raw_name = handle.read(128)
    ascii_name = raw_name.split(b"\x00", 1)[0].decode("ascii", errors="replace")
    media_id = struct.unpack("<I", handle.read(4))[0]
    reserved = struct.unpack("<I", handle.read(4))[0]
    offset_low = struct.unpack("<I", handle.read(4))[0]
    offset_high = struct.unpack("<I", handle.read(4))[0]
    size_low = struct.unpack("<I", handle.read(4))[0]
    size_high = struct.unpack("<I", handle.read(4))[0]
    return AespEntry(
        ascii_name=ascii_name,
        media_id=media_id,
        reserved=reserved,
        data_offset=(offset_high << 32) | offset_low,
        data_size=(size_high << 32) | size_low,
    )


def _find_aesp_entry(path: Path, media_id: int) -> tuple[int, AespEntry]:
    """Find the index entry for *media_id*.

    Returns ``(entry_file_offset, parsed_entry)``.
    Raises ``RuntimeError`` if *media_id* is not found.
    """
    entry_start, entry_count = _read_aesp_header(path)
    with path.open("rb") as handle:
        for index in range(entry_count):
            entry_offset = entry_start + index * AESP_ENTRY_SIZE
            entry = _parse_entry_at(handle, entry_offset)
            if entry.media_id == media_id:
                return entry_offset, entry
    raise RuntimeError(f"Media ID {media_id} not found in '{path.name}' ({entry_count} entries scanned).")


def _find_aesp_entry_by_name(path: Path, name: str) -> tuple[int, AespEntry]:
    """Find the index entry whose ASCII name matches *name*.

    Returns ``(entry_file_offset, parsed_entry)``.
    Raises ``RuntimeError`` if not found.
    """
    entry_start, entry_count = _read_aesp_header(path)
    with path.open("rb") as handle:
        for index in range(entry_count):
            entry_offset = entry_start + index * AESP_ENTRY_SIZE
            entry = _parse_entry_at(handle, entry_offset)
            if entry.ascii_name == name:
                return entry_offset, entry
    raise RuntimeError(f"Entry named '{name}' not found in '{path.name}' ({entry_count} entries scanned).")


def _update_aesp_entry_in_place(
    path: Path,
    entry_file_offset: int,
    new_data_offset: int,
    new_data_size: int,
) -> None:
    """Overwrite the offset and size fields of an AESP index entry."""
    offset_low = new_data_offset & 0xFFFFFFFF
    offset_high = (new_data_offset >> 32) & 0xFFFFFFFF
    size_low = new_data_size & 0xFFFFFFFF
    size_high = (new_data_size >> 32) & 0xFFFFFFFF
    payload = struct.pack("<IIII", offset_low, offset_high, size_low, size_high)
    with path.open("r+b") as handle:
        handle.seek(entry_file_offset + AESP_ENTRY_OFFSET_FIELD)
        handle.write(payload)
        handle.flush()


def _append_data(path: Path, data: bytes) -> int:
    """Append *data* to the end of *path* and return the offset where it was written."""
    with path.open("r+b") as handle:
        handle.seek(0, 2)  # seek to end
        new_offset = handle.tell()
        handle.write(data)
        handle.flush()
    return new_offset


# ---------------------------------------------------------------------------
# BNK in_memory_media_size patching
# ---------------------------------------------------------------------------
# Wwise Sound objects (HIRC type 2) store an ``in_memory_media_size`` field
# that must match the WEM size in the AESP index.  The game uses this field
# to allocate buffers and read audio data, so a size mismatch causes silence.
#
# HIRC Sound object layout (Wwise v150):
#   +0  object_id       u32
#   +4  plugin_id       u32
#   +8  stream_type     u8   (0=embedded/aesp, 1=streamed, 2=prefetch)
#   +9  source_id       u32  (media / source ID)
#   +13 in_memory_size  u32  (byte count the runtime reads)
# ---------------------------------------------------------------------------

_BNK_HIRC_SOUND_TYPE = 2
_HIRC_SOUND_SOURCE_ID_OFFSET = 9
_HIRC_SOUND_IN_MEM_SIZE_OFFSET = 13


def _patch_bnk_media_size_in_meta(
    meta_path: Path,
    media_id: int,
    new_size: int,
    log: LogCallback,
) -> int:
    """Update ``in_memory_media_size`` for *media_id* inside BNKs stored in *meta_path*.

    Searches every entry in the meta AESP for BKHD data that contains a
    HIRC Sound object referencing *media_id* and patches the size field
    in-place.  Returns the number of patches applied.
    """
    media_id_bytes = struct.pack("<I", media_id)
    new_size_bytes = struct.pack("<I", new_size)
    patches_applied = 0

    entry_start, entry_count = _read_aesp_header(meta_path)

    with meta_path.open("r+b") as meta:
        for index in range(entry_count):
            entry_abs = entry_start + index * AESP_ENTRY_SIZE
            entry = _parse_entry_at(meta, entry_abs)

            # Only process entries large enough to be BNKs
            if entry.data_size < 16:
                continue

            meta.seek(entry.data_offset)
            magic = meta.read(4)
            if magic != b"BKHD":
                continue

            # Scan for HIRC section
            bank_start = entry.data_offset
            pos = bank_start
            bank_end = bank_start + entry.data_size
            while pos < bank_end - 8:
                meta.seek(pos)
                sec_id = meta.read(4)
                sec_size = struct.unpack("<I", meta.read(4))[0]
                if sec_id == b"HIRC":
                    hirc_start = pos + 8
                    meta.seek(hirc_start)
                    obj_count = struct.unpack("<I", meta.read(4))[0]
                    obj_pos = hirc_start + 4
                    for _ in range(obj_count):
                        if obj_pos + 5 >= bank_end:
                            break
                        meta.seek(obj_pos)
                        obj_type = meta.read(1)[0]
                        obj_size = struct.unpack("<I", meta.read(4))[0]
                        obj_data_start = obj_pos + 5
                        if obj_type == _BNK_HIRC_SOUND_TYPE and obj_size >= 17:
                            # Read source_id at offset 9
                            meta.seek(obj_data_start + _HIRC_SOUND_SOURCE_ID_OFFSET)
                            source_id_raw = meta.read(4)
                            if source_id_raw == media_id_bytes:
                                # Read current in_memory_media_size
                                meta.seek(obj_data_start + _HIRC_SOUND_IN_MEM_SIZE_OFFSET)
                                old_size_raw = meta.read(4)
                                old_size = struct.unpack("<I", old_size_raw)[0]
                                if old_size != new_size:
                                    meta.seek(obj_data_start + _HIRC_SOUND_IN_MEM_SIZE_OFFSET)
                                    meta.write(new_size_bytes)
                                    log(
                                        f"Patched in_memory_media_size in '{entry.ascii_name}' bank: "
                                        f"{old_size:,} -> {new_size:,} for media {media_id}."
                                    )
                                    patches_applied += 1
                        obj_pos += 5 + obj_size
                pos += 8 + sec_size
    return patches_applied

def _rebuild_bank_with_replacement(
    bank_data: bytes,
    media_id: int,
    replacement_wem: bytes,
) -> bytes:
    """Return a new BNK blob with *media_id*'s WEM replaced in DIDX/DATA.

    Also patches the HIRC ``in_memory_media_size`` field for any Sound
    object that references *media_id*.
    """
    media_id_bytes = struct.pack("<I", media_id)
    new_size = len(replacement_wem)
    new_size_bytes = struct.pack("<I", new_size)

    # Parse chunks
    chunks: list[tuple[bytes, bytes]] = []  # (chunk_id_4bytes, payload)
    pos = 0
    didx_index: int | None = None
    data_index: int | None = None
    hirc_index: int | None = None

    while pos + 8 <= len(bank_data):
        chunk_id = bank_data[pos : pos + 4]
        chunk_size = struct.unpack_from("<I", bank_data, pos + 4)[0]
        payload = bank_data[pos + 8 : pos + 8 + chunk_size]
        idx = len(chunks)
        if chunk_id == b"DIDX":
            didx_index = idx
        elif chunk_id == b"DATA":
            data_index = idx
        elif chunk_id == b"HIRC":
            hirc_index = idx
        chunks.append((chunk_id, payload))
        pos += 8 + chunk_size

    if didx_index is None or data_index is None:
        raise RuntimeError("BNK does not contain DIDX/DATA sections — cannot replace embedded media.")

    # Parse DIDX entries — each is 12 bytes: media_id(u32), rel_offset(u32), size(u32)
    didx_payload = chunks[didx_index][1]
    entry_count = len(didx_payload) // 12
    didx_entries: list[tuple[int, int, int]] = []
    target_idx: int | None = None
    for i in range(entry_count):
        mid, rel_off, sz = struct.unpack_from("<III", didx_payload, i * 12)
        didx_entries.append((mid, rel_off, sz))
        if mid == media_id:
            target_idx = i

    if target_idx is None:
        raise RuntimeError(f"Media ID {media_id} not found in BNK DIDX ({entry_count} entries).")

    # Rebuild DATA payload with the replacement WEM spliced in
    old_data_payload = chunks[data_index][1]
    old_mid, old_rel_offset, old_size = didx_entries[target_idx]
    size_delta = new_size - old_size

    new_data_payload = bytearray()
    new_data_payload.extend(old_data_payload[:old_rel_offset])
    new_data_payload.extend(replacement_wem)
    new_data_payload.extend(old_data_payload[old_rel_offset + old_size :])

    # Rebuild DIDX with updated offsets and size
    new_didx_payload = bytearray()
    for i, (mid, rel_off, sz) in enumerate(didx_entries):
        if i == target_idx:
            new_didx_payload.extend(struct.pack("<III", mid, rel_off, new_size))
        elif rel_off > old_rel_offset:
            new_didx_payload.extend(struct.pack("<III", mid, rel_off + size_delta, sz))
        else:
            new_didx_payload.extend(struct.pack("<III", mid, rel_off, sz))

    chunks[didx_index] = (b"DIDX", bytes(new_didx_payload))
    chunks[data_index] = (b"DATA", bytes(new_data_payload))

    # Patch HIRC Sound objects referencing this media_id
    if hirc_index is not None:
        hirc_payload = bytearray(chunks[hirc_index][1])
        obj_count = struct.unpack_from("<I", hirc_payload, 0)[0]
        obj_pos = 4
        for _ in range(obj_count):
            if obj_pos + 5 > len(hirc_payload):
                break
            obj_type = hirc_payload[obj_pos]
            obj_size = struct.unpack_from("<I", hirc_payload, obj_pos + 1)[0]
            obj_data_start = obj_pos + 5
            if obj_type == _BNK_HIRC_SOUND_TYPE and obj_size >= 17:
                src_id_off = obj_data_start + _HIRC_SOUND_SOURCE_ID_OFFSET
                if hirc_payload[src_id_off : src_id_off + 4] == media_id_bytes:
                    mem_size_off = obj_data_start + _HIRC_SOUND_IN_MEM_SIZE_OFFSET
                    hirc_payload[mem_size_off : mem_size_off + 4] = new_size_bytes
            obj_pos += 5 + obj_size
        chunks[hirc_index] = (b"HIRC", bytes(hirc_payload))

    # Reassemble
    result = bytearray()
    for chunk_id, payload in chunks:
        result.extend(chunk_id)
        result.extend(struct.pack("<I", len(payload)))
        result.extend(payload)
    return bytes(result)


def _find_bank_containing_media(
    meta_path: Path,
    media_id: int,
) -> tuple[int, AespEntry, bytes] | None:
    """Scan meta.aesp banks for one whose DIDX references *media_id*.

    Returns ``(entry_file_offset, entry, bank_data)`` or ``None``.
    """
    entry_start, entry_count = _read_aesp_header(meta_path)
    media_id_bytes = struct.pack("<I", media_id)
    with meta_path.open("rb") as handle:
        for index in range(entry_count):
            entry_offset = entry_start + index * AESP_ENTRY_SIZE
            entry = _parse_entry_at(handle, entry_offset)
            if entry.data_size < 16:
                continue
            handle.seek(entry.data_offset)
            magic = handle.read(4)
            if magic != b"BKHD":
                continue
            # Quick scan: look for DIDX and check for the media_id
            handle.seek(entry.data_offset)
            bank_data = handle.read(entry.data_size)
            pos = 0
            while pos + 8 <= len(bank_data):
                chunk_id = bank_data[pos : pos + 4]
                chunk_size = struct.unpack_from("<I", bank_data, pos + 4)[0]
                if chunk_id == b"DIDX":
                    didx_start = pos + 8
                    didx_end = didx_start + chunk_size
                    scan = didx_start
                    while scan + 12 <= didx_end:
                        if bank_data[scan : scan + 4] == media_id_bytes:
                            return entry_offset, entry, bank_data
                        scan += 12
                    break  # DIDX scanned, not found in this bank
                pos += 8 + chunk_size
    return None


def replace_aesp_bank_media(
    meta_path: str | Path,
    media_id: int,
    replacement_path: str | Path,
    log: LogCallback,
    *,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> AespReplacementResult:
    """Replace an embedded WEM inside a BNK entry in meta.aesp.

    Finds the bank containing *media_id* in its DIDX, rebuilds the bank
    with the replacement WEM, appends the rebuilt bank to meta.aesp, and
    updates the AESP index entry.
    """
    resolved_meta = Path(meta_path).expanduser().resolve()
    resolved_replacement = Path(replacement_path).expanduser().resolve()

    if not resolved_meta.exists():
        raise FileNotFoundError(f"Archive not found: {resolved_meta}")
    if not resolved_replacement.exists():
        raise FileNotFoundError(f"Replacement file not found: {resolved_replacement}")

    replacement_bytes = resolved_replacement.read_bytes()
    if not replacement_bytes:
        raise ValueError(f"Replacement file is empty: {resolved_replacement}")
    if replacement_bytes[:4] != RIFF_MAGIC:
        raise ValueError(
            f"Replacement file does not start with a RIFF header: {resolved_replacement.name}."
        )

    _raise_if_cancelled(cancel_event)

    if progress is not None:
        progress(f"Scanning banks in {resolved_meta.name} for media {media_id}...", 0, 3)

    result = _find_bank_containing_media(resolved_meta, media_id)
    if result is None:
        entry_start, entry_count = _read_aesp_header(resolved_meta)
        raise RuntimeError(
            f"Media ID {media_id} not found in any bank in '{resolved_meta.name}' ({entry_count} entries scanned)."
        )
    entry_offset, entry, bank_data = result
    log(f"Found media {media_id} in bank '{entry.ascii_name}' (offset 0x{entry.data_offset:X}, {entry.data_size:,} bytes).")

    _raise_if_cancelled(cancel_event)

    if progress is not None:
        progress(f"Rebuilding bank '{entry.ascii_name}' with replacement...", 1, 3)
    new_bank_data = _rebuild_bank_with_replacement(bank_data, media_id, replacement_bytes)
    log(f"Rebuilt bank '{entry.ascii_name}': {len(bank_data):,} -> {len(new_bank_data):,} bytes.")

    _raise_if_cancelled(cancel_event)

    if progress is not None:
        progress(f"Writing rebuilt bank to {resolved_meta.name}...", 2, 3)
    backup_path = _ensure_aesp_backup(resolved_meta)
    new_offset = _append_data(resolved_meta, new_bank_data)
    _update_aesp_entry_in_place(resolved_meta, entry_offset, new_offset, len(new_bank_data))
    log(f"Replaced bank '{entry.ascii_name}' in {resolved_meta.name}: new offset 0x{new_offset:X}.")

    _log_replacement(
        resolved_meta,
        media_id,
        entry.data_offset,
        entry.data_size,
        new_offset,
        len(new_bank_data),
        resolved_replacement,
    )

    return AespReplacementResult(
        archive_path=resolved_meta,
        backup_path=backup_path,
        media_id=media_id,
        replacement_path=resolved_replacement,
        replacement_size=len(replacement_bytes),
        new_offset=new_offset,
    )


def _log_replacement(
    archive_path: Path,
    media_id: int,
    old_offset: int,
    old_size: int,
    new_offset: int,
    new_size: int,
    replacement_source: Path,
) -> None:
    """Append a replacement record to the manifest JSON log."""
    manifest_path = archive_path.parent / f"{archive_path.stem}_replacements.json"
    records: list[dict] = []
    if manifest_path.exists():
        try:
            records = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            records = []
    records.append({
        "media_id": media_id,
        "old_offset": old_offset,
        "old_size": old_size,
        "new_offset": new_offset,
        "new_size": new_size,
        "replacement_source": str(replacement_source),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    manifest_path.write_text(json.dumps(records, indent=2), encoding="utf-8")


def replace_aesp_external_media(
    archive_path: str | Path,
    media_id: int,
    replacement_path: str | Path,
    log: LogCallback,
    *,
    meta_path: str | Path | None = None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> AespReplacementResult:
    """Replace an external WEM entry in an AESP archive (sfx or streams).

    The replacement WEM is appended to the end of the archive and the
    index entry is updated in-place.  A ``.bak`` backup is created on the
    first modification.

    When *meta_path* is provided (path to ``meta.aesp``), the
    ``in_memory_media_size`` field in the corresponding BNK Sound object
    is patched to match the new WEM size.  The game uses this field to
    allocate read buffers, so a mismatch causes silent playback.
    """
    resolved_archive = Path(archive_path).expanduser().resolve()
    resolved_replacement = Path(replacement_path).expanduser().resolve()

    if not resolved_archive.exists():
        raise FileNotFoundError(f"Archive not found: {resolved_archive}")
    if not resolved_replacement.exists():
        raise FileNotFoundError(f"Replacement file not found: {resolved_replacement}")

    replacement_bytes = resolved_replacement.read_bytes()
    if not replacement_bytes:
        raise ValueError(f"Replacement file is empty: {resolved_replacement}")
    if replacement_bytes[:4] != RIFF_MAGIC:
        raise ValueError(
            f"Replacement file does not start with a RIFF header: {resolved_replacement.name}. "
            "Only WEM (RIFF/WAVE) files are supported for AESP replacement."
        )

    _raise_if_cancelled(cancel_event)

    if progress is not None:
        progress(f"Looking up media {media_id} in {resolved_archive.name}...", 0, 3)
    entry_offset, entry = _find_aesp_entry(resolved_archive, media_id)
    log(f"Found media {media_id} at index offset 0x{entry_offset:X} (data at 0x{entry.data_offset:X}, {entry.data_size} bytes).")

    _raise_if_cancelled(cancel_event)

    if progress is not None:
        progress(f"Backing up {resolved_archive.name}...", 1, 3)
    backup_path = _ensure_aesp_backup(resolved_archive)
    log(f"Backup: {backup_path}")

    _raise_if_cancelled(cancel_event)

    if progress is not None:
        progress(f"Writing replacement for media {media_id}...", 2, 3)
    new_offset = _append_data(resolved_archive, replacement_bytes)
    new_size = len(replacement_bytes)
    _update_aesp_entry_in_place(resolved_archive, entry_offset, new_offset, new_size)
    log(f"Replaced media {media_id}: new offset 0x{new_offset:X}, new size {new_size}.")

    # Patch in_memory_media_size in the BNK inside meta.aesp so the game
    # allocates the correct read buffer for the replacement WEM.
    if meta_path is not None:
        resolved_meta = Path(meta_path).expanduser().resolve()
        if resolved_meta.exists():
            _ensure_aesp_backup(resolved_meta)
            _patch_bnk_media_size_in_meta(resolved_meta, media_id, new_size, log)

    _log_replacement(
        resolved_archive,
        media_id,
        entry.data_offset,
        entry.data_size,
        new_offset,
        new_size,
        resolved_replacement,
    )
    log(f"Logged replacement to {resolved_archive.stem}_replacements.json.")

    return AespReplacementResult(
        archive_path=resolved_archive,
        backup_path=backup_path,
        media_id=media_id,
        replacement_path=resolved_replacement,
        replacement_size=new_size,
        new_offset=new_offset,
    )
