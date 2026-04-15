from __future__ import annotations

import csv
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import os
import shutil
import struct
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import threading
from typing import Callable

from dyingaudio.background import TaskCancelled
from dyingaudio.core.media_tools import run_hidden
from dyingaudio.core.wwise_audio_type import infer_audio_type, normalize_object_types
from dyingaudio.core.wwise_workspace import ExtractedBank, NamedAudioLink, UnresolvedAudioLink


ProgressCallback = Callable[[str, float | None, float | None], None]
LogCallback = Callable[[str], None]

BKHD_PATTERN = b"BKHD"
CHUNK_HEADER_SIZE = 8
EXTERNAL_ENTRY_SIZE = 0x98
LINKING_BANK_DELAY_SECONDS = 30.0
LINKING_BANK_DELAY_SUFFIX = " still loading; larger banks can take a minute or more."


@dataclass(slots=True)
class PreloadInfo:
    preload_id: int
    name: str
    bank_hash: int


@dataclass(slots=True)
class MediaInfo:
    archive: str
    media_id: int
    offset: int
    size: int
    source: Path
    exists: bool
    non_audio: bool = False


@dataclass(slots=True)
class HircObject:
    object_type: int
    payload: bytes


@dataclass(slots=True)
class ParsedBank:
    bank_id: int | None
    name: str | None
    offset: int
    length: int
    objects: dict[int, HircObject]


@dataclass(slots=True)
class WorkspaceBuildResult:
    named_links: list[NamedAudioLink]
    extracted_banks: list[ExtractedBank]
    unresolved: list[UnresolvedAudioLink]
    summary_text: str


@dataclass(slots=True)
class BankLinkResult:
    bank_index: int
    bank_name: str
    named_links: list[NamedAudioLink]
    unresolved: list[UnresolvedAudioLink]


def fnv1_lower(value: str) -> int:
    hash_value = 2166136261
    for character in value.lower():
        hash_value = (hash_value * 16777619) & 0xFFFFFFFF
        hash_value ^= ord(character) & 0xFF
    return hash_value & 0xFFFFFFFF


def get_safe_segment(value: str, max_length: int = 80) -> str:
    clean = "".join("_" if character in '<>:"/\\|?*' else character for character in value)
    clean = " ".join(clean.split()).strip(" .")
    if not clean:
        clean = "unnamed"
    if len(clean) > max_length:
        suffix = f"{fnv1_lower(clean):08X}"
        keep = max(1, max_length - len(suffix) - 1)
        clean = f"{clean[:keep]}_{suffix}"
    return clean


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise TaskCancelled()


def _linking_delay_message(base_message: str) -> str:
    return f"{base_message}{LINKING_BANK_DELAY_SUFFIX}"


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def create_hard_link_safe(path: Path, target: Path) -> None:
    if path.exists():
        return
    ensure_directory(path.parent)
    try:
        os.link(target, path)
    except FileExistsError:
        return
    except OSError:
        if path.exists():
            return
        shutil.copy2(target, path)


def write_file_slice(source_path: Path, offset: int, length: int, destination_path: Path) -> None:
    ensure_directory(destination_path.parent)
    with source_path.open("rb") as input_handle, destination_path.open("wb") as output_handle:
        input_handle.seek(offset)
        remaining = length
        while remaining > 0:
            chunk = input_handle.read(min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeError(f"Unexpected EOF while copying slice from {source_path} at offset {offset}.")
            output_handle.write(chunk)
            remaining -= len(chunk)


def find_ascii_offsets(path: Path, pattern: bytes) -> list[int]:
    hits: list[int] = []
    overlap = max(len(pattern) - 1, 0)
    chunk_size = 8 * 1024 * 1024
    carry = b""
    position = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            data = carry + chunk
            search_limit = len(data) - len(pattern) + 1
            for index in range(max(search_limit, 0)):
                if data[index:index + len(pattern)] == pattern:
                    hits.append(position - len(carry) + index)
            carry = data[-overlap:] if overlap else b""
            position += len(chunk)
    return hits


def parse_external_pack_index(file_path: Path, archive: str, audio_root: Path) -> dict[int, list[MediaInfo]]:
    global_media: dict[int, list[MediaInfo]] = defaultdict(list)
    with file_path.open("rb") as handle:
        handle.seek(0x88)
        entry_start = struct.unpack("<Q", handle.read(8))[0]
        entry_count = struct.unpack("<Q", handle.read(8))[0]
        for index in range(entry_count):
            entry_offset = entry_start + (index * EXTERNAL_ENTRY_SIZE)
            handle.seek(entry_offset + 128)
            media_id = struct.unpack("<I", handle.read(4))[0]
            handle.read(4)
            riff_offset_low = struct.unpack("<I", handle.read(4))[0]
            riff_offset_high = struct.unpack("<I", handle.read(4))[0]
            riff_offset = (riff_offset_high << 32) | riff_offset_low
            size_low = struct.unpack("<I", handle.read(4))[0]
            size_high = struct.unpack("<I", handle.read(4))[0]
            size = (size_high << 32) | size_low
            flat_path = audio_root / archive / f"{archive}.aesp_{riff_offset:010X}.wav"
            global_media[media_id].append(
                MediaInfo(
                    archive=archive,
                    media_id=media_id,
                    offset=riff_offset,
                    size=size,
                    source=flat_path,
                    exists=flat_path.exists(),
                )
            )
    return global_media


def parse_hirc_objects(reader: Path, payload_offset: int) -> dict[int, HircObject]:
    objects: dict[int, HircObject] = {}
    with reader.open("rb") as handle:
        handle.seek(payload_offset)
        count = struct.unpack("<I", handle.read(4))[0]
        for _ in range(count):
            object_type = struct.unpack("<B", handle.read(1))[0]
            size = struct.unpack("<I", handle.read(4))[0]
            object_id = struct.unpack("<I", handle.read(4))[0]
            payload = handle.read(size - 4)
            objects[object_id] = HircObject(object_type=object_type, payload=payload)
    return objects


def get_event_actions(payload: bytes) -> list[int]:
    if len(payload) < 1:
        return []
    count = payload[0]
    if len(payload) < 1 + (count * 4):
        return []
    return [struct.unpack_from("<I", payload, 1 + (index * 4))[0] for index in range(count)]


def get_any_object_refs(payload: bytes, known_object_ids: set[int]) -> list[int]:
    refs: list[int] = []
    for offset in range(0, len(payload) - 3):
        value = struct.unpack_from("<I", payload, offset)[0]
        if value in known_object_ids:
            refs.append(value)
    return sorted(set(refs))


def get_action_target(payload: bytes, known_object_ids: set[int]) -> int | None:
    if len(payload) >= 6:
        target = struct.unpack_from("<I", payload, 2)[0]
        if target in known_object_ids:
            return target
    targets = get_any_object_refs(payload, known_object_ids)
    if len(targets) == 1:
        return targets[0]
    return None


def get_tail_children(payload: bytes, known_object_ids: set[int]) -> list[int]:
    if len(payload) < 8:
        return []
    max_count = min(512, (len(payload) - 4) // 4)
    for count in range(max_count, 0, -1):
        count_offset = len(payload) - 4 - (count * 4)
        if count_offset < 0:
            continue
        stored_count = struct.unpack_from("<I", payload, count_offset)[0]
        if stored_count != count:
            continue
        children: list[int] = []
        valid = True
        for index in range(count):
            child = struct.unpack_from("<I", payload, count_offset + 4 + (index * 4))[0]
            if child not in known_object_ids:
                valid = False
                break
            children.append(child)
        if valid:
            return children
    return []


def get_action_lookup_keys(payload: bytes) -> list[int]:
    keys: list[int] = []
    if len(payload) >= 6:
        keys.append(struct.unpack_from("<I", payload, 2)[0])
    if len(payload) >= 13:
        keys.append(struct.unpack_from("<I", payload, 9)[0])
    if len(payload) >= 17:
        keys.append(struct.unpack_from("<I", payload, 13)[0])
    return sorted(set(keys))


def get_state_mapped_children(local_objects: dict[int, HircObject], keys: list[int], known_object_ids: set[int]) -> list[int]:
    children: list[int] = []
    target_keys = set(keys)
    for object_id, obj in local_objects.items():
        if obj.object_type not in {10, 12, 13}:
            continue
        payload = obj.payload
        for offset in range(0, len(payload) - 7):
            value = struct.unpack_from("<I", payload, offset)[0]
            if value not in target_keys:
                continue
            child_id = struct.unpack_from("<I", payload, offset + 4)[0]
            if child_id in known_object_ids:
                children.append(child_id)
    return sorted(set(children))


def get_sound_media(payload: bytes, global_media: dict[int, list[MediaInfo]]) -> list[int]:
    hits: list[int] = []
    if len(payload) >= 9:
        primary = struct.unpack_from("<I", payload, 5)[0]
        if primary in global_media:
            hits.append(primary)
    if hits:
        return sorted(set(hits))
    for offset in range(0, len(payload) - 3):
        value = struct.unpack_from("<I", payload, offset)[0]
        if value in global_media:
            hits.append(value)
    return sorted(set(hits))


def get_music_track_media(payload: bytes, global_media: dict[int, list[MediaInfo]]) -> list[int]:
    hits: list[int] = []
    for offset in (10, 27):
        if len(payload) < offset + 4:
            continue
        media_id = struct.unpack_from("<I", payload, offset)[0]
        if media_id in global_media:
            hits.append(media_id)
    return sorted(set(hits))


def resolve_object_media(
    object_id: int,
    local_objects: dict[int, HircObject],
    global_objects: dict[int, HircObject],
    known_object_ids: set[int],
    global_media: dict[int, list[MediaInfo]],
    memo: dict[int, list[int]],
    stack: set[int],
    cancel_event: threading.Event | None = None,
) -> list[int]:
    _raise_if_cancelled(cancel_event)
    if object_id in memo:
        return memo[object_id]
    if object_id in stack:
        return []
    stack.add(object_id)

    obj = local_objects.get(object_id) or global_objects.get(object_id)
    result: list[int] = []
    if obj is not None:
        payload = obj.payload
        if obj.object_type == 2:
            result.extend(get_sound_media(payload, global_media))
        elif obj.object_type == 11:
            result.extend(get_music_track_media(payload, global_media))
        elif obj.object_type == 3:
            target = get_action_target(payload, known_object_ids)
            if target is not None:
                result.extend(resolve_object_media(target, local_objects, global_objects, known_object_ids, global_media, memo, stack, cancel_event))
            if not result and len(payload) >= 2 and payload[1] == 0x12:
                lookup_keys = get_action_lookup_keys(payload)
                for child_id in get_state_mapped_children(local_objects, lookup_keys, known_object_ids):
                    _raise_if_cancelled(cancel_event)
                    result.extend(resolve_object_media(child_id, local_objects, global_objects, known_object_ids, global_media, memo, stack, cancel_event))
        elif obj.object_type == 4:
            for action_id in get_event_actions(payload):
                _raise_if_cancelled(cancel_event)
                result.extend(resolve_object_media(action_id, local_objects, global_objects, known_object_ids, global_media, memo, stack, cancel_event))
        elif obj.object_type == 5:
            child_ids = get_tail_children(payload, known_object_ids) or get_any_object_refs(payload, known_object_ids)
            for child_id in child_ids:
                _raise_if_cancelled(cancel_event)
                result.extend(resolve_object_media(child_id, local_objects, global_objects, known_object_ids, global_media, memo, stack, cancel_event))
        elif obj.object_type in {7, 10, 12, 13}:
            child_ids = get_tail_children(payload, known_object_ids) if obj.object_type == 7 else get_any_object_refs(payload, known_object_ids)
            for child_id in child_ids:
                _raise_if_cancelled(cancel_event)
                result.extend(resolve_object_media(child_id, local_objects, global_objects, known_object_ids, global_media, memo, stack, cancel_event))
        else:
            for child_id in get_tail_children(payload, known_object_ids):
                _raise_if_cancelled(cancel_event)
                result.extend(resolve_object_media(child_id, local_objects, global_objects, known_object_ids, global_media, memo, stack, cancel_event))

    stack.remove(object_id)
    unique = sorted(set(result))
    memo[object_id] = unique
    return unique


def _merge_media_detail_maps(target: dict[int, set[int]], incoming: dict[int, tuple[int, ...]]) -> None:
    for media_id, object_types in incoming.items():
        bucket = target.setdefault(media_id, set())
        bucket.update(normalize_object_types(object_types))


def resolve_object_media_details(
    object_id: int,
    local_objects: dict[int, HircObject],
    global_objects: dict[int, HircObject],
    known_object_ids: set[int],
    global_media: dict[int, list[MediaInfo]],
    memo: dict[int, dict[int, tuple[int, ...]]],
    stack: set[int],
    cancel_event: threading.Event | None = None,
) -> dict[int, tuple[int, ...]]:
    _raise_if_cancelled(cancel_event)
    if object_id in memo:
        return {media_id: tuple(object_types) for media_id, object_types in memo[object_id].items()}
    if object_id in stack:
        return {}
    stack.add(object_id)

    obj = local_objects.get(object_id) or global_objects.get(object_id)
    merged: dict[int, set[int]] = {}
    if obj is not None:
        payload = obj.payload
        if obj.object_type == 2:
            for media_id in get_sound_media(payload, global_media):
                merged[media_id] = {2}
        elif obj.object_type == 11:
            for media_id in get_music_track_media(payload, global_media):
                merged[media_id] = {11}
        elif obj.object_type == 3:
            target = get_action_target(payload, known_object_ids)
            if target is not None:
                _merge_media_detail_maps(
                    merged,
                    resolve_object_media_details(
                        target,
                        local_objects,
                        global_objects,
                        known_object_ids,
                        global_media,
                        memo,
                        stack,
                        cancel_event,
                    ),
                )
            if not merged and len(payload) >= 2 and payload[1] == 0x12:
                lookup_keys = get_action_lookup_keys(payload)
                for child_id in get_state_mapped_children(local_objects, lookup_keys, known_object_ids):
                    _raise_if_cancelled(cancel_event)
                    _merge_media_detail_maps(
                        merged,
                        resolve_object_media_details(
                            child_id,
                            local_objects,
                            global_objects,
                            known_object_ids,
                            global_media,
                            memo,
                            stack,
                            cancel_event,
                        ),
                    )
        elif obj.object_type == 4:
            for action_id in get_event_actions(payload):
                _raise_if_cancelled(cancel_event)
                _merge_media_detail_maps(
                    merged,
                    resolve_object_media_details(
                        action_id,
                        local_objects,
                        global_objects,
                        known_object_ids,
                        global_media,
                        memo,
                        stack,
                        cancel_event,
                    ),
                )
        elif obj.object_type == 5:
            child_ids = get_tail_children(payload, known_object_ids) or get_any_object_refs(payload, known_object_ids)
            for child_id in child_ids:
                _raise_if_cancelled(cancel_event)
                _merge_media_detail_maps(
                    merged,
                    resolve_object_media_details(
                        child_id,
                        local_objects,
                        global_objects,
                        known_object_ids,
                        global_media,
                        memo,
                        stack,
                        cancel_event,
                    ),
                )
        elif obj.object_type in {7, 10, 12, 13}:
            child_ids = get_tail_children(payload, known_object_ids) if obj.object_type == 7 else get_any_object_refs(payload, known_object_ids)
            for child_id in child_ids:
                _raise_if_cancelled(cancel_event)
                _merge_media_detail_maps(
                    merged,
                    resolve_object_media_details(
                        child_id,
                        local_objects,
                        global_objects,
                        known_object_ids,
                        global_media,
                        memo,
                        stack,
                        cancel_event,
                    ),
                )
        else:
            for child_id in get_tail_children(payload, known_object_ids):
                _raise_if_cancelled(cancel_event)
                _merge_media_detail_maps(
                    merged,
                    resolve_object_media_details(
                        child_id,
                        local_objects,
                        global_objects,
                        known_object_ids,
                        global_media,
                        memo,
                        stack,
                        cancel_event,
                    ),
                )

    stack.remove(object_id)
    normalized = {media_id: tuple(sorted(object_types)) for media_id, object_types in merged.items()}
    memo[object_id] = normalized
    return {media_id: tuple(object_types) for media_id, object_types in normalized.items()}


def get_bank_media_links(
    bank: ParsedBank,
    preloads_by_name: dict[str, PreloadInfo],
    events_by_preload_id: dict[int, list[str]],
    global_objects: dict[int, HircObject],
    global_media: dict[int, list[MediaInfo]],
    cancel_event: threading.Event | None = None,
) -> list[tuple[str, str, int, MediaInfo, tuple[int, ...]]]:
    _raise_if_cancelled(cancel_event)
    if bank.name is None or bank.name not in preloads_by_name:
        return []
    preload = preloads_by_name[bank.name]
    event_names = events_by_preload_id.get(preload.preload_id)
    if not event_names:
        return []

    memo: dict[int, dict[int, tuple[int, ...]]] = {}
    links: list[tuple[str, str, int, MediaInfo, tuple[int, ...]]] = []
    local_known_object_ids = set(bank.objects.keys())
    for event_name in event_names:
        _raise_if_cancelled(cancel_event)
        event_hash = fnv1_lower(event_name)
        if event_hash not in bank.objects:
            continue
        media_details = resolve_object_media_details(
            event_hash,
            bank.objects,
            global_objects,
            local_known_object_ids,
            global_media,
            memo,
            set(),
            cancel_event,
        )
        for media_id in sorted(media_details):
            _raise_if_cancelled(cancel_event)
            infos = list(global_media.get(media_id, []))
            if not infos:
                continue
            existing_infos = [info for info in infos if info.exists]
            for info in (existing_infos or infos):
                links.append((bank.name, event_name, media_id, info, media_details.get(media_id, ())))
    return links


def ensure_decoded_flat_source(
    info: MediaInfo,
    archive_files: dict[str, Path],
    vgmstream_cli_path: Path,
    temp_root: Path,
    cancel_event: threading.Event | None = None,
) -> bool:
    _raise_if_cancelled(cancel_event)
    if info.source.exists():
        return True
    archive_path = archive_files.get(info.archive)
    if archive_path is None or not archive_path.exists() or not vgmstream_cli_path.exists():
        return False

    ensure_directory(info.source.parent)
    ensure_directory(temp_root)
    with archive_path.open("rb") as handle:
        handle.seek(info.offset)
        header = handle.read(12)
        if len(header) != 12:
            return False
        if header[:4] != b"RIFF":
            info.non_audio = True
            return False
        if header[8:12] != b"WAVE":
            info.non_audio = True
            return False
        riff_size = struct.unpack_from("<I", header, 4)[0] + 8
        handle.seek(info.offset)
        riff_bytes = handle.read(riff_size)
        if len(riff_bytes) != riff_size:
            return False

    temp_input = temp_root / f"{info.archive}_{info.offset:010X}.wem"
    try:
        temp_input.write_bytes(riff_bytes)
        _raise_if_cancelled(cancel_event)
        result = run_hidden(
            [str(vgmstream_cli_path), "-o", str(info.source), str(temp_input)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        _raise_if_cancelled(cancel_event)
        return result.returncode == 0 and info.source.exists()
    finally:
        temp_input.unlink(missing_ok=True)


def parse_mapping_xml(xml_path: Path) -> tuple[dict[str, PreloadInfo], dict[int, list[str]], dict[int, str]]:
    root = ET.fromstring(xml_path.read_text(encoding="utf-8"))
    preloads_by_name: dict[str, PreloadInfo] = {}
    events_by_preload_id: dict[int, list[str]] = defaultdict(list)
    bank_id_to_name: dict[int, str] = {}

    preloads_node = root.find("Preloads")
    if preloads_node is not None:
        for preload in preloads_node.findall("Preload"):
            name = preload.attrib.get("name", "").strip()
            if not name:
                continue
            preload_id = int(preload.attrib.get("id", "0") or 0)
            info = PreloadInfo(preload_id=preload_id, name=name, bank_hash=fnv1_lower(name))
            preloads_by_name[name] = info
            bank_id_to_name[info.bank_hash] = name

    events_node = root.find("Events")
    if events_node is not None:
        for event in events_node.findall("Event"):
            preload_id = int(event.attrib.get("preload_id", "0") or 0)
            event_name = event.attrib.get("name", "").strip()
            if event_name:
                events_by_preload_id[preload_id].append(event_name)

    return preloads_by_name, dict(events_by_preload_id), bank_id_to_name


def parse_meta_banks(
    meta_file: Path,
    bank_id_to_name: dict[int, str],
    audio_root: Path,
    global_media: dict[int, list[MediaInfo]],
    log: LogCallback,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> tuple[list[ParsedBank], dict[int, HircObject], set[int], list[int]]:
    bank_offsets = find_ascii_offsets(meta_file, BKHD_PATTERN)
    meta_length = meta_file.stat().st_size
    banks: list[ParsedBank] = []
    global_objects: dict[int, HircObject] = {}
    known_object_ids: set[int] = set()
    duplicate_object_ids: list[int] = []

    total = max(len(bank_offsets), 1)
    with meta_file.open("rb") as handle:
        for index, bank_offset in enumerate(bank_offsets):
            _raise_if_cancelled(cancel_event)
            if progress is not None:
                progress(f"Parsing Wwise bank {index + 1}/{len(bank_offsets)}...", index + 1, total)
            next_offset = bank_offsets[index + 1] if index + 1 < len(bank_offsets) else meta_length
            position = bank_offset
            bank_id: int | None = None
            bank_name: str | None = None
            data_payload_offset: int | None = None
            didx_entries: list[tuple[int, int, int]] = []
            hirc_objects: dict[int, HircObject] = {}

            while position + CHUNK_HEADER_SIZE <= next_offset:
                _raise_if_cancelled(cancel_event)
                handle.seek(position)
                chunk_id = handle.read(4)
                if len(chunk_id) != 4:
                    break
                chunk_length_data = handle.read(4)
                if len(chunk_length_data) != 4:
                    break
                chunk_length = struct.unpack("<I", chunk_length_data)[0]
                payload_offset = position + CHUNK_HEADER_SIZE
                if payload_offset + chunk_length > next_offset:
                    break
                try:
                    chunk_name = chunk_id.decode("ascii")
                except UnicodeDecodeError:
                    break
                if not chunk_name.isalnum() or len(chunk_name) != 4 or not chunk_name.isupper():
                    break

                if chunk_name == "BKHD":
                    handle.seek(payload_offset)
                    handle.read(4)
                    bank_id = struct.unpack("<I", handle.read(4))[0]
                    bank_name = bank_id_to_name.get(bank_id, f"bank_{bank_id:08X}")
                elif chunk_name == "DIDX":
                    handle.seek(payload_offset)
                    entry_count = chunk_length // 12
                    for _ in range(entry_count):
                        media_id, rel_offset, size = struct.unpack("<III", handle.read(12))
                        didx_entries.append((media_id, rel_offset, size))
                elif chunk_name == "DATA":
                    data_payload_offset = payload_offset
                elif chunk_name == "HIRC":
                    hirc_objects = parse_hirc_objects(meta_file, payload_offset)

                position = payload_offset + chunk_length

            if bank_name and data_payload_offset is not None:
                for media_id, rel_offset, size in didx_entries:
                    riff_offset = data_payload_offset + rel_offset
                    flat_path = audio_root / "meta" / f"meta.aesp_{riff_offset:010X}.wav"
                    global_media.setdefault(media_id, []).append(
                        MediaInfo(
                            archive="meta",
                            media_id=media_id,
                            offset=riff_offset,
                            size=size,
                            source=flat_path,
                            exists=flat_path.exists(),
                        )
                    )

            for object_id, obj in hirc_objects.items():
                known_object_ids.add(object_id)
                if object_id not in global_objects:
                    global_objects[object_id] = obj
                else:
                    duplicate_object_ids.append(object_id)

            banks.append(
                ParsedBank(
                    bank_id=bank_id,
                    name=bank_name,
                    offset=bank_offset,
                    length=next_offset - bank_offset,
                    objects=hirc_objects,
                )
            )
    log(f"Parsed {len(banks)} Wwise bank(s) from {meta_file.name}.")
    if progress is not None:
        progress(f"Parsed {len(banks)} Wwise bank(s).", total, total)
    return banks, global_objects, known_object_ids, duplicate_object_ids


def build_named_audio_tree(
    *,
    audio_root: Path,
    meta_file: Path,
    sfx_file: Path,
    streams_file: Path,
    xml_file: Path,
    tree_root: Path,
    banks_root: Path,
    vgmstream_cli_path: Path,
    log: LogCallback,
    progress: ProgressCallback | None = None,
    include_banks: list[str] | None = None,
    cancel_event: threading.Event | None = None,
) -> WorkspaceBuildResult:
    started = time.monotonic()
    logs_root = audio_root / "logs"
    temp_decode_root = logs_root / "temp_decode"
    if banks_root.exists():
        shutil.rmtree(banks_root)
    if tree_root.exists():
        shutil.rmtree(tree_root)
    ensure_directory(banks_root)
    ensure_directory(tree_root)
    ensure_directory(logs_root)

    preloads_by_name, events_by_preload_id, bank_id_to_name = parse_mapping_xml(xml_file)
    log(f"Loaded {len(preloads_by_name)} preload(s) and {sum(len(values) for values in events_by_preload_id.values())} event mapping(s).")
    if progress is not None:
        progress("Parsing external Wwise media indexes...", 0, 4)
    global_media = parse_external_pack_index(sfx_file, "sfx", audio_root)
    streams_media = parse_external_pack_index(streams_file, "streams", audio_root)
    for media_id, infos in streams_media.items():
        global_media.setdefault(media_id, []).extend(infos)
    log(f"Indexed {len(global_media)} global media entr{'y' if len(global_media) == 1 else 'ies'}.")
    _raise_if_cancelled(cancel_event)

    banks, global_objects, _known_object_ids, duplicate_object_ids = parse_meta_banks(
        meta_file,
        bank_id_to_name,
        audio_root,
        global_media,
        log,
        progress=progress,
        cancel_event=cancel_event,
    )
    archive_files = {"meta": meta_file, "sfx": sfx_file, "streams": streams_file}

    extracted_banks: list[ExtractedBank] = []
    named_links: list[NamedAudioLink] = []
    unresolved: list[UnresolvedAudioLink] = []

    bank_filter_set = {name.lower() for name in include_banks or []}
    bank_name_counts: dict[str, int] = {}
    total_banks = max(len(banks), 1)
    for index, bank in enumerate(banks):
        _raise_if_cancelled(cancel_event)
        if progress is not None:
            progress(f"Extracting bank {index + 1}/{len(banks)}...", index + 1, total_banks)
        if bank_filter_set and (bank.name or "").lower() not in bank_filter_set:
            continue
        export_bank_name = bank.name or f"unnamed_bank_{bank.offset:010X}"
        safe_bank_name = get_safe_segment(export_bank_name, max_length=96)
        bank_name_counts[safe_bank_name] = bank_name_counts.get(safe_bank_name, 0) + 1
        if bank_name_counts[safe_bank_name] == 1:
            leaf_name = f"{safe_bank_name}.bnk"
        else:
            bank_id = bank.bank_id or 0
            leaf_name = f"{safe_bank_name}__{bank_id:08X}__{bank.offset:010X}.bnk"
        bank_path = banks_root / leaf_name
        write_file_slice(meta_file, bank.offset, bank.length, bank_path)
        extracted_banks.append(
            ExtractedBank(
                bank=export_bank_name,
                bank_id=bank.bank_id or 0,
                offset=bank.offset,
                length=bank.length,
                path=bank_path,
            )
        )

    _raise_if_cancelled(cancel_event)
    linkable_banks = [
        (bank_index, bank)
        for bank_index, bank in enumerate(banks)
        if bank.name and bank.objects and not (bank_filter_set and bank.name.lower() not in bank_filter_set)
    ]
    if progress is not None:
        if linkable_banks:
            progress("Linking named media in parallel...", 0, len(linkable_banks))
        else:
            progress("No named media banks to link.", total_banks, total_banks)

    source_lock_guard = threading.Lock()
    source_locks: dict[Path, threading.Lock] = {}
    active_bank_guard = threading.Lock()
    active_banks: set[str] = set()

    def source_lock_for(path: Path) -> threading.Lock:
        with source_lock_guard:
            lock = source_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                source_locks[path] = lock
            return lock

    def active_bank_name(bank: ParsedBank, bank_index: int) -> str:
        return bank.name or f"bank_{bank_index + 1}"

    def build_bank_links(bank_index: int, bank: ParsedBank) -> BankLinkResult:
        _raise_if_cancelled(cancel_event)
        display_name = active_bank_name(bank, bank_index)
        with active_bank_guard:
            active_banks.add(display_name)
        try:
            links = get_bank_media_links(
                bank,
                preloads_by_name,
                events_by_preload_id,
                global_objects,
                global_media,
                cancel_event=cancel_event,
            )
            bank_named_links: list[NamedAudioLink] = []
            bank_unresolved: list[UnresolvedAudioLink] = []
            if not links:
                bank_unresolved.append(UnresolvedAudioLink(bank=bank.name or "", event="", media_id=None, note="No mapped event media found"))
                return BankLinkResult(bank_index=bank_index, bank_name=display_name, named_links=bank_named_links, unresolved=bank_unresolved)

            grouped_by_event: dict[tuple[str, str], list[tuple[str, str, int, MediaInfo, tuple[int, ...]]]] = defaultdict(list)
            for bank_name, event_name, media_id, info, object_types in links:
                grouped_by_event[(bank_name, event_name)].append((bank_name, event_name, media_id, info, object_types))

            for (bank_name, event_name), group in grouped_by_event.items():
                _raise_if_cancelled(cancel_event)
                bank_folder = get_safe_segment(bank_name, max_length=64)
                event_folder = get_safe_segment(event_name, max_length=96)
                grouped_by_archive: dict[str, list[tuple[str, str, int, MediaInfo, tuple[int, ...]]]] = defaultdict(list)
                for item in group:
                    grouped_by_archive[item[3].archive].append(item)
                for archive_name, archive_group in grouped_by_archive.items():
                    _raise_if_cancelled(cancel_event)
                    event_dir = tree_root / archive_name / bank_folder / event_folder
                    event_dir_created = False
                    sorted_media = sorted(archive_group, key=lambda item: item[2])
                    for media_index, (_, _, media_id, info, object_types) in enumerate(sorted_media):
                        _raise_if_cancelled(cancel_event)
                        leaf = f"media_{media_id}.wav" if len(sorted_media) == 1 else f"{media_index + 1:03d}__media_{media_id}.wav"
                        target_path = event_dir / leaf
                        if not info.source.exists():
                            lock = source_lock_for(info.source)
                            with lock:
                                _raise_if_cancelled(cancel_event)
                                if not info.non_audio and not info.source.exists():
                                    restored = ensure_decoded_flat_source(
                                        info,
                                        archive_files,
                                        vgmstream_cli_path,
                                        temp_decode_root,
                                        cancel_event=cancel_event,
                                    )
                                    if restored:
                                        info.exists = True
                        if info.non_audio:
                            continue
                        if not info.source.exists():
                            bank_unresolved.append(
                                UnresolvedAudioLink(
                                    bank=bank_name,
                                    event=event_name,
                                    media_id=media_id,
                                    note=f"Missing flat source file: {info.source}",
                                )
                            )
                            continue
                        if not event_dir_created:
                            ensure_directory(event_dir)
                            event_dir_created = True
                        create_hard_link_safe(target_path, info.source)
                        resolution = infer_audio_type(
                            object_types=object_types,
                            archive_name=archive_name,
                            bank_name=bank_name,
                            event_name=event_name,
                        )
                        bank_named_links.append(
                            NamedAudioLink(
                                archive=archive_name,
                                bank=bank_name,
                                event=event_name,
                                media_id=media_id,
                                source=info.source,
                                link=target_path,
                                resolved_object_types=normalize_object_types(object_types),
                                audio_type=resolution.audio_type,
                                audio_type_confidence=resolution.confidence,
                                audio_type_note=resolution.note,
                            )
                        )
            return BankLinkResult(bank_index=bank_index, bank_name=display_name, named_links=bank_named_links, unresolved=bank_unresolved)
        finally:
            with active_bank_guard:
                active_banks.discard(display_name)

    if linkable_banks:
        max_workers = max(1, min(8, len(linkable_banks), os.cpu_count() or 1))
        link_results: list[BankLinkResult] = []
        linking_completed = 0
        linking_delay_cancelled = threading.Event()
        linking_delay_timer: threading.Timer | None = None

        def emit_linking_delay_notice() -> None:
            if progress is not None and not linking_delay_cancelled.is_set():
                with active_bank_guard:
                    active_snapshot = sorted(active_banks)
                active_label = ", ".join(active_snapshot[:3]) if active_snapshot else "final bank"
                progress(
                    _linking_delay_message(f"Linking named media in parallel... active: {active_label}"),
                    linking_completed,
                    len(linkable_banks),
                )

        if progress is not None:
            progress("Linking named media in parallel...", 0, len(linkable_banks))
        if len(linkable_banks) > 0:
            linking_delay_timer = threading.Timer(LINKING_BANK_DELAY_SECONDS, emit_linking_delay_notice)
            linking_delay_timer.daemon = True
            linking_delay_timer.start()

        try:
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dyingaudio_bank_link") as executor:
                futures = {executor.submit(build_bank_links, bank_index, bank): (bank_index, bank) for bank_index, bank in linkable_banks}
                try:
                    pending = set(futures)
                    linking_wait_started = time.monotonic()
                    while pending:
                        _raise_if_cancelled(cancel_event)
                        done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                        if not done:
                            if progress is not None:
                                with active_bank_guard:
                                    active_snapshot = sorted(active_banks)
                                active_label = ", ".join(active_snapshot[:3]) if active_snapshot else "final bank"
                                base_message = f"Linking named media... {linking_completed}/{len(linkable_banks)} complete; active: {active_label}"
                                if time.monotonic() - linking_wait_started >= LINKING_BANK_DELAY_SECONDS:
                                    base_message = _linking_delay_message(base_message)
                                progress(base_message, linking_completed, len(linkable_banks))
                            continue
                        for future in done:
                            result = future.result()
                            _raise_if_cancelled(cancel_event)
                            link_results.append(result)
                            linking_completed += 1
                            linking_wait_started = time.monotonic()
                            if progress is not None:
                                progress(
                                    f"Linked bank {result.bank_name} ({linking_completed}/{len(linkable_banks)})",
                                    linking_completed,
                                    len(linkable_banks),
                                )
                except TaskCancelled:
                    for future in futures:
                        future.cancel()
                    raise
        finally:
            linking_delay_cancelled.set()
            if linking_delay_timer is not None:
                linking_delay_timer.cancel()

        for result in sorted(link_results, key=lambda item: item.bank_index):
            named_links.extend(result.named_links)
            unresolved.extend(result.unresolved)

    _raise_if_cancelled(cancel_event)
    named_links.sort(key=lambda row: (row.archive.lower(), row.bank.lower(), row.event.lower(), row.media_id))
    extracted_banks.sort(key=lambda row: (row.bank.lower(), row.bank_id, row.offset))

    manifest_path = logs_root / "named_tree_manifest.csv"
    unresolved_path = logs_root / "named_tree_unresolved.csv"
    summary_path = logs_root / "named_tree_summary.txt"
    banks_manifest_path = logs_root / "extracted_banks_manifest.csv"

    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "archive",
                "bank",
                "event",
                "media_id",
                "source",
                "link",
                "object_types",
                "audio_type",
                "audio_type_confidence",
                "audio_type_note",
            ),
        )
        writer.writeheader()
        for row in named_links:
            writer.writerow(
                {
                    "archive": row.archive,
                    "bank": row.bank,
                    "event": row.event,
                    "media_id": row.media_id,
                    "source": row.source,
                    "link": row.link,
                    "object_types": "|".join(str(value) for value in row.resolved_object_types),
                    "audio_type": row.audio_type,
                    "audio_type_confidence": row.audio_type_confidence,
                    "audio_type_note": row.audio_type_note,
                }
            )

    with unresolved_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("bank", "event", "media_id", "note"))
        writer.writeheader()
        for row in unresolved:
            writer.writerow(
                {
                    "bank": row.bank,
                    "event": row.event,
                    "media_id": "" if row.media_id is None else row.media_id,
                    "note": row.note,
                }
            )

    with banks_manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("bank", "bank_id", "offset", "length", "path"))
        writer.writeheader()
        for row in extracted_banks:
            writer.writerow(
                {
                    "bank": row.bank,
                    "bank_id": row.bank_id,
                    "offset": row.offset,
                    "length": row.length,
                    "path": row.path,
                }
            )

    archive_counts = {archive_name: sum(1 for row in named_links if row.archive == archive_name) for archive_name in ("meta", "sfx", "streams")}
    summary_lines = [
        f"Tree root: {tree_root}",
        f"Banks root: {banks_root}",
        f"Banks parsed: {len(banks)}",
        f"Banks extracted: {len(extracted_banks)}",
        f"Global media entries: {len(global_media)}",
        f"Global objects: {len(global_objects)}",
        f"Duplicate object IDs seen: {len(duplicate_object_ids)}",
        f"Links created: {len(named_links)}",
        f"Unresolved notes: {len(unresolved)}",
        "",
        *[f"{archive_name}: {archive_counts[archive_name]} links" for archive_name in ("meta", "sfx", "streams")],
    ]
    summary_text = "\n".join(summary_lines)
    summary_path.write_text(summary_text, encoding="utf-8")
    log(f"Named tree build finished in {int(time.monotonic() - started)}s with {len(named_links)} link(s).")
    if progress is not None:
        progress(f"Finished named tree build with {len(named_links)} link(s).", total_banks, total_banks)

    return WorkspaceBuildResult(
        named_links=named_links,
        extracted_banks=extracted_banks,
        unresolved=unresolved,
        summary_text=summary_text,
    )
