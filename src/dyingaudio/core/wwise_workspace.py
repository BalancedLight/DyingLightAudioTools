from __future__ import annotations

import csv
import hashlib
import json
import mmap
import shutil
import threading
import wave
import xml.etree.ElementTree as ET
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from dyingaudio.audio_info import probe_audio_metadata
from dyingaudio.background import TaskCancelled
from dyingaudio.core.media_tools import discover_media_tools


DL2_GAME = "DL2"
DLTB_GAME = "DLTB"
BASE_ARCHIVE_SET = "base"
MAPPING_START_TOKEN = b"<Mapping"
MAPPING_END_TOKEN = b"</Mapping>"
WORK_SUBPATHS = {
    DL2_GAME: Path("ph") / "work",
    DLTB_GAME: Path("ph_ft") / "work",
}
GAME_LABELS = {
    DL2_GAME: "Dying Light 2",
    DLTB_GAME: "Dying Light The Beast",
}


@dataclass(slots=True)
class ArchiveSetDescriptor:
    key: str
    label: str
    audio_root: Path


@dataclass(slots=True)
class ArchiveBundle:
    audio_root: Path
    archive_set: str
    meta_path: Path
    sfx_path: Path
    streams_path: Path

    def files(self) -> tuple[Path, Path, Path]:
        return self.meta_path, self.sfx_path, self.streams_path


@dataclass(slots=True)
class NamedAudioLink:
    archive: str
    bank: str
    event: str
    media_id: int
    source: Path
    link: Path


@dataclass(slots=True)
class ExtractedBank:
    bank: str
    bank_id: int
    offset: int
    length: int
    path: Path


@dataclass(slots=True)
class UnresolvedAudioLink:
    bank: str
    event: str
    media_id: int | None
    note: str


@dataclass(slots=True)
class WwiseWorkspace:
    game: str
    archive_set: str
    fingerprint: str
    archive_bundle: ArchiveBundle
    root: Path
    tree_root: Path
    banks_root: Path
    logs_root: Path
    mapping_xml_path: Path
    summary_path: Path
    named_links: list[NamedAudioLink]
    extracted_banks: list[ExtractedBank]
    unresolved: list[UnresolvedAudioLink]
    summary_text: str
    metadata_path: Path


def game_label(game: str) -> str:
    return GAME_LABELS.get(game, game)


def game_work_root(game: str, install_root: str | Path) -> Path:
    work_subpath = WORK_SUBPATHS.get(game)
    if work_subpath is None:
        raise ValueError(f"Unsupported game '{game}'.")
    return Path(install_root).resolve() / work_subpath


def detect_archive_sets(game: str, install_root: str | Path) -> list[ArchiveSetDescriptor]:
    work_root = game_work_root(game, install_root)
    descriptors: list[ArchiveSetDescriptor] = []

    base_audio_root = work_root / "data" / "audio"
    if base_audio_root.exists():
        descriptors.append(ArchiveSetDescriptor(key=BASE_ARCHIVE_SET, label="Base Audio", audio_root=base_audio_root))

    data_lang_root = work_root / "data_lang"
    if data_lang_root.exists():
        for candidate in sorted(data_lang_root.iterdir(), key=lambda path: path.name.lower()):
            audio_root = candidate / "data" / "audio"
            if not candidate.is_dir() or not audio_root.exists():
                continue
            descriptors.append(
                ArchiveSetDescriptor(
                    key=candidate.name,
                    label=f"Language: {candidate.name}",
                    audio_root=audio_root,
                )
            )

    return descriptors


def _find_archive_file(audio_root: Path, prefix: str) -> Path:
    matches = sorted(audio_root.glob(f"{prefix}*.aesp"), key=lambda path: (len(path.name), path.name.lower()))
    if not matches:
        raise FileNotFoundError(f"Could not find '{prefix}*.aesp' in '{audio_root}'.")
    return matches[0]


def resolve_archive_bundle(game: str, install_root: str | Path, archive_set: str) -> ArchiveBundle:
    descriptors = detect_archive_sets(game, install_root)
    descriptor = next((item for item in descriptors if item.key == archive_set), None)
    if descriptor is None:
        raise FileNotFoundError(f"Archive set '{archive_set}' is not available for {game_label(game)}.")
    return ArchiveBundle(
        audio_root=descriptor.audio_root,
        archive_set=archive_set,
        meta_path=_find_archive_file(descriptor.audio_root, "meta"),
        sfx_path=_find_archive_file(descriptor.audio_root, "sfx"),
        streams_path=_find_archive_file(descriptor.audio_root, "streams"),
    )


def archive_fingerprint(bundle: ArchiveBundle) -> str:
    payload_parts: list[str] = [bundle.archive_set, str(bundle.audio_root.resolve())]
    for path in bundle.files():
        stat = path.stat()
        payload_parts.extend([str(path.resolve()), str(stat.st_size), str(stat.st_mtime_ns)])
    return hashlib.sha1("|".join(payload_parts).encode("utf-8")).hexdigest()[:16]


def extract_mapping_xml_text(meta_path: str | Path) -> str:
    resolved = Path(meta_path).resolve()
    with resolved.open("rb") as handle:
        with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            start = mapped.rfind(MAPPING_START_TOKEN)
            end = mapped.rfind(MAPPING_END_TOKEN)
            if start < 0 or end < 0 or end <= start:
                raise ValueError(f"Could not locate embedded <Mapping> XML in '{resolved}'.")
            xml_bytes = mapped[start:end + len(MAPPING_END_TOKEN)]

    try:
        xml_text = xml_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Embedded mapping XML in '{resolved}' is not valid UTF-8.") from exc

    try:
        ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError(f"Embedded mapping XML in '{resolved}' is not valid XML.") from exc
    return xml_text


def write_mapping_xml(meta_path: str | Path, destination: str | Path) -> Path:
    destination_path = Path(destination).resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(extract_mapping_xml_text(meta_path), encoding="utf-8")
    return destination_path


def workspace_root_for(cache_root: str | Path, game: str, archive_set: str, fingerprint: str) -> Path:
    return Path(cache_root).resolve() / game.lower() / archive_set / fingerprint


def _workspace_metadata_payload(game: str, archive_set: str, bundle: ArchiveBundle, fingerprint: str) -> dict[str, object]:
    return {
        "game": game,
        "archive_set": archive_set,
        "fingerprint": fingerprint,
        "audio_root": str(bundle.audio_root),
        "meta_path": str(bundle.meta_path),
        "sfx_path": str(bundle.sfx_path),
        "streams_path": str(bundle.streams_path),
    }


def _workspace_paths(root: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    tree_root = root / "tree"
    banks_root = root / "banks"
    logs_root = root / "logs"
    mapping_xml_path = logs_root / "meta_mapping.xml"
    summary_path = logs_root / "named_tree_summary.txt"
    metadata_path = root / "workspace.json"
    return tree_root, banks_root, logs_root, mapping_xml_path, summary_path, metadata_path


def _parse_int(value: str | None) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise TaskCancelled()


@lru_cache(maxsize=8192)
def media_signature_for_path(path_text: str) -> tuple[int, int]:
    path = Path(path_text)
    if path.suffix.lower() == ".wav":
        try:
            with wave.open(str(path), "rb") as handle:
                sample_count = handle.getnframes()
                sample_rate = handle.getframerate()
            if sample_rate > 0:
                duration_ms = int(round((sample_count / float(sample_rate)) * 1000.0))
            else:
                duration_ms = 0
            return duration_ms, sample_count
        except Exception:
            pass

    try:
        metadata = probe_audio_metadata(path)
    except Exception:
        return 0, 0
    return metadata.duration_ms, metadata.sample_count_48k


def warm_media_signature_cache(
    rows: list[NamedAudioLink],
    progress: Callable[[str, float | None, float | None], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> int:
    unique_paths: list[str] = []
    seen_paths: set[str] = set()
    for row in rows:
        source = row.source if row.source.exists() else row.link
        source_text = str(source.resolve())
        if source_text in seen_paths:
            continue
        seen_paths.add(source_text)
        unique_paths.append(source_text)

    total = max(len(unique_paths), 1)
    for index, source_text in enumerate(unique_paths):
        _raise_if_cancelled(cancel_event)
        media_signature_for_path(source_text)
        if progress is not None:
            progress("Indexing media metadata...", index + 1, total)
    return len(unique_paths)


def _load_named_links(path: Path) -> list[NamedAudioLink]:
    if not path.exists():
        return []
    entries: list[NamedAudioLink] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            entries.append(
                NamedAudioLink(
                    archive=row.get("archive", ""),
                    bank=row.get("bank", ""),
                    event=row.get("event", ""),
                    media_id=_parse_int(row.get("media_id")),
                    source=Path(row.get("source", "")).resolve(),
                    link=Path(row.get("link", "")).resolve(),
                )
            )
    return entries


def _load_extracted_banks(path: Path) -> list[ExtractedBank]:
    if not path.exists():
        return []
    entries: list[ExtractedBank] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            entries.append(
                ExtractedBank(
                    bank=row.get("bank", ""),
                    bank_id=_parse_int(row.get("bank_id")),
                    offset=_parse_int(row.get("offset")),
                    length=_parse_int(row.get("length")),
                    path=Path(row.get("path", "")).resolve(),
                )
            )
    return entries


def _load_unresolved(path: Path) -> list[UnresolvedAudioLink]:
    if not path.exists():
        return []
    entries: list[UnresolvedAudioLink] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            media_id_text = (row.get("media_id") or "").strip()
            entries.append(
                UnresolvedAudioLink(
                    bank=row.get("bank", ""),
                    event=row.get("event", ""),
                    media_id=_parse_int(media_id_text) if media_id_text else None,
                    note=row.get("note", ""),
                )
            )
    return entries


def load_workspace(root: str | Path) -> WwiseWorkspace:
    workspace_root = Path(root).resolve()
    tree_root, banks_root, logs_root, mapping_xml_path, summary_path, metadata_path = _workspace_paths(workspace_root)
    if not metadata_path.exists():
        raise FileNotFoundError(f"Workspace metadata is missing: {metadata_path}")

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    bundle = ArchiveBundle(
        audio_root=Path(metadata["audio_root"]).resolve(),
        archive_set=str(metadata["archive_set"]),
        meta_path=Path(metadata["meta_path"]).resolve(),
        sfx_path=Path(metadata["sfx_path"]).resolve(),
        streams_path=Path(metadata["streams_path"]).resolve(),
    )
    named_links = _load_named_links(logs_root / "named_tree_manifest.csv")
    extracted_banks = _load_extracted_banks(logs_root / "extracted_banks_manifest.csv")
    unresolved = _load_unresolved(logs_root / "named_tree_unresolved.csv")
    summary_text = summary_path.read_text(encoding="utf-8") if summary_path.exists() else ""
    return WwiseWorkspace(
        game=str(metadata["game"]),
        archive_set=str(metadata["archive_set"]),
        fingerprint=str(metadata["fingerprint"]),
        archive_bundle=bundle,
        root=workspace_root,
        tree_root=tree_root,
        banks_root=banks_root,
        logs_root=logs_root,
        mapping_xml_path=mapping_xml_path,
        summary_path=summary_path,
        named_links=named_links,
        extracted_banks=extracted_banks,
        unresolved=unresolved,
        summary_text=summary_text,
        metadata_path=metadata_path,
    )


def build_or_load_workspace(
    *,
    game: str,
    install_root: str | Path,
    archive_set: str,
    cache_root: str | Path,
    log: Callable[[str], None],
    force_rebuild: bool = False,
    builder: Callable[..., object] | None = None,
    progress: Callable[[str, float | None, float | None], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> WwiseWorkspace:
    if progress is not None:
        progress("Resolving Wwise archives...", 0, 5)
    _raise_if_cancelled(cancel_event)
    bundle = resolve_archive_bundle(game, install_root, archive_set)
    fingerprint = archive_fingerprint(bundle)
    workspace_root = workspace_root_for(cache_root, game, archive_set, fingerprint)
    tree_root, banks_root, logs_root, mapping_xml_path, _summary_path, metadata_path = _workspace_paths(workspace_root)
    manifest_path = logs_root / "named_tree_manifest.csv"
    banks_manifest_path = logs_root / "extracted_banks_manifest.csv"
    unresolved_path = logs_root / "named_tree_unresolved.csv"

    if (
        not force_rebuild
        and metadata_path.exists()
        and manifest_path.exists()
        and banks_manifest_path.exists()
        and unresolved_path.exists()
    ):
        if progress is not None:
            progress("Loading cached workspace...", 4, 5)
        workspace = load_workspace(workspace_root)
        warm_media_signature_cache(workspace.named_links, progress=progress, cancel_event=cancel_event)
        return workspace

    workspace_root.mkdir(parents=True, exist_ok=True)
    tree_root.mkdir(parents=True, exist_ok=True)
    banks_root.mkdir(parents=True, exist_ok=True)
    logs_root.mkdir(parents=True, exist_ok=True)
    try:
        if progress is not None:
            progress("Extracting embedded mapping XML...", 1, 5)
        write_mapping_xml(bundle.meta_path, mapping_xml_path)
        _raise_if_cancelled(cancel_event)

        tools = discover_media_tools()
        if tools.vgmstream_path is None:
            raise RuntimeError("vgmstream is required to build the experimental Wwise workspace.")
        if progress is not None:
            progress("Generating named audio tree in Python...", 2, 5)
        if builder is None:
            from dyingaudio.core.wwise_named_tree import build_named_audio_tree

            build_named_audio_tree(
                audio_root=workspace_root,
                meta_file=bundle.meta_path,
                sfx_file=bundle.sfx_path,
                streams_file=bundle.streams_path,
                xml_file=mapping_xml_path,
                tree_root=tree_root,
                banks_root=banks_root,
                vgmstream_cli_path=tools.vgmstream_path,
                log=log,
                progress=progress,
                cancel_event=cancel_event,
            )
        else:
            builder(
                audio_root=workspace_root,
                meta_file=bundle.meta_path,
                sfx_file=bundle.sfx_path,
                streams_file=bundle.streams_path,
                xml_file=mapping_xml_path,
                tree_root=tree_root,
                banks_root=banks_root,
                vgmstream_cli_path=tools.vgmstream_path,
                log=log,
                progress=progress,
            )
    except TaskCancelled:
        shutil.rmtree(workspace_root, ignore_errors=True)
        raise
    _raise_if_cancelled(cancel_event)
    if progress is not None:
        progress("Writing workspace metadata...", 4, 5)
    metadata_path.write_text(
        json.dumps(_workspace_metadata_payload(game, archive_set, bundle, fingerprint), indent=2),
        encoding="utf-8",
    )
    if progress is not None:
        progress("Loading generated workspace...", 5, 5)
    workspace = load_workspace(workspace_root)
    warm_media_signature_cache(workspace.named_links, progress=progress, cancel_event=cancel_event)
    return workspace


def _iter_files(root: Path) -> list[Path]:
    return [path for path in root.rglob("*") if path.is_file()]


def export_workspace_dump(
    workspace: WwiseWorkspace,
    destination_root: str | Path,
    progress: Callable[[str, float | None, float | None], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> Path:
    destination_root_path = Path(destination_root).resolve()
    destination_root_path.mkdir(parents=True, exist_ok=True)
    destination = destination_root_path / f"{workspace.game.lower()}_{workspace.archive_set}_{workspace.fingerprint}"
    if destination.exists():
        shutil.rmtree(destination)
    files = _iter_files(workspace.root)
    total = max(len(files), 1)
    for index, source in enumerate(files):
        _raise_if_cancelled(cancel_event)
        relative = source.relative_to(workspace.root)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if progress is not None:
            progress(f"Exporting workspace dump: {relative}", index + 1, total)
    return destination


def export_media_files(
    rows: list[NamedAudioLink],
    destination_root: str | Path,
    progress: Callable[[str, float | None, float | None], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> list[Path]:
    destination_root_path = Path(destination_root).resolve()
    destination_root_path.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    total = max(len(rows), 1)
    for index, row in enumerate(rows):
        _raise_if_cancelled(cancel_event)
        destination = destination_root_path / row.link.name
        shutil.copy2(row.link, destination)
        exported.append(destination)
        if progress is not None:
            progress(f"Exporting {row.link.name}", index + 1, total)
    return exported


def event_directory(workspace: WwiseWorkspace, archive: str, bank: str, event: str) -> Path:
    return workspace.tree_root / archive / bank / event


def export_event_folder(
    workspace: WwiseWorkspace,
    archive: str,
    bank: str,
    event: str,
    destination_root: str | Path,
    progress: Callable[[str, float | None, float | None], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> Path:
    source = event_directory(workspace, archive, bank, event)
    if not source.exists():
        raise FileNotFoundError(f"Event folder does not exist: {source}")
    destination_root_path = Path(destination_root).resolve()
    destination_root_path.mkdir(parents=True, exist_ok=True)
    destination = destination_root_path / archive / bank / event
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    files = _iter_files(source)
    total = max(len(files), 1)
    for index, source_file in enumerate(files):
        _raise_if_cancelled(cancel_event)
        relative = source_file.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target)
        if progress is not None:
            progress(f"Exporting {relative}", index + 1, total)
    return destination


def export_bank_files(
    workspace: WwiseWorkspace,
    bank_name: str,
    destination_root: str | Path,
    progress: Callable[[str, float | None, float | None], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> list[Path]:
    destination_root_path = Path(destination_root).resolve()
    destination_root_path.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    matching_banks = [bank for bank in workspace.extracted_banks if bank.bank == bank_name]
    total = max(len(matching_banks), 1)
    for index, bank in enumerate(matching_banks):
        _raise_if_cancelled(cancel_event)
        if bank.bank != bank_name:
            continue
        destination = destination_root_path / bank.path.name
        shutil.copy2(bank.path, destination)
        exported.append(destination)
        if progress is not None:
            progress(f"Exporting {bank.path.name}", index + 1, total)
    return exported


def workspace_details_text(workspace: WwiseWorkspace) -> str:
    lines = [
        f"Game: {game_label(workspace.game)}",
        f"Archive set: {workspace.archive_set}",
        f"Fingerprint: {workspace.fingerprint}",
        f"Audio root: {workspace.archive_bundle.audio_root}",
        f"Workspace root: {workspace.root}",
        f"Named links: {len(workspace.named_links)}",
        f"Extracted banks: {len(workspace.extracted_banks)}",
        f"Unresolved items: {len(workspace.unresolved)}",
        "",
        workspace.summary_text.strip(),
    ]
    return "\n".join(line for line in lines if line is not None)
