from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from dyingaudio.audio_info import probe_audio_metadata
from dyingaudio.core.csb import WORKSHOP_MAGIC, pack_csb
from dyingaudio.core.dldt import DldtToolchain, compile_audio_to_fsb
from dyingaudio.core.media_tools import discover_media_tools, run_hidden
from dyingaudio.core.scriptgen import generate_audiodata_scr
from dyingaudio.models import AudioEntry


@dataclass(slots=True)
class BuildArtifacts:
    mod_root: Path
    csb_path: Path
    modinfo_path: Path
    script_path: Path | None
    built_entries: list[AudioEntry]


@dataclass(slots=True)
class CsbBuildArtifacts:
    csb_path: Path
    built_entries: list[AudioEntry]


def _write_modinfo(path: Path) -> None:
    path.write_text("enabled=true\npriority=true\n", encoding="utf-8")


def _safe_temp_name(entry: AudioEntry, source_path: Path) -> str:
    safe_stem = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in entry.entry_name)
    safe_stem = safe_stem.strip("._") or source_path.stem or "audio"
    unique_suffix = hashlib.sha1(str(source_path.resolve()).encode("utf-8")).hexdigest()[:10]
    return f"{safe_stem}_{unique_suffix}"


def _compile_entries(
    entries: list[AudioEntry],
    toolchain: DldtToolchain,
    work_root: Path,
    log: Callable[[str], None],
    progress: Callable[[str, float | None, float | None], None] | None = None,
    progress_offset: int = 0,
    progress_total: int | None = None,
) -> list[AudioEntry]:
    cache_dir = work_root / "cache"
    fsb_dir = work_root / "fsb"
    raw_dir = work_root / "raw"
    cache_dir.mkdir(parents=True, exist_ok=True)
    fsb_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    media_tools = discover_media_tools()

    compiled_entries: list[AudioEntry] = []
    total = progress_total or max(len(entries), 1)
    for index, entry in enumerate(entries):
        if progress is not None:
            progress(
                f"Compiling {entry.entry_name} ({progress_offset + index + 1}/{total})",
                progress_offset + index,
                total,
            )
        source_path = entry.resolved_source_path()
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(f"Missing audio source for '{entry.entry_name}'.")

        compile_source_path = source_path
        if source_path.suffix.lower() not in {".wav", ".ogg"}:
            if media_tools.ffmpeg_path is None:
                raise RuntimeError(
                    f"FFmpeg is required to convert '{source_path.suffix or 'unknown'}' files for '{entry.entry_name}'."
                )
            compile_source_path = raw_dir / f"{_safe_temp_name(entry, source_path)}.wav"
            command = [
                str(media_tools.ffmpeg_path),
                "-y",
                "-loglevel",
                "error",
                "-i",
                str(source_path),
                "-acodec",
                "pcm_s16le",
                str(compile_source_path),
            ]
            log(" ".join(command))
            result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
            if result.stdout.strip():
                log(result.stdout.strip())
            if result.stderr.strip():
                log(result.stderr.strip())
            if result.returncode != 0 or not compile_source_path.exists():
                raise RuntimeError(f"Could not convert '{source_path.name}' for '{entry.entry_name}'.")

        metadata = probe_audio_metadata(source_path)
        output_path = fsb_dir / f"{entry.entry_name}.fsb"
        result = compile_audio_to_fsb(toolchain, compile_source_path, output_path, cache_dir)
        log(" ".join(result.command))
        if result.stdout:
            log(result.stdout)
        if result.stderr:
            log(result.stderr)
        if not result.success:
            raise RuntimeError(f"FSB compile failed for '{entry.entry_name}'.")

        compiled_entries.append(
            AudioEntry(
                entry_name=entry.entry_name,
                source_path=str(source_path),
                source_mode="fsb",
                fsb_path=str(output_path),
                entry_type=entry.entry_type,
                sample_count=entry.sample_count or metadata.sample_count_48k,
                duration_ms=entry.duration_ms or metadata.duration_ms,
                reserved=entry.reserved,
                notes=metadata.notes or "Compiled by DLDT.",
            )
        )

    if progress is not None and entries:
        progress(
            f"Compiled {len(entries)} audio entr{'y' if len(entries) == 1 else 'ies'}.",
            progress_offset + len(entries),
            total,
        )
    return compiled_entries


def _prepare_existing_fsb_entry(entry: AudioEntry) -> AudioEntry:
    fsb_path = entry.resolved_fsb_path()
    if fsb_path is None or not fsb_path.exists():
        raise FileNotFoundError(f"Missing FSB input for '{entry.entry_name}'.")
    if fsb_path.suffix.lower() != ".fsb":
        raise ValueError(
            f"Entry '{entry.entry_name}' is not an FSB file. Switch to 'Raw Audio via DLDT' or replace it with a .fsb input."
        )

    return AudioEntry(
        entry_name=entry.entry_name,
        source_path=str(fsb_path),
        source_mode="fsb",
        fsb_path=str(fsb_path),
        entry_type=entry.entry_type,
        sample_count=entry.sample_count,
        duration_ms=entry.duration_ms,
        reserved=entry.reserved,
        notes=entry.notes or "Existing FSB file.",
    )


def _prepare_entries(
    entries: list[AudioEntry],
    builder_mode: str,
    toolchain: DldtToolchain | None,
    work_root: Path,
    log: Callable[[str], None],
    progress: Callable[[str, float | None, float | None], None] | None = None,
) -> list[AudioEntry]:
    prepared_entries: list[AudioEntry] = []
    total = max(len(entries) + 1, 1)
    for index, entry in enumerate(entries):
        if entry.source_mode == "fsb":
            if progress is not None:
                progress(f"Preparing {entry.entry_name} ({index + 1}/{len(entries)})", index, total)
            prepared_entries.append(_prepare_existing_fsb_entry(entry))
            continue

        if builder_mode != "Raw Audio via DLDT":
            raise ValueError(
                f"Entry '{entry.entry_name}' is raw audio. Use 'Raw Audio via DLDT' to compile it, or add an existing .fsb instead."
            )
        if toolchain is None:
            raise ValueError("A valid DLDT toolchain is required for raw audio builds.")

        prepared_entries.extend(
            _compile_entries(
                [entry],
                toolchain,
                work_root,
                log,
                progress=progress,
                progress_offset=index,
                progress_total=total,
            )
        )

    if progress is not None:
        progress("Packing CSB payload...", total - 1, total)
    return prepared_entries


def build_mod(
    *,
    entries: list[AudioEntry],
    mods_root: str | Path,
    mod_name: str,
    bundle_name: str,
    generate_script: bool,
    proc_names_text: str,
    builder_mode: str,
    toolchain: DldtToolchain | None,
    log: Callable[[str], None],
    magic: int | None = WORKSHOP_MAGIC,
    progress: Callable[[str, float | None, float | None], None] | None = None,
) -> BuildArtifacts:
    resolved_mods_root = Path(mods_root).expanduser().resolve()
    mod_name = mod_name.strip()
    bundle_name = Path(bundle_name.strip()).stem

    if not mod_name:
        raise ValueError("Mod name is required.")
    if not bundle_name:
        raise ValueError("Bundle name is required.")

    mod_root = resolved_mods_root / mod_name
    data_root = mod_root / "data"
    script_root = data_root / "scripts" / "audio"
    data_root.mkdir(parents=True, exist_ok=True)

    csb_result = build_csb_file(
        entries=entries,
        output_path=data_root / f"{bundle_name}.csb",
        builder_mode=builder_mode,
        toolchain=toolchain,
        log=log,
        magic=magic,
        progress=progress,
    )

    modinfo_path = mod_root / "modinfo.ini"
    _write_modinfo(modinfo_path)

    script_path: Path | None = None
    if generate_script:
        script_root.mkdir(parents=True, exist_ok=True)
        script_path = script_root / "audiodata.scr"
        script_path.write_text(generate_audiodata_scr(bundle_name, proc_names_text), encoding="utf-8")

    return BuildArtifacts(
        mod_root=mod_root,
        csb_path=csb_result.csb_path,
        modinfo_path=modinfo_path,
        script_path=script_path,
        built_entries=csb_result.built_entries,
    )


def build_csb_file(
    *,
    entries: list[AudioEntry],
    output_path: str | Path,
    builder_mode: str,
    toolchain: DldtToolchain | None,
    log: Callable[[str], None],
    magic: int | None = WORKSHOP_MAGIC,
    progress: Callable[[str, float | None, float | None], None] | None = None,
) -> CsbBuildArtifacts:
    destination = Path(output_path).expanduser().resolve()
    if destination.suffix.lower() != ".csb":
        destination = destination.with_suffix(".csb")

    with tempfile.TemporaryDirectory(prefix="dyingaudio_build_") as temp_dir:
        temp_root = Path(temp_dir)
        prepared_entries = _prepare_entries(entries, builder_mode, toolchain, temp_root, log, progress=progress)
        pack_csb(prepared_entries, destination, magic=magic, progress=progress)

    return CsbBuildArtifacts(csb_path=destination, built_entries=prepared_entries)
