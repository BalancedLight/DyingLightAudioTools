from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from html import escape
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from dyingaudio.settings import application_root


COMMON_AUDIO_FILETYPES = [
    ("Audio files", "*.ogg *.wav *.mp3 *.flac *.m4a *.aac *.wma *.opus *.mp4"),
    ("All files", "*.*"),
]

WINDOWS_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0


def _hide_console_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    if WINDOWS_NO_WINDOW:
        creationflags = int(kwargs.get("creationflags", 0))
        kwargs["creationflags"] = creationflags | WINDOWS_NO_WINDOW
    return kwargs


def run_hidden(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
    return subprocess.run(command, **_hide_console_kwargs(kwargs))


def popen_hidden(command: list[str], **kwargs: Any) -> subprocess.Popen[Any]:
    return subprocess.Popen(command, **_hide_console_kwargs(kwargs))


def find_tool(executable: str, fallback_paths: list[Path]) -> Path | None:
    for fallback in fallback_paths:
        if fallback.exists():
            return fallback

    found = shutil.which(executable)
    if found:
        return Path(found)

    return None


def portable_tools_root() -> Path:
    return application_root() / "tools"


def ensure_portable_tool_layout() -> dict[str, Path]:
    root = portable_tools_root()
    layout = {
        "root": root,
        "wwise": root / "wwise",
        "ffmpeg": root / "ffmpeg",
    }
    for path in layout.values():
        path.mkdir(parents=True, exist_ok=True)
    return layout


def _portable_tool_candidates(*relative_paths: str) -> list[Path]:
    root = portable_tools_root()
    return [root / relative_path for relative_path in relative_paths]


def _wwise_authoring_root(console_path: Path) -> Path | None:
    resolved = console_path.resolve()
    for ancestor in resolved.parents:
        if ancestor.name.lower() != "authoring":
            continue
        if (ancestor / "Data").exists():
            return ancestor
    return None


def _is_valid_wwise_console(console_path: Path) -> bool:
    return _wwise_authoring_root(console_path) is not None


@dataclass(slots=True)
class MediaTools:
    ffmpeg_path: Path | None
    ffplay_path: Path | None
    ffprobe_path: Path | None
    vgmstream_path: Path | None
    wwise_console_path: Path | None

    def summary(self) -> str:
        parts: list[str] = []
        parts.append("FFmpeg ready" if self.ffmpeg_path else "FFmpeg missing")
        parts.append("FFplay ready" if self.ffplay_path else "FFplay missing")
        parts.append("FFprobe ready" if self.ffprobe_path else "FFprobe missing")
        parts.append("vgmstream ready" if self.vgmstream_path else "vgmstream missing")
        parts.append("WwiseConsole ready" if self.wwise_console_path else "WwiseConsole missing")
        return "Preview tools: " + ", ".join(parts)


def _discover_wwise_console() -> Path | None:
    direct = find_tool(
        "WwiseConsole.exe",
        _portable_tool_candidates(
            r"wwise\Authoring\x64\Release\bin\WwiseConsole.exe",
            r"wwise\Authoring\Win32\Release\bin\WwiseConsole.exe",
        ),
    )
    if direct is not None and _is_valid_wwise_console(direct):
        return direct

    candidates: list[Path] = []
    for root in (Path(r"C:\Audiokinetic"), Path(r"C:\Program Files\Audiokinetic"), Path(r"C:\Program Files (x86)\Audiokinetic")):
        if not root.exists():
            continue
        candidates.extend(root.glob(r"Wwise*\Authoring\x64\Release\bin\WwiseConsole.exe"))
        candidates.extend(root.glob(r"Wwise*\Authoring\Win32\Release\bin\WwiseConsole.exe"))
    if not candidates:
        return None
    candidates = sorted({candidate.resolve() for candidate in candidates}, key=lambda path: str(path).lower(), reverse=True)
    for candidate in candidates:
        if _is_valid_wwise_console(candidate):
            return candidate
    return None


def discover_media_tools() -> MediaTools:
    local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
    ffmpeg_path = find_tool(
        "ffmpeg.exe",
        [
            *_portable_tool_candidates(r"ffmpeg\ffmpeg.exe", r"ffmpeg\bin\ffmpeg.exe"),
            Path(r"C:\ProgramData\chocolatey\bin\ffmpeg.exe"),
        ],
    )
    ffplay_path = find_tool(
        "ffplay.exe",
        [
            *_portable_tool_candidates(r"ffmpeg\ffplay.exe", r"ffmpeg\bin\ffplay.exe"),
            Path(r"C:\ProgramData\chocolatey\bin\ffplay.exe"),
        ],
    )
    ffprobe_path = find_tool(
        "ffprobe.exe",
        [
            *_portable_tool_candidates(r"ffmpeg\ffprobe.exe", r"ffmpeg\bin\ffprobe.exe"),
            Path(r"C:\ProgramData\chocolatey\bin\ffprobe.exe"),
        ],
    )
    vgmstream_path = find_tool(
        "vgmstream-cli.exe",
        [
            local_app_data
            / "Microsoft"
            / "WinGet"
            / "Packages"
            / "vgmstream.vgmstream_Microsoft.Winget.Source_8wekyb3d8bbwe"
            / "vgmstream-cli.exe",
        ],
    )
    wwise_console_path = _discover_wwise_console()
    return MediaTools(
        ffmpeg_path=ffmpeg_path,
        ffplay_path=ffplay_path,
        ffprobe_path=ffprobe_path,
        vgmstream_path=vgmstream_path,
        wwise_console_path=wwise_console_path,
    )


def _ensure_blank_wwise_project(project_path: Path, tools: MediaTools) -> Path:
    if tools.wwise_console_path is None:
        raise RuntimeError(
            "WwiseConsole was not found. Install Audiokinetic Wwise Authoring to convert non-WEM audio into .wem files."
        )
    if project_path.exists():
        return project_path

    project_dir = project_path.parent
    if project_dir.exists():
        try:
            shutil.rmtree(project_dir)
        except OSError as exc:
            raise RuntimeError(
                f"Could not clear the stale temporary Wwise project folder: {project_dir}. {exc}"
            ) from exc

    project_path.parent.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(tools.wwise_console_path),
        "create-new-project",
        str(project_path),
        "--platform",
        "Windows",
    ]
    result = run_hidden(
        command,
        cwd=str(tools.wwise_console_path.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0 or not project_path.exists():
        details = (result.stderr or result.stdout or "").strip()
        authoring_root = _wwise_authoring_root(tools.wwise_console_path)
        guidance = (
            f"WwiseConsole path: {tools.wwise_console_path}. "
            + (
                f"Detected Authoring root: {authoring_root}. "
                if authoring_root is not None
                else "Portable Wwise layout is invalid. Expected tools/wwise/Authoring/Data and tools/wwise/Authoring/x64/Release/bin/WwiseConsole.exe. "
            )
        )
        raise RuntimeError(
            f"Could not create the temporary Wwise project. {guidance}{f' Details: {details}' if details else ''}"
        )
    return project_path


def _convert_to_wav(source: Path, destination: Path, tools: MediaTools) -> Path:
    if tools.ffmpeg_path is None:
        raise RuntimeError(
            "FFmpeg is required to prepare non-WAV replacement audio before Wwise conversion."
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(tools.ffmpeg_path),
        "-y",
        "-loglevel",
        "error",
        "-i",
        str(source),
        "-vn",
        "-acodec",
        "pcm_s16le",
        str(destination),
    ]
    result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
    if result.returncode != 0 or not destination.exists():
        details = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"Could not convert '{source.name}' to WAV.{f' {details}' if details else ''}")
    return destination


def _write_wsources(path: Path, source_path: Path, destination_name: str) -> Path:
    xml_text = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<ExternalSourcesList SchemaVersion="1" Root="{root}">\n'
        '  <Source Path="{source}" Destination="{destination}" Conversion="Default Conversion" />\n'
        "</ExternalSourcesList>\n"
    ).format(
        root=escape(str(source_path.parent)),
        source=escape(source_path.name),
        destination=escape(destination_name),
    )
    path.write_text(xml_text, encoding="utf-8")
    return path


def convert_audio_to_wem(
    source: str | Path,
    work_root: str | Path,
    *,
    log: Callable[[str], None] | None = None,
    tools: MediaTools | None = None,
) -> Path:
    resolved_source = Path(source).expanduser().resolve()
    if not resolved_source.exists():
        raise FileNotFoundError(f"Missing audio source: {resolved_source}")
    if resolved_source.suffix.lower() == ".wem":
        return resolved_source

    tools = tools or discover_media_tools()
    conversion_root = Path(work_root).expanduser().resolve()
    conversion_root.mkdir(parents=True, exist_ok=True)
    helper_project = _ensure_blank_wwise_project(conversion_root / "WwiseConvert" / "WwiseConvert.wproj", tools)
    logger = log or (lambda _message: None)

    with tempfile.TemporaryDirectory(prefix="dyingaudio_wem_", dir=str(conversion_root)) as temp_dir:
        temp_root = Path(temp_dir)
        working_source = resolved_source
        if resolved_source.suffix.lower() != ".wav":
            working_source = _convert_to_wav(resolved_source, temp_root / "input.wav", tools)
            logger(f"Prepared WAV source from {resolved_source.name}.")

        output_dir = temp_root / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        destination_name = "replacement.wem"
        wsources_path = _write_wsources(temp_root / "external_sources.wsources", working_source, destination_name)
        command = [
            str(tools.wwise_console_path),
            "convert-external-source",
            str(helper_project),
            "--platform",
            "Windows",
            "--source-file",
            str(wsources_path),
            "--output",
            str(output_dir),
        ]
        logger(" ".join(command))
        result = run_hidden(
            command,
            cwd=str(tools.wwise_console_path.parent),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if result.stdout.strip():
            logger(result.stdout.strip())
        if result.stderr.strip():
            logger(result.stderr.strip())

        generated = output_dir / destination_name
        if not generated.exists():
            nested_matches = sorted(output_dir.rglob(destination_name))
            if nested_matches:
                generated = nested_matches[0]
        if result.returncode not in {0, 1, 2} and not generated.exists():
            details = (result.stderr or result.stdout or "").strip()
            authoring_root = _wwise_authoring_root(tools.wwise_console_path)
            raise RuntimeError(
                "Could not convert the selected file to WEM with WwiseConsole. "
                f"WwiseConsole path: {tools.wwise_console_path}. "
                + (
                    f"Detected Authoring root: {authoring_root}. "
                    if authoring_root is not None
                    else "Portable Wwise layout is invalid. Expected tools/wwise/Authoring/Data and tools/wwise/Authoring/x64/Release/bin/WwiseConsole.exe. "
                )
                + (f" Details: {details}" if details else "")
            )

        suffix = next(tempfile._get_candidate_names())
        final_destination = conversion_root / f"{resolved_source.stem}_{suffix}.wem"
        shutil.copy2(generated, final_destination)
        logger(f"Converted {resolved_source.name} to {final_destination.name}.")
        return final_destination


def missing_wem_conversion_requirements(source: str | Path, tools: MediaTools | None = None) -> list[str]:
    resolved_source = Path(source).expanduser().resolve()
    suffix = resolved_source.suffix.lower()
    if suffix == ".wem":
        return []
    tools = tools or discover_media_tools()
    missing: list[str] = []
    if tools.wwise_console_path is None:
        missing.append("WwiseConsole.exe")
    if suffix != ".wav" and tools.ffmpeg_path is None:
        missing.append("ffmpeg.exe")
    return missing


def ffprobe_audio(path: str | Path, tools: MediaTools) -> tuple[float, int, str] | None:
    if tools.ffprobe_path is None:
        return None

    source = Path(path).resolve()
    command = [
        str(tools.ffprobe_path),
        "-v",
        "error",
        "-show_entries",
        "stream=sample_rate:format=duration",
        "-of",
        "json",
        str(source),
    ]
    result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
    if result.returncode != 0:
        return None

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None

    duration_raw = payload.get("format", {}).get("duration")
    streams = payload.get("streams", [])
    stream_rate = 0
    for stream in streams:
        sample_rate = stream.get("sample_rate")
        if sample_rate:
            try:
                stream_rate = int(sample_rate)
                break
            except ValueError:
                continue

    try:
        duration_seconds = float(duration_raw)
    except (TypeError, ValueError):
        return None

    return duration_seconds, stream_rate, "Metadata loaded via ffprobe."


def vgmstream_probe_audio(path: str | Path, tools: MediaTools) -> tuple[float, int, int, str] | None:
    if tools.vgmstream_path is None:
        return None

    source = Path(path).resolve()
    command = [str(tools.vgmstream_path), "-m", str(source)]
    result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
    if result.returncode != 0:
        return None

    sample_rate = 0
    sample_count = 0
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if line.startswith("sample rate:"):
            try:
                sample_rate = int(line.split(":", 1)[1].strip().split()[0])
            except (IndexError, ValueError):
                sample_rate = 0
        elif line.startswith("stream total samples:"):
            try:
                sample_count = int(line.split(":", 1)[1].strip().split()[0])
            except (IndexError, ValueError):
                sample_count = 0

    if sample_rate <= 0 or sample_count <= 0:
        return None
    return sample_count / float(sample_rate), sample_rate, sample_count, "Metadata loaded via vgmstream."
