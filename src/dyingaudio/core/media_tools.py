from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
    found = shutil.which(executable)
    if found:
        return Path(found)

    for fallback in fallback_paths:
        if fallback.exists():
            return fallback

    return None


@dataclass(slots=True)
class MediaTools:
    ffmpeg_path: Path | None
    ffplay_path: Path | None
    ffprobe_path: Path | None
    vgmstream_path: Path | None

    def summary(self) -> str:
        parts: list[str] = []
        parts.append("FFmpeg ready" if self.ffmpeg_path else "FFmpeg missing")
        parts.append("FFplay ready" if self.ffplay_path else "FFplay missing")
        parts.append("FFprobe ready" if self.ffprobe_path else "FFprobe missing")
        parts.append("vgmstream ready" if self.vgmstream_path else "vgmstream missing")
        return "Preview tools: " + ", ".join(parts)


def discover_media_tools() -> MediaTools:
    local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
    ffmpeg_path = find_tool(
        "ffmpeg.exe",
        [Path(r"C:\ProgramData\chocolatey\bin\ffmpeg.exe")],
    )
    ffplay_path = find_tool(
        "ffplay.exe",
        [Path(r"C:\ProgramData\chocolatey\bin\ffplay.exe")],
    )
    ffprobe_path = find_tool(
        "ffprobe.exe",
        [Path(r"C:\ProgramData\chocolatey\bin\ffprobe.exe")],
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
    return MediaTools(
        ffmpeg_path=ffmpeg_path,
        ffplay_path=ffplay_path,
        ffprobe_path=ffprobe_path,
        vgmstream_path=vgmstream_path,
    )


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
