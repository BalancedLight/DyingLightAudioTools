from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import time
import winsound
from pathlib import Path
from typing import Callable

from dyingaudio.core.media_tools import MediaTools, discover_media_tools, run_hidden
from dyingaudio.models import AudioEntry


WINDOWS_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
WINDOWS_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0


def preview_strategy_for_entry(entry: AudioEntry, environment: MediaTools) -> str:
    candidate = entry.resolved_source_path() if entry.source_mode == "raw" else entry.resolved_fsb_path()
    if candidate is None:
        return "Preview unavailable: no source file is attached to this entry."

    suffix = candidate.suffix.lower()
    if entry.source_mode == "raw":
        if suffix == ".wav":
            return "Preview will play the WAV file directly."
        if environment.ffplay_path:
            return f"Preview will play the {suffix or 'raw audio'} file directly with FFplay."
        if environment.ffmpeg_path:
            return f"Preview will decode the {suffix or 'raw audio'} file to a temporary WAV, then play it."
        return f"Preview unavailable: FFmpeg is required to decode {suffix or 'raw audio'} files."

    if environment.vgmstream_path and environment.ffplay_path:
        return "Preview will stream the selected FSB bank through vgmstream into FFplay."
    if environment.vgmstream_path:
        return "Preview will decode the selected FSB bank with vgmstream, then play it."
    return "Preview unavailable: vgmstream is required to decode FSB banks."


class PreviewPlayer:
    def __init__(self) -> None:
        self.environment = discover_media_tools()
        self._temp_dir = tempfile.TemporaryDirectory(prefix="dyingaudio_preview_")
        self._current_path: Path | None = None
        self._playback_kind: str | None = None
        self._cache: dict[str, Path] = {}
        self._player_process: subprocess.Popen[bytes] | None = None
        self._decoder_process: subprocess.Popen[bytes] | None = None

    def play_entry(self, entry: AudioEntry, log: Callable[[str], None]) -> Path:
        self.stop()
        if entry.source_mode == "raw":
            source = entry.resolved_source_path()
            if source is None or not source.exists():
                raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")
            if source.suffix.lower() == ".wav":
                winsound.PlaySound(str(source), winsound.SND_ASYNC | winsound.SND_FILENAME)
                self._current_path = source
                self._playback_kind = "winsound"
                log(f"Fast preview: winsound {source}")
                return source
        if self._start_fast_preview(entry, log):
            preview_source = entry.resolved_source_path() if entry.source_mode == "raw" else entry.resolved_fsb_path()
            if preview_source is None:
                raise FileNotFoundError(f"Missing preview source for '{entry.entry_name}'.")
            return preview_source
        preview_path = self._prepare_preview_wav(entry, log)
        winsound.PlaySound(None, 0)
        winsound.PlaySound(str(preview_path), winsound.SND_ASYNC | winsound.SND_FILENAME)
        self._current_path = preview_path
        self._playback_kind = "winsound"
        return preview_path

    def play_combined_sources(self, sources: list[Path], log: Callable[[str], None]) -> Path:
        if len(sources) < 2:
            raise ValueError("At least two sources are required to preview a combined mix.")

        resolved_sources: list[Path] = []
        for source in sources:
            resolved = Path(source)
            if not resolved.exists():
                raise FileNotFoundError(f"Missing preview source: {resolved}")
            resolved_sources.append(resolved)

        self.stop()
        preview_path = self._prepare_combined_preview_wav(resolved_sources, log)
        winsound.PlaySound(None, 0)
        winsound.PlaySound(str(preview_path), winsound.SND_ASYNC | winsound.SND_FILENAME)
        self._current_path = preview_path
        self._playback_kind = "winsound"
        return preview_path

    def export_combined_sources(self, sources: list[Path], destination: str | Path, log: Callable[[str], None]) -> Path:
        if len(sources) < 2:
            raise ValueError("At least two sources are required to export a combined mix.")

        resolved_sources: list[Path] = []
        for source in sources:
            resolved = Path(source)
            if not resolved.exists():
                raise FileNotFoundError(f"Missing preview source: {resolved}")
            resolved_sources.append(resolved)

        self.stop()
        destination_path = Path(destination).resolve()
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return self._render_combined_sources_wav(resolved_sources, destination_path, log)

    def stop(self) -> None:
        winsound.PlaySound(None, 0)
        self._stop_player_process()
        self._terminate_process(self._decoder_process)
        self._player_process = None
        self._decoder_process = None
        self._current_path = None
        self._playback_kind = None

    def playback_kind(self) -> str | None:
        return self._playback_kind

    def has_live_process(self) -> bool:
        return any(process is not None and process.poll() is None for process in (self._player_process, self._decoder_process))

    def clear_cache(self) -> None:
        self.stop()
        self._cache.clear()
        root = Path(self._temp_dir.name)
        for path in root.glob("*"):
            if path.is_file():
                path.unlink(missing_ok=True)

    def close(self) -> None:
        self.clear_cache()
        self._temp_dir.cleanup()

    def _terminate_process(self, process: subprocess.Popen[bytes] | None) -> None:
        if process is None:
            return
        if process.poll() is not None:
            self._close_process_streams(process)
            return
        pid = getattr(process, "pid", None)
        try:
            if os.name == "nt" and pid is not None:
                self._kill_windows_process_tree(pid)
            else:
                process.kill()
        except OSError:
            pass
        self._close_process_streams(process)

    def _stop_player_process(self) -> None:
        process = self._player_process
        if process is None:
            return
        if process.poll() is not None:
            return
        try:
            if process.stdin is not None:
                process.stdin.write(b"q\n")
                process.stdin.flush()
                process.stdin.close()
                process.wait(timeout=0.1)
                self._close_process_streams(process)
                return
        except (BrokenPipeError, OSError, subprocess.TimeoutExpired):
            pass
        self._terminate_process(process)

    def _close_process_streams(self, process: subprocess.Popen[bytes]) -> None:
        for stream_name in ("stdin", "stdout", "stderr"):
            stream = getattr(process, stream_name, None)
            if stream is None:
                continue
            try:
                stream.close()
            except OSError:
                pass

    def _kill_windows_process_tree(self, pid: int) -> None:
        command = [
            "taskkill",
            "/PID",
            str(pid),
            "/T",
            "/F",
        ]
        run_hidden(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            creationflags=WINDOWS_NO_WINDOW,
        )

    def _spawn_ffplay(self, source: str | Path, *, stdin: object = None) -> subprocess.Popen[bytes]:
        if self.environment.ffplay_path is None:
            raise RuntimeError("FFplay is required for direct preview playback.")
        command = [
            str(self.environment.ffplay_path),
            "-nodisp",
            "-autoexit",
            "-loglevel",
            "error",
            "-hide_banner",
            "-nostats",
        ]
        if stdin is None:
            command.append(str(source))
            return subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=WINDOWS_NO_WINDOW | WINDOWS_NEW_PROCESS_GROUP,
            )
        command.extend(["-i", "-"])
        return subprocess.Popen(
            command,
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=WINDOWS_NO_WINDOW | WINDOWS_NEW_PROCESS_GROUP,
        )

    def _start_fast_preview(self, entry: AudioEntry, log: Callable[[str], None]) -> bool:
        if entry.source_mode == "raw":
            source = entry.resolved_source_path()
            if source is None or not source.exists():
                raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")
            if self.environment.ffplay_path is None:
                return False
            self._player_process = self._spawn_ffplay(source)
            self._current_path = source
            self._playback_kind = "process"
            log(f"Fast preview: FFplay {source}")
            return True

        fsb_path = entry.resolved_fsb_path()
        if fsb_path is None or not fsb_path.exists():
            raise FileNotFoundError(f"Missing FSB file for '{entry.entry_name}'.")
        if self.environment.vgmstream_path is None or self.environment.ffplay_path is None:
            return False

        decode_command = [str(self.environment.vgmstream_path), "-p", str(fsb_path)]
        log(" ".join(decode_command))
        self._decoder_process = subprocess.Popen(
            decode_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            creationflags=WINDOWS_NO_WINDOW | WINDOWS_NEW_PROCESS_GROUP,
        )
        assert self._decoder_process.stdout is not None
        self._player_process = self._spawn_ffplay("-", stdin=self._decoder_process.stdout)
        self._decoder_process.stdout.close()
        time.sleep(0.08)
        if self._decoder_process.poll() not in (None, 0):
            self.stop()
            return False
        self._current_path = fsb_path
        self._playback_kind = "process"
        log(f"Fast preview: streaming {entry.entry_name} through vgmstream + FFplay")
        return True

    def _cache_key(self, entry: AudioEntry, source: Path) -> str:
        stat = source.stat()
        payload = "|".join(
            [
                entry.source_mode,
                str(source.resolve()),
                str(stat.st_mtime_ns),
                str(stat.st_size),
            ]
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def _cached_destination(self, entry: AudioEntry, source: Path) -> tuple[str, Path]:
        key = self._cache_key(entry, source)
        destination = Path(self._temp_dir.name) / f"{key}_{source.stem}.wav"
        return key, destination

    def _combined_cache_key(self, sources: list[Path]) -> str:
        payload_parts: list[str] = ["combined"]
        for source in sources:
            stat = source.stat()
            payload_parts.extend(
                [
                    str(source.resolve()),
                    str(stat.st_mtime_ns),
                    str(stat.st_size),
                ]
            )
        return hashlib.sha1("|".join(payload_parts).encode("utf-8")).hexdigest()

    def _render_combined_sources_wav(self, sources: list[Path], destination: Path, log: Callable[[str], None]) -> Path:
        if self.environment.ffmpeg_path is None:
            raise RuntimeError("FFmpeg is required to mix multiple audio sources.")

        command = [str(self.environment.ffmpeg_path), "-y", "-loglevel", "error"]
        for source in sources:
            command.extend(["-i", str(source)])
        input_labels = "".join(f"[{index}:a]" for index in range(len(sources)))
        filter_complex = f"{input_labels}amix=inputs={len(sources)}:duration=first:dropout_transition=0[mix]"
        command.extend([
            "-filter_complex",
            filter_complex,
            "-map",
            "[mix]",
            "-c:a",
            "pcm_s16le",
            str(destination),
        ])
        log(" ".join(command))
        result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if result.stdout.strip():
            log(result.stdout.strip())
        if result.stderr.strip():
            log(result.stderr.strip())
        if result.returncode != 0 or not destination.exists():
            raise RuntimeError("Could not decode combined preview for the selected audio group.")
        return destination

    def _prepare_combined_preview_wav(self, sources: list[Path], log: Callable[[str], None]) -> Path:
        cache_key = self._combined_cache_key(sources)
        destination = Path(self._temp_dir.name) / f"{cache_key}_mix.wav"
        cached = self._cache.get(cache_key)
        if cached is not None and cached.exists():
            log(f"Using cached preview: {cached}")
            return cached

        rendered = self._render_combined_sources_wav(sources, destination, log)
        self._cache[cache_key] = destination
        return rendered

    def _prepare_preview_wav(self, entry: AudioEntry, log: Callable[[str], None]) -> Path:
        if entry.source_mode == "raw":
            source = entry.resolved_source_path()
            if source is None or not source.exists():
                raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")

            if source.suffix.lower() == ".wav":
                return source
            if self.environment.ffmpeg_path is None:
                raise RuntimeError(f"FFmpeg is required to preview '{source.suffix or 'unknown'}' files.")
            cache_key, destination = self._cached_destination(entry, source)
            cached = self._cache.get(cache_key)
            if cached is not None and cached.exists():
                log(f"Using cached preview: {cached}")
                return cached
            command = [
                str(self.environment.ffmpeg_path),
                "-y",
                "-loglevel",
                "error",
                "-i",
                str(source),
                str(destination),
            ]
            log(" ".join(command))
            result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
            if result.stdout.strip():
                log(result.stdout.strip())
            if result.stderr.strip():
                log(result.stderr.strip())
            if result.returncode != 0 or not destination.exists():
                raise RuntimeError(f"Could not decode preview for '{entry.entry_name}'.")
            self._cache[cache_key] = destination
            return destination

        fsb_path = entry.resolved_fsb_path()
        if fsb_path is None or not fsb_path.exists():
            raise FileNotFoundError(f"Missing FSB file for '{entry.entry_name}'.")
        if self.environment.vgmstream_path is None:
            raise RuntimeError("vgmstream is required to preview FSB banks.")

        cache_key, destination = self._cached_destination(entry, fsb_path)
        cached = self._cache.get(cache_key)
        if cached is not None and cached.exists():
            log(f"Using cached preview: {cached}")
            return cached
        command = [str(self.environment.vgmstream_path), "-o", str(destination), str(fsb_path)]
        log(" ".join(command))
        result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if result.stdout.strip():
            log(result.stdout.strip())
        if result.stderr.strip():
            log(result.stderr.strip())
        if result.returncode != 0 or not destination.exists():
            raise RuntimeError(f"Could not decode FSB preview for '{entry.entry_name}'.")
        self._cache[cache_key] = destination
        return destination
