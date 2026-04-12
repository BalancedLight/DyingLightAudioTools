from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dyingaudio.core.media_tools import run_hidden


@dataclass(slots=True)
class DldtToolchain:
    root_dir: Path
    fsb_dir: Path
    compiler_exe: Path
    linker_exe: Path
    fsbank_dll: Path


@dataclass(slots=True)
class CommandResult:
    success: bool
    command: list[str]
    stdout: str
    stderr: str
    output_path: Path


def discover_toolchain(path: str | Path) -> tuple[DldtToolchain | None, list[str]]:
    candidate = Path(path).expanduser().resolve()
    errors: list[str] = []

    fsb_dir = candidate / "FSB"
    if not (fsb_dir / "FSBCompiler.exe").exists():
        if (candidate / "FSBCompiler.exe").exists():
            fsb_dir = candidate
            candidate = candidate.parent
        else:
            return None, [f"Could not find FSBCompiler.exe under '{path}'."]

    compiler = fsb_dir / "FSBCompiler.exe"
    linker = fsb_dir / "FSBLinker.exe"
    fsbank_dll = fsb_dir / "fsbanklibex.dll"

    for required in (compiler, linker, fsbank_dll):
        if not required.exists():
            errors.append(f"Missing required toolchain file: {required}")

    if errors:
        return None, errors

    return (
        DldtToolchain(
            root_dir=candidate,
            fsb_dir=fsb_dir,
            compiler_exe=compiler,
            linker_exe=linker,
            fsbank_dll=fsbank_dll,
        ),
        [],
    )


def _build_env(toolchain: DldtToolchain) -> dict[str, str]:
    env = os.environ.copy()
    current_path = env.get("PATH", "")
    env["PATH"] = os.pathsep.join([str(toolchain.fsb_dir), str(toolchain.root_dir), current_path])
    return env


def compile_audio_to_fsb(
    toolchain: DldtToolchain,
    source_path: str | Path,
    output_path: str | Path,
    cache_dir: str | Path,
) -> CommandResult:
    source = Path(source_path).resolve()
    output = Path(output_path).resolve()
    cache = Path(cache_dir).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)

    command = [
        str(toolchain.compiler_exe),
        "-input",
        str(source),
        "-output",
        str(output),
        "-cache",
        str(cache),
    ]

    result = run_hidden(
        command,
        cwd=str(toolchain.fsb_dir),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_build_env(toolchain),
        check=False,
    )

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    success = output.exists() and output.stat().st_size > 0 and "FSB created successfully" in stdout

    return CommandResult(
        success=success,
        command=command,
        stdout=stdout,
        stderr=stderr,
        output_path=output,
    )


def link_fsb_list(
    toolchain: DldtToolchain,
    list_path: str | Path,
    output_path: str | Path,
) -> CommandResult:
    manifest = Path(list_path).resolve()
    output = Path(output_path).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    command = [str(toolchain.linker_exe), str(manifest), "-output", str(output)]
    result = run_hidden(
        command,
        cwd=str(toolchain.fsb_dir),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_build_env(toolchain),
        check=False,
    )

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    success = output.exists() and output.stat().st_size > 0 and "Linking process terminated!" not in stdout

    return CommandResult(
        success=success,
        command=command,
        stdout=stdout,
        stderr=stderr,
        output_path=output,
    )
