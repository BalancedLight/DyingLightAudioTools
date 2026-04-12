from __future__ import annotations

import json
import os
import sys
import re
import string
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_MODS_ROOT = ""
DEFAULT_DLDT_ROOT = ""
DEFAULT_DL2_ROOT = ""
DEFAULT_DLTB_ROOT = ""
DEFAULT_AUDIO_PROCS = ["map_default", "example"]
DEFAULT_MOD_NAME = "ReAudio"
DEFAULT_BUNDLE_NAME = "workshop_audio"
DEFAULT_EXPERIMENTAL_GAME = "DLTB"
DEFAULT_EXPERIMENTAL_ARCHIVE_SET = "base"
DEFAULT_EXPERIMENTAL_CACHE_ROOT = str(Path(os.environ.get("LOCALAPPDATA", "")) / "DyingAudio" / "wwise_cache")
STEAM_COMMON_SUBPATH = Path("steamapps") / "common"
GAME_INSTALL_NAMES = {
    "DL1": "Dying Light",
    "DL2": "Dying Light 2",
    "DLTB": "Dying Light The Beast",
    "DLDT": "Dying Light Developer Tools",
}


def _windows_apps_use_light_theme() -> bool | None:
    if sys.platform != "win32":
        return None

    try:
        import winreg
    except ImportError:
        return None

    try:
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize",
        ) as key:
            value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
            return bool(value)
    except OSError:
        return None


def is_windows_dark_mode() -> bool:
    return _windows_apps_use_light_theme() is False


@dataclass(slots=True)
class DL1Settings:
    mods_root: str = DEFAULT_MODS_ROOT
    dldt_root: str = DEFAULT_DLDT_ROOT
    builder_mode: str = "Raw Audio via DLDT"
    mod_name: str = DEFAULT_MOD_NAME
    bundle_name: str = DEFAULT_BUNDLE_NAME
    generate_audiodata: bool = True
    audio_proc_names: list[str] = field(default_factory=lambda: list(DEFAULT_AUDIO_PROCS))
    last_output_folder: str = ""


@dataclass(slots=True)
class ExperimentalSettings:
    selected_game: str = DEFAULT_EXPERIMENTAL_GAME
    dl2_root: str = DEFAULT_DL2_ROOT
    dltb_root: str = DEFAULT_DLTB_ROOT
    archive_set: str = DEFAULT_EXPERIMENTAL_ARCHIVE_SET
    cache_root: str = DEFAULT_EXPERIMENTAL_CACHE_ROOT
    last_export_folder: str = ""


@dataclass(slots=True)
class AppSettings:
    dl1: DL1Settings = field(default_factory=DL1Settings)
    experimental: ExperimentalSettings = field(default_factory=ExperimentalSettings)

    # Compatibility accessors for the existing DL1 workspace.
    @property
    def mods_root(self) -> str:
        return self.dl1.mods_root

    @property
    def dldt_root(self) -> str:
        return self.dl1.dldt_root

    @property
    def builder_mode(self) -> str:
        return self.dl1.builder_mode

    @property
    def mod_name(self) -> str:
        return self.dl1.mod_name

    @property
    def bundle_name(self) -> str:
        return self.dl1.bundle_name

    @property
    def generate_audiodata(self) -> bool:
        return self.dl1.generate_audiodata

    @property
    def audio_proc_names(self) -> list[str]:
        return self.dl1.audio_proc_names

    @property
    def last_output_folder(self) -> str:
        return self.dl1.last_output_folder


def application_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def settings_path() -> Path:
    return application_root() / "settings.json"


def _update_dataclass(instance: Any, payload: dict[str, Any]) -> Any:
    for field_name in asdict(instance):
        if field_name in payload:
            setattr(instance, field_name, payload[field_name])
    return instance


def _unique_existing_paths(paths: list[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        try:
            resolved = path.expanduser().resolve()
        except OSError:
            continue
        key = str(resolved).casefold()
        if key in seen or not resolved.exists():
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _steam_client_roots() -> list[Path]:
    candidates: list[Path] = []
    env_vars = (
        "PROGRAMFILES(X86)",
        "PROGRAMFILES",
        "PROGRAMW6432",
        "LOCALAPPDATA",
    )
    for env_var in env_vars:
        raw_root = os.environ.get(env_var, "")
        if not raw_root:
            continue
        if env_var == "LOCALAPPDATA":
            candidates.append(Path(raw_root) / "Programs" / "Steam")
        else:
            candidates.append(Path(raw_root) / "Steam")

    for drive_letter in string.ascii_uppercase:
        drive_root = Path(f"{drive_letter}:\\")
        if not drive_root.exists():
            continue
        candidates.append(drive_root / "Steam")

    return _unique_existing_paths(candidates)


def _steam_library_roots() -> list[Path]:
    candidates = list(_steam_client_roots())
    env_roots: list[Path] = []
    for env_var in ("PROGRAMFILES(X86)", "PROGRAMFILES", "LOCALAPPDATA"):
        raw_root = os.environ.get(env_var, "")
        if raw_root:
            env_roots.append(Path(raw_root) / "SteamLibrary")
    candidates.extend(_unique_existing_paths(env_roots))

    for drive_letter in string.ascii_uppercase:
        drive_root = Path(f"{drive_letter}:\\")
        if not drive_root.exists():
            continue
        for leaf in (Path("SteamLibrary"), Path("Steam")):
            candidate = drive_root / leaf
            if candidate.exists():
                candidates.append(candidate)

    for client_root in list(candidates):
        config_path = client_root / "steamapps" / "libraryfolders.vdf"
        if not config_path.exists():
            continue
        try:
            text = config_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for match in re.finditer(r'"path"\s*"([^"]+)"', text):
            raw_path = match.group(1).replace("\\\\", "\\")
            candidate = Path(raw_path)
            if candidate.exists():
                candidates.append(candidate)

    return _unique_existing_paths(candidates)


def _discover_steam_install(folder_name: str, *, subpath: Path | None = None) -> Path | None:
    for library_root in _steam_library_roots():
        candidate = library_root / STEAM_COMMON_SUBPATH / folder_name
        if subpath is not None:
            candidate = candidate / subpath
        if candidate.exists():
            return candidate.resolve()
    return None


def discover_mods_root() -> Path | None:
    return _discover_steam_install(GAME_INSTALL_NAMES["DL1"], subpath=Path("Mods"))


def discover_dldt_root() -> Path | None:
    return _discover_steam_install(GAME_INSTALL_NAMES["DLDT"])


def discover_game_root(game: str) -> Path | None:
    folder_name = GAME_INSTALL_NAMES.get(game)
    if folder_name is None:
        return None
    return _discover_steam_install(folder_name)


def _migrate_legacy_settings(payload: dict[str, Any]) -> AppSettings:
    settings = AppSettings()
    legacy_dl1_payload = {
        "mods_root": payload.get("mods_root", settings.dl1.mods_root),
        "dldt_root": payload.get("dldt_root", settings.dl1.dldt_root),
        "builder_mode": payload.get("builder_mode", settings.dl1.builder_mode),
        "mod_name": payload.get("mod_name", settings.dl1.mod_name),
        "bundle_name": payload.get("bundle_name", settings.dl1.bundle_name),
        "generate_audiodata": payload.get("generate_audiodata", settings.dl1.generate_audiodata),
        "audio_proc_names": payload.get("audio_proc_names", list(settings.dl1.audio_proc_names)),
        "last_output_folder": payload.get("last_output_folder", settings.dl1.last_output_folder),
    }
    settings.dl1 = _update_dataclass(settings.dl1, legacy_dl1_payload)
    if not settings.dl1.audio_proc_names:
        settings.dl1.audio_proc_names = list(DEFAULT_AUDIO_PROCS)
    return settings


def load_settings() -> AppSettings:
    path = settings_path()
    if not path.exists():
        return AppSettings()

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return AppSettings()

    if "dl1" not in payload and "experimental" not in payload:
        return _migrate_legacy_settings(payload)

    settings = AppSettings()
    if isinstance(payload.get("dl1"), dict):
        settings.dl1 = _update_dataclass(settings.dl1, payload["dl1"])
    if isinstance(payload.get("experimental"), dict):
        settings.experimental = _update_dataclass(settings.experimental, payload["experimental"])

    if not settings.dl1.audio_proc_names:
        settings.dl1.audio_proc_names = list(DEFAULT_AUDIO_PROCS)
    if not settings.experimental.cache_root:
        settings.experimental.cache_root = DEFAULT_EXPERIMENTAL_CACHE_ROOT
    if not settings.experimental.archive_set:
        settings.experimental.archive_set = DEFAULT_EXPERIMENTAL_ARCHIVE_SET
    if not settings.experimental.selected_game:
        settings.experimental.selected_game = DEFAULT_EXPERIMENTAL_GAME
    return settings


def save_settings(settings: AppSettings) -> None:
    settings_path().write_text(json.dumps(asdict(settings), indent=2), encoding="utf-8")
