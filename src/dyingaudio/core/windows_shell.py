from __future__ import annotations

import ctypes
from pathlib import Path
from typing import Any

try:
    import winreg
except ImportError:  # pragma: no cover - non-Windows fallback
    winreg = None  # type: ignore[assignment]


CSB_EXTENSION = ".csb"
SPB_EXTENSION = ".spb"
CSB_ASSOCIATION_APP_NAME = "DyingAudio"
CSB_ASSOCIATION_EXE_NAME = "DyingAudio.exe"
CSB_ASSOCIATION_PROGID = "DyingAudio.CompiledSoundBank"
SPB_ASSOCIATION_PROGID = "DyingAudio.SpeechPatternBank"
CSB_FRIENDLY_NAME = "Compiled Sound Bank"
SPB_FRIENDLY_NAME = "Speech Pattern Bank"

_WINDOWS_CLASSES_ROOT = r"Software\Classes"
_APPLICATIONS_ROOT = rf"{_WINDOWS_CLASSES_ROOT}\Applications\{CSB_ASSOCIATION_EXE_NAME}"
_CSB_PROGID_ROOT = rf"{_WINDOWS_CLASSES_ROOT}\{CSB_ASSOCIATION_PROGID}"
_SPB_PROGID_ROOT = rf"{_WINDOWS_CLASSES_ROOT}\{SPB_ASSOCIATION_PROGID}"
_CSB_EXTENSION_ROOT = rf"{_WINDOWS_CLASSES_ROOT}\{CSB_EXTENSION}"
_SPB_EXTENSION_ROOT = rf"{_WINDOWS_CLASSES_ROOT}\{SPB_EXTENSION}"
_EXTENSION_OPEN_WITH_PROGIDS_ROOT = rf"{_WINDOWS_CLASSES_ROOT}\{CSB_EXTENSION}\OpenWithProgids"


def _require_winreg() -> Any:
    if winreg is None:
        raise RuntimeError("Windows registry support is only available on Windows.")
    return winreg


def _normalize_executable_path(exe_path: str | Path) -> Path:
    resolved = Path(exe_path).expanduser().resolve()
    if resolved.suffix.lower() != ".exe":
        raise ValueError(f"Expected an .exe path, got: {resolved}")
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    return resolved


def _open_command(exe_path: Path) -> str:
    return f'"{exe_path}" "%1"'


def _set_default_value(registry: Any, key: Any, value: str) -> None:
    registry.SetValueEx(key, "", 0, registry.REG_SZ, value)


def _set_string_value(registry: Any, key: Any, name: str, value: str) -> None:
    registry.SetValueEx(key, name, 0, registry.REG_SZ, value)


def _create_key(registry: Any, root: Any, path: str) -> Any:
    return registry.CreateKey(root, path)


def _open_key(registry: Any, root: Any, path: str) -> Any:
    return registry.OpenKey(root, path, 0, registry.KEY_READ | registry.KEY_WRITE)


def _delete_value_if_exists(registry: Any, root: Any, path: str, value_name: str) -> None:
    try:
        key = _open_key(registry, root, path)
    except FileNotFoundError:
        return

    try:
        registry.DeleteValue(key, value_name)
    except FileNotFoundError:
        pass
    finally:
        registry.CloseKey(key)


def _delete_tree(registry: Any, root: Any, path: str) -> None:
    try:
        key = _open_key(registry, root, path)
    except FileNotFoundError:
        return

    try:
        while True:
            child_name = registry.EnumKey(key, 0)
            _delete_tree(registry, key, child_name)
    except OSError:
        pass
    finally:
        registry.CloseKey(key)

    try:
        registry.DeleteKey(root, path)
    except FileNotFoundError:
        pass


def _notify_shell_association_changed() -> None:
    try:
        ctypes.windll.shell32.SHChangeNotify(0x08000000, 0x0000, None, None)
    except Exception:  # pragma: no cover - shell refresh is best-effort
        pass


def _register_prog_id(
    registry: Any,
    *,
    root: Any,
    progid_root: str,
    friendly_name: str,
    exe_path: Path | None,
    open_command: bool,
) -> None:
    progid_key = _create_key(registry, root, progid_root)
    try:
        _set_default_value(registry, progid_key, friendly_name)
        _set_string_value(registry, progid_key, "FriendlyTypeName", friendly_name)

        if exe_path is not None:
            icon_key = _create_key(registry, progid_key, "DefaultIcon")
            try:
                _set_default_value(registry, icon_key, f"{exe_path},0")
            finally:
                registry.CloseKey(icon_key)

        if open_command and exe_path is not None:
            command_key = _create_key(registry, progid_key, r"shell\open\command")
            try:
                _set_default_value(registry, command_key, _open_command(exe_path))
            finally:
                registry.CloseKey(command_key)
    finally:
        registry.CloseKey(progid_key)


def _register_extension(
    registry: Any,
    *,
    root: Any,
    extension_root: str,
    progid: str,
    open_with_progid: str | None = None,
) -> None:
    extension_key = _create_key(registry, root, extension_root)
    try:
        _set_default_value(registry, extension_key, progid)
        if open_with_progid is not None:
            open_with_key = _create_key(registry, extension_key, "OpenWithProgids")
            try:
                _set_string_value(registry, open_with_key, open_with_progid, "")
            finally:
                registry.CloseKey(open_with_key)
    finally:
        registry.CloseKey(extension_key)


def register_dyingaudio_file_types(exe_path: str | Path) -> None:
    registry = _require_winreg()
    resolved_exe = _normalize_executable_path(exe_path)
    command = _open_command(resolved_exe)
    exe_name = resolved_exe.name
    applications_root = rf"{_WINDOWS_CLASSES_ROOT}\Applications\{exe_name}"

    app_key = _create_key(registry, registry.HKEY_CURRENT_USER, applications_root)
    try:
        command_key = _create_key(registry, app_key, r"shell\open\command")
        try:
            _set_default_value(registry, command_key, command)
        finally:
            registry.CloseKey(command_key)

        icon_key = _create_key(registry, app_key, "DefaultIcon")
        try:
            _set_default_value(registry, icon_key, f"{resolved_exe},0")
        finally:
            registry.CloseKey(icon_key)

        supported_types_key = _create_key(registry, app_key, "SupportedTypes")
        try:
            _set_string_value(registry, supported_types_key, CSB_EXTENSION, "")
        finally:
            registry.CloseKey(supported_types_key)
    finally:
        registry.CloseKey(app_key)

    _register_prog_id(
        registry,
        root=registry.HKEY_CURRENT_USER,
        progid_root=_CSB_PROGID_ROOT,
        friendly_name=CSB_FRIENDLY_NAME,
        exe_path=resolved_exe,
        open_command=True,
    )
    _register_prog_id(
        registry,
        root=registry.HKEY_CURRENT_USER,
        progid_root=_SPB_PROGID_ROOT,
        friendly_name=SPB_FRIENDLY_NAME,
        exe_path=resolved_exe,
        open_command=False,
    )
    _register_extension(
        registry,
        root=registry.HKEY_CURRENT_USER,
        extension_root=_CSB_EXTENSION_ROOT,
        progid=CSB_ASSOCIATION_PROGID,
        open_with_progid=CSB_ASSOCIATION_PROGID,
    )
    _register_extension(
        registry,
        root=registry.HKEY_CURRENT_USER,
        extension_root=_SPB_EXTENSION_ROOT,
        progid=SPB_ASSOCIATION_PROGID,
    )
    _notify_shell_association_changed()


def unregister_dyingaudio_file_types() -> None:
    registry = _require_winreg()
    _delete_value_if_exists(registry, registry.HKEY_CURRENT_USER, _EXTENSION_OPEN_WITH_PROGIDS_ROOT, CSB_ASSOCIATION_PROGID)
    _delete_tree(registry, registry.HKEY_CURRENT_USER, _CSB_EXTENSION_ROOT)
    _delete_tree(registry, registry.HKEY_CURRENT_USER, _SPB_EXTENSION_ROOT)
    _delete_tree(registry, registry.HKEY_CURRENT_USER, _APPLICATIONS_ROOT)
    _delete_tree(registry, registry.HKEY_CURRENT_USER, _CSB_PROGID_ROOT)
    _delete_tree(registry, registry.HKEY_CURRENT_USER, _SPB_PROGID_ROOT)
    _notify_shell_association_changed()


def register_csb_open_with(exe_path: str | Path) -> None:
    register_dyingaudio_file_types(exe_path)


def unregister_csb_open_with() -> None:
    unregister_dyingaudio_file_types()
