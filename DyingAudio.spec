# -*- mode: python ; coding: utf-8 -*-

import ast
import pkgutil
import sys
from pathlib import Path

project_root = Path.cwd()
src_root = project_root / "src"
assets_root = project_root / "assets"
help_root = project_root / "help"
package_root = src_root / "dyingaudio"


def _collect_source_top_level_modules() -> set[str]:
    imported_modules = {"dyingaudio"}

    for path in package_root.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except OSError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_modules.add(alias.name.split(".", 1)[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    imported_modules.add("dyingaudio")
                elif node.module:
                    imported_modules.add(node.module.split(".", 1)[0])

    return imported_modules


def _collect_auto_excludes() -> list[str]:
    stdlib_modules = set(getattr(sys, "stdlib_module_names", ()))
    stdlib_modules.update(sys.builtin_module_names)
    allowed_modules = _collect_source_top_level_modules()
    excluded_modules = set()

    for module_info in pkgutil.iter_modules():
        module_name = module_info.name
        if module_name in stdlib_modules or module_name in allowed_modules:
            continue
        excluded_modules.add(module_name)

    return sorted(excluded_modules)


auto_excludes = _collect_auto_excludes()
print(f"Auto-excluding {len(auto_excludes)} optional top-level modules")

def _collect_data_files(root: Path, target_root: str) -> list[tuple[str, str]]:
    if not root.exists():
        return []
    return [
        (str(path), str(Path(target_root) / path.relative_to(root).parent))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    ]


asset_datas = _collect_data_files(assets_root, "assets")
help_datas = _collect_data_files(help_root, "help")

block_cipher = None

a = Analysis(
    [str(src_root / "dyingaudio" / "__main__.py")],
    pathex=[str(src_root)],
    binaries=[],
    datas=asset_datas + help_datas,
    hiddenimports=[
        "tkinter",
        "tkinter.filedialog",
        "tkinter.ttk",
        "tkinter.scrolledtext",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=auto_excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="DyingAudio",
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(project_root / "assets" / "dyinglight_devtools.ico"),
)
