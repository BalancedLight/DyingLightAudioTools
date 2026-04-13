# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks.tcl_tk import tcltk_info

project_root = Path.cwd()
src_root = project_root / "src"

if not tcltk_info.available:
    raise SystemExit(
        "PyInstaller could not discover Tcl/Tk data for this build environment. "
        "Build with a standard Windows Python installation that includes working tkinter/Tcl/Tk files."
    )

block_cipher = None

a = Analysis(
    [str(src_root / "dyingaudio" / "__main__.py")],
    pathex=[str(src_root)],
    binaries=[],
    datas=[
        (str(project_root / "assets" / "dyinglight_devtools.ico"), "assets"),
        *[(src, dest) for dest, src, _typecode in tcltk_info.data_files],
    ],
    hiddenimports=[
        "tkinter",
        "tkinter.filedialog",
        "tkinter.ttk",
        "tkinter.scrolledtext",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
    strip=False,
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
