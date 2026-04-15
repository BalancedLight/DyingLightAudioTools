# DyingAudio

Windows-first Python GUI tools for working with Dying Light series audio.

## Current Workspaces

- `Dying Light 1`: edit `.csb` bundles, mix raw audio and existing `.fsb` files, preview entries, save rebuilt banks, and build mods
- `Dying Light 2 / The Beast (Experimental)`: read-only Wwise browser for named trees, preview, and export
- `Other`: read-only AKPK / `.pck` browser with preview, export, and experimental replacement tools

## First-Time Setup

1. Install a normal Windows Python build with `tkinter` support. Python from the Microsoft Store alias alone is usually not enough.
2. Open the app once and let it remember your paths in `settings.json`.
3. For DL1 raw-audio builds, point `DLDT Root` at the Dying Light Developer Tools folder.
4. For DL1 mod output, point `Mods Root` at your Dying Light `Mods` folder.

You can run from source with:

```powershell
python -m pip install -e .
python -m dyingaudio
```

Or with the included helper:

```powershell
.\run_dyingaudio.bat
```

## Dying Light 1 Quick Start

### Requirements

For some editing behaviour, you will need to have **Dying Light Developer Tools** installed. 

### Create a new DL1 audio project

1. Open the `Dying Light 1` tab.
2. Click `Add Audio / FSB`.
3. Pick raw audio files, `.fsb` files, or a mix of both in the same browse dialog.
4. Select an entry in the list to review it in `Selected Entry`.
5. Click `Apply Entry Changes` after editing any fields.
6. Use `Save CSB File` to write a standalone bank, or `Build Mod` to create a mod folder.

### Edit an existing `.csb`

1. Click `Open CSB For Edit`.
2. Select the entry you want to change.
3. Click `Replace Audio / FSB`.
4. Choose a raw audio file or an `.fsb`.
5. Preview, adjust entry details if needed, then save or build.


### Selected Entry behavior

- If you change selection while the current entry has unapplied edits, DyingAudio will ask whether to apply, discard, or keep editing.
- Press `Enter` inside an editable field or click `Apply Entry Changes` to commit the change.

### Builder Modes

- `Raw Audio via DLDT`: use this when any entry comes from raw audio and must be compiled into FSB during build/save
- `Existing FSB Files`: use this when every entry already points at ready-made `.fsb` content

Replacing or adding raw audio keeps the project in raw-audio mode because DLDT is required to compile those files.

### Useful DL1 actions

- `Preview`: play the selected entry directly from the source file or extracted FSB data
- `Inspect CSB`: view entry names, channel info, duration, samples, and notes without opening for edit
- `Extract CSB`: unpack embedded FSBs from an existing bundle
- Right-click an entry for replace, export, duplicate, rename, or remove
- Search and sort the entry list without losing the original underlying entry indices

## Dying Light 2 / The Beast (Experimental)

This workspace is intentionally read-only right now.

It supports:

- switching between `DL2` and `DLTB`
- detecting available archive sets such as `base` and `speech_*`
- building a cached named workspace under `%LOCALAPPDATA%\DyingAudio\wwise_cache`
- browsing `archive -> bank -> event`
- previewing and exporting selected media
- exporting selected event folders, selected bank files, or a full workspace dump

## Other Workspace

The `Other` tab targets AKPK / `.pck` packs.

It supports:

- building or refreshing a cached browser workspace
- browsing packs and media rows
- previewing and exporting selected media
- exporting mixed audio where supported
- optional experimental AKPK replacement tools via local Wwise installation

## Notes

- Default experimental cache root: `%LOCALAPPDATA%\DyingAudio\wwise_cache`
- The app remembers install roots, cache roots, and last-used output paths in `settings.json`
- If a path box is empty, `Browse` first tries auto-discovery before falling back to manual selection
- Raw formats beyond `.wav` and `.ogg` can use FFmpeg for preview/build helpers when available
- Preview prefers direct playback tools when possible and falls back to cached WAV generation when needed
- Retail `DW\Data` banks using CSB magic `0x00000002` are supported for inspect, edit, and save
- Dark mode combobox dropdowns are styled for readability across the app

## Packaging

This repo includes:

- `DyingAudio.spec`
- `scripts\build_exe.ps1`

After installing PyInstaller:

```powershell
pwsh -ExecutionPolicy Bypass -File .\scripts\build_exe.ps1
```

The packaged build still requires a Windows Python environment with Tcl/Tk available while building so `tkinter` can be bundled correctly.

### Some code here was generated with AI