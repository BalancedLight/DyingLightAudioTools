# DyingAudio

Windows-first Python GUI tools for working with Dying Light series audio.

## Current Workspaces

- `Dying Light 1`: edit `.csb` bundles, mix raw audio and existing `.fsb` files, preview entries, save rebuilt banks, and build mods
- `Dying Light 2 / The Beast`: Wwise browser for named trees, preview, export, and experimental replacement
- `Other`: read-only AKPK / `.pck` browser with preview, export, and experimental replacement tools

## First-Time Setup

### Via Releases
1. Download the executable
2. Run it!
3. If you want Windows to label `.csb` and `.spb` files, open DyingAudio and use `Settings > .csb/.spb File Types...`, then click `Register File Types`
4. You can also launch the packaged EXE with a `.csb` path on the command line and it will open that bank for editing immediately

If you get an alert from Windows Security or another antimalware executable, ignore it. Antimalware engines do not like packed Python executables!
If you still feel uncomfortable, you can run via Python below.

### Via Python
1. Install a normal Windows Python build with `tkinter` support. Python from the Microsoft Store alias alone is usually not enough.
2. Open the app once and let it remember your paths in `settings.json`.
3. Use `Settings > Folders and Tools` to point DyingAudio at DLDT, game roots, caches, FFmpeg, vgmstream, and Wwise.
4. The welcome menu can check those requirements on startup and can be reopened from `Settings > Open Welcome Menu`.

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
You may also need to install ffmpeg.

### Create a new DL1 audio project

1. Open the `Dying Light 1` tab.
2. Click `Add Audio / FSB`.
3. Pick raw audio files, `.fsb` files, or a mix of both in the same browse dialog.
4. Select an entry in the list to review it in `Selected Entry`.
5. Click `Apply Entry Changes` after editing any fields.
6. Use `File > Save CSB File` to write a standalone bank.

### Edit an existing `.csb`

1. Choose `File > Open CSB`.
2. Select the entry you want to change.
3. Click `Replace Audio / FSB`.
4. Choose a raw audio file such as `.mp3`, `.ogg`, or `.wav`, or pick an `.fsb`.
5. Preview, adjust entry details if needed, then save or build.
6. If the compiled app is registered with Explorer, `.csb` files can show DyingAudio in `Open with`, and `.spb` files will show the friendly Speech Pattern Bank type label.


### Selected Entry behavior

- If you change selection while the current entry has unapplied edits, DyingAudio will ask whether to apply, discard, or keep editing.
- Press `Enter` inside an editable field or click `Apply Entry Changes` to commit the change.

### Builder Modes

- `Raw Audio via DLDT`: use this when any entry comes from raw audio and must be compiled into FSB during build/save
- `Existing FSB Files`: use this when every entry already points at ready-made `.fsb` content

Replacing or adding raw audio keeps the project in raw-audio mode because DLDT is required to compile those files.

### Audio Quality

- `Audio Quality` controls how raw DL1 audio is normalized before rebuilt banks are compiled
- Vorbis presets rebuild through `.ogg` intermediates and also become the default export format for `Export Audio` and decoded `Extract CSB`
- `PCM WAV` keeps decoded exports and rebuild intermediates as `.wav` (Very high file sizes!)

### Speech Pattern Bank Creation

You can now generate `.spb` files for use in localized audio banks.

Enable by checking the "Localized Speech Bank" option and "Generate SPB" option. Select your text `.scr` containing your line data for your audio.
If you're creating a workshop mod, this will almost always be `texts_steam_workshop.scr`.

### Useful DL1 actions

- `Preview`: play the selected entry directly from the source file or extracted FSB data
- `File > Inspect CSB`: view entry names, channel info, duration, samples, and notes without opening for edit
- `File > Extract CSB`: unpack embedded FSBs from an existing bundle, or export decoded audio plus a reusable manifest
- Right-click an entry for replace, export, duplicate, rename, or remove
- Search and sort the entry list without losing the original underlying entry indices
- `Help > Tool Status` shows playback and editing tool readiness, with missing tools highlighted in red
- `Tabs > Open Console` reopens the DL1 console after it has been closed


## Dying Light 2 / The Beast

This workspace supports browsing and replacing Wwise audio in DL2 and DLTB AESP archives.

### Browsing & Export

- Switching between `DL2` and `DLTB`
- Detecting available archive sets such as `base` and `speech_*` (language packs)
- Building a cached named workspace under `%LOCALAPPDATA%\DyingAudio\wwise_cache`
- Browsing `archive → bank → event`
- Previewing and exporting selected media
- Exporting selected event folders, selected bank files, or a full workspace dump

### Audio Replacement

Replace any media entry in `sfx`, `streams`, or `meta` archives with custom audio:

1. Install Wwise from [Audiokinetic's website](https://www.audiokinetic.com/en/download) and install Wwise 2023.1.13.8732 (You can probably use a newer version, but this is the one DL2 uses.)
2. Enable the **"Enable experimental AESP replacement"** checkbox
3. Select one or more media rows in the media tree
4. Click **Replace Selected Audio** (or right-click → Replace)
5. Pick a `.wem`, `.wav`, `.ogg`, `.mp3`, or other audio file
6. Non-WEM files are automatically converted via a local Wwise installation

**Multi-replacement**: Select multiple rows (Ctrl+Click or Shift+Click) to replace them all with the same audio file in one operation. Useful for replacing grouped or duplicate entries.

**Hide warnings**: Check **"Hide replacement warnings"** to skip sample-rate, duration, and confirmation dialogs. A one-time warning explains what this disables before it takes effect. The setting persists across sessions.

**Backups**: A `.bak` file is created on the first modification to each archive. Use **Restore Original AESP** to undo all replacements at once. BNK metadata in `meta.aesp` is also patched and backed up automatically.

**Speech archives**: Language packs (e.g. `speech_en`) appear as separate archive sets and support the same replacement workflow as `base`.

This is an experimental workflow, but it shows promising results in-game.
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
