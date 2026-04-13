# DyingAudio: The Definitive Dying Light Series Audio Tool

## Note: Most code here was generated with AI. Please proceed with caution.

DyingAudio is a Windows-first Python GUI for Dying Light audio work.

It now ships with two main workspaces:

- `Dying Light 1`: the current CSB/FSB editor for Dying Light 1 mods
- `Dying Light 2 / The Beast (Experimental)`: a read-only Wwise browser for generating named trees, previewing audio, and exporting organized dumps

## Dying Light 1

The DL1 tab supports:

- opening an existing `.csb` into the editor and replacing individual bank entries
- importing raw `.ogg`, `.wav`, `.mp3`, `.flac`, `.m4a`, `.aac`, `.wma`, `.opus`, and similar files
- mixing raw audio entries with existing `.fsb` entries in the same project
- searching and sorting the current entry list inside the editor
- using a right-click menu on entries for replace, export, duplicate, rename, and remove actions
- previewing selected audio entries from raw files or extracted `.fsb` banks
- caching decoded preview audio in temp storage for faster repeat playback
- compiling `.fsb` banks with the installed Dying Light Developer Tools
- packing `.csb` bundles with an internal writer
- inspecting and extracting existing `.csb` bundles
- generating a placeholder `audiodata.scr` that loads a chosen bank in every listed `AudioProc`
- exporting a full mod folder into a selectable `Mods` root
- saving the edited result directly to a chosen `.csb` file, including overwriting the opened bank

## Dying Light 2 / The Beast (Experimental)

The experimental tab supports:

- switching between `DL2` and `DLTB` roots in one shared Wwise-family workspace
- detecting `base` and available `speech_*` archive sets from the selected game install
- extracting the embedded `<Mapping>` XML directly from `meta*.aesp`
- generating and caching a named workspace under `%LOCALAPPDATA%\\DyingAudio\\wwise_cache`
- browsing `archive -> bank -> event` groups with media rows in the center pane
- previewing generated WAV files directly from the cached workspace
- exporting selected media, selected event folders, selected bank files, or a full workspace dump

The experimental workspace is intentionally read-only in v1. It does not edit or rebuild Wwise banks yet.

## Running From Source

You need a real Python install on Windows. The Microsoft Store alias alone is not enough.

```powershell
set PYTHONPATH=.\src
python -m dyingaudio
```

Or use:

```powershell
.\run_dyingaudio.bat
```

## Packaging

The project includes:

- `DyingAudio.spec`
- `scripts\build_exe.ps1`

After installing PyInstaller:

```powershell
pwsh -ExecutionPolicy Bypass -File .\scripts\build_exe.ps1
```

> The build requires a Windows Python installation with Tcl/Tk support so `tkinter` can be packaged correctly.

## Notes

- Default experimental cache root: `%LOCALAPPDATA%\DyingAudio\wwise_cache`
- The app remembers the last selected install folders in `settings.json`.
- If an install root is blank, click `Browse` to auto-find a Steam install first, then fall back to manual selection.
- If you type a bundle name with `.csb`, DyingAudio normalizes it so the output stays `name.csb` and `LoadAudioBanks("name")`
- Replacing an entry with raw audio automatically switches the build mode to `Raw Audio via DLDT`
- Raw formats beyond `.wav` and `.ogg` are converted to a temporary WAV automatically during preview/build when FFmpeg is available
- Preview now prefers direct FFplay/vgmstream streaming for much faster playback startup, with cached temp WAV fallback when needed
- Retail `DW\\Data` banks using CSB magic `0x00000002` are supported for inspect, edit, and save
- The DL1 editor uses a normal resizable layout with tabs for Selected Entry and Script Generation, so preview controls stay visible at the default window size
- The experimental Wwise workspace now builds its named tree directly in Python and extracts mapping XML from `meta*.aesp` automatically
- Generated mod folders include:
  - `modinfo.ini`
  - `data\<bundle>.csb`
  - `data\scripts\audio\audiodata.scr` when enabled
