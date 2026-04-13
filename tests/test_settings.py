from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dyingaudio.settings import (
    AppSettings,
    DEFAULT_AUDIO_PROCS,
    DEFAULT_OTHER_SOURCE_TYPE,
    discover_dldt_root,
    discover_game_root,
    discover_mods_root,
    load_settings,
    save_settings,
)


class SettingsTests(unittest.TestCase):
    def test_load_settings_migrates_legacy_flat_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings_file = Path(temp_dir) / "settings.json"
            settings_file.write_text(
                json.dumps(
                    {
                        "mods_root": r"X:\Mods",
                        "dldt_root": r"X:\DLDT",
                        "builder_mode": "Existing FSB Files",
                        "mod_name": "LegacyMod",
                        "bundle_name": "legacy_audio",
                        "generate_audiodata": False,
                        "audio_proc_names": ["map_default", "custom_proc"],
                        "last_output_folder": r"X:\Mods\LegacyMod",
                    }
                ),
                encoding="utf-8",
            )
            with patch("dyingaudio.settings.settings_path", return_value=settings_file):
                settings = load_settings()

            self.assertEqual(settings.dl1.mods_root, r"X:\Mods")
            self.assertEqual(settings.dl1.dldt_root, r"X:\DLDT")
            self.assertEqual(settings.dl1.builder_mode, "Existing FSB Files")
            self.assertEqual(settings.dl1.mod_name, "LegacyMod")
            self.assertEqual(settings.dl1.bundle_name, "legacy_audio")
            self.assertFalse(settings.dl1.generate_audiodata)
            self.assertEqual(settings.dl1.audio_proc_names, ["map_default", "custom_proc"])
            self.assertEqual(settings.experimental.dl2_root, "")
            self.assertEqual(settings.experimental.dltb_root, "")

    def test_save_settings_writes_nested_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings_file = Path(temp_dir) / "settings.json"
            settings = AppSettings()
            settings.dl1.mod_name = "NestedMod"
            settings.experimental.selected_game = "DL2"
            settings.experimental.archive_set = "speech_en"
            settings.other.root = r"X:\AudioAssets"
            settings.other.selected_source_type = DEFAULT_OTHER_SOURCE_TYPE
            with patch("dyingaudio.settings.settings_path", return_value=settings_file):
                save_settings(settings)

            payload = json.loads(settings_file.read_text(encoding="utf-8"))
            self.assertIn("dl1", payload)
            self.assertIn("experimental", payload)
            self.assertIn("other", payload)
            self.assertEqual(payload["dl1"]["mod_name"], "NestedMod")
            self.assertEqual(payload["experimental"]["selected_game"], "DL2")
            self.assertEqual(payload["experimental"]["archive_set"], "speech_en")
            self.assertEqual(payload["other"]["root"], r"X:\AudioAssets")
            self.assertEqual(payload["other"]["selected_source_type"], DEFAULT_OTHER_SOURCE_TYPE)

    def test_load_settings_restores_default_audio_proc_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings_file = Path(temp_dir) / "settings.json"
            settings_file.write_text(json.dumps({"dl1": {"audio_proc_names": []}}), encoding="utf-8")
            with patch("dyingaudio.settings.settings_path", return_value=settings_file):
                settings = load_settings()
            self.assertEqual(settings.dl1.audio_proc_names, DEFAULT_AUDIO_PROCS)

    def test_load_settings_restores_other_defaults_and_reads_nested_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings_file = Path(temp_dir) / "settings.json"
            settings_file.write_text(
                json.dumps(
                    {
                        "other": {
                            "selected_source_type": "",
                            "root": r"Y:\Genshin\AudioAssets",
                            "cache_root": "",
                            "last_export_folder": r"Y:\Exports",
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch("dyingaudio.settings.settings_path", return_value=settings_file):
                settings = load_settings()
            self.assertEqual(settings.other.selected_source_type, DEFAULT_OTHER_SOURCE_TYPE)
            self.assertEqual(settings.other.root, r"Y:\Genshin\AudioAssets")
            self.assertEqual(settings.other.last_export_folder, r"Y:\Exports")

    def test_discover_install_roots_finds_steam_library_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            steam_library = root / "SteamLibrary"
            (steam_library / "steamapps" / "common" / "Dying Light" / "Mods").mkdir(parents=True)
            (steam_library / "steamapps" / "common" / "Dying Light Developer Tools").mkdir(parents=True)
            (steam_library / "steamapps" / "common" / "Dying Light 2").mkdir(parents=True)
            (steam_library / "steamapps" / "common" / "Dying Light The Beast").mkdir(parents=True)

            with patch("dyingaudio.settings._steam_library_roots", return_value=[steam_library]):
                self.assertEqual(discover_mods_root(), steam_library / "steamapps" / "common" / "Dying Light" / "Mods")
                self.assertEqual(discover_dldt_root(), steam_library / "steamapps" / "common" / "Dying Light Developer Tools")
                self.assertEqual(discover_game_root("DL2"), steam_library / "steamapps" / "common" / "Dying Light 2")
                self.assertEqual(discover_game_root("DLTB"), steam_library / "steamapps" / "common" / "Dying Light The Beast")
                self.assertIsNone(discover_game_root("unknown"))


if __name__ == "__main__":
    unittest.main()
