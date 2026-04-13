from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dyingaudio.app import DyingAudioApp
from dyingaudio.settings import AppSettings


class AppSmokeTests(unittest.TestCase):
    def test_main_window_has_dl1_experimental_and_other_tabs(self) -> None:
        with patch("dyingaudio.app.load_settings", return_value=AppSettings()), patch("dyingaudio.app.save_settings") as _mock_save:
            app = DyingAudioApp()
        try:
            app.update_idletasks()
            tab_ids = app.notebook.tabs()
            tab_titles = [app.notebook.tab(tab_id, "text") for tab_id in tab_ids]
            self.assertEqual(tab_titles, ["Dying Light 1", "Dying Light 2 / The Beast (Experimental)", "Other"])
        finally:
            app._on_close()

    def test_loading_window_opens_and_closes(self) -> None:
        with patch("dyingaudio.app.load_settings", return_value=AppSettings()), patch("dyingaudio.app.save_settings") as _mock_save:
            app = DyingAudioApp()
        try:
            app._show_loading_window("Building mod...")
            app.update_idletasks()
            self.assertIsNotNone(app.loading_window)
            self.assertTrue(app.loading_window.winfo_exists())
            self.assertEqual(app.task_status_var.get(), "Building mod...")
            self.assertIsNotNone(app.loading_progress)
            app._close_loading_window()
            self.assertIsNone(app.loading_window)
            self.assertIsNone(app.loading_progress)
        finally:
            app._on_close()


if __name__ == "__main__":
    unittest.main()
