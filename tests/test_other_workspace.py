from __future__ import annotations

import sys
import tempfile
import time
import tkinter as tk
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dyingaudio.core.pck_workspace import (  # noqa: E402
    AKPK_SOURCE_TYPE,
    PckAudioRow,
    PckPackDescriptor,
    PckPackRows,
    PckWorkspaceIndex,
)
from dyingaudio.other_workspace import (  # noqa: E402
    OtherWorkspaceFrame,
    PckMediaGroup,
    build_pck_view_items,
)
from dyingaudio.settings import OtherSettings  # noqa: E402


def _make_descriptor(path: Path, relative_path: str) -> PckPackDescriptor:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"AKPK")
    return PckPackDescriptor(
        absolute_path=path,
        relative_path=relative_path,
        display_name=path.name,
        fingerprint=relative_path.replace("/", "_"),
        file_size=4,
        mtime_ns=1,
        header_size=32,
        bank_count=1 if "Banks" in path.name else 0,
        sound_count=1 if "Music" in path.name else 0,
        external_count=0,
        kind_summary="bank" if "Banks" in path.name else "sound",
    )


def _make_index(root: Path, descriptors: list[PckPackDescriptor]) -> PckWorkspaceIndex:
    workspace_root = root / "cache" / "workspace"
    workspace_root.mkdir(parents=True, exist_ok=True)
    metadata_path = workspace_root / "workspace.json"
    metadata_path.write_text("{}", encoding="utf-8")
    return PckWorkspaceIndex(
        source_type=AKPK_SOURCE_TYPE,
        root=root,
        cache_root=root / "cache",
        fingerprint="abc123",
        workspace_root=workspace_root,
        metadata_path=metadata_path,
        packs=descriptors,
        direct_media_lookup={},
    )


def _make_row(root: Path, row_key: str, file_id: int, duration_ms: int, samples: int, source_pack: str = "Banks0.pck") -> PckAudioRow:
    cached_path = root / f"{row_key}.wem"
    cached_path.write_bytes(b"media")
    return PckAudioRow(
        row_key=row_key,
        display_name=f"media_{file_id}.wem",
        file_id=file_id,
        playable_offset=file_id * 10,
        size=64,
        source_pack=source_pack,
        row_kind="linked_bank_media",
        cached_path=cached_path,
        duration_ms=duration_ms,
        sample_count_48k=samples,
    )


class OtherWorkspaceTests(unittest.TestCase):
    def test_build_pck_view_items_groups_matching_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = [
                _make_row(root, "a", 101, 100, 4_800),
                _make_row(root, "b", 202, 200, 9_600),
                _make_row(root, "c", 303, 100, 4_800),
            ]
            items = build_pck_view_items(rows, True)
            self.assertEqual(len(items), 2)
            self.assertIsInstance(items[0], PckMediaGroup)
            self.assertEqual([row.file_id for row in items[0].rows], [101, 303])

    def test_populate_pack_browser_is_flat_and_adds_context_for_duplicate_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            descriptors = [
                _make_descriptor(root_dir / "LocaleA" / "Banks0.pck", "LocaleA/Banks0.pck"),
                _make_descriptor(root_dir / "LocaleB" / "Banks0.pck", "LocaleB/Banks0.pck"),
            ]
            descriptors[0].display_name = "Banks0.pck (LocaleA)"
            descriptors[1].display_name = "Banks0.pck (LocaleB)"
            root = tk.Tk()
            root.withdraw()
            frame = OtherWorkspaceFrame(root, OtherSettings())
            try:
                frame.index = _make_index(root_dir, descriptors)
                frame._populate_pack_browser()
                children = frame.pack_tree.get_children()
                self.assertEqual(len(children), 2)
                self.assertEqual(frame.pack_tree.parent(children[0]), "")
                texts = [frame.pack_tree.item(child, "text") for child in children]
                self.assertEqual(texts, ["Banks0.pck (LocaleA)", "Banks0.pck (LocaleB)"])
            finally:
                frame.shutdown()
                root.destroy()

    def test_on_pack_select_loads_pack_rows_lazily(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            descriptor = _make_descriptor(root_dir / "Banks0.pck", "Banks0.pck")
            pack_rows = PckPackRows(
                descriptor=descriptor,
                rows=[_make_row(root_dir, "a", 101, 100, 4_800)],
                unresolved=[],
                summary_text="Rows: 1",
                metadata_path=root_dir / "rows.json",
            )
            root = tk.Tk()
            root.withdraw()
            frame = OtherWorkspaceFrame(root, OtherSettings())
            try:
                frame.index = _make_index(root_dir, [descriptor])
                frame._populate_pack_browser()
                if frame._pack_select_after_id is not None:
                    frame.after_cancel(frame._pack_select_after_id)
                    frame._pack_select_after_id = None
                child = frame.pack_tree.get_children()[0]
                frame.pack_tree.selection_set(child)
                with patch("dyingaudio.other_workspace.load_pck_pack_rows", return_value=pack_rows) as mocked_loader:
                    frame._on_pack_select(None)
                    timeout = time.monotonic() + 5.0
                    while frame.task_runner.is_running and time.monotonic() < timeout:
                        root.update()
                    root.update()
                mocked_loader.assert_called_once()
                self.assertIsNotNone(frame.current_pack_rows)
                self.assertEqual(frame.current_pack_rows.descriptor.relative_path, "Banks0.pck")
                self.assertIn("Banks0.pck", frame.pack_rows_cache)
            finally:
                frame.shutdown()
                root.destroy()

    def test_refresh_media_tree_groups_rows_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            descriptor = _make_descriptor(root_dir / "Banks0.pck", "Banks0.pck")
            rows = [
                _make_row(root_dir, "a", 101, 100, 4_800),
                _make_row(root_dir, "b", 202, 100, 4_800),
                _make_row(root_dir, "c", 303, 200, 9_600),
            ]
            root = tk.Tk()
            root.withdraw()
            frame = OtherWorkspaceFrame(root, OtherSettings())
            try:
                frame.current_pack_rows = PckPackRows(
                    descriptor=descriptor,
                    rows=rows,
                    unresolved=[],
                    summary_text="Rows: 3",
                    metadata_path=root_dir / "rows.json",
                )
                frame.group_similar_var.set(True)
                frame._refresh_media_tree()
                timeout = time.monotonic() + 5.0
                while frame._media_render_after_id is not None and time.monotonic() < timeout:
                    root.update()
                root.update()
                group_nodes = [iid for iid in frame.media_tree.get_children() if iid.startswith("group::")]
                self.assertEqual(len(group_nodes), 1)
                self.assertIn("groups", frame.media_count_var.get())
            finally:
                frame.shutdown()
                root.destroy()

    def test_default_pane_layout_keeps_left_browser_visible(self) -> None:
        root = tk.Tk()
        root.geometry("1500x920")
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        frame = OtherWorkspaceFrame(root, OtherSettings())
        frame.grid(row=0, column=0, sticky="nsew")
        try:
            root.update_idletasks()
            root.update()
            frame._ensure_default_pane_layout()
            root.update_idletasks()
            self.assertIsNotNone(frame.content_paned)
            assert frame.content_paned is not None
            self.assertGreaterEqual(frame.content_paned.sashpos(0), 300)
        finally:
            frame.shutdown()
            root.destroy()


if __name__ == "__main__":
    unittest.main()
