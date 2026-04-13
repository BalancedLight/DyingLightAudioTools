from __future__ import annotations

import os
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText
from typing import Callable

from dyingaudio.background import BackgroundTaskRunner, TaskCancelled, TaskProgress
from dyingaudio.core.pck_workspace import (
    AKPK_SOURCE_TYPE,
    PckAudioRow,
    PckPackDescriptor,
    PckPackRows,
    PckWorkspaceIndex,
    export_pck_media_rows,
    load_pck_pack_rows,
    scan_pck_root,
    workspace_details_text,
)
from dyingaudio.core.preview import PreviewPlayer
from dyingaudio.models import AudioEntry
from dyingaudio.settings import (
    DEFAULT_OTHER_CACHE_ROOT,
    DEFAULT_OTHER_SOURCE_TYPE,
    OtherSettings,
    application_root,
    is_windows_dark_mode,
)


MEDIA_SORT_FIELDS = ("Original Order", "ID", "File", "Offset", "Source Pack", "Kind", "Duration", "Samples", "Playable Path")


@dataclass(slots=True)
class PckMediaGroup:
    duration_ms: int
    sample_count: int
    rows: list[PckAudioRow]

    @property
    def label(self) -> str:
        count = len(self.rows)
        return f"{count} similar file{'s' if count != 1 else ''}"


@dataclass(slots=True)
class PckMediaRenderState:
    context_rows: list[PckAudioRow]
    visible_rows: list[PckAudioRow]
    view_items: list[PckAudioRow | PckMediaGroup]
    selected_iids: list[str]
    selected_row_keys: set[str]
    first_leaf_iid: str | None
    grouped_count: int
    rendered_count: int
    total_count: int
    final_count_text: str


def _row_signature(row: PckAudioRow) -> tuple[int, int]:
    return row.duration_ms, row.sample_count_48k


def _shared_text(values: list[str]) -> str:
    if not values:
        return ""
    first = values[0]
    if all(value == first for value in values[1:]):
        return first
    return "multiple"


def _media_signature_text(duration_ms: int, sample_count: int) -> str:
    return f"{duration_ms} ms / {sample_count} samples"


def matching_pck_group_rows(rows: list[PckAudioRow]) -> list[PckAudioRow]:
    if len(rows) < 2:
        return []
    expected_signature = _row_signature(rows[0])
    if any(_row_signature(row) != expected_signature for row in rows[1:]):
        return []
    return rows


def build_pck_view_items(rows: list[PckAudioRow], group_similar: bool) -> list[PckAudioRow | PckMediaGroup]:
    if not group_similar:
        return list(rows)

    items: list[PckAudioRow | PckMediaGroup] = []
    group_positions: dict[tuple[int, int], int] = {}
    for row in rows:
        signature = _row_signature(row)
        position = group_positions.get(signature)
        if position is None:
            group_positions[signature] = len(items)
            items.append(row)
            continue
        existing = items[position]
        if isinstance(existing, PckMediaGroup):
            existing.rows.append(row)
        else:
            items[position] = PckMediaGroup(duration_ms=signature[0], sample_count=signature[1], rows=[existing, row])
    return items


def filter_and_sort_pck_rows(
    rows: list[PckAudioRow],
    search_text: str,
    sort_field: str,
    descending: bool,
) -> list[PckAudioRow]:
    normalized_search = search_text.strip().lower()
    indexed_rows = list(enumerate(rows))
    if normalized_search:
        indexed_rows = [
            (index, row)
            for index, row in indexed_rows
            if normalized_search in str(row.file_id).lower()
            or normalized_search in row.display_name.lower()
            or normalized_search in str(row.playable_offset).lower()
            or normalized_search in row.source_pack.lower()
            or normalized_search in row.row_kind.lower()
            or normalized_search in str(row.cached_path).lower()
            or normalized_search in str(row.duration_ms).lower()
            or normalized_search in str(row.sample_count_48k).lower()
        ]

    if sort_field != "Original Order":
        key_map: dict[str, Callable[[tuple[int, PckAudioRow]], object]] = {
            "ID": lambda pair: pair[1].file_id,
            "File": lambda pair: pair[1].display_name.lower(),
            "Offset": lambda pair: pair[1].playable_offset,
            "Source Pack": lambda pair: pair[1].source_pack.lower(),
            "Kind": lambda pair: pair[1].row_kind.lower(),
            "Duration": lambda pair: pair[1].duration_ms,
            "Samples": lambda pair: pair[1].sample_count_48k,
            "Playable Path": lambda pair: str(pair[1].cached_path).lower(),
        }
        indexed_rows.sort(key=key_map.get(sort_field, lambda pair: pair[0]), reverse=descending)
    return [row for _index, row in indexed_rows]


class OtherWorkspaceFrame(ttk.Frame):
    def __init__(self, parent: tk.Misc, settings: OtherSettings, app: tk.Tk) -> None:
        super().__init__(parent)
        self.app = app
        self.preview_player = PreviewPlayer()
        self.index: PckWorkspaceIndex | None = None
        self.pack_rows_cache: dict[str, PckPackRows] = {}
        self.current_pack_rows: PckPackRows | None = None
        self.pack_iid_map: dict[str, PckPackDescriptor] = {}
        self.visible_rows: list[PckAudioRow] = []
        self.media_iid_rows: dict[str, list[PckAudioRow]] = {}
        self.media_group_iids: set[str] = set()
        self.task_runner = BackgroundTaskRunner(self)
        self._busy_widgets: list[tk.Widget] = []
        self.loading_window: tk.Toplevel | None = None
        self.loading_status_label: ttk.Label | None = None
        self.loading_progress: ttk.Progressbar | None = None
        self.loading_gif_label: ttk.Label | None = None
        self._loading_gif_cache: dict[int, tk.PhotoImage] = {}
        self._loading_gif_frame_count: int | None = None
        self._loading_gif_subsample = 1
        self._loading_gif_after_id: str | None = None
        self._loading_gif_frame_index = 0
        self._media_render_after_id: str | None = None
        self._media_render_state: PckMediaRenderState | None = None
        self._pack_select_after_id: str | None = None
        self.content_paned: ttk.Panedwindow | None = None
        self._pane_layout_initialized = False
        self._pane_layout_after_ids: list[str] = []
        self._last_export_folder_value = settings.last_export_folder

        self.source_type_var = tk.StringVar(value=settings.selected_source_type or DEFAULT_OTHER_SOURCE_TYPE)
        self.root_var = tk.StringVar(value=settings.root)
        self.cache_root_var = tk.StringVar(value=settings.cache_root or DEFAULT_OTHER_CACHE_ROOT)
        self.status_var = tk.StringVar(value="Other workspace idle.")
        self.workspace_var = tk.StringVar(value="Workspace: none")
        self.preview_tools_var = tk.StringVar(value=self.preview_player.environment.summary())
        self.selection_var = tk.StringVar(value="Select a .pck file to browse its media.")
        self.details_var = tk.StringVar(value="No media selected.")
        self.media_search_var = tk.StringVar()
        self.media_sort_field_var = tk.StringVar(value="Original Order")
        self.media_sort_descending_var = tk.BooleanVar(value=False)
        self.media_sort_button_var = tk.StringVar(value="Ascending")
        self.media_count_var = tk.StringVar(value="0 shown / 0 total")
        self.group_similar_var = tk.BooleanVar(value=False)
        self.task_status_var = tk.StringVar(value="No Other task running.")
        self.task_progress_var = tk.DoubleVar(value=0.0)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._build_ui()

        self.media_search_var.trace_add("write", self._on_media_filter_changed)
        self.media_sort_field_var.trace_add("write", self._on_media_filter_changed)
        self.media_sort_descending_var.trace_add("write", self._on_media_filter_changed)
        self.group_similar_var.trace_add("write", self._on_media_filter_changed)
        self._update_media_sort_controls()

    def build_settings(self) -> OtherSettings:
        return OtherSettings(
            selected_source_type=self.source_type_var.get().strip() or DEFAULT_OTHER_SOURCE_TYPE,
            root=self.root_var.get().strip(),
            cache_root=self.cache_root_var.get().strip() or DEFAULT_OTHER_CACHE_ROOT,
            last_export_folder=self._last_export_folder_value,
        )

    def shutdown(self) -> None:
        self.task_runner.cancel()
        self.task_runner.cancel_polling()
        if self._pack_select_after_id is not None:
            try:
                self.after_cancel(self._pack_select_after_id)
            except tk.TclError:
                pass
            self._pack_select_after_id = None
        self._cancel_pane_layout_callbacks()
        self._cancel_media_render()
        self.preview_player.close()

    def _build_ui(self) -> None:
        controls = ttk.LabelFrame(self, text="Other Workspace")
        controls.grid(row=0, column=0, sticky="nsew", padx=12, pady=(12, 6))
        for column in range(9):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="Source Type").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.source_type_combo = ttk.Combobox(
            controls,
            textvariable=self.source_type_var,
            values=(AKPK_SOURCE_TYPE,),
            state="readonly",
        )
        self.source_type_combo.grid(row=0, column=1, sticky="ew", padx=6, pady=6)

        ttk.Label(controls, text="Pack Root").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        self.root_entry = ttk.Entry(controls, textvariable=self.root_var)
        self.root_entry.grid(row=0, column=3, columnspan=3, sticky="ew", padx=6, pady=6)
        self.root_browse_button = ttk.Button(controls, text="Browse", command=self._browse_root)
        self.root_browse_button.grid(row=0, column=6, sticky="ew", padx=6, pady=6)
        self.open_cache_button = ttk.Button(controls, text="Open Cache Folder", command=self._open_cache_folder)
        self.open_cache_button.grid(row=0, column=7, sticky="ew", padx=6, pady=6)

        ttk.Label(controls, text="Cache Root").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        self.cache_root_entry = ttk.Entry(controls, textvariable=self.cache_root_var)
        self.cache_root_entry.grid(row=1, column=1, columnspan=3, sticky="ew", padx=6, pady=6)
        self.cache_root_browse_button = ttk.Button(controls, text="Browse", command=self._browse_cache_root)
        self.cache_root_browse_button.grid(row=1, column=4, sticky="ew", padx=6, pady=6)
        self.build_workspace_button = ttk.Button(controls, text="Build / Refresh Workspace", command=self._build_workspace)
        self.build_workspace_button.grid(row=1, column=5, sticky="ew", padx=6, pady=6)
        self.cancel_task_button = ttk.Button(controls, text="Cancel", command=self._cancel_task, state="disabled")
        self.cancel_task_button.grid(row=1, column=6, sticky="ew", padx=6, pady=6)

        ttk.Label(controls, textvariable=self.workspace_var).grid(row=2, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 4))
        ttk.Label(controls, textvariable=self.preview_tools_var).grid(row=3, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 4))
        ttk.Label(controls, textvariable=self.task_status_var).grid(row=4, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 4))

        content = ttk.Panedwindow(self, orient="horizontal")
        content.grid(row=1, column=0, sticky="nsew", padx=12, pady=6)
        self.content_paned = content

        left = ttk.LabelFrame(content, text="PCK Files")
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)
        content.add(left, weight=3)

        center = ttk.LabelFrame(content, text="Media")
        center.columnconfigure(0, weight=1)
        center.rowconfigure(1, weight=1)
        content.add(center, weight=6)

        right = ttk.Notebook(content)
        content.add(right, weight=3)

        self.pack_tree = ttk.Treeview(left, show="tree")
        self.pack_tree.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        self.pack_tree.bind("<<TreeviewSelect>>", self._on_pack_select)
        pack_scroll = ttk.Scrollbar(left, orient="vertical", command=self.pack_tree.yview)
        pack_scroll.grid(row=0, column=1, sticky="ns", pady=6)
        self.pack_tree.configure(yscrollcommand=pack_scroll.set)

        media_filter_bar = ttk.Frame(center)
        media_filter_bar.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 0))
        for column, weight in enumerate((0, 2, 0, 1, 0, 0, 0, 1)):
            media_filter_bar.columnconfigure(column, weight=weight)

        ttk.Label(media_filter_bar, text="Search").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.media_search_entry = ttk.Entry(media_filter_bar, textvariable=self.media_search_var)
        self.media_search_entry.grid(row=0, column=1, sticky="ew", padx=(0, 12))
        ttk.Label(media_filter_bar, text="Sort By").grid(row=0, column=2, sticky="w", padx=(0, 6))
        self.media_sort_combo = ttk.Combobox(
            media_filter_bar,
            textvariable=self.media_sort_field_var,
            values=MEDIA_SORT_FIELDS,
            state="readonly",
            width=18,
        )
        self.media_sort_combo.grid(row=0, column=3, sticky="ew", padx=(0, 8))
        self.media_sort_order_button = ttk.Button(
            media_filter_bar,
            textvariable=self.media_sort_button_var,
            command=self._toggle_media_sort_direction,
        )
        self.media_sort_order_button.grid(row=0, column=4, sticky="ew", padx=(0, 8))
        ttk.Button(media_filter_bar, text="Clear Search", command=self._clear_media_search).grid(
            row=0, column=5, sticky="ew", padx=(0, 8)
        )
        ttk.Checkbutton(media_filter_bar, text="Group similar audio", variable=self.group_similar_var).grid(
            row=0, column=6, sticky="w", padx=(0, 8)
        )
        ttk.Label(media_filter_bar, textvariable=self.media_count_var).grid(row=0, column=7, sticky="e")

        media_tree_frame = ttk.Frame(center)
        media_tree_frame.grid(row=1, column=0, sticky="nsew", padx=6, pady=(6, 0))
        media_tree_frame.columnconfigure(0, weight=1)
        media_tree_frame.rowconfigure(0, weight=1)

        self.media_tree = ttk.Treeview(
            media_tree_frame,
            columns=("file_id", "offset", "source_pack", "kind", "duration", "samples", "path"),
            show="tree headings",
            height=18,
        )
        self.media_tree.grid(row=0, column=0, sticky="nsew")
        self.media_tree.bind("<<TreeviewSelect>>", self._on_media_select)
        self.media_tree.heading("#0", text="File / Group", command=lambda: self._set_media_sort_field_from_heading("File"))
        self.media_tree.column("#0", width=260, anchor="w")
        for column, title, width in (
            ("file_id", "ID", 150),
            ("offset", "Offset", 110),
            ("source_pack", "Source Pack", 190),
            ("kind", "Kind", 140),
            ("duration", "Duration", 110),
            ("samples", "Samples", 110),
            ("path", "Playable Path", 420),
        ):
            self.media_tree.heading(column, text=title, command=lambda value=title: self._set_media_sort_field_from_heading(value))
            self.media_tree.column(column, width=width, anchor="w")
        media_scroll_y = ttk.Scrollbar(media_tree_frame, orient="vertical", command=self.media_tree.yview)
        media_scroll_y.grid(row=0, column=1, sticky="ns")
        self.media_tree.configure(yscrollcommand=media_scroll_y.set)
        media_scroll_x = ttk.Scrollbar(media_tree_frame, orient="horizontal", command=self.media_tree.xview)
        media_scroll_x.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.media_tree.configure(xscrollcommand=media_scroll_x.set)

        self.media_context_menu = tk.Menu(self, tearoff=False)
        self.media_context_menu.add_command(label="Export Selected Media...", command=self._export_selected_media)
        self.media_context_menu.add_command(label="Export Mixed Audio...", command=self._export_selected_media_mixed)
        self.media_tree.bind("<Button-3>", self._show_media_context_menu)
        self.media_tree.bind("<Shift-F10>", self._show_media_context_menu)

        center_actions = ttk.Frame(center)
        center_actions.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 6))
        for column in range(2):
            center_actions.columnconfigure(column, weight=1)
        self.export_media_button = ttk.Button(center_actions, text="Export Selected Media", command=self._export_selected_media)
        self.export_media_button.grid(row=0, column=0, sticky="ew", padx=2)
        self.export_mixed_button = ttk.Button(center_actions, text="Export Mixed Audio", command=self._export_selected_media_mixed)
        self.export_mixed_button.grid(row=0, column=1, sticky="ew", padx=2)

        preview_tab = ttk.Frame(right, padding=6)
        preview_tab.columnconfigure(0, weight=1)
        right.add(preview_tab, text="Preview")
        ttk.Label(preview_tab, textvariable=self.selection_var, wraplength=360).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))
        self.play_button = ttk.Button(preview_tab, text="Play Selected", command=self._play_selected)
        self.play_button.grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(0, 6))
        self.stop_button = ttk.Button(preview_tab, text="Stop", command=self._stop_preview)
        self.stop_button.grid(row=1, column=1, sticky="ew", pady=(0, 6))
        self.play_all_button = ttk.Button(preview_tab, text="Play Selected Together", command=self._play_selected_together)
        self.play_all_button.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Label(preview_tab, textvariable=self.details_var, wraplength=360, justify="left").grid(row=3, column=0, columnspan=2, sticky="w")

        details_tab = ttk.Frame(right, padding=6)
        details_tab.columnconfigure(0, weight=1)
        details_tab.rowconfigure(0, weight=1)
        right.add(details_tab, text="Details")
        self.details_text = ScrolledText(details_tab, wrap="word", height=18)
        self.details_text.grid(row=0, column=0, sticky="nsew")
        self.details_text.configure(state="disabled")

        logs_tab = ttk.Frame(right, padding=6)
        logs_tab.columnconfigure(0, weight=1)
        logs_tab.rowconfigure(0, weight=1)
        right.add(logs_tab, text="Logs")
        self.logs_text = ScrolledText(logs_tab, wrap="word", height=18)
        self.logs_text.grid(row=0, column=0, sticky="nsew")
        self.logs_text.configure(state="disabled")

        self._busy_widgets = [
            self.source_type_combo,
            self.root_entry,
            self.root_browse_button,
            self.cache_root_entry,
            self.cache_root_browse_button,
            self.build_workspace_button,
            self.media_search_entry,
            self.media_sort_combo,
            self.media_sort_order_button,
            self.export_media_button,
            self.export_mixed_button,
            self.play_button,
            self.stop_button,
        ]
        self._update_preview_action_controls()
        self._pane_layout_after_ids.append(self.after_idle(self._ensure_default_pane_layout))
        self._pane_layout_after_ids.append(self.after(100, self._ensure_default_pane_layout))
        self._pane_layout_after_ids.append(self.after(500, self._ensure_default_pane_layout))
        self.bind("<Configure>", self._on_frame_configure)
        self.bind("<Visibility>", self._on_frame_configure, add="+")

    def _append_status(self, message: str) -> None:
        self.status_var.set(message)

    def _cancel_pane_layout_callbacks(self) -> None:
        for after_id in self._pane_layout_after_ids:
            try:
                self.after_cancel(after_id)
            except tk.TclError:
                pass
        self._pane_layout_after_ids.clear()

    def _on_frame_configure(self, _event: object) -> None:
        self._ensure_default_pane_layout()

    def _ensure_default_pane_layout(self) -> None:
        if self.content_paned is None:
            return
        try:
            total_width = self.content_paned.winfo_width()
            if total_width <= 1:
                return

            left_width = max(320, int(total_width * 0.26))
            middle_width = max(500, int(total_width * 0.46))
            right_min_start = total_width - 320
            first_sash = self.content_paned.sashpos(0)
            second_sash = self.content_paned.sashpos(1)

            if (not self._pane_layout_initialized) or first_sash < 240 or second_sash <= first_sash + 200:
                first_target = min(left_width, total_width - 860)
                first_target = max(320, first_target)
                second_target = min(max(first_target + middle_width, first_target + 500), right_min_start)
                second_target = max(first_target + 500, second_target)
                self.content_paned.sashpos(0, first_target)
                self.content_paned.sashpos(1, second_target)
                self._pane_layout_initialized = True
        except tk.TclError:
            return

    def _set_text_widget(self, widget: ScrolledText, text: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert("1.0", text)
        widget.configure(state="disabled")

    def _append_log(self, text: str) -> None:
        self.logs_text.configure(state="normal")
        if self.logs_text.index("end-1c") != "1.0":
            self.logs_text.insert(tk.END, "\n")
        self.logs_text.insert(tk.END, text)
        self.logs_text.see(tk.END)
        self.logs_text.configure(state="disabled")

    def _persist_settings(self) -> None:
        app = self.winfo_toplevel()
        save_settings = getattr(app, "_save_settings", None)
        if callable(save_settings):
            save_settings()

    def _popup_context_menu(self, menu: tk.Menu, widget: tk.Misc, event: object) -> None:
        x_root = getattr(event, "x_root", None)
        y_root = getattr(event, "y_root", None)
        if x_root is None or y_root is None:
            x_root = widget.winfo_rootx() + 20
            y_root = widget.winfo_rooty() + 20
        try:
            menu.tk_popup(x_root, y_root)
        finally:
            menu.grab_release()

    def _cancel_loading_animation(self) -> None:
        if self._loading_gif_after_id is None:
            return
        try:
            self.after_cancel(self._loading_gif_after_id)
        except tk.TclError:
            pass
        self._loading_gif_after_id = None

    def _animate_loading_gif(self) -> None:
        self._loading_gif_after_id = None
        if self.loading_gif_label is None:
            return
        frame = self._load_loading_gif_frame(self._loading_gif_frame_index)
        if frame is None:
            self.loading_gif_label.configure(text="Working...")
            self.loading_gif_label.image = None
            return
        self.loading_gif_label.configure(image=frame)
        self.loading_gif_label.image = frame
        next_index = self._loading_gif_frame_index + 1
        if self._load_loading_gif_frame(next_index) is None:
            next_index = 0
        self._loading_gif_frame_index = next_index
        self._loading_gif_after_id = self.after(80, self._animate_loading_gif)

    def _load_loading_gif_frame(self, index: int) -> tk.PhotoImage | None:
        if index < 0:
            return None
        if self._loading_gif_frame_count is not None and index >= self._loading_gif_frame_count:
            return None
        if index in self._loading_gif_cache:
            return self._loading_gif_cache[index]
        gif_path = application_root() / "assets" / "MovingGears.gif"
        if not gif_path.exists():
            self._loading_gif_frame_count = 0
            return None
        try:
            frame = tk.PhotoImage(file=str(gif_path), format=f"gif -index {index}")
        except tk.TclError:
            self._loading_gif_frame_count = index
            return None

        if not self._loading_gif_cache:
            max_dimension = max(frame.width(), frame.height())
            self._loading_gif_subsample = max(1, (max_dimension + 159) // 160)
        if self._loading_gif_subsample > 1:
            frame = frame.subsample(self._loading_gif_subsample, self._loading_gif_subsample)
        self._loading_gif_cache[index] = frame
        return frame

    def _center_child_window(self, window: tk.Toplevel, width: int, height: int) -> None:
        if not window.winfo_exists():
            return
        try:
            self.update_idletasks()
            window.update_idletasks()
            root_x = self.winfo_rootx()
            root_y = self.winfo_rooty()
            root_width = self.winfo_width()
            root_height = self.winfo_height()
            x = root_x + max(0, (root_width - width) // 2)
            y = root_y + max(0, (root_height - height) // 2)
            window.geometry(f"{width}x{height}+{x}+{y}")
        except tk.TclError:
            return

    def _show_loading_window(self, message: str) -> None:
        if self.loading_window is not None and self.loading_window.winfo_exists():
            self.task_status_var.set(message)
            self._center_loading_window()
            try:
                self.loading_window.deiconify()
                self.loading_window.lift()
                self.loading_window.update_idletasks()
            except tk.TclError:
                self._close_loading_window()
            return

        window = tk.Toplevel(self)
        window.title("DyingAudio Progress")
        window.transient(self)
        window.minsize(340, 240)
        window.resizable(False, False)
        window.protocol("WM_DELETE_WINDOW", lambda: None)
        if is_windows_dark_mode():
            window.configure(bg="#1e1e1e")
        window.columnconfigure(0, weight=1)
        window.rowconfigure(0, weight=1)

        container = ttk.Frame(window, padding=18)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=0)
        container.rowconfigure(1, weight=1)
        container.rowconfigure(2, weight=0)

        status_frame = ttk.Frame(container, height=68)
        status_frame.grid(row=0, column=0, sticky="ew", pady=(0, 14))
        status_frame.columnconfigure(0, weight=1)
        status_frame.grid_propagate(False)

        status_label = ttk.Label(status_frame, textvariable=self.task_status_var, anchor="center", justify="center", wraplength=320)
        status_label.grid(row=0, column=0, sticky="nsew")

        gif_frame = ttk.Frame(container, width=160, height=160)
        gif_frame.grid(row=1, column=0, sticky="n", pady=(0, 14))
        gif_frame.grid_propagate(False)

        gif_label = ttk.Label(gif_frame, anchor="center")
        gif_label.place(relx=0.5, rely=0.5, anchor="center")

        progress = ttk.Progressbar(container, maximum=100, variable=self.task_progress_var)
        progress.grid(row=2, column=0, sticky="ew")

        self.loading_window = window
        self.loading_status_label = status_label
        self.loading_progress = progress
        self.loading_gif_label = gif_label
        self.task_status_var.set(message)

        self._loading_gif_frame_index = 0
        self._cancel_loading_animation()
        self._animate_loading_gif()

        window.bind("<Configure>", self._on_loading_window_configure)
        self._center_loading_window()
        try:
            if window.winfo_exists():
                window.lift()
                window.update_idletasks()
                window.focus_set()
        except tk.TclError:
            self._close_loading_window()

    def _center_loading_window(self) -> None:
        if self.loading_window is None or not self.loading_window.winfo_exists():
            return
        width = max(self.loading_window.winfo_width(), self.loading_window.winfo_reqwidth(), 420)
        height = max(self.loading_window.winfo_height(), self.loading_window.winfo_reqheight(), 300)
        self._center_child_window(self.loading_window, width, height)

    def _on_loading_window_configure(self, event: object) -> None:
        if self.loading_status_label is None or not hasattr(event, "width"):
            return
        width = max(220, int(event.width) - 36)
        self.loading_status_label.configure(wraplength=width)

    def _close_loading_window(self) -> None:
        self._cancel_loading_animation()
        if self.loading_progress is not None:
            self.loading_progress.stop()
        if self.loading_window is not None and self.loading_window.winfo_exists():
            self.loading_window.destroy()
        self.loading_window = None
        self.loading_status_label = None
        self.loading_progress = None
        self.loading_gif_label = None

    def _set_task_busy(self, busy: bool) -> None:
        for widget in self._busy_widgets:
            widget.configure(state="disabled" if busy else "normal")
        self.cancel_task_button.configure(state="normal" if busy else "disabled")
        self.source_type_combo.configure(state="disabled" if busy else "readonly")
        self.media_sort_combo.configure(state="disabled" if busy else "readonly")

    def _apply_task_progress(self, progress: TaskProgress) -> None:
        self.task_status_var.set(progress.message or "Working...")
        if self.loading_progress is None:
            return
        if progress.is_determinate:
            self.loading_progress.stop()
            self.loading_progress.configure(mode="determinate")
            self.task_progress_var.set(progress.percent)
        else:
            self.task_progress_var.set(0.0)
            self.loading_progress.configure(mode="indeterminate")
            self.loading_progress.start(15)

    def _finish_task_ui(self) -> None:
        if self.loading_progress is not None:
            self.loading_progress.stop()
            self.loading_progress.configure(mode="determinate")
        self.task_progress_var.set(0.0)
        self._close_loading_window()
        self._set_task_busy(False)

    def _cancel_task(self) -> None:
        if not self.task_runner.is_running:
            return
        self.task_status_var.set("Cancelling task...")
        self._append_status("Cancelling Other task...")
        self.task_runner.cancel()

    def _run_task(
        self,
        *,
        start_message: str,
        error_title: str,
        worker: callable,
        on_success: callable,
    ) -> None:
        if self.task_runner.is_running:
            messagebox.showinfo("Other workspace busy", "Wait for the current Other workspace task to finish first.")
            return
        self._set_task_busy(True)
        self.task_status_var.set(start_message)
        self._append_status(start_message)
        self._set_text_widget(self.logs_text, start_message)
        self._show_loading_window(start_message)

        def handle_error(exc: BaseException, details: str) -> None:
            if isinstance(exc, TaskCancelled):
                self._append_log("Task cancelled.")
                self._append_status("Other task cancelled.")
                self.task_status_var.set("Cancelled.")
                return
            self._set_text_widget(self.logs_text, details)
            self.app._show_error_window(error_title, str(exc))
            self._append_status(error_title.replace(" failed", " failed."))

        self.task_runner.start(
            worker,
            on_progress=self._apply_task_progress,
            on_log=lambda message: (self._append_status(message), self._append_log(message)),
            on_success=on_success,
            on_error=handle_error,
            on_finally=self._finish_task_ui,
        )

    def _resolve_root(self) -> Path | None:
        current = self.root_var.get().strip()
        if not current:
            return None
        candidate = Path(current).expanduser()
        if not candidate.exists():
            return None
        return candidate.resolve()

    def _resolve_cache_root(self) -> Path:
        current = self.cache_root_var.get().strip()
        if current:
            return Path(current).expanduser().resolve()
        return Path(DEFAULT_OTHER_CACHE_ROOT).expanduser().resolve()

    def _browse_root(self) -> None:
        initial = self._resolve_root() or Path.cwd()
        selection = filedialog.askdirectory(title="Select PCK root", initialdir=str(initial))
        if not selection:
            return
        self.root_var.set(str(Path(selection).resolve()))
        self._persist_settings()

    def _browse_cache_root(self) -> None:
        initial = self._resolve_cache_root()
        selection = filedialog.askdirectory(title="Select Other cache root", initialdir=str(initial))
        if not selection:
            return
        self.cache_root_var.set(str(Path(selection).resolve()))
        self._persist_settings()

    def _open_cache_folder(self) -> None:
        target = self.index.workspace_root if self.index is not None else self._resolve_cache_root()
        target.mkdir(parents=True, exist_ok=True)
        os.startfile(str(target))

    def _build_workspace(self) -> None:
        root_path = self._resolve_root()
        if root_path is None:
            messagebox.showinfo("Build workspace", "Select the folder containing the .pck files first.")
            return
        cache_root = str(self._resolve_cache_root())
        source_type = self.source_type_var.get().strip() or AKPK_SOURCE_TYPE
        self.root_var.set(str(root_path))
        self.cache_root_var.set(cache_root)
        self._persist_settings()

        def worker(progress, log):
            return scan_pck_root(
                source_type,
                root_path,
                cache_root,
                log,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            )

        def on_success(result: object) -> None:
            self.index = result if isinstance(result, PckWorkspaceIndex) else None
            self.pack_rows_cache.clear()
            self.current_pack_rows = None
            if self.index is None:
                return
            self.workspace_var.set(f"Workspace: {self.index.workspace_root}")
            self.task_status_var.set("Workspace build complete.")
            self._append_status(f"Indexed {len(self.index.packs)} .pck file(s).")
            self._populate_pack_browser()
            self._refresh_logs()
            self._persist_settings()

        self._run_task(
            start_message="Building Other workspace...",
            error_title="Build workspace failed",
            worker=worker,
            on_success=on_success,
        )

    def _populate_pack_browser(self) -> None:
        self.pack_tree.delete(*self.pack_tree.get_children())
        self.pack_iid_map.clear()
        self.current_pack_rows = None
        self._cancel_media_render()
        self.media_tree.delete(*self.media_tree.get_children())
        self.media_iid_rows.clear()
        self.media_group_iids.clear()
        self.visible_rows = []
        self.media_count_var.set("0 shown / 0 total")
        self.selection_var.set("Select a .pck file to browse its media.")
        self.details_var.set("No media selected.")
        self._set_text_widget(self.details_text, workspace_details_text(self.index) if self.index is not None else "")
        if self.index is None:
            return

        first_iid: str | None = None
        for descriptor in self.index.packs:
            iid = f"pack::{descriptor.relative_path}"
            self.pack_tree.insert("", "end", iid=iid, text=descriptor.display_name)
            self.pack_iid_map[iid] = descriptor
            if first_iid is None:
                first_iid = iid

        if first_iid is not None:
            self.pack_tree.selection_set(first_iid)
            self.pack_tree.focus(first_iid)
            self.pack_tree.see(first_iid)
            if self._pack_select_after_id is not None:
                try:
                    self.after_cancel(self._pack_select_after_id)
                except tk.TclError:
                    pass
            self._pack_select_after_id = self.after_idle(self._run_initial_pack_selection)

    def _run_initial_pack_selection(self) -> None:
        self._pack_select_after_id = None
        self._on_pack_select(None)

    def _selected_descriptor(self) -> PckPackDescriptor | None:
        selection = self.pack_tree.selection()
        if not selection:
            return None
        return self.pack_iid_map.get(selection[0])

    def _on_pack_select(self, _event: object | None) -> None:
        descriptor = self._selected_descriptor()
        if descriptor is None:
            return
        self.selection_var.set(descriptor.relative_path)
        cached = self.pack_rows_cache.get(descriptor.relative_path)
        if cached is not None:
            self.current_pack_rows = cached
            self._set_text_widget(self.details_text, cached.summary_text)
            self._refresh_media_tree()
            self._refresh_logs()
            return
        if self.task_runner.is_running or self.index is None:
            return

        def worker(progress, log):
            return load_pck_pack_rows(
                self.index,
                descriptor,
                log,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            )

        def on_success(result: object) -> None:
            pack_rows = result if isinstance(result, PckPackRows) else None
            if pack_rows is None:
                return
            self.pack_rows_cache[pack_rows.descriptor.relative_path] = pack_rows
            current = self._selected_descriptor()
            if current is None or current.relative_path != pack_rows.descriptor.relative_path:
                return
            self.current_pack_rows = pack_rows
            self._set_text_widget(self.details_text, pack_rows.summary_text)
            self._refresh_media_tree()
            self._refresh_logs()

        self._run_task(
            start_message=f"Loading {descriptor.display_name}...",
            error_title="Load pack failed",
            worker=worker,
            on_success=on_success,
        )

    def _refresh_logs(self) -> None:
        if self.current_pack_rows is not None:
            lines = [self.current_pack_rows.summary_text]
            if self.current_pack_rows.unresolved:
                lines.extend(["", "Unresolved items:"])
                for item in self.current_pack_rows.unresolved[:200]:
                    bits = [item.source_pack]
                    if item.bank_name:
                        bits.append(item.bank_name)
                    if item.object_id is not None:
                        bits.append(f"object {item.object_id}")
                    bits.append(item.note)
                    lines.append(" | ".join(bits))
                if len(self.current_pack_rows.unresolved) > 200:
                    lines.append(f"... {len(self.current_pack_rows.unresolved) - 200} more unresolved item(s)")
            self._set_text_widget(self.logs_text, "\n".join(lines))
            return
        if self.index is None:
            self._set_text_widget(self.logs_text, "No Other workspace loaded.")
            return
        self._set_text_widget(self.logs_text, workspace_details_text(self.index))

    def _on_media_filter_changed(self, *_args: object) -> None:
        if self.current_pack_rows is None:
            return
        self._refresh_media_tree()

    def _clear_media_search(self) -> None:
        self.media_search_var.set("")

    def _toggle_media_sort_direction(self) -> None:
        self.media_sort_descending_var.set(not self.media_sort_descending_var.get())
        self._update_media_sort_controls()

    def _update_media_sort_controls(self) -> None:
        self.media_sort_button_var.set("Descending" if self.media_sort_descending_var.get() else "Ascending")

    def _set_media_sort_field_from_heading(self, heading: str) -> None:
        if heading == "Duration":
            self.media_sort_field_var.set("Duration")
        elif heading == "Samples":
            self.media_sort_field_var.set("Samples")
        else:
            self.media_sort_field_var.set(heading)

    def _media_row_key(self, row: PckAudioRow) -> str:
        return row.row_key

    def _media_leaf_values(self, row: PckAudioRow) -> tuple[str, str, str, str, str, str, str]:
        return (
            str(row.file_id),
            str(row.playable_offset),
            row.source_pack,
            row.row_kind,
            str(row.duration_ms),
            str(row.sample_count_48k),
            str(row.cached_path),
        )

    def _media_group_values(self, group: PckMediaGroup) -> tuple[str, str, str, str, str, str, str]:
        return (
            "",
            "",
            _shared_text([row.source_pack for row in group.rows]),
            _shared_text([row.row_kind for row in group.rows]),
            str(group.duration_ms),
            str(group.sample_count),
            _media_signature_text(group.duration_ms, group.sample_count),
        )

    def _refresh_media_tree(self) -> None:
        selected_iids = list(self.media_tree.selection())
        selected_row_keys: set[str] = set()
        for iid in selected_iids:
            for row in self.media_iid_rows.get(iid, []):
                selected_row_keys.add(self._media_row_key(row))

        self._cancel_media_render()
        self.media_tree.delete(*self.media_tree.get_children())
        self.media_iid_rows.clear()
        self.media_group_iids.clear()
        self.visible_rows = []

        if self.current_pack_rows is None:
            self.media_count_var.set("0 shown / 0 total")
            self._update_preview_action_controls()
            return

        context_rows = self.current_pack_rows.rows
        visible_rows = filter_and_sort_pck_rows(
            context_rows,
            self.media_search_var.get(),
            self.media_sort_field_var.get(),
            self.media_sort_descending_var.get(),
        )
        self.visible_rows = visible_rows

        if not context_rows:
            self.media_count_var.set("0 shown / 0 total")
            self.details_var.set("No media rows matched this pack.")
            self._set_text_widget(self.details_text, self.current_pack_rows.summary_text)
            self._update_preview_action_controls()
            return

        view_items = build_pck_view_items(visible_rows, self.group_similar_var.get())
        grouped_count = sum(1 for item in view_items if isinstance(item, PckMediaGroup))
        count_text = f"{len(visible_rows)} shown / {len(context_rows)} total"
        if self.group_similar_var.get():
            count_text = f"{count_text}, {grouped_count} groups"
        self.media_count_var.set(f"Loading {count_text}...")
        self.details_var.set("Loading media rows...")
        self._media_render_state = PckMediaRenderState(
            context_rows=context_rows,
            visible_rows=visible_rows,
            view_items=view_items,
            selected_iids=selected_iids,
            selected_row_keys=selected_row_keys,
            first_leaf_iid=None,
            grouped_count=grouped_count,
            rendered_count=0,
            total_count=len(context_rows),
            final_count_text=count_text,
        )
        self._media_render_after_id = self.after_idle(self._render_media_tree_chunk)

    def _cancel_media_render(self) -> None:
        if self._media_render_after_id is not None:
            try:
                self.after_cancel(self._media_render_after_id)
            except tk.TclError:
                pass
        self._media_render_after_id = None
        self._media_render_state = None

    def _render_media_tree_chunk(self) -> None:
        state = self._media_render_state
        self._media_render_after_id = None
        if state is None:
            return

        batch_size = 80
        end_index = min(state.rendered_count + batch_size, len(state.view_items))
        for item in state.view_items[state.rendered_count:end_index]:
            if isinstance(item, PckMediaGroup):
                group_iid = f"group::{item.duration_ms}::{item.sample_count}::{state.rendered_count}"
                self.media_group_iids.add(group_iid)
                self.media_iid_rows[group_iid] = list(item.rows)
                self.media_tree.insert("", "end", iid=group_iid, text=item.label, values=self._media_group_values(item))
                for row in item.rows:
                    leaf_iid = f"media::{self._media_row_key(row)}"
                    self.media_iid_rows[leaf_iid] = [row]
                    self.media_tree.insert(group_iid, "end", iid=leaf_iid, text=row.display_name, values=self._media_leaf_values(row))
                    if state.first_leaf_iid is None:
                        state.first_leaf_iid = leaf_iid
            else:
                leaf_iid = f"media::{self._media_row_key(item)}"
                self.media_iid_rows[leaf_iid] = [item]
                self.media_tree.insert("", "end", iid=leaf_iid, text=item.display_name, values=self._media_leaf_values(item))
                if state.first_leaf_iid is None:
                    state.first_leaf_iid = leaf_iid

        state.rendered_count = end_index
        if state.rendered_count < len(state.view_items):
            self.media_count_var.set(f"Loading {state.final_count_text}...")
            self._media_render_after_id = self.after(1, self._render_media_tree_chunk)
            return

        self._media_render_state = None
        self.media_count_var.set(state.final_count_text)
        restored_any = False
        for iid in state.selected_iids:
            if self.media_tree.exists(iid):
                self.media_tree.selection_add(iid)
                restored_any = True
        if not restored_any and state.selected_row_keys:
            for iid, rows in self.media_iid_rows.items():
                if any(self._media_row_key(row) in state.selected_row_keys for row in rows):
                    self.media_tree.selection_add(iid)
                    restored_any = True

        current_selection = list(self.media_tree.selection())
        if current_selection:
            self.media_tree.focus(current_selection[0])
            self.media_tree.see(current_selection[0])
        elif state.first_leaf_iid is not None:
            self.media_tree.selection_set(state.first_leaf_iid)
            self.media_tree.focus(state.first_leaf_iid)
            self.media_tree.see(state.first_leaf_iid)

        if self.media_tree.selection():
            self._on_media_select(None)
        else:
            self.details_var.set("No media rows matched this pack.")
        self._update_preview_action_controls()

    def _selected_rows(self) -> list[PckAudioRow]:
        rows: list[PckAudioRow] = []
        seen_keys: set[str] = set()
        for item_id in self.media_tree.selection():
            for row in self.media_iid_rows.get(item_id, []):
                row_key = self._media_row_key(row)
                if row_key in seen_keys:
                    continue
                seen_keys.add(row_key)
                rows.append(row)
        return rows

    def _on_media_select(self, _event: object | None) -> None:
        rows = self._selected_rows()
        if not rows:
            if self.visible_rows:
                rows = [self.visible_rows[0]]
            else:
                self.details_var.set("No media selected.")
                return
        row = rows[0]
        duration_ms, sample_count = _row_signature(row)
        selected_is_group = any(item_id in self.media_group_iids for item_id in self.media_tree.selection())

        if selected_is_group and len(rows) > 1:
            self.details_var.set(f"{len(rows)} similar files\n{_media_signature_text(duration_ms, sample_count)}")
            detail_lines = [
                f"Similar audio group: {len(rows)} file(s)",
                f"Signature: {_media_signature_text(duration_ms, sample_count)}",
                f"Source pack: {_shared_text([item.source_pack for item in rows])}",
                f"Kind: {_shared_text([item.row_kind for item in rows])}",
                "",
                "Members:",
            ]
            for item in rows[:200]:
                detail_lines.append(
                    f"{item.display_name} | ID {item.file_id} | Offset {item.playable_offset} | {item.source_pack}"
                )
            if len(rows) > 200:
                detail_lines.append(f"... {len(rows) - 200} more grouped file(s)")
        else:
            self.details_var.set(
                f"ID {row.file_id}\nPack: {row.source_pack}\nKind: {row.row_kind}\nFile: {row.display_name}\nSignature: {_media_signature_text(duration_ms, sample_count)}"
            )
            detail_lines = [
                f"File: {row.display_name}",
                f"ID: {row.file_id}",
                f"Offset: {row.playable_offset}",
                f"Source pack: {row.source_pack}",
                f"Kind: {row.row_kind}",
                f"Duration: {duration_ms} ms",
                f"Samples: {sample_count}",
                f"Playable file: {row.cached_path}",
                "",
                "Origins:",
            ]
            for origin in row.origins:
                bits = [origin.source_kind, origin.source_pack]
                if origin.bank_name:
                    bits.append(origin.bank_name)
                if origin.object_id is not None:
                    bits.append(f"object {origin.object_id}")
                if origin.note:
                    bits.append(origin.note)
                detail_lines.append(" | ".join(bits))
        self._set_text_widget(self.details_text, "\n".join(detail_lines))
        self._update_preview_action_controls()

    def _selected_group_preview_rows(self) -> list[PckAudioRow]:
        return matching_pck_group_rows(self._selected_rows())

    def _selected_preview_entry(self) -> AudioEntry | None:
        rows = self._selected_rows()
        if not rows:
            return None
        row = rows[0]
        return AudioEntry(
            entry_name=row.display_name,
            source_mode="raw",
            source_path=str(row.cached_path),
            duration_ms=row.duration_ms,
            sample_count=row.sample_count_48k,
        )

    def _update_preview_action_controls(self) -> None:
        group_rows = self._selected_group_preview_rows()
        if len(group_rows) < 2:
            self.play_all_button.grid_remove()
            return
        if self.preview_player.environment.ffmpeg_path is None:
            self.play_all_button.configure(state="disabled")
        else:
            self.play_all_button.configure(state="normal")
        self.play_all_button.grid()

    def _play_selected(self) -> None:
        entry = self._selected_preview_entry()
        if entry is None:
            messagebox.showinfo("Preview media", "Select a media row to preview first.")
            return
        try:
            preview_path = self.preview_player.play_entry(entry, self._append_status)
        except Exception as exc:
            self.app._show_error_window("Preview failed", str(exc))
            self._append_status("Other preview failed.")
            return
        self._append_status(f"Previewing {preview_path.name}.")

    def _play_selected_together(self) -> None:
        rows = self._selected_group_preview_rows()
        if len(rows) < 2:
            messagebox.showinfo("Preview media", "Select two or more matching media rows first.")
            return
        if self.preview_player.environment.ffmpeg_path is None:
            self.app._show_error_window("Preview failed", "FFmpeg is required to mix multiple audio files together.")
            return
        try:
            preview_path = self.preview_player.play_combined_sources([row.cached_path for row in rows], self._append_status)
        except Exception as exc:
            self.app._show_error_window("Preview failed", str(exc))
            self._append_status("Other group preview failed.")
            return
        self._append_status(f"Previewing {len(rows)} files together from {preview_path.name}.")

    def _stop_preview(self) -> None:
        self.preview_player.stop()
        self._append_status("Other preview stopped.")

    def _show_media_context_menu(self, event: object) -> str | None:
        y = getattr(event, "y", None)
        node_id = self.media_tree.identify_row(y) if y is not None else ""
        if node_id:
            self.media_tree.selection_set(node_id)
            self.media_tree.focus(node_id)
            self._on_media_select(event)
        self._popup_context_menu(self.media_context_menu, self.media_tree, event)
        return "break"

    def _ask_export_directory(self, title: str) -> Path | None:
        initial = self._last_export_folder_value or (
            str(self.index.workspace_root) if self.index is not None else str(self._resolve_cache_root())
        )
        selection = filedialog.askdirectory(title=title, initialdir=initial)
        if not selection:
            return None
        resolved = Path(selection).resolve()
        self._last_export_folder_value = str(resolved)
        self._persist_settings()
        return resolved

    def _export_selected_media(self) -> None:
        rows = self._selected_rows()
        if not rows:
            messagebox.showinfo("Export selected media", "Select one or more media rows first.")
            return
        destination = self._ask_export_directory("Export selected media")
        if destination is None:
            return
        self._run_task(
            start_message=f"Exporting {len(rows)} selected media file(s)...",
            error_title="Export selected media failed",
            worker=lambda progress, _log: export_pck_media_rows(
                rows,
                destination,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            ),
            on_success=lambda result: self._append_status(
                f"Exported {len(result) if isinstance(result, list) else 0} media file(s) to {destination}."
            ),
        )

    def _mixed_audio_export_name(self, rows: list[PckAudioRow]) -> str:
        first = rows[0]
        base_parts = [Path(first.source_pack).stem, str(first.file_id), "mixed"]
        raw_name = "_".join(part for part in base_parts if part)
        safe_name = "".join(character if character.isalnum() or character in {"_", "-", "."} else "_" for character in raw_name)
        return f"{safe_name}.wav"

    def _export_selected_media_mixed(self) -> None:
        rows = self._selected_group_preview_rows()
        if len(rows) < 2:
            messagebox.showinfo("Export mixed audio", "Select two or more matching media rows first.")
            return
        if self.preview_player.environment.ffmpeg_path is None:
            self.app._show_error_window("Export mixed audio failed", "FFmpeg is required to mix multiple audio files together.")
            return

        destination_root = self._ask_export_directory("Export mixed audio")
        if destination_root is None:
            return
        destination = destination_root / self._mixed_audio_export_name(rows)
        self._run_task(
            start_message=f"Exporting mixed audio from {len(rows)} file(s)...",
            error_title="Export mixed audio failed",
            worker=lambda _progress, log: self.preview_player.export_combined_sources(
                [row.cached_path for row in rows],
                destination,
                log,
            ),
            on_success=lambda result: self._append_status(f"Exported mixed audio to {result}."),
        )
