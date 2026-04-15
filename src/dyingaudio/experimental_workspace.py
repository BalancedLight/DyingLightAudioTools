from __future__ import annotations

import hashlib
import os
import shutil
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, ttk
from tkinter.scrolledtext import ScrolledText
from typing import Callable

from dyingaudio.background import BackgroundTaskRunner, TaskCancelled, TaskProgress
from dyingaudio.core.preview import PreviewPlayer
from dyingaudio.core.wwise_audio_type import audio_type_label
from dyingaudio.core.wwise_workspace import (
    BASE_ARCHIVE_SET,
    DL2_GAME,
    DLTB_GAME,
    ArchiveSetDescriptor,
    NamedAudioLink,
    WwiseWorkspace,
    build_or_load_workspace,
    detect_archive_sets,
    export_bank_files,
    export_event_folder,
    export_media_files,
    export_workspace_dump,
    game_label,
    media_signature_for_path,
    workspace_details_text,
)
from dyingaudio.models import AudioEntry
from dyingaudio.popups import ask_yes_no_dialog, show_error_dialog, show_info_dialog
from dyingaudio.settings import (
    DEFAULT_EXPERIMENTAL_ARCHIVE_SET,
    DEFAULT_EXPERIMENTAL_CACHE_ROOT,
    DEFAULT_EXPERIMENTAL_GAME,
    ExperimentalSettings,
    bundled_resource_root,
    discover_game_root,
    is_windows_dark_mode,
)


MEDIA_SORT_FIELDS = (
    "Original Order",
    "Media ID",
    "File",
    "Archive",
    "Bank",
    "Event",
    "Audio Type",
    "Duration",
    "Samples",
    "Source",
)


@dataclass(slots=True)
class MediaGroup:
    duration_ms: int
    sample_count: int
    rows: list[NamedAudioLink]

    @property
    def label(self) -> str:
        count = len(self.rows)
        return f"{count} similar file{'s' if count != 1 else ''}"


@dataclass(slots=True)
class MediaRenderState:
    context_rows: list[NamedAudioLink]
    visible_rows: list[NamedAudioLink]
    view_items: list[NamedAudioLink | MediaGroup]
    selected_iids: list[str]
    selected_row_keys: set[str]
    first_leaf_iid: str | None
    grouped_count: int
    rendered_count: int
    total_count: int
    final_count_text: str


def _media_signature_for_row(row: NamedAudioLink) -> tuple[int, int]:
    source_path = row.source if row.source.exists() else row.link
    return media_signature_for_path(str(source_path))


def _shared_text(values: list[str]) -> str:
    if not values:
        return ""
    first = values[0]
    if all(value == first for value in values[1:]):
        return first
    return "multiple"


def _media_signature_text(duration_ms: int, sample_count: int) -> str:
    return f"{duration_ms} ms / {sample_count} samples"


def _audio_type_text(row: NamedAudioLink) -> str:
    return audio_type_label(row.audio_type, row.audio_type_confidence)


def matching_audio_group_rows(rows: list[NamedAudioLink]) -> list[NamedAudioLink]:
    if len(rows) < 2:
        return []
    expected_signature = _media_signature_for_row(rows[0])
    if any(_media_signature_for_row(row) != expected_signature for row in rows[1:]):
        return []
    return rows


def build_media_view_items(rows: list[NamedAudioLink], group_similar: bool) -> list[NamedAudioLink | MediaGroup]:
    if not group_similar:
        return list(rows)

    items: list[NamedAudioLink | MediaGroup] = []
    group_positions: dict[tuple[int, int], int] = {}
    for row in rows:
        signature = _media_signature_for_row(row)
        position = group_positions.get(signature)
        if position is None:
            group_positions[signature] = len(items)
            items.append(row)
            continue
        existing = items[position]
        if isinstance(existing, MediaGroup):
            existing.rows.append(row)
        else:
            items[position] = MediaGroup(duration_ms=signature[0], sample_count=signature[1], rows=[existing, row])
    return items


def filter_and_sort_media_rows(
    rows: list[NamedAudioLink],
    search_text: str,
    sort_field: str,
    descending: bool,
) -> list[NamedAudioLink]:
    normalized_search = search_text.strip().lower()
    needs_signature = sort_field in {"Duration", "Samples"} or any(character.isdigit() for character in normalized_search)
    indexed_rows: list[tuple[int, NamedAudioLink, int, int]] = []
    for index, row in enumerate(rows):
        duration_ms = sample_count = 0
        if needs_signature:
            duration_ms, sample_count = _media_signature_for_row(row)
        indexed_rows.append((index, row, duration_ms, sample_count))

    if normalized_search:
        indexed_rows = [
            (index, row, duration_ms, sample_count)
            for index, row, duration_ms, sample_count in indexed_rows
            if normalized_search in str(row.media_id).lower()
            or normalized_search in row.link.name.lower()
            or normalized_search in row.archive.lower()
            or normalized_search in row.bank.lower()
            or normalized_search in row.event.lower()
            or normalized_search in _audio_type_text(row).lower()
            or normalized_search in row.audio_type_note.lower()
            or normalized_search in str(row.source).lower()
            or normalized_search in str(row.link).lower()
            or (needs_signature and (normalized_search in str(duration_ms).lower() or normalized_search in str(sample_count).lower()))
        ]

    if sort_field != "Original Order":
        key_map: dict[str, Callable[[tuple[int, NamedAudioLink, int, int]], object]] = {
            "Media ID": lambda pair: pair[1].media_id,
            "File": lambda pair: pair[1].link.name.lower(),
            "Archive": lambda pair: pair[1].archive.lower(),
            "Bank": lambda pair: pair[1].bank.lower(),
            "Event": lambda pair: pair[1].event.lower(),
            "Audio Type": lambda pair: _audio_type_text(pair[1]).lower(),
            "Duration": lambda pair: pair[2],
            "Samples": lambda pair: pair[3],
            "Source": lambda pair: str(pair[1].link).lower(),
        }
        indexed_rows.sort(key=key_map.get(sort_field, lambda pair: pair[0]), reverse=descending)

    return [row for _index, row, _duration_ms, _sample_count in indexed_rows]


class ExperimentalWwiseFrame(ttk.Frame):
    def __init__(self, parent: tk.Misc, settings: ExperimentalSettings, app: tk.Tk | None = None) -> None:
        super().__init__(parent)
        self.app = app if app is not None else self.winfo_toplevel()
        self.preview_player = PreviewPlayer()
        self.workspace: WwiseWorkspace | None = None
        self.available_archive_sets: list[ArchiveSetDescriptor] = []
        self.tree_node_context: dict[str, tuple[str, str, str, str]] = {}
        self.visible_rows: list[NamedAudioLink] = []
        self._game_roots = {
            DL2_GAME: settings.dl2_root,
            DLTB_GAME: settings.dltb_root,
        }
        self._active_game = settings.selected_game or DEFAULT_EXPERIMENTAL_GAME

        self.game_var = tk.StringVar(value=self._active_game)
        self.install_root_var = tk.StringVar(value=self._game_roots[self._active_game])
        self.archive_set_var = tk.StringVar(value=settings.archive_set or DEFAULT_EXPERIMENTAL_ARCHIVE_SET)
        self.cache_root_var = tk.StringVar(value=settings.cache_root or DEFAULT_EXPERIMENTAL_CACHE_ROOT)
        self.status_var = tk.StringVar(value="Experimental workspace idle.")
        self.workspace_var = tk.StringVar(value="Workspace: none")
        self.preview_tools_var = tk.StringVar(value=self.preview_player.environment.summary())
        self.selection_var = tk.StringVar(value="Select an archive, bank, or event to browse media.")
        self.details_var = tk.StringVar(value="No media selected.")
        self.media_search_var = tk.StringVar()
        self.media_sort_field_var = tk.StringVar(value="Original Order")
        self.media_sort_descending_var = tk.BooleanVar(value=False)
        self.media_sort_button_var = tk.StringVar(value="Ascending")
        self.media_count_var = tk.StringVar(value="0 shown / 0 total")
        self.group_similar_var = tk.BooleanVar(value=False)
        self.task_status_var = tk.StringVar(value="No experimental task running.")
        self.task_progress_var = tk.DoubleVar(value=0.0)
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
        self.media_iid_rows: dict[str, list[NamedAudioLink]] = {}
        self.media_group_iids: set[str] = set()
        self._media_render_after_id: str | None = None
        self._media_render_state: MediaRenderState | None = None
        self.content_paned: ttk.Panedwindow | None = None
        self._pane_layout_initialized = False
        self._pane_layout_after_ids: list[str] = []

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._build_ui()
        self._refresh_archive_set_choices()

        self.game_var.trace_add("write", self._on_game_changed)
        self.install_root_var.trace_add("write", self._on_install_root_changed)
        self.media_search_var.trace_add("write", self._on_media_filter_changed)
        self.media_sort_field_var.trace_add("write", self._on_media_filter_changed)
        self.media_sort_descending_var.trace_add("write", self._on_media_filter_changed)
        self.group_similar_var.trace_add("write", self._on_media_filter_changed)
        self._update_media_sort_controls()

    def build_settings(self) -> ExperimentalSettings:
        self._commit_current_game_root()
        return ExperimentalSettings(
            selected_game=self.game_var.get().strip() or DEFAULT_EXPERIMENTAL_GAME,
            dl2_root=self._game_roots[DL2_GAME],
            dltb_root=self._game_roots[DLTB_GAME],
            archive_set=self.archive_set_var.get().strip() or BASE_ARCHIVE_SET,
            cache_root=self.cache_root_var.get().strip() or DEFAULT_EXPERIMENTAL_CACHE_ROOT,
            last_export_folder=self._last_export_folder(),
        )

    def shutdown(self) -> None:
        self.task_runner.cancel()
        self.task_runner.cancel_polling()
        self._close_loading_window()
        self._cancel_media_render()
        self._cancel_pane_layout_callbacks()
        self.preview_player.close()

    def _build_ui(self) -> None:
        controls = ttk.LabelFrame(self, text="Experimental Workspace")
        controls.grid(row=0, column=0, sticky="nsew", padx=12, pady=(12, 6))
        for column in range(9):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="Game").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Combobox(
            controls,
            textvariable=self.game_var,
            values=(DL2_GAME, DLTB_GAME),
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=6, pady=6)

        ttk.Label(controls, text="Game Root").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.install_root_var).grid(row=0, column=3, columnspan=3, sticky="ew", padx=6, pady=6)
        self.game_root_browse_button = ttk.Button(controls, text="Browse", command=self._browse_game_root)
        self.game_root_browse_button.grid(row=0, column=6, sticky="ew", padx=6, pady=6)
        self.open_cache_button = ttk.Button(controls, text="Open Cache Folder", command=self._open_cache_folder)
        self.open_cache_button.grid(row=0, column=7, sticky="ew", padx=6, pady=6)
        self.clear_cache_button = ttk.Button(controls, text="Clear Cache", command=self._clear_cache)
        self.clear_cache_button.grid(row=0, column=8, sticky="ew", padx=6, pady=6)

        ttk.Label(controls, text="Archive Set").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        self.archive_set_combo = ttk.Combobox(controls, textvariable=self.archive_set_var, state="readonly")
        self.archive_set_combo.grid(row=1, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(controls, text="Cache Root").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.cache_root_var).grid(row=1, column=3, columnspan=3, sticky="ew", padx=6, pady=6)
        self.cache_root_browse_button = ttk.Button(controls, text="Browse", command=self._browse_cache_root)
        self.cache_root_browse_button.grid(row=1, column=6, sticky="ew", padx=6, pady=6)
        self.build_workspace_button = ttk.Button(controls, text="Build / Refresh Workspace", command=self._build_workspace)
        self.build_workspace_button.grid(row=1, column=7, sticky="ew", padx=6, pady=6)
        self.cancel_task_button = ttk.Button(controls, text="Cancel", command=self._cancel_task, state="disabled")
        self.cancel_task_button.grid(row=1, column=8, sticky="ew", padx=6, pady=6)

        ttk.Label(controls, textvariable=self.workspace_var).grid(row=2, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 4))
        ttk.Label(controls, textvariable=self.preview_tools_var).grid(row=3, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 6))
        ttk.Label(controls, textvariable=self.task_status_var).grid(row=4, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 6))

        content = ttk.Panedwindow(self, orient="horizontal")
        content.grid(row=1, column=0, sticky="nsew", padx=12, pady=6)
        self.content_paned = content

        left = ttk.LabelFrame(content, text="Archives / Banks / Events")
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)
        content.add(left, weight=4)

        center = ttk.LabelFrame(content, text="Media")
        center.columnconfigure(0, weight=1)
        center.rowconfigure(1, weight=1)
        content.add(center, weight=5)

        right = ttk.Notebook(content)
        content.add(right, weight=2)

        self.browser_tree = ttk.Treeview(left, show="tree")
        self.browser_tree.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        self.browser_tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.browser_tree.bind("<Button-3>", self._show_browser_context_menu)
        self.browser_tree.bind("<Shift-F10>", self._show_browser_context_menu)
        browser_scroll = ttk.Scrollbar(left, orient="vertical", command=self.browser_tree.yview)
        browser_scroll.grid(row=0, column=1, sticky="ns", pady=6)
        self.browser_tree.configure(yscrollcommand=browser_scroll.set)
        self.browser_context_menu = tk.Menu(self, tearoff=False)
        self.browser_context_menu.add_command(label="Export Selected Event Folder...", command=self._export_selected_event)
        self.browser_context_menu.add_command(label="Export Selected Bank Files...", command=self._export_selected_bank_files)

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
            columns=("media_id", "archive", "bank", "event", "audio_type", "duration", "samples", "source"),
            show="tree headings",
            height=18,
        )
        self.media_tree.grid(row=0, column=0, sticky="nsew")
        self.media_tree.bind("<<TreeviewSelect>>", self._on_media_select)
        self.media_tree.bind("<Button-3>", self._show_media_context_menu)
        self.media_tree.bind("<Shift-F10>", self._show_media_context_menu)
        self.media_tree.heading("#0", text="File / Group", command=lambda: self._set_media_sort_field_from_heading("File"))
        self.media_tree.column("#0", width=260, anchor="w")
        for column, title, width in (
            ("media_id", "Media ID", 110),
            ("archive", "Archive", 90),
            ("bank", "Bank", 140),
            ("event", "Event", 160),
            ("audio_type", "Audio Type", 150),
            ("duration", "Duration (ms)", 110),
            ("samples", "Samples", 100),
            ("source", "Playable Path", 420),
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

        center_actions = ttk.Frame(center)
        center_actions.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 6))
        for column in range(3):
            center_actions.columnconfigure(column, weight=1)
        self.export_media_button = ttk.Button(center_actions, text="Export Selected Media", command=self._export_selected_media)
        self.export_media_button.grid(row=0, column=0, sticky="ew", padx=2)
        self.export_event_button = ttk.Button(center_actions, text="Export Selected Event Folder", command=self._export_selected_event)
        self.export_event_button.grid(row=0, column=1, sticky="ew", padx=2)
        self.export_bank_button = ttk.Button(center_actions, text="Export Selected Bank Files", command=self._export_selected_bank_files)
        self.export_bank_button.grid(row=0, column=2, sticky="ew", padx=2)

        preview_tab = ttk.Frame(right, padding=6)
        preview_tab.columnconfigure(0, weight=1)
        right.add(preview_tab, text="Preview")
        ttk.Label(preview_tab, textvariable=self.selection_var, wraplength=380).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))
        self.play_button = ttk.Button(preview_tab, text="Play Selected", command=self._play_selected)
        self.play_button.grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(0, 6))
        self.stop_button = ttk.Button(preview_tab, text="Stop", command=self._stop_preview)
        self.stop_button.grid(row=1, column=1, sticky="ew", pady=(0, 6))
        self.play_all_button = ttk.Button(preview_tab, text="Play All Together", command=self._play_selected_together)
        self.play_all_button.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        self.play_all_button.grid_remove()
        ttk.Label(preview_tab, textvariable=self.details_var, wraplength=380, justify="left").grid(row=3, column=0, columnspan=2, sticky="w")

        details_tab = ttk.Frame(right, padding=6)
        details_tab.columnconfigure(0, weight=1)
        details_tab.rowconfigure(0, weight=1)
        right.add(details_tab, text="Details")
        self.details_text = ScrolledText(details_tab, wrap="word", height=22, state="disabled")
        self.details_text.grid(row=0, column=0, sticky="nsew")

        logs_tab = ttk.Frame(right, padding=6)
        logs_tab.columnconfigure(0, weight=1)
        logs_tab.rowconfigure(0, weight=1)
        right.add(logs_tab, text="Logs")
        self.logs_text = ScrolledText(logs_tab, wrap="word", height=22, state="disabled")
        self.logs_text.grid(row=0, column=0, sticky="nsew")
        self.export_dump_button = ttk.Button(logs_tab, text="Export Workspace Dump", command=self._export_workspace_dump)
        self.export_dump_button.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        ttk.Label(self, textvariable=self.status_var).grid(row=2, column=0, sticky="w", padx=12, pady=(0, 12))
        self._busy_widgets = [
            self.game_root_browse_button,
            self.open_cache_button,
            self.cache_root_browse_button,
            self.build_workspace_button,
            self.export_media_button,
            self.export_event_button,
            self.export_bank_button,
            self.export_dump_button,
        ]
        self._pane_layout_after_ids.append(self.after_idle(self._ensure_default_pane_layout))
        self._pane_layout_after_ids.append(self.after(100, self._ensure_default_pane_layout))
        self._pane_layout_after_ids.append(self.after(500, self._ensure_default_pane_layout))
        self.bind("<Configure>", self._on_frame_configure)
        self.bind("<Visibility>", self._on_frame_configure, add="+")

    def _append_status(self, message: str) -> None:
        self.status_var.set(message)

    def _show_info_window(self, title: str, message: str) -> None:
        show_info = getattr(self.app, "_show_info_window", None)
        if callable(show_info):
            show_info(title, message)
            return
        show_info_dialog(self, title, message)

    def _ask_yes_no_window(self, title: str, message: str, *, kind: str = "warning") -> bool:
        ask_yes_no = getattr(self.app, "_ask_yes_no_window", None)
        if callable(ask_yes_no):
            return ask_yes_no(title, message, kind=kind)
        return ask_yes_no_dialog(self, title, message, kind=kind)

    def _show_error_window(self, title: str, message: str) -> None:
        show_error = getattr(self.app, "_show_error_window", None)
        if callable(show_error):
            show_error(title, message)
            return
        show_error_dialog(self, title, message)

    def _cancel_pane_layout_callbacks(self) -> None:
        for after_id in self._pane_layout_after_ids:
            try:
                self.after_cancel(after_id)
            except tk.TclError:
                pass
        self._pane_layout_after_ids.clear()

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
            try:
                self.loading_gif_label.configure(text="Working...")
                self.loading_gif_label.image = None
            except tk.TclError:
                self._close_loading_window()
            return
        try:
            self.loading_gif_label.configure(image=frame)
            self.loading_gif_label.image = frame
        except tk.TclError:
            self._close_loading_window()
            return
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
        gif_path = bundled_resource_root() / "assets" / "MovingGears.gif"
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
        self.update_idletasks()
        window.update_idletasks()
        root_x = self.winfo_rootx()
        root_y = self.winfo_rooty()
        root_width = self.winfo_width()
        root_height = self.winfo_height()
        x = root_x + max(0, (root_width - width) // 2)
        y = root_y + max(0, (root_height - height) // 2)
        window.geometry(f"{width}x{height}+{x}+{y}")

    def _show_loading_window(self, message: str) -> None:
        if self.loading_window is not None and self.loading_window.winfo_exists():
            self.task_status_var.set(message)
            self._center_loading_window()
            self.loading_window.deiconify()
            self.loading_window.lift()
            self.loading_window.update_idletasks()
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
        window.lift()
        window.update_idletasks()
        window.focus_set()

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

    def _on_frame_configure(self, _event: object) -> None:
        self._ensure_default_pane_layout()

    def _ensure_default_pane_layout(self) -> None:
        if self.content_paned is None:
            return
        try:
            total_width = self.content_paned.winfo_width()
            if total_width <= 1:
                return

            left_width = max(320, int(total_width * 0.30))
            middle_width = max(420, int(total_width * 0.46))
            right_min_start = total_width - 280
            first_sash = self.content_paned.sashpos(0)
            second_sash = self.content_paned.sashpos(1)

            if (not self._pane_layout_initialized) or first_sash < 240 or second_sash <= first_sash + 160:
                first_target = min(left_width, total_width - 760)
                first_target = max(320, first_target)
                second_target = min(max(first_target + middle_width, first_target + 460), right_min_start)
                second_target = max(first_target + 460, second_target)
                self.content_paned.sashpos(0, first_target)
                self.content_paned.sashpos(1, second_target)
                self._pane_layout_initialized = True
        except tk.TclError:
            return

    def _set_task_busy(self, busy: bool) -> None:
        for widget in self._busy_widgets:
            widget.configure(state="disabled" if busy else "normal")
        self.cancel_task_button.configure(state="normal" if busy else "disabled")
        self.archive_set_combo.configure(state="disabled" if busy else "readonly")

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
        self._append_status("Cancelling experimental task...")
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
            self._show_info_window("Experimental workspace busy", "Wait for the current experimental task to finish first.")
            return
        self._set_task_busy(True)
        self.task_status_var.set(start_message)
        self._append_status(start_message)
        self._set_text_widget(self.logs_text, start_message)
        self._show_loading_window(start_message)

        def handle_error(exc: BaseException, details: str) -> None:
            if isinstance(exc, TaskCancelled):
                self._append_log("Task cancelled.")
                self._append_status("Experimental task cancelled.")
                self.task_status_var.set("Cancelled.")
                return
            self._set_text_widget(self.logs_text, details)
            self._show_error_window(error_title, str(exc))
            self._append_status(error_title.replace(" failed", " failed."))

        self.task_runner.start(
            worker,
            on_progress=self._apply_task_progress,
            on_log=lambda message: (self._append_status(message), self._append_log(message)),
            on_success=on_success,
            on_error=handle_error,
            on_finally=self._finish_task_ui,
        )

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

    def _show_browser_context_menu(self, event: object) -> str | None:
        y = getattr(event, "y", None)
        node_id = self.browser_tree.identify_row(y) if y is not None else ""
        if node_id:
            self.browser_tree.selection_set(node_id)
            self.browser_tree.focus(node_id)
            self._on_tree_select(event)

        context = self._selected_context()
        if context is None:
            return "break"
        node_type, _archive, _bank, _event = context
        self.browser_context_menu.entryconfigure("Export Selected Event Folder...", state="normal" if node_type == "event" else "disabled")
        self.browser_context_menu.entryconfigure("Export Selected Bank Files...", state="normal" if node_type in {"bank", "event"} else "disabled")
        self._popup_context_menu(self.browser_context_menu, self.browser_tree, event)
        return "break"

    def _show_media_context_menu(self, event: object) -> str | None:
        y = getattr(event, "y", None)
        node_id = self.media_tree.identify_row(y) if y is not None else ""
        if node_id:
            self.media_tree.selection_set(node_id)
            self.media_tree.focus(node_id)
            self._on_media_select(event)

        group_rows = self._selected_group_preview_rows()
        mixed_export_ready = len(group_rows) >= 2 and self.preview_player.environment.ffmpeg_path is not None
        self.media_context_menu.entryconfigure("Export Mixed Audio...", state="normal" if mixed_export_ready else "disabled")
        self._popup_context_menu(self.media_context_menu, self.media_tree, event)
        return "break"

    def _last_export_folder(self) -> str:
        if self.workspace is not None:
            return str(self.workspace.root)
        return ""

    def _resolve_game_root(self, game: str, *, allow_discovery: bool) -> Path | None:
        current = self._game_roots.get(game, "").strip()
        if current:
            candidate = Path(current).expanduser()
            if candidate.exists():
                return candidate.resolve()
        if allow_discovery:
            return discover_game_root(game)
        return None

    def _resolve_cache_root(self) -> Path:
        current = self.cache_root_var.get().strip()
        if current:
            return Path(current).expanduser().resolve()
        return Path(DEFAULT_EXPERIMENTAL_CACHE_ROOT).expanduser().resolve()

    def _commit_current_game_root(self) -> None:
        game = self._active_game
        self._game_roots[game] = self.install_root_var.get().strip()

    def _on_game_changed(self, *_args: object) -> None:
        next_game = self.game_var.get().strip() or DEFAULT_EXPERIMENTAL_GAME
        if next_game == self._active_game:
            return
        self._commit_current_game_root()
        self._active_game = next_game
        self.install_root_var.set(self._game_roots.get(next_game, ""))
        self._refresh_archive_set_choices()

    def _on_install_root_changed(self, *_args: object) -> None:
        self._refresh_archive_set_choices()

    def _browse_game_root(self) -> None:
        game = self.game_var.get().strip() or DEFAULT_EXPERIMENTAL_GAME
        selection = discover_game_root(game)
        if selection is None:
            current = self._game_roots.get(game, "").strip()
            if current:
                candidate = Path(current).expanduser()
                if candidate.exists():
                    selection = candidate.resolve()
        if selection is None:
            initialdir = self.install_root_var.get().strip() or None
            chosen = filedialog.askdirectory(title=f"Select {game_label(game)} root", initialdir=initialdir)
            if chosen:
                selection = Path(chosen).resolve()
        if selection is not None:
            self.install_root_var.set(str(selection))
            self._persist_settings()

    def _browse_cache_root(self) -> None:
        selection = filedialog.askdirectory(
            title="Select experimental cache root",
            initialdir=self.cache_root_var.get().strip() or None,
        )
        if selection:
            self.cache_root_var.set(selection)
            self._persist_settings()

    def _open_cache_folder(self) -> None:
        target = self.workspace.root if self.workspace is not None else self._resolve_cache_root()
        target.mkdir(parents=True, exist_ok=True)
        os.startfile(str(target))

    def _clear_cache(self) -> None:
        if self.task_runner.is_running:
            self._show_info_window("Clear cache", "Wait for the current experimental workspace task to finish first.")
            return

        target = self._resolve_cache_root()
        if not target.exists() and self.workspace is None:
            self._show_info_window("Clear cache", "The experimental cache is already empty.")
            return

        game = self.game_var.get().strip() or DEFAULT_EXPERIMENTAL_GAME
        archive_set = self.archive_set_var.get().strip() or DEFAULT_EXPERIMENTAL_ARCHIVE_SET
        prompt = (
            f"Delete the experimental cache at:\n{target}\n\n"
            f"This will remove the current {game_label(game)} / {archive_set} workspace cache and clear the loaded browser data."
        )
        if not self._ask_yes_no_window("Clear cache", prompt):
            return

        self.preview_player.stop()

        def worker(progress, log):
            progress("Clearing experimental cache...")
            log(f"Clearing experimental cache: {target}")
            if target.exists():
                shutil.rmtree(target, ignore_errors=False)
            return target

        def on_success(result: object) -> None:
            cleared_target = result if isinstance(result, Path) else target
            self.preview_player.clear_cache()
            self.workspace = None
            self.tree_node_context.clear()
            self.visible_rows = []
            self.browser_tree.delete(*self.browser_tree.get_children())
            self._cancel_media_render()
            self.media_tree.delete(*self.media_tree.get_children())
            self.media_iid_rows.clear()
            self.media_group_iids.clear()
            self.workspace_var.set("Workspace: none")
            self.selection_var.set("Select an archive, bank, or event to browse media.")
            self.details_var.set("No media selected.")
            self.media_count_var.set("0 shown / 0 total")
            self.task_status_var.set("Experimental cache cleared.")
            self._set_text_widget(self.details_text, "")
            self._set_text_widget(self.logs_text, "No experimental workspace loaded.")
            self._update_preview_action_controls()
            self._append_status(f"Cleared experimental cache: {cleared_target}")

        self._run_task(
            start_message="Clearing experimental cache...",
            error_title="Clear cache failed",
            worker=worker,
            on_success=on_success,
        )

    def _refresh_archive_set_choices(self) -> None:
        game = self.game_var.get().strip() or DEFAULT_EXPERIMENTAL_GAME
        install_root = self.install_root_var.get().strip()
        if not install_root:
            discovered = self._resolve_game_root(game, allow_discovery=False)
            install_root = str(discovered) if discovered is not None else ""
        try:
            descriptors = detect_archive_sets(game, install_root)
        except Exception:
            descriptors = []
        self.available_archive_sets = descriptors
        values = [descriptor.key for descriptor in descriptors]
        self.archive_set_combo.configure(values=values)
        current = self.archive_set_var.get().strip() or BASE_ARCHIVE_SET
        if current not in values:
            self.archive_set_var.set(values[0] if values else BASE_ARCHIVE_SET)

    def _build_workspace(self) -> None:
        self._commit_current_game_root()
        game = self.game_var.get().strip() or DEFAULT_EXPERIMENTAL_GAME
        install_root_path = self._resolve_game_root(game, allow_discovery=True)
        if install_root_path is None:
            self._show_info_window("Build workspace", f"Select the {game_label(game)} install folder first, or click Browse to auto-find it.")
            return
        if self.install_root_var.get().strip() != str(install_root_path):
            self.install_root_var.set(str(install_root_path))
            self._persist_settings()
        install_root = str(install_root_path)
        archive_set = self.archive_set_var.get().strip() or BASE_ARCHIVE_SET
        cache_root = str(self._resolve_cache_root())

        def worker(progress, log):
            return build_or_load_workspace(
                game=game,
                install_root=install_root,
                archive_set=archive_set,
                cache_root=cache_root,
                log=log,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            )

        def on_success(result: object) -> None:
            workspace = result if isinstance(result, WwiseWorkspace) else None
            if workspace is None:
                raise RuntimeError("Experimental workspace build returned an unexpected result.")
            self.workspace = workspace
            self.workspace_var.set(f"Workspace: {workspace.root}")
            self._populate_browser_tree()
            self._refresh_logs()
            self.task_status_var.set("Experimental workspace ready.")
            self._append_status(
                f"Loaded {len(workspace.named_links)} named link(s) and {len(workspace.extracted_banks)} extracted bank(s)."
            )

        self._run_task(
            start_message=f"Building experimental workspace for {game_label(game)} ({archive_set})...",
            error_title="Build experimental workspace failed",
            worker=worker,
            on_success=on_success,
        )

    def _populate_browser_tree(self) -> None:
        self._cancel_media_render()
        self.browser_tree.delete(*self.browser_tree.get_children())
        self.media_tree.delete(*self.media_tree.get_children())
        self.visible_rows = []
        self.media_iid_rows.clear()
        self.media_group_iids.clear()
        self.media_count_var.set("0 shown / 0 total")
        self.tree_node_context.clear()
        self.selection_var.set("Select an archive, bank, or event to browse media.")
        self.details_var.set("No media selected.")
        self._set_text_widget(self.details_text, self.workspace.summary_text if self.workspace is not None else "")

        if self.workspace is None:
            return

        archive_nodes: dict[str, str] = {}
        bank_nodes: dict[tuple[str, str], str] = {}
        event_nodes: dict[tuple[str, str, str], str] = {}
        first_archive_iid: str | None = None
        grouped = sorted(self.workspace.named_links, key=lambda row: (row.archive.lower(), row.bank.lower(), row.event.lower(), row.media_id))
        for row in grouped:
            if row.archive not in archive_nodes:
                archive_iid = f"archive::{row.archive}"
                archive_nodes[row.archive] = archive_iid
                if first_archive_iid is None:
                    first_archive_iid = archive_iid
                self.browser_tree.insert("", "end", iid=archive_iid, text=row.archive)
                self.tree_node_context[archive_iid] = ("archive", row.archive, "", "")
            bank_key = (row.archive, row.bank)
            if bank_key not in bank_nodes:
                bank_iid = f"bank::{row.archive}::{row.bank}"
                bank_nodes[bank_key] = bank_iid
                self.browser_tree.insert(archive_nodes[row.archive], "end", iid=bank_iid, text=row.bank)
                self.tree_node_context[bank_iid] = ("bank", row.archive, row.bank, "")
            event_key = (row.archive, row.bank, row.event)
            if event_key not in event_nodes:
                event_iid = f"event::{row.archive}::{row.bank}::{row.event}"
                event_nodes[event_key] = event_iid
                self.browser_tree.insert(bank_nodes[bank_key], "end", iid=event_iid, text=row.event)
                self.tree_node_context[event_iid] = ("event", row.archive, row.bank, row.event)

        for archive_iid in archive_nodes.values():
            self.browser_tree.item(archive_iid, open=True)

        if first_archive_iid is not None and self.browser_tree.exists(first_archive_iid):
            self.browser_tree.selection_set(first_archive_iid)
            self.browser_tree.focus(first_archive_iid)
            self.browser_tree.see(first_archive_iid)
            self._on_tree_select(None)
        elif self.workspace.named_links:
            self.media_count_var.set(f"0 shown / {len(self.workspace.named_links)} total")
            self.details_var.set("Select an archive, bank, or event to browse media.")
        else:
            self.media_count_var.set("0 shown / 0 total")
            self.details_var.set("No named audio links were generated for this workspace.")

    def _rows_for_context(self, node_type: str, archive: str, bank: str, event: str) -> list[NamedAudioLink]:
        if self.workspace is None:
            return []
        if node_type == "archive":
            return [row for row in self.workspace.named_links if row.archive == archive]
        if node_type == "bank":
            return [row for row in self.workspace.named_links if row.archive == archive and row.bank == bank]
        if node_type == "event":
            return [
                row
                for row in self.workspace.named_links
                if row.archive == archive and row.bank == bank and row.event == event
            ]
        return []

    def _on_media_filter_changed(self, *_args: object) -> None:
        self._update_media_sort_controls()
        self._refresh_media_tree()

    def _update_media_sort_controls(self) -> None:
        is_manual_order = self.media_sort_field_var.get().strip() == "Original Order"
        self.media_sort_button_var.set("Manual Order" if is_manual_order else ("Descending" if self.media_sort_descending_var.get() else "Ascending"))
        self.media_sort_order_button.configure(state="disabled" if is_manual_order else "normal")

    def _toggle_media_sort_direction(self) -> None:
        if self.media_sort_field_var.get().strip() == "Original Order":
            return
        self.media_sort_descending_var.set(not self.media_sort_descending_var.get())

    def _clear_media_search(self) -> None:
        self.media_search_var.set("")

    def _set_media_sort_field_from_heading(self, heading_title: str) -> None:
        field_map = {
            "File": "File",
            "File / Group": "File",
            "Media ID": "Media ID",
            "Archive": "Archive",
            "Bank": "Bank",
            "Event": "Event",
            "Duration (ms)": "Duration",
            "Samples": "Samples",
            "Playable Path": "Source",
        }
        target = field_map.get(heading_title, "Original Order")
        if self.media_sort_field_var.get().strip() == target:
            self._toggle_media_sort_direction()
            return
        self.media_sort_field_var.set(target)

    def _media_row_key(self, row: NamedAudioLink) -> str:
        payload = "\0".join(
            (
                row.archive,
                row.bank,
                row.event,
                str(row.media_id),
                str(row.source),
                str(row.link),
            )
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

    def _media_leaf_values(self, row: NamedAudioLink) -> tuple[str, str, str, str, str, str, str, str]:
        duration_ms, sample_count = _media_signature_for_row(row)
        return (
            str(row.media_id),
            row.archive,
            row.bank,
            row.event,
            _audio_type_text(row),
            str(duration_ms),
            str(sample_count),
            str(row.link),
        )

    def _media_group_values(self, group: MediaGroup) -> tuple[str, str, str, str, str, str, str, str]:
        return (
            "",
            _shared_text([row.archive for row in group.rows]),
            _shared_text([row.bank for row in group.rows]),
            _shared_text([row.event for row in group.rows]),
            _shared_text([_audio_type_text(row) for row in group.rows]),
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

        context = self._selected_context()
        if self.workspace is None or context is None:
            self.media_count_var.set("0 shown / 0 total")
            self._update_preview_action_controls()
            return

        node_type, archive, bank, event = context
        context_rows = self._rows_for_context(node_type, archive, bank, event)
        visible_rows = filter_and_sort_media_rows(
            context_rows,
            self.media_search_var.get(),
            self.media_sort_field_var.get(),
            self.media_sort_descending_var.get(),
        )
        self.visible_rows = visible_rows

        if not context_rows:
            self.media_count_var.set("0 shown / 0 total")
            self.details_var.set("No media rows matched this selection.")
            self._set_text_widget(self.details_text, self.selection_var.get())
            self._update_preview_action_controls()
            return

        view_items = build_media_view_items(visible_rows, self.group_similar_var.get())
        grouped_count = sum(1 for item in view_items if isinstance(item, MediaGroup))
        count_text = f"{len(visible_rows)} shown / {len(context_rows)} total"
        if self.group_similar_var.get():
            count_text = f"{count_text}, {grouped_count} groups"
        self.media_count_var.set(f"Loading {count_text}...")
        self.details_var.set("Loading media rows...")
        self._set_text_widget(self.details_text, f"{self.selection_var.get()}\nLoading media rows...")
        self._media_render_state = MediaRenderState(
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

    def _on_tree_select(self, _event: object) -> None:
        selection = self.browser_tree.selection()
        if not selection:
            return
        node_type, archive, bank, event = self.tree_node_context.get(selection[0], ("", "", "", ""))
        if node_type == "event":
            self.selection_var.set(f"{archive} / {bank} / {event}")
        elif node_type == "bank":
            self.selection_var.set(f"{archive} / {bank}")
        elif node_type == "archive":
            self.selection_var.set(archive)
        else:
            self.selection_var.set("Select an archive, bank, or event to browse media.")
        self._refresh_media_tree()

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
            if isinstance(item, MediaGroup):
                group_iid = f"group::{item.duration_ms}::{item.sample_count}::{state.rendered_count}"
                self.media_group_iids.add(group_iid)
                self.media_iid_rows[group_iid] = list(item.rows)
                self.media_tree.insert(
                    "",
                    "end",
                    iid=group_iid,
                    text=item.label,
                    values=self._media_group_values(item),
                )
                for row in item.rows:
                    leaf_iid = f"media::{self._media_row_key(row)}"
                    self.media_iid_rows[leaf_iid] = [row]
                    self.media_tree.insert(
                        group_iid,
                        "end",
                        iid=leaf_iid,
                        text=row.link.name,
                        values=self._media_leaf_values(row),
                    )
                    if state.first_leaf_iid is None:
                        state.first_leaf_iid = leaf_iid
            else:
                leaf_iid = f"media::{self._media_row_key(item)}"
                self.media_iid_rows[leaf_iid] = [item]
                self.media_tree.insert(
                    "",
                    "end",
                    iid=leaf_iid,
                    text=item.link.name,
                    values=self._media_leaf_values(item),
                )
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
            self.details_var.set("No media rows matched this selection.")
            self._set_text_widget(self.details_text, self.selection_var.get())
        self._update_preview_action_controls()

    def _selected_rows(self) -> list[NamedAudioLink]:
        rows: list[NamedAudioLink] = []
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
        duration_ms, sample_count = _media_signature_for_row(row)
        selected_is_group = any(item_id in self.media_group_iids for item_id in self.media_tree.selection())

        if selected_is_group and len(rows) > 1:
            self.details_var.set(f"{len(rows)} similar files\n{_media_signature_text(duration_ms, sample_count)}")
            detail_lines = [
                f"Similar audio group: {len(rows)} file(s)",
                f"Signature: {_media_signature_text(duration_ms, sample_count)}",
                f"Archive: {_shared_text([item.archive for item in rows])}",
                f"Bank: {_shared_text([item.bank for item in rows])}",
                f"Event: {_shared_text([item.event for item in rows])}",
                f"Audio type: {_shared_text([_audio_type_text(item) for item in rows])}",
                "",
                "Members:",
            ]
            for item in rows[:200]:
                detail_lines.append(f"{item.archive} / {item.bank} / {item.event} | Media {item.media_id} | {item.link.name}")
            if len(rows) > 200:
                detail_lines.append(f"... {len(rows) - 200} more grouped file(s)")
        else:
            self.details_var.set(
                f"Media {row.media_id}\nArchive: {row.archive}\nBank: {row.bank}\nEvent: {row.event}\nAudio Type: {_audio_type_text(row)}\nFile: {row.link.name}\nSignature: {_media_signature_text(duration_ms, sample_count)}"
            )
            detail_lines = [
                f"Archive: {row.archive}",
                f"Bank: {row.bank}",
                f"Event: {row.event}",
                f"Media ID: {row.media_id}",
                f"Audio type: {_audio_type_text(row)}",
                f"Type note: {row.audio_type_note or 'n/a'}",
                f"Resolved object types: {', '.join(str(value) for value in row.resolved_object_types) or 'n/a'}",
                f"Duration: {duration_ms} ms",
                f"Samples: {sample_count}",
                f"Playable file: {row.link}",
                f"Flat source: {row.source}",
            ]
            if self.workspace is not None:
                matching_banks = [bank.path for bank in self.workspace.extracted_banks if bank.bank == row.bank]
                if matching_banks:
                    detail_lines.extend(["", "Related bank files:"])
                    detail_lines.extend(str(path) for path in matching_banks)
        self._set_text_widget(self.details_text, "\n".join(detail_lines))
        self._update_preview_action_controls()

    def _refresh_logs(self) -> None:
        if self.workspace is None:
            self._set_text_widget(self.logs_text, "No experimental workspace loaded.")
            return
        lines = [workspace_details_text(self.workspace)]
        if self.workspace.unresolved:
            lines.extend(["", "Unresolved items:"])
            for row in self.workspace.unresolved[:500]:
                pieces = [row.bank]
                if row.event:
                    pieces.append(row.event)
                if row.media_id is not None:
                    pieces.append(str(row.media_id))
                pieces.append(row.note)
                lines.append(" | ".join(piece for piece in pieces if piece))
            if len(self.workspace.unresolved) > 500:
                lines.append(f"... {len(self.workspace.unresolved) - 500} more unresolved row(s)")
        self._set_text_widget(self.logs_text, "\n".join(lines))

    def _selected_group_preview_rows(self) -> list[NamedAudioLink]:
        return matching_audio_group_rows(self._selected_rows())

    def _mixed_audio_export_name(self, rows: list[NamedAudioLink]) -> str:
        first = rows[0]
        base_parts = [first.archive, first.bank, first.event, f"media_{first.media_id}", "mixed"]
        raw_name = "_".join(part for part in base_parts if part)
        safe_name = "".join(character if character.isalnum() or character in {"_", "-", "."} else "_" for character in raw_name)
        return f"{safe_name}.wav"

    def _selected_preview_entry(self) -> AudioEntry | None:
        rows = self._selected_rows()
        if not rows:
            return None
        row = rows[0]
        return AudioEntry(
            entry_name=f"{row.bank}_{row.media_id}",
            source_mode="raw",
            source_path=str(row.link),
            duration_ms=0,
            sample_count=0,
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
            self._show_info_window("Preview media", "Select a media row to preview first.")
            return
        try:
            preview_path = self.preview_player.play_entry(entry, self._append_status)
        except Exception as exc:
            self._show_error_window("Preview failed", str(exc))
            self._append_status("Experimental preview failed.")
            return
        self._append_status(f"Previewing {preview_path.name}.")

    def _play_selected_together(self) -> None:
        rows = self._selected_group_preview_rows()
        if len(rows) < 2:
            self._show_info_window(
                "Preview media",
                "Select two or more matching media rows with the same duration and sample count first.",
            )
            return
        if self.preview_player.environment.ffmpeg_path is None:
            self._show_error_window("Preview failed", "FFmpeg is required to mix multiple audio files together.")
            return

        sources = [row.source if row.source.exists() else row.link for row in rows]
        try:
            preview_path = self.preview_player.play_combined_sources(sources, self._append_status)
        except Exception as exc:
            self._show_error_window("Preview failed", str(exc))
            self._append_status("Experimental group preview failed.")
            return
        self._append_status(f"Previewing {len(rows)} files together from {preview_path.name}.")

    def _export_selected_media_mixed(self) -> None:
        rows = self._selected_group_preview_rows()
        if len(rows) < 2:
            self._show_info_window(
                "Export mixed audio",
                "Select two or more matching media rows with the same duration and sample count first.",
            )
            return
        if self.preview_player.environment.ffmpeg_path is None:
            self._show_error_window("Export mixed audio failed", "FFmpeg is required to mix multiple audio files together.")
            return

        destination_root = self._ask_export_directory("Export mixed audio")
        if destination_root is None:
            return
        destination = destination_root / self._mixed_audio_export_name(rows)

        self._run_task(
            start_message=f"Exporting mixed audio from {len(rows)} file(s)...",
            error_title="Export mixed audio failed",
            worker=lambda _progress, log: self.preview_player.export_combined_sources(
                [row.source if row.source.exists() else row.link for row in rows],
                destination,
                log,
            ),
            on_success=lambda result: self._append_status(f"Exported mixed audio to {result}."),
        )

    def _stop_preview(self) -> None:
        self.preview_player.stop()
        self._append_status("Experimental preview stopped.")

    def _ask_export_directory(self, title: str) -> Path | None:
        initial = self.workspace.root if self.workspace is not None else Path(self.cache_root_var.get().strip() or DEFAULT_EXPERIMENTAL_CACHE_ROOT)
        selection = filedialog.askdirectory(title=title, initialdir=str(initial))
        if not selection:
            return None
        return Path(selection).resolve()

    def _export_selected_media(self) -> None:
        rows = self._selected_rows()
        if not rows:
            self._show_info_window("Export selected media", "Select one or more media rows first.")
            return
        destination = self._ask_export_directory("Export selected media")
        if destination is None:
            return
        self._run_task(
            start_message=f"Exporting {len(rows)} selected media file(s)...",
            error_title="Export selected media failed",
            worker=lambda progress, _log: export_media_files(
                rows,
                destination,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            ),
            on_success=lambda result: self._append_status(
                f"Exported {len(result) if isinstance(result, list) else 0} media file(s) to {destination}."
            ),
        )

    def _selected_context(self) -> tuple[str, str, str, str] | None:
        selection = self.browser_tree.selection()
        if not selection:
            return None
        return self.tree_node_context.get(selection[0])

    def _export_selected_event(self) -> None:
        if self.workspace is None:
            self._show_info_window("Export selected event", "Build the experimental workspace first.")
            return
        context = self._selected_context()
        if context is None:
            self._show_info_window("Export selected event", "Select an event first.")
            return
        node_type, archive, bank, event = context
        if node_type != "event":
            self._show_info_window("Export selected event", "Select a specific event in the left tree first.")
            return
        destination = self._ask_export_directory("Export selected event folder")
        if destination is None:
            return
        self._run_task(
            start_message=f"Exporting event '{event}'...",
            error_title="Export selected event failed",
            worker=lambda progress, _log: export_event_folder(
                self.workspace,
                archive,
                bank,
                event,
                destination,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            ),
            on_success=lambda result: self._append_status(f"Exported event folder to {result}."),
        )

    def _export_selected_bank_files(self) -> None:
        if self.workspace is None:
            self._show_info_window("Export selected bank files", "Build the experimental workspace first.")
            return
        context = self._selected_context()
        if context is None:
            self._show_info_window("Export selected bank files", "Select a bank or event first.")
            return
        _node_type, _archive, bank, _event = context
        if not bank:
            self._show_info_window("Export selected bank files", "Select a bank or event first.")
            return
        destination = self._ask_export_directory("Export selected bank files")
        if destination is None:
            return
        def on_success(result: object) -> None:
            exported = result if isinstance(result, list) else []
            if not exported:
                self._show_info_window("Export selected bank files", f"No extracted .bnk files were found for '{bank}'.")
                return
            self._append_status(f"Exported {len(exported)} bank file(s) for '{bank}' to {destination}.")

        self._run_task(
            start_message=f"Exporting bank files for '{bank}'...",
            error_title="Export selected bank files failed",
            worker=lambda progress, _log: export_bank_files(
                self.workspace,
                bank,
                destination,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            ),
            on_success=on_success,
        )

    def _export_workspace_dump(self) -> None:
        if self.workspace is None:
            self._show_info_window("Export workspace dump", "Build the experimental workspace first.")
            return
        destination = self._ask_export_directory("Export experimental workspace dump")
        if destination is None:
            return
        self._run_task(
            start_message="Exporting experimental workspace dump...",
            error_title="Export workspace dump failed",
            worker=lambda progress, _log: export_workspace_dump(
                self.workspace,
                destination,
                progress=progress,
                cancel_event=self.task_runner.cancel_event,
            ),
            on_success=lambda result: self._append_status(f"Exported workspace dump to {result}."),
        )
