from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import traceback
import time
import tkinter as tk
from dataclasses import replace
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from tkinter.scrolledtext import ScrolledText

from dyingaudio.audio_info import probe_audio_metadata
from dyingaudio.background import BackgroundTaskRunner, TaskProgress
from dyingaudio.core.csb import WORKSHOP_MAGIC, extract_csb, parse_csb
from dyingaudio.core.dldt import DldtToolchain, compile_audio_to_fsb, discover_toolchain
from dyingaudio.core.manifest import load_manifest, write_manifest
from dyingaudio.core.media_tools import COMMON_AUDIO_FILETYPES, discover_media_tools, run_hidden
from dyingaudio.core.mod_writer import build_csb_file, build_mod
from dyingaudio.core.preview import PreviewPlayer, preview_strategy_for_entry
from dyingaudio.core.scriptgen import generate_audiodata_scr
from dyingaudio.experimental_workspace import ExperimentalWwiseFrame
from dyingaudio.models import AudioEntry
from dyingaudio.settings import (
    AppSettings,
    DEFAULT_AUDIO_PROCS,
    DEFAULT_BUNDLE_NAME,
    DEFAULT_MOD_NAME,
    discover_dldt_root,
    discover_mods_root,
    application_root,
    is_windows_dark_mode,
    load_settings,
    save_settings,
)


class DyingAudioApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("DyingAudio")
        self._apply_window_icon()
        self._configure_appearance()
        self.geometry("1500x920")
        self.minsize(1200, 760)

        self.settings = load_settings()
        self.experimental_frame: ExperimentalWwiseFrame | None = None
        self.entries: list[AudioEntry] = []
        self.current_toolchain: DldtToolchain | None = None
        self.last_built_mod_root: Path | None = None
        self.loaded_csb_path: Path | None = None
        self.loaded_csb_magic: int | None = None
        self.loaded_csb_layout: str | None = None
        self.edit_session_dir: tempfile.TemporaryDirectory[str] | None = None
        self.preview_player = PreviewPlayer()
        self.task_runner = BackgroundTaskRunner(self)

        self.mod_name_var = tk.StringVar(value=self.settings.mod_name or DEFAULT_MOD_NAME)
        self.bundle_name_var = tk.StringVar(value=self.settings.bundle_name or DEFAULT_BUNDLE_NAME)
        self.mods_root_var = tk.StringVar(value=self.settings.mods_root)
        self.dldt_root_var = tk.StringVar(value=self.settings.dldt_root)
        self.builder_mode_var = tk.StringVar(value=self.settings.builder_mode or "Raw Audio via DLDT")
        self.generate_script_var = tk.BooleanVar(value=self.settings.generate_audiodata)
        self.status_var = tk.StringVar(value="Ready.")
        self.toolchain_status_var = tk.StringVar(value="")
        self.loaded_csb_var = tk.StringVar(value="Loaded CSB: none")
        self.preview_tools_var = tk.StringVar(value=self.preview_player.environment.summary())
        self.preview_info_var = tk.StringVar(value="Select an entry to preview it.")
        self.entry_search_var = tk.StringVar()
        self.sort_field_var = tk.StringVar(value="Original Order")
        self.sort_descending_var = tk.BooleanVar(value=False)
        self.sort_button_var = tk.StringVar(value="Ascending")
        self.entry_count_var = tk.StringVar(value="0 entries")
        self.playback_status_var = tk.StringVar(value="Playback idle.")
        self.playback_progress_var = tk.DoubleVar(value=0.0)
        self.task_progress_var = tk.DoubleVar(value=0.0)
        self.task_status_var = tk.StringVar(value="No DL1 task running.")
        self._dl1_busy_widgets: list[tk.Widget] = []
        self.loading_window: tk.Toplevel | None = None
        self.loading_status_label: ttk.Label | None = None
        self.loading_progress: ttk.Progressbar | None = None
        self.loading_gif_label: ttk.Label | None = None
        self._loading_gif_cache: dict[int, tk.PhotoImage] = {}
        self._loading_gif_frame_count: int | None = None
        self._loading_gif_subsample = 1
        self._loading_gif_after_id: str | None = None
        self._loading_gif_frame_index = 0

        self.selected_name_var = tk.StringVar()
        self.selected_type_var = tk.StringVar(value="2")
        self.selected_sample_count_var = tk.StringVar(value="0")
        self.selected_duration_var = tk.StringVar(value="0")
        self.selected_source_var = tk.StringVar(value="")
        self.selected_fsb_var = tk.StringVar(value="")
        self.selected_notes_var = tk.StringVar(value="")
        self._preview_after_id: str | None = None
        self._preview_started_at: float | None = None
        self._preview_duration_ms = 0
        self._preview_playing = False
        self._preview_indeterminate = False
        self._preview_entry_name = ""
        self._preview_playback_kind: str | None = None

        self._build_ui()
        self._load_proc_names()
        self._update_sort_controls()
        self._refresh_tree()
        self._update_toolchain_status()
        self._update_script_preview()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        for traced_var in (
            self.bundle_name_var,
            self.generate_script_var,
            self.dldt_root_var,
            self.builder_mode_var,
        ):
            traced_var.trace_add("write", self._on_settings_changed)
        self.entry_search_var.trace_add("write", self._on_entry_filter_changed)
        self.sort_field_var.trace_add("write", self._on_entry_filter_changed)
        self.sort_descending_var.trace_add("write", self._on_entry_filter_changed)

    def _apply_window_icon(self) -> None:
        icon_path = application_root() / "assets" / "dyinglight_devtools.ico"
        if not icon_path.exists():
            return
        try:
            self.iconbitmap(default=str(icon_path))
        except tk.TclError:
            pass

    def _configure_appearance(self) -> None:
        if not is_windows_dark_mode():
            return

        self.style = ttk.Style(self)
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass

        background = "#1e1e1e"
        panel_background = "#252526"
        field_background = "#2d2d30"
        foreground = "#d4d4d4"
        border_color = "#3f3f46"
        selected_background = "#0a84ff"
        selected_foreground = "#ffffff"

        self.configure(bg=background)
        self.option_add("*Background", background)
        self.option_add("*foreground", foreground)
        self.option_add("*FieldBackground", field_background)
        self.option_add("*Entry.Background", field_background)
        self.option_add("*Text.background", field_background)
        self.option_add("*Text.foreground", foreground)
        self.option_add("*Menu.background", background)
        self.option_add("*Menu.foreground", foreground)
        self.option_add("*Menu.activeBackground", panel_background)
        self.option_add("*Menu.activeForeground", foreground)

        self.style.configure(".", background=background, foreground=foreground)
        self.style.configure("TFrame", background=background)
        self.style.configure("TLabel", background=background, foreground=foreground)
        self.style.configure("TLabelframe", background=background, foreground=foreground)
        self.style.configure("TLabelframe.Label", background=background, foreground=foreground)
        self.style.configure("TButton", background=panel_background, foreground=foreground)
        self.style.map(
            "TButton",
            background=[("active", border_color), ("pressed", border_color)],
            foreground=[("disabled", "#777777")],
        )
        self.style.configure("TEntry", fieldbackground=field_background, foreground=foreground, background=background)
        self.style.configure("TCombobox", fieldbackground=field_background, foreground=foreground, background=background)
        self.style.configure(
            "Treeview",
            background=field_background,
            fieldbackground=field_background,
            foreground=foreground,
            bordercolor=border_color,
            lightcolor=border_color,
            darkcolor=border_color,
        )
        self.style.configure("Treeview.Heading", background=panel_background, foreground=foreground)
        self.style.map(
            "Treeview",
            background=[("selected", selected_background)],
            foreground=[("selected", selected_foreground)],
        )
        self.style.configure("Vertical.TScrollbar", background=background, troughcolor=panel_background)
        self.style.configure("Horizontal.TScrollbar", background=background, troughcolor=panel_background)
        self.style.configure("TNotebook", background=background)
        self.style.configure("TNotebook.Tab", background=panel_background, foreground=foreground)
        self.style.map(
            "TNotebook.Tab",
            background=[("selected", background)],
            foreground=[("selected", foreground)],
        )
        self.style.configure("TProgressbar", troughcolor=panel_background, background=selected_background)

    def report_callback_exception(self, exc: type[BaseException], val: BaseException, tb: object) -> None:
        details = "".join(traceback.format_exception(exc, val, tb))
        self._append_log(details.rstrip())
        self.status_var.set("An unexpected error occurred.")
        messagebox.showerror("Unexpected error", str(val))

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.notebook = ttk.Notebook(self)
        self.notebook.grid(row=0, column=0, sticky="nsew")

        self.dl1_tab = ttk.Frame(self.notebook)
        self.dl1_tab.columnconfigure(0, weight=1)
        self.dl1_tab.rowconfigure(0, weight=1)
        self.notebook.add(self.dl1_tab, text="Dying Light 1")

        self.experimental_frame = ExperimentalWwiseFrame(self.notebook, self.settings.experimental)
        self.notebook.add(self.experimental_frame, text="Dying Light 2 / The Beast (Experimental)")

        self.main_frame = ttk.Frame(self.dl1_tab)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(1, weight=3)
        self.main_frame.rowconfigure(2, weight=2)

        settings_frame = ttk.LabelFrame(self.main_frame, text="Build Output")
        settings_frame.grid(row=0, column=0, sticky="nsew", padx=12, pady=(12, 6))
        for column in range(6):
            settings_frame.columnconfigure(column, weight=1)

        ttk.Label(settings_frame, text="Mod Name").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(settings_frame, textvariable=self.mod_name_var).grid(row=0, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(settings_frame, text="Bundle Name").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(settings_frame, textvariable=self.bundle_name_var).grid(row=0, column=3, sticky="ew", padx=6, pady=6)
        ttk.Label(settings_frame, text="Builder Mode").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Combobox(
            settings_frame,
            textvariable=self.builder_mode_var,
            values=("Raw Audio via DLDT", "Existing FSB Files"),
            state="readonly",
        ).grid(row=0, column=5, sticky="ew", padx=6, pady=6)

        ttk.Label(settings_frame, text="Mods Root").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(settings_frame, textvariable=self.mods_root_var).grid(row=1, column=1, columnspan=4, sticky="ew", padx=6, pady=6)
        ttk.Button(settings_frame, text="Browse", command=self._browse_mods_root).grid(row=1, column=5, sticky="ew", padx=6, pady=6)

        ttk.Label(settings_frame, text="DLDT Root").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(settings_frame, textvariable=self.dldt_root_var).grid(row=2, column=1, columnspan=4, sticky="ew", padx=6, pady=6)
        ttk.Button(settings_frame, text="Browse", command=self._browse_dldt_root).grid(row=2, column=5, sticky="ew", padx=6, pady=6)

        ttk.Label(settings_frame, textvariable=self.toolchain_status_var).grid(
            row=3,
            column=0,
            columnspan=6,
            sticky="w",
            padx=6,
            pady=(0, 6),
        )
        ttk.Label(settings_frame, textvariable=self.loaded_csb_var).grid(
            row=4,
            column=0,
            columnspan=6,
            sticky="w",
            padx=6,
            pady=(0, 6),
        )
        ttk.Label(settings_frame, textvariable=self.preview_tools_var).grid(
            row=5,
            column=0,
            columnspan=6,
            sticky="w",
            padx=6,
            pady=(0, 6),
        )

        content = ttk.Panedwindow(self.main_frame, orient="horizontal")
        content.grid(row=1, column=0, sticky="nsew", padx=12, pady=6)

        left = ttk.Frame(content)
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)
        content.add(left, weight=3)

        right = ttk.Frame(content)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)
        content.add(right, weight=2)

        self._build_entries_frame(left)
        self._build_detail_frame(right)
        self._build_bottom_frame()

    def _build_entries_frame(self, parent: ttk.Frame) -> None:
        entries_frame = ttk.LabelFrame(parent, text="Audio Entries")
        entries_frame.grid(row=0, column=0, sticky="nsew")
        entries_frame.columnconfigure(0, weight=1)
        entries_frame.rowconfigure(2, weight=1)

        toolbar = ttk.Frame(entries_frame)
        toolbar.grid(row=0, column=0, sticky="ew", padx=6, pady=6)
        for index in range(9):
            toolbar.columnconfigure(index, weight=1)

        self.add_audio_button = ttk.Button(toolbar, text="Add Audio", command=self._add_audio_files)
        self.add_audio_button.grid(row=0, column=0, sticky="ew", padx=2)
        self.add_fsb_button = ttk.Button(toolbar, text="Add FSB", command=self._add_fsb_files)
        self.add_fsb_button.grid(row=0, column=1, sticky="ew", padx=2)
        self.import_manifest_button = ttk.Button(toolbar, text="Import Manifest", command=self._import_manifest)
        self.import_manifest_button.grid(row=0, column=2, sticky="ew", padx=2)
        self.replace_audio_button = ttk.Button(toolbar, text="Replace Audio", command=self._replace_selected_with_audio)
        self.replace_audio_button.grid(row=0, column=3, sticky="ew", padx=2)
        self.replace_fsb_button = ttk.Button(toolbar, text="Replace FSB", command=self._replace_selected_with_fsb)
        self.replace_fsb_button.grid(row=0, column=4, sticky="ew", padx=2)
        self.remove_entry_button = ttk.Button(toolbar, text="Remove", command=self._remove_selected)
        self.remove_entry_button.grid(row=0, column=5, sticky="ew", padx=2)
        self.move_up_button = ttk.Button(toolbar, text="Move Up", command=lambda: self._move_selected(-1))
        self.move_up_button.grid(row=0, column=6, sticky="ew", padx=2)
        self.move_down_button = ttk.Button(toolbar, text="Move Down", command=lambda: self._move_selected(1))
        self.move_down_button.grid(row=0, column=7, sticky="ew", padx=2)
        self.clear_entries_button = ttk.Button(toolbar, text="Clear", command=self._clear_entries)
        self.clear_entries_button.grid(row=0, column=8, sticky="ew", padx=2)

        filter_bar = ttk.Frame(entries_frame)
        filter_bar.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))
        for index, weight in enumerate((0, 2, 0, 1, 0, 0, 0)):
            filter_bar.columnconfigure(index, weight=weight)

        ttk.Label(filter_bar, text="Search").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.search_entry = ttk.Entry(filter_bar, textvariable=self.entry_search_var)
        self.search_entry.grid(row=0, column=1, sticky="ew", padx=(0, 12))
        ttk.Label(filter_bar, text="Sort By").grid(row=0, column=2, sticky="w", padx=(0, 6))
        self.sort_combo = ttk.Combobox(
            filter_bar,
            textvariable=self.sort_field_var,
            values=("Original Order", "Name", "Mode", "Source", "Type", "Duration", "Samples"),
            state="readonly",
            width=18,
        )
        self.sort_combo.grid(row=0, column=3, sticky="ew", padx=(0, 8))
        self.sort_order_button = ttk.Button(filter_bar, textvariable=self.sort_button_var, command=self._toggle_sort_direction)
        self.sort_order_button.grid(row=0, column=4, sticky="ew", padx=(0, 8))
        ttk.Button(filter_bar, text="Clear Search", command=self._clear_search).grid(row=0, column=5, sticky="ew", padx=(0, 8))
        ttk.Label(filter_bar, textvariable=self.entry_count_var).grid(row=0, column=6, sticky="e")

        columns = ("name", "mode", "source", "type", "duration", "samples")
        self.tree = ttk.Treeview(entries_frame, columns=columns, show="headings", height=14)
        self.tree.grid(row=2, column=0, sticky="nsew", padx=6, pady=(0, 6))
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Button-3>", self._show_tree_context_menu)
        self.tree.bind("<Shift-F10>", self._show_tree_context_menu)

        headings = {
            "name": ("Entry Name", 220),
            "mode": ("Mode", 110),
            "source": ("Source", 420),
            "type": ("Type", 70),
            "duration": ("Duration (ms)", 110),
            "samples": ("Samples @ 48k", 120),
        }
        for column, (title, width) in headings.items():
            self.tree.heading(column, text=title, command=lambda value=title: self._set_sort_field_from_heading(value))
            self.tree.column(column, width=width, anchor="w")

        scroll_x = ttk.Scrollbar(entries_frame, orient="horizontal", command=self.tree.xview)
        scroll_x.grid(row=3, column=0, sticky="ew", padx=6, pady=(0, 6))
        self.tree.configure(xscrollcommand=scroll_x.set)

        self.entry_context_menu = tk.Menu(self, tearoff=False)
        self.entry_context_menu.add_command(label="Replace Audio...", command=self._replace_selected_with_audio)
        self.entry_context_menu.add_command(label="Replace FSB...", command=self._replace_selected_with_fsb)
        self.entry_context_menu.add_separator()
        self.entry_context_menu.add_command(label="Export Audio...", command=self._export_selected_audio)
        self.entry_context_menu.add_command(label="Export FSB...", command=self._export_selected_fsb)
        self.entry_context_menu.add_separator()
        self.entry_context_menu.add_command(label="Duplicate Entry", command=self._duplicate_selected_entry)
        self.entry_context_menu.add_command(label="Rename Entry...", command=self._rename_selected_entry)
        self.entry_context_menu.add_command(label="Remove Entry", command=self._remove_selected)

    def _build_detail_frame(self, parent: ttk.Frame) -> None:
        right_tabs = ttk.Notebook(parent)
        right_tabs.grid(row=0, column=0, sticky="nsew")

        detail_frame = ttk.Frame(right_tabs, padding=6)
        detail_frame.columnconfigure(1, weight=1)
        right_tabs.add(detail_frame, text="Selected Entry")

        ttk.Label(detail_frame, text="Entry Name").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.selected_name_entry = ttk.Entry(detail_frame, textvariable=self.selected_name_var)
        self.selected_name_entry.grid(row=0, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(detail_frame, text="Type").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        self.selected_type_entry = ttk.Entry(detail_frame, textvariable=self.selected_type_var)
        self.selected_type_entry.grid(row=1, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(detail_frame, text="Samples @ 48k").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        self.selected_sample_count_entry = ttk.Entry(detail_frame, textvariable=self.selected_sample_count_var)
        self.selected_sample_count_entry.grid(row=2, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(detail_frame, text="Duration (ms)").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        self.selected_duration_entry = ttk.Entry(detail_frame, textvariable=self.selected_duration_var)
        self.selected_duration_entry.grid(row=3, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(detail_frame, text="Source").grid(row=4, column=0, sticky="nw", padx=6, pady=6)
        ttk.Label(detail_frame, textvariable=self.selected_source_var, wraplength=420).grid(row=4, column=1, sticky="w", padx=6, pady=6)
        ttk.Label(detail_frame, text="FSB").grid(row=5, column=0, sticky="nw", padx=6, pady=6)
        ttk.Label(detail_frame, textvariable=self.selected_fsb_var, wraplength=420).grid(row=5, column=1, sticky="w", padx=6, pady=6)
        ttk.Label(detail_frame, text="Notes").grid(row=6, column=0, sticky="nw", padx=6, pady=6)
        ttk.Label(detail_frame, textvariable=self.selected_notes_var, wraplength=420).grid(row=6, column=1, sticky="w", padx=6, pady=6)

        preview_frame = ttk.LabelFrame(detail_frame, text="Preview")
        preview_frame.grid(row=7, column=0, columnspan=2, sticky="ew", padx=6, pady=(6, 6))
        preview_frame.columnconfigure(0, weight=1)
        ttk.Label(preview_frame, textvariable=self.preview_info_var, wraplength=420).grid(
            row=0, column=0, columnspan=2, sticky="w", padx=6, pady=6
        )
        ttk.Button(preview_frame, text="Play Selected", command=self._play_selected_entry).grid(
            row=1, column=0, sticky="ew", padx=6, pady=(0, 6)
        )
        ttk.Button(preview_frame, text="Stop", command=self._stop_preview).grid(
            row=1, column=1, sticky="ew", padx=6, pady=(0, 6)
        )
        self.preview_progress = ttk.Progressbar(
            preview_frame,
            maximum=100,
            variable=self.playback_progress_var,
        )
        self.preview_progress.grid(row=2, column=0, columnspan=2, sticky="ew", padx=6, pady=(0, 2))
        ttk.Label(preview_frame, textvariable=self.playback_status_var).grid(
            row=3, column=0, columnspan=2, sticky="w", padx=6, pady=(0, 6)
        )

        ttk.Button(detail_frame, text="Apply Entry Changes", command=self._apply_selected_entry).grid(
            row=8, column=0, columnspan=2, sticky="ew", padx=6, pady=(6, 8)
        )

        for widget in (
            self.selected_name_entry,
            self.selected_type_entry,
            self.selected_sample_count_entry,
            self.selected_duration_entry,
        ):
            widget.bind("<FocusOut>", self._commit_selected_entry_from_focus)
            widget.bind("<Return>", self._commit_selected_entry_from_focus)

        script_frame = ttk.Frame(right_tabs, padding=6)
        script_frame.columnconfigure(0, weight=1)
        script_frame.rowconfigure(2, weight=1)
        script_frame.rowconfigure(4, weight=1)
        right_tabs.add(script_frame, text="Script Generation")

        ttk.Checkbutton(
            script_frame,
            text="Generate placeholder audiodata.scr",
            variable=self.generate_script_var,
            command=self._update_script_preview,
        ).grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Label(script_frame, text="AudioProc names (one per line)").grid(row=1, column=0, sticky="w", padx=6, pady=(0, 6))

        self.proc_text = ScrolledText(script_frame, height=6, wrap="none")
        self.proc_text.grid(row=2, column=0, sticky="nsew", padx=6, pady=(0, 6))
        self.proc_text.bind("<<Modified>>", self._on_proc_text_modified)

        ttk.Label(script_frame, text="Preview").grid(row=3, column=0, sticky="w", padx=6, pady=(0, 6))
        self.preview_text = ScrolledText(script_frame, height=10, wrap="none", state="disabled")
        self.preview_text.grid(row=4, column=0, sticky="nsew", padx=6, pady=(0, 6))

    def _build_bottom_frame(self) -> None:
        bottom = ttk.Frame(self.main_frame)
        bottom.grid(row=2, column=0, sticky="nsew", padx=12, pady=(6, 12))
        bottom.columnconfigure(0, weight=1)
        bottom.rowconfigure(1, weight=1)

        actions = ttk.Frame(bottom)
        actions.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        for index in range(6):
            actions.columnconfigure(index, weight=1)

        self.open_csb_button = ttk.Button(actions, text="Open CSB For Edit", command=self._open_csb_for_editing)
        self.open_csb_button.grid(row=0, column=0, sticky="ew", padx=2)
        self.inspect_csb_button = ttk.Button(actions, text="Inspect CSB", command=self._inspect_csb)
        self.inspect_csb_button.grid(row=0, column=1, sticky="ew", padx=2)
        self.extract_csb_button = ttk.Button(actions, text="Extract CSB", command=self._extract_csb)
        self.extract_csb_button.grid(row=0, column=2, sticky="ew", padx=2)
        self.save_csb_button = ttk.Button(actions, text="Save CSB File", command=self._save_csb_file)
        self.save_csb_button.grid(row=0, column=3, sticky="ew", padx=2)
        self.build_mod_button = ttk.Button(actions, text="Build Mod", command=self._build_mod)
        self.build_mod_button.grid(row=0, column=4, sticky="ew", padx=2)
        self.open_mod_folder_button = ttk.Button(actions, text="Open Mod Folder", command=self._open_mod_folder)
        self.open_mod_folder_button.grid(row=0, column=5, sticky="ew", padx=2)
        ttk.Label(actions, textvariable=self.status_var).grid(row=1, column=0, columnspan=6, sticky="e", padx=4, pady=(4, 0))
        self._dl1_busy_widgets = [
            self.add_audio_button,
            self.add_fsb_button,
            self.import_manifest_button,
            self.replace_audio_button,
            self.replace_fsb_button,
            self.remove_entry_button,
            self.move_up_button,
            self.move_down_button,
            self.clear_entries_button,
            self.open_csb_button,
            self.inspect_csb_button,
            self.extract_csb_button,
            self.save_csb_button,
            self.build_mod_button,
            self.open_mod_folder_button,
        ]

        self.log_text = ScrolledText(bottom, height=9, wrap="word")
        self.log_text.grid(row=1, column=0, sticky="nsew")

    def _load_proc_names(self) -> None:
        proc_names = self.settings.audio_proc_names or list(DEFAULT_AUDIO_PROCS)
        self.proc_text.delete("1.0", tk.END)
        self.proc_text.insert("1.0", "\n".join(proc_names))
        self.proc_text.edit_modified(False)

    def _on_settings_changed(self, *_args: object) -> None:
        self._update_toolchain_status()
        self._update_script_preview()

    def _on_entry_filter_changed(self, *_args: object) -> None:
        self._update_sort_controls()
        self._refresh_tree()

    def _update_sort_controls(self) -> None:
        is_manual_order = self.sort_field_var.get().strip() == "Original Order"
        self.sort_button_var.set("Manual Order" if is_manual_order else ("Descending" if self.sort_descending_var.get() else "Ascending"))
        self.sort_order_button.configure(state="disabled" if is_manual_order else "normal")

    def _set_dl1_busy(self, busy: bool) -> None:
        for widget in self._dl1_busy_widgets:
            widget.configure(state="disabled" if busy else "normal")

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
        self._set_dl1_busy(False)

    def _run_dl1_task(
        self,
        *,
        start_message: str,
        error_title: str,
        worker: callable,
        on_success: callable,
    ) -> None:
        if self.task_runner.is_running:
            messagebox.showinfo("Dying Light 1 workspace busy", "Wait for the current task to finish first.")
            return
        self._set_dl1_busy(True)
        self.task_status_var.set(start_message)
        self.status_var.set(start_message)
        self.task_progress_var.set(0.0)
        self._show_loading_window(start_message)

        def handle_error(exc: BaseException, details: str) -> None:
            self._append_log(details.rstrip())
            messagebox.showerror(error_title, str(exc))
            self.status_var.set(error_title.replace(" failed", " failed."))

        def start_background_task() -> None:
            self.task_runner.start(
                worker,
                on_progress=self._apply_task_progress,
                on_log=self._append_log,
                on_success=on_success,
                on_error=handle_error,
                on_finally=self._finish_task_ui,
            )

        self.after(10, start_background_task)

    def _toggle_sort_direction(self) -> None:
        if self.sort_field_var.get().strip() == "Original Order":
            return
        self.sort_descending_var.set(not self.sort_descending_var.get())

    def _clear_search(self) -> None:
        self.entry_search_var.set("")

    def _set_sort_field_from_heading(self, heading_title: str) -> None:
        field_map = {
            "Entry Name": "Name",
            "Mode": "Mode",
            "Source": "Source",
            "Type": "Type",
            "Duration (ms)": "Duration",
            "Samples @ 48k": "Samples",
        }
        target = field_map.get(heading_title, "Original Order")
        if self.sort_field_var.get() == target:
            self._toggle_sort_direction()
            return
        self.sort_field_var.set(target)

    def _update_toolchain_status(self) -> None:
        root = self._resolve_dldt_root(allow_discovery=False)
        if root is None:
            self.current_toolchain = None
            self.toolchain_status_var.set("DLDT toolchain not selected. Click Browse to auto-find it.")
            return
        toolchain, errors = discover_toolchain(root)
        self.current_toolchain = toolchain
        if toolchain:
            self.toolchain_status_var.set(f"DLDT toolchain ready: {toolchain.fsb_dir}")
        elif self.builder_mode_var.get().strip() == "Existing FSB Files":
            self.toolchain_status_var.set("DLDT toolchain not ready, but Existing FSB Files mode can still build.")
        else:
            self.toolchain_status_var.set("DLDT toolchain not ready: " + "; ".join(errors))

    def _on_proc_text_modified(self, _event: object) -> None:
        if self.proc_text.edit_modified():
            self.proc_text.edit_modified(False)
            self._update_script_preview()

    def _set_preview_text(self, text: str) -> None:
        self.preview_text.configure(state="normal")
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert("1.0", text)
        self.preview_text.configure(state="disabled")

    def _update_script_preview(self) -> None:
        if not self.generate_script_var.get():
            self._set_preview_text("// audiodata.scr generation is disabled.\n")
            return

        try:
            preview = generate_audiodata_scr(self.bundle_name_var.get().strip(), self.proc_text.get("1.0", tk.END))
        except ValueError:
            preview = "// Enter a bundle name to preview audiodata.scr.\n"
        self._set_preview_text(preview)

    def _append_log(self, message: str) -> None:
        if not message:
            return
        self.log_text.insert(tk.END, message.rstrip() + "\n")
        self.log_text.see(tk.END)

    def _format_preview_time(self, milliseconds: int) -> str:
        total_seconds = max(0, milliseconds) // 1000
        minutes, seconds = divmod(total_seconds, 60)
        return f"{minutes:02d}:{seconds:02d}"

    def _cancel_preview_progress_updates(self) -> None:
        if self._preview_after_id is None:
            return
        try:
            self.after_cancel(self._preview_after_id)
        except tk.TclError:
            pass
        self._preview_after_id = None

    def _reset_preview_progress(self, status: str = "Playback idle.") -> None:
        self._cancel_preview_progress_updates()
        if self._preview_indeterminate:
            self.preview_progress.stop()
            self._preview_indeterminate = False
        self.preview_progress.configure(mode="determinate")
        self._preview_started_at = None
        self._preview_duration_ms = 0
        self._preview_playing = False
        self._preview_entry_name = ""
        self._preview_playback_kind = None
        self.playback_progress_var.set(0.0)
        self.playback_status_var.set(status)

    def _begin_preview_progress(self, entry: AudioEntry) -> None:
        self._cancel_preview_progress_updates()
        self._preview_started_at = time.monotonic()
        self._preview_duration_ms = max(entry.duration_ms, 0)
        self._preview_playing = True
        self._preview_entry_name = entry.entry_name
        self._preview_playback_kind = self.preview_player.playback_kind()
        if self._preview_duration_ms <= 0:
            if not self._preview_indeterminate:
                self.preview_progress.configure(mode="indeterminate")
                self.preview_progress.start(15)
                self._preview_indeterminate = True
            self.playback_status_var.set(f"Playing {entry.entry_name}...")
        else:
            if self._preview_indeterminate:
                self.preview_progress.stop()
                self._preview_indeterminate = False
            self.preview_progress.configure(mode="determinate")
            self.playback_progress_var.set(0.0)
            self.playback_status_var.set(
                f"Playing {entry.entry_name}: 00:00 / {self._format_preview_time(self._preview_duration_ms)}"
            )
        self._preview_after_id = self.after(100, self._update_preview_progress)

    def _update_preview_progress(self) -> None:
        self._preview_after_id = None
        if not self._preview_playing or self._preview_started_at is None:
            return

        elapsed_ms = int((time.monotonic() - self._preview_started_at) * 1000)
        duration_ms = self._preview_duration_ms
        live_process = self.preview_player.has_live_process()

        if duration_ms > 0:
            if self._preview_indeterminate:
                self.preview_progress.stop()
                self._preview_indeterminate = False
            self.preview_progress.configure(mode="determinate")
            progress = min(100.0, max(0.0, (elapsed_ms * 100.0) / duration_ms))
            self.playback_progress_var.set(progress)
            self.playback_status_var.set(
                f"Playing {self._preview_entry_name}: {self._format_preview_time(elapsed_ms)} / "
                f"{self._format_preview_time(duration_ms)}"
            )
            if self._preview_playback_kind != "process" and elapsed_ms >= duration_ms:
                self._reset_preview_progress("Playback idle.")
                return
            if self._preview_playback_kind == "process" and not live_process:
                self._reset_preview_progress("Playback idle.")
                return
        else:
            if not self._preview_indeterminate:
                self.preview_progress.configure(mode="indeterminate")
                self.preview_progress.start(15)
                self._preview_indeterminate = True
            self.playback_status_var.set(f"Playing {self._preview_entry_name}...")
            if self._preview_playback_kind == "process" and not live_process:
                self._reset_preview_progress("Playback idle.")
                return

        self._preview_after_id = self.after(100, self._update_preview_progress)

    def _run_logged_command(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        result = run_hidden(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if result.stdout.strip():
            self._append_log(result.stdout.strip())
        if result.stderr.strip():
            self._append_log(result.stderr.strip())
        return result

    def _has_raw_entries(self) -> bool:
        return any(entry.source_mode == "raw" for entry in self.entries)

    def _ensure_raw_builder_mode(self, *, notify: bool = False) -> None:
        if not self._has_raw_entries():
            return
        if self.builder_mode_var.get().strip() != "Raw Audio via DLDT":
            self.builder_mode_var.set("Raw Audio via DLDT")
            self._append_log("Switched builder mode to Raw Audio via DLDT because the current project includes raw audio.")
            if notify:
                self.status_var.set("Switched to Raw Audio via DLDT for raw audio entries.")

    def _warn_if_raw_entries_need_toolchain(self) -> None:
        if self._has_raw_entries() and self.current_toolchain is None:
            messagebox.showwarning(
                "DLDT toolchain required",
                "This project includes raw audio. Saving or building it will require a valid DLDT toolchain path.",
            )

    def _commit_selected_entry_from_focus(self, _event: object) -> None:
        self._apply_selected_entry()

    def _select_entry(self, index: int) -> None:
        if index < 0 or index >= len(self.entries):
            return
        iid = str(index)
        self.tree.selection_set(iid)
        self.tree.focus(iid)
        self.tree.see(iid)
        self._on_tree_select(None)

    def _set_loaded_csb(self, path: str | Path | None) -> None:
        self.loaded_csb_path = Path(path).resolve() if path else None
        if self.loaded_csb_path is None:
            self.loaded_csb_var.set("Loaded CSB: none")
        else:
            self.loaded_csb_var.set(f"Loaded CSB: {self.loaded_csb_path}")

    def _set_loaded_csb_magic(self, magic: int | None) -> None:
        self.loaded_csb_magic = magic

    def _set_loaded_csb_layout(self, layout: str | None) -> None:
        self.loaded_csb_layout = layout

    def _effective_output_magic(self) -> int | None:
        return self.loaded_csb_magic if self.loaded_csb_magic is not None else WORKSHOP_MAGIC

    def _format_csb_variant(self, magic: int | None, layout: str | None) -> str:
        if layout == "compact_no_magic":
            return "compact header (no magic)"
        if magic is None:
            return "unknown header"
        return f"magic 0x{magic:08X}"

    def _update_preview_info(self) -> None:
        index = self._selected_index()
        if index is None:
            self.preview_info_var.set("Select an entry to preview it.")
            return
        self.preview_info_var.set(preview_strategy_for_entry(self.entries[index], self.preview_player.environment))

    def _cleanup_edit_session(self) -> None:
        if self.edit_session_dir is not None:
            self.edit_session_dir.cleanup()
            self.edit_session_dir = None

    def _resolve_mods_root(self, *, allow_discovery: bool) -> Path | None:
        current = self.mods_root_var.get().strip()
        if current:
            candidate = Path(current).expanduser()
            if candidate.exists():
                return candidate.resolve()
        if allow_discovery:
            return discover_mods_root()
        return None

    def _resolve_dldt_root(self, *, allow_discovery: bool) -> Path | None:
        current = self.dldt_root_var.get().strip()
        if current:
            candidate = Path(current).expanduser()
            if candidate.exists():
                return candidate.resolve()
        if allow_discovery:
            return discover_dldt_root()
        return None

    def _browse_mods_root(self) -> None:
        selection = discover_mods_root()
        if selection is None:
            current = self.mods_root_var.get().strip()
            if current:
                candidate = Path(current).expanduser()
                if candidate.exists():
                    selection = candidate.resolve()
        if selection is None:
            initialdir = self.mods_root_var.get().strip() or None
            chosen = filedialog.askdirectory(title="Select Dying Light Mods root", initialdir=initialdir)
            if chosen:
                selection = Path(chosen).resolve()
        if selection is not None:
            self.mods_root_var.set(str(selection))
            self._save_settings()

    def _browse_dldt_root(self) -> None:
        selection = discover_dldt_root()
        if selection is None:
            current = self.dldt_root_var.get().strip()
            if current:
                candidate = Path(current).expanduser()
                if candidate.exists():
                    selection = candidate.resolve()
        if selection is None:
            initialdir = self.dldt_root_var.get().strip() or None
            chosen = filedialog.askdirectory(title="Select Dying Light Developer Tools root", initialdir=initialdir)
            if chosen:
                selection = Path(chosen).resolve()
        if selection is not None:
            self.dldt_root_var.set(str(selection))
            self._save_settings()

    def _refresh_tree(self) -> None:
        selected_index = self._selected_index()
        self.tree.delete(*self.tree.get_children())
        visible_indices = self._visible_entry_indices()
        for index in visible_indices:
            entry = self.entries[index]
            self.tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    entry.entry_name,
                    entry.source_mode,
                    entry.display_source(),
                    entry.entry_type,
                    entry.duration_ms,
                    entry.sample_count,
                ),
            )
        self.entry_count_var.set(f"{len(visible_indices)} shown / {len(self.entries)} total")
        if selected_index is not None and str(selected_index) in self.tree.get_children():
            self.tree.selection_set(str(selected_index))
            self.tree.focus(str(selected_index))
            self.tree.see(str(selected_index))
        elif visible_indices:
            self.tree.selection_set(str(visible_indices[0]))
            self.tree.focus(str(visible_indices[0]))
        self._on_tree_select(None)

    def _visible_entry_indices(self) -> list[int]:
        search_text = self.entry_search_var.get().strip().lower()
        indexed_entries = list(enumerate(self.entries))
        if search_text:
            indexed_entries = [
                (index, entry)
                for index, entry in indexed_entries
                if search_text in entry.entry_name.lower()
                or search_text in entry.display_source().lower()
                or search_text in entry.notes.lower()
            ]

        sort_field = self.sort_field_var.get().strip()
        if sort_field != "Original Order":
            key_map = {
                "Name": lambda pair: pair[1].entry_name.lower(),
                "Mode": lambda pair: pair[1].source_mode.lower(),
                "Source": lambda pair: pair[1].display_source().lower(),
                "Type": lambda pair: pair[1].entry_type,
                "Duration": lambda pair: pair[1].duration_ms,
                "Samples": lambda pair: pair[1].sample_count,
            }
            indexed_entries.sort(key=key_map.get(sort_field, lambda pair: pair[0]), reverse=self.sort_descending_var.get())

        return [index for index, _entry in indexed_entries]

    def _selected_index(self) -> int | None:
        selected = self.tree.selection()
        if not selected:
            return None
        return int(selected[0])

    def _selected_entry(self) -> AudioEntry | None:
        index = self._selected_index()
        if index is None:
            return None
        return self.entries[index]

    def _on_tree_select(self, _event: object) -> None:
        index = self._selected_index()
        if index is None:
            self.selected_name_var.set("")
            self.selected_type_var.set("2")
            self.selected_sample_count_var.set("0")
            self.selected_duration_var.set("0")
            self.selected_source_var.set("")
            self.selected_fsb_var.set("")
            self.selected_notes_var.set("")
            self._update_preview_info()
            return
        entry = self.entries[index]
        self.selected_name_var.set(entry.entry_name)
        self.selected_type_var.set(str(entry.entry_type))
        self.selected_sample_count_var.set(str(entry.sample_count))
        self.selected_duration_var.set(str(entry.duration_ms))
        self.selected_source_var.set(entry.source_path)
        self.selected_fsb_var.set(entry.fsb_path)
        self.selected_notes_var.set(entry.notes)
        self._update_preview_info()

    def _show_tree_context_menu(self, event: object) -> str | None:
        if not hasattr(event, "x") or not hasattr(event, "y"):
            return None

        row_id = self.tree.identify_row(event.y)
        if row_id:
            self.tree.selection_set(row_id)
            self.tree.focus(row_id)
            self._on_tree_select(None)

        selected_entry = self._selected_entry()
        has_selection = selected_entry is not None
        can_export_audio = has_selection
        can_export_fsb = has_selection and (
            selected_entry.source_mode == "fsb" or self.current_toolchain is not None
        )

        self.entry_context_menu.entryconfigure("Replace Audio...", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Replace FSB...", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Export Audio...", state="normal" if can_export_audio else "disabled")
        self.entry_context_menu.entryconfigure("Export FSB...", state="normal" if can_export_fsb else "disabled")
        self.entry_context_menu.entryconfigure("Duplicate Entry", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Rename Entry...", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Remove Entry", state="normal" if has_selection else "disabled")

        if hasattr(event, "x_root") and hasattr(event, "y_root"):
            self.entry_context_menu.tk_popup(event.x_root, event.y_root)
            self.entry_context_menu.grab_release()
        return "break"

    def _apply_selected_entry(self) -> bool:
        index = self._selected_index()
        if index is None:
            return True

        try:
            entry_type = int(self.selected_type_var.get() or 2)
            sample_count = int(self.selected_sample_count_var.get() or 0)
            duration_ms = int(self.selected_duration_var.get() or 0)
        except ValueError:
            messagebox.showerror(
                "Invalid entry values",
                "Type, Samples @ 48k, and Duration (ms) must be whole numbers.",
            )
            self.status_var.set("Entry update failed.")
            return False

        entry = self.entries[index]
        entry.entry_name = self.selected_name_var.get().strip() or entry.entry_name
        entry.entry_type = entry_type
        entry.sample_count = sample_count
        entry.duration_ms = duration_ms
        self._refresh_tree()
        self.tree.selection_set(str(index))
        self.tree.focus(str(index))
        self.status_var.set(f"Updated entry '{entry.entry_name}'.")
        return True

    def _add_audio_files(self) -> None:
        selections = filedialog.askopenfilenames(
            title="Select audio files",
            filetypes=COMMON_AUDIO_FILETYPES,
        )
        start_index = len(self.entries)
        for selection in selections:
            metadata = probe_audio_metadata(selection)
            path = Path(selection)
            self.entries.append(
                AudioEntry(
                    entry_name=path.stem,
                    source_path=str(path),
                    source_mode="raw",
                    entry_type=2,
                    sample_count=metadata.sample_count_48k,
                    duration_ms=metadata.duration_ms,
                    notes=metadata.notes,
                )
            )
        self._ensure_raw_builder_mode()
        self._refresh_tree()
        if selections:
            self._select_entry(start_index)
            self.status_var.set(f"Added {len(selections)} raw audio file(s).")
            self._warn_if_raw_entries_need_toolchain()
        else:
            self._update_preview_info()

    def _add_fsb_files(self) -> None:
        selections = filedialog.askopenfilenames(
            title="Select FSB files",
            filetypes=[("FSB files", "*.fsb"), ("All files", "*.*")],
        )
        start_index = len(self.entries)
        for selection in selections:
            path = Path(selection)
            self.entries.append(
                AudioEntry(
                    entry_name=path.stem,
                    source_path=str(path),
                    fsb_path=str(path),
                    source_mode="fsb",
                    entry_type=2,
                    notes="Existing FSB file.",
                )
            )
        self._refresh_tree()
        if selections:
            self._select_entry(start_index)
            self.status_var.set(f"Added {len(selections)} FSB file(s).")
        else:
            self._update_preview_info()

    def _import_manifest(self) -> None:
        selection = filedialog.askopenfilename(
            title="Import manifest",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not selection:
            return
        imported = load_manifest(selection)
        start_index = len(self.entries)
        self.entries.extend(imported)
        self._ensure_raw_builder_mode()
        self._refresh_tree()
        if imported:
            self._select_entry(start_index)
            self._warn_if_raw_entries_need_toolchain()
        self.status_var.set(f"Imported {len(imported)} entry/entries from manifest.")

    def _open_csb_for_editing(self) -> None:
        selection = filedialog.askopenfilename(
            title="Open CSB For Editing",
            filetypes=[("CSB files", "*.csb"), ("All files", "*.*")],
        )
        if not selection:
            return

        def worker(progress, _log):
            progress("Parsing CSB header...", None, None)
            parsed = parse_csb(selection)
            session_dir = tempfile.TemporaryDirectory(prefix="dyingaudio_edit_")
            extracted = extract_csb(selection, session_dir.name, progress=progress)
            return parsed, session_dir, extracted

        def on_success(result: object) -> None:
            parsed, session_dir, extracted = result
            self.preview_player.stop()
            self._reset_preview_progress()
            self.preview_player.clear_cache()
            self._cleanup_edit_session()
            self.edit_session_dir = session_dir
            self.entries = extracted
            self.bundle_name_var.set(Path(selection).stem)
            self.builder_mode_var.set("Existing FSB Files")
            self._set_loaded_csb(selection)
            self._set_loaded_csb_magic(parsed.magic)
            self._set_loaded_csb_layout(parsed.layout)
            self._refresh_tree()
            if self.entries:
                self._select_entry(0)
            else:
                self._update_preview_info()
            self.task_status_var.set("Open complete.")
            self._append_log(
                f"Loaded {Path(selection).name} for editing with {self._format_csb_variant(parsed.magic, parsed.layout)}."
            )
            self.status_var.set(f"Opened {Path(selection).name} for editing.")

        self._run_dl1_task(
            start_message=f"Opening {Path(selection).name}...",
            error_title="Open CSB failed",
            worker=worker,
            on_success=on_success,
        )

    def _replace_selected_with_audio(self) -> None:
        if not self._apply_selected_entry():
            return

        index = self._selected_index()
        if index is None:
            messagebox.showinfo("Replace audio", "Select an entry to replace first.")
            return

        selection = filedialog.askopenfilename(
            title="Replace selected entry with audio",
            filetypes=COMMON_AUDIO_FILETYPES,
        )
        if not selection:
            return

        try:
            metadata = probe_audio_metadata(selection)
        except Exception as exc:
            messagebox.showerror("Replace audio failed", str(exc))
            self.status_var.set("Replace failed.")
            self._append_log(f"ERROR: {exc}")
            return

        entry = self.entries[index]
        self.preview_player.stop()
        self._reset_preview_progress()
        entry.source_path = selection
        entry.source_mode = "raw"
        entry.fsb_path = ""
        entry.sample_count = metadata.sample_count_48k
        entry.duration_ms = metadata.duration_ms
        entry.notes = f"Replacement audio: {metadata.notes or Path(selection).name}"
        self._ensure_raw_builder_mode()
        self._warn_if_raw_entries_need_toolchain()
        self._refresh_tree()
        self._select_entry(index)
        self.status_var.set(f"Replaced '{entry.entry_name}' with new audio.")

    def _replace_selected_with_fsb(self) -> None:
        if not self._apply_selected_entry():
            return

        index = self._selected_index()
        if index is None:
            messagebox.showinfo("Replace FSB", "Select an entry to replace first.")
            return

        selection = filedialog.askopenfilename(
            title="Replace selected entry with FSB",
            filetypes=[("FSB files", "*.fsb"), ("All files", "*.*")],
        )
        if not selection:
            return

        entry = self.entries[index]
        self.preview_player.stop()
        self._reset_preview_progress()
        entry.source_path = selection
        entry.source_mode = "fsb"
        entry.fsb_path = selection
        entry.notes = f"Replacement FSB: {Path(selection).name}"
        self._refresh_tree()
        self._select_entry(index)
        self.status_var.set(f"Replaced '{entry.entry_name}' with new FSB.")

    def _suggest_export_audio_name(self, entry: AudioEntry) -> str:
        if entry.source_mode == "raw":
            source = entry.resolved_source_path()
            suffix = source.suffix if source is not None and source.suffix else ".wav"
            return f"{entry.entry_name}{suffix}"
        return f"{entry.entry_name}.wav"

    def _export_selected_audio(self) -> None:
        if not self._apply_selected_entry():
            return

        entry = self._selected_entry()
        if entry is None:
            messagebox.showinfo("Export audio", "Select an entry to export first.")
            return

        selection = filedialog.asksaveasfilename(
            title="Export audio",
            defaultextension=Path(self._suggest_export_audio_name(entry)).suffix,
            initialfile=self._suggest_export_audio_name(entry),
            filetypes=COMMON_AUDIO_FILETYPES + [("WAV files", "*.wav")],
        )
        if not selection:
            return

        destination = Path(selection).resolve()

        def worker(progress, log):
            progress(f"Exporting audio for {entry.entry_name}...", 0, 2)
            if entry.source_mode == "raw":
                source = entry.resolved_source_path()
                if source is None or not source.exists():
                    raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")
                shutil.copyfile(source, destination)
                progress(f"Copied {destination.name}.", 2, 2)
                return destination
            progress(f"Decoding {entry.entry_name} to WAV...", 1, 2)
            preview_path = self.preview_player._prepare_preview_wav(entry, log)
            shutil.copyfile(preview_path, destination)
            progress(f"Copied {destination.name}.", 2, 2)
            return destination

        self._run_dl1_task(
            start_message=f"Exporting audio for '{entry.entry_name}'...",
            error_title="Export audio failed",
            worker=worker,
            on_success=lambda result: (
                self.status_var.set(f"Exported audio for '{entry.entry_name}'."),
                self._append_log(f"Exported audio to {result}"),
                self.task_status_var.set("Audio export complete."),
            ),
        )

    def _export_selected_fsb(self) -> None:
        if not self._apply_selected_entry():
            return

        entry = self._selected_entry()
        if entry is None:
            messagebox.showinfo("Export FSB", "Select an entry to export first.")
            return

        selection = filedialog.asksaveasfilename(
            title="Export FSB",
            defaultextension=".fsb",
            initialfile=f"{entry.entry_name}.fsb",
            filetypes=[("FSB files", "*.fsb"), ("All files", "*.*")],
        )
        if not selection:
            return

        destination = Path(selection).resolve()

        def worker(progress, log):
            progress(f"Exporting FSB for {entry.entry_name}...", 0, 3)
            if entry.source_mode == "fsb":
                source = entry.resolved_fsb_path()
                if source is None or not source.exists():
                    raise FileNotFoundError(f"Missing FSB file for '{entry.entry_name}'.")
                shutil.copyfile(source, destination)
                progress(f"Copied {destination.name}.", 3, 3)
                return destination

            if self.current_toolchain is None:
                raise RuntimeError("A valid DLDT toolchain is required to export raw audio as FSB.")
            with tempfile.TemporaryDirectory(prefix="dyingaudio_export_fsb_") as temp_dir:
                temp_root = Path(temp_dir)
                source = entry.resolved_source_path()
                if source is None or not source.exists():
                    raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")
                compile_source = source
                if source.suffix.lower() not in {".wav", ".ogg"}:
                    media_tools = discover_media_tools()
                    if media_tools.ffmpeg_path is None:
                        raise RuntimeError(
                            f"FFmpeg is required to convert '{source.suffix or 'unknown'}' files for FSB export."
                        )
                    compile_source = temp_root / f"{entry.entry_name}.wav"
                    command = [
                        str(media_tools.ffmpeg_path),
                        "-y",
                        "-loglevel",
                        "error",
                        "-i",
                        str(source),
                        "-acodec",
                        "pcm_s16le",
                        str(compile_source),
                    ]
                    log(" ".join(command))
                    progress(f"Converting {source.name} for FSB export...", 1, 3)
                    result = run_hidden(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
                    if result.stdout.strip():
                        log(result.stdout.strip())
                    if result.stderr.strip():
                        log(result.stderr.strip())
                    if result.returncode != 0 or not compile_source.exists():
                        raise RuntimeError(f"Could not convert '{source.name}' for FSB export.")
                progress(f"Compiling {entry.entry_name} to FSB...", 2, 3)
                compile_result = compile_audio_to_fsb(
                    self.current_toolchain,
                    compile_source,
                    destination,
                    temp_root / "cache",
                )
                log(" ".join(compile_result.command))
                if compile_result.stdout:
                    log(compile_result.stdout)
                if compile_result.stderr:
                    log(compile_result.stderr)
                if not compile_result.success:
                    raise RuntimeError(f"Could not compile '{entry.entry_name}' to FSB.")
            progress(f"Exported {destination.name}.", 3, 3)
            return destination

        self._run_dl1_task(
            start_message=f"Exporting FSB for '{entry.entry_name}'...",
            error_title="Export FSB failed",
            worker=worker,
            on_success=lambda result: (
                self.status_var.set(f"Exported FSB for '{entry.entry_name}'."),
                self._append_log(f"Exported FSB to {result}"),
                self.task_status_var.set("FSB export complete."),
            ),
        )

    def _make_duplicate_name(self, entry_name: str) -> str:
        existing_names = {entry.entry_name.lower() for entry in self.entries}
        base_name = entry_name
        candidate = f"{base_name}_copy"
        counter = 2
        while candidate.lower() in existing_names:
            candidate = f"{base_name}_copy{counter}"
            counter += 1
        return candidate

    def _duplicate_selected_entry(self) -> None:
        if not self._apply_selected_entry():
            return

        index = self._selected_index()
        if index is None:
            messagebox.showinfo("Duplicate entry", "Select an entry to duplicate first.")
            return

        source_entry = self.entries[index]
        duplicate = replace(source_entry, entry_name=self._make_duplicate_name(source_entry.entry_name))
        self.entries.insert(index + 1, duplicate)
        self._refresh_tree()
        self._select_entry(index + 1)
        self.status_var.set(f"Duplicated '{source_entry.entry_name}'.")

    def _rename_selected_entry(self) -> None:
        if not self._apply_selected_entry():
            return

        index = self._selected_index()
        if index is None:
            messagebox.showinfo("Rename entry", "Select an entry to rename first.")
            return

        entry = self.entries[index]
        new_name = simpledialog.askstring("Rename entry", "Entry name:", initialvalue=entry.entry_name, parent=self)
        if new_name is None:
            return

        cleaned_name = new_name.strip()
        if not cleaned_name:
            messagebox.showerror("Rename entry", "Entry name cannot be empty.")
            return

        entry.entry_name = cleaned_name
        self._refresh_tree()
        self._select_entry(index)
        self.status_var.set(f"Renamed entry to '{cleaned_name}'.")

    def _remove_selected(self) -> None:
        if not self._apply_selected_entry():
            return
        index = self._selected_index()
        if index is None:
            return
        removed = self.entries.pop(index)
        self.preview_player.stop()
        self._reset_preview_progress()
        self._refresh_tree()
        if self.entries:
            self._select_entry(min(index, len(self.entries) - 1))
        else:
            self._update_preview_info()
        self.status_var.set(f"Removed entry '{removed.entry_name}'.")

    def _move_selected(self, delta: int) -> None:
        if not self._apply_selected_entry():
            return
        if self.sort_field_var.get().strip() != "Original Order":
            messagebox.showinfo("Reorder entries", "Switch sorting back to Original Order before moving entries manually.")
            return
        index = self._selected_index()
        if index is None:
            return

        target = index + delta
        if target < 0 or target >= len(self.entries):
            return

        self.entries[index], self.entries[target] = self.entries[target], self.entries[index]
        self._refresh_tree()
        self.tree.selection_set(str(target))
        self.tree.focus(str(target))
        self.status_var.set("Reordered entries.")

    def _clear_entries(self) -> None:
        if not self._apply_selected_entry():
            return
        if not self.entries:
            return
        if not messagebox.askyesno("Clear entries", "Remove all current entries?"):
            return
        self.preview_player.stop()
        self._reset_preview_progress()
        self.preview_player.clear_cache()
        self.entries.clear()
        self._set_loaded_csb(None)
        self._set_loaded_csb_magic(None)
        self._set_loaded_csb_layout(None)
        self._refresh_tree()
        self._update_preview_info()
        self.status_var.set("Cleared all entries.")

    def _inspect_csb(self) -> None:
        selection = filedialog.askopenfilename(
            title="Inspect CSB",
            filetypes=[("CSB files", "*.csb"), ("All files", "*.*")],
        )
        if not selection:
            return

        def on_success(result: object) -> None:
            parsed = result
            window = tk.Toplevel(self)
            window.title(f"Inspect CSB - {Path(selection).name}")
            window.geometry("1100x600")
            window.columnconfigure(0, weight=1)
            window.rowconfigure(1, weight=1)

            ttk.Label(
                window,
                text=(
                    f"Entries: {parsed.entry_count}    Size: {parsed.size} bytes    "
                    f"Variant: {self._format_csb_variant(parsed.magic, parsed.layout)}"
                ),
            ).grid(row=0, column=0, sticky="w", padx=8, pady=8)

            tree = ttk.Treeview(window, columns=("name", "type", "duration", "samples", "notes"), show="headings")
            tree.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
            columns = {
                "name": ("Entry Name", 280),
                "type": ("Type", 80),
                "duration": ("Duration (ms)", 120),
                "samples": ("Samples @ 48k", 140),
                "notes": ("Notes", 360),
            }
            for column, (title, width) in columns.items():
                tree.heading(column, text=title)
                tree.column(column, width=width, anchor="w")

            for entry in parsed.entries:
                tree.insert(
                    "",
                    "end",
                    values=(entry.entry_name, entry.entry_type, entry.duration_ms, entry.sample_count, entry.notes),
                )
            self.task_status_var.set("Inspect complete.")
            self.status_var.set(f"Inspected {Path(selection).name}.")

        self._run_dl1_task(
            start_message=f"Inspecting {Path(selection).name}...",
            error_title="Inspect CSB failed",
            worker=lambda progress, _log: (progress("Parsing CSB...", None, None), parse_csb(selection))[1],
            on_success=on_success,
        )

    def _extract_csb(self) -> None:
        csb_path = filedialog.askopenfilename(
            title="Extract CSB",
            filetypes=[("CSB files", "*.csb"), ("All files", "*.*")],
        )
        if not csb_path:
            return

        output_dir = filedialog.askdirectory(title="Select extraction folder")
        if not output_dir:
            return

        def worker(progress, _log):
            extracted = extract_csb(csb_path, output_dir, progress=progress)
            manifest_path = write_manifest(Path(output_dir) / "manifest.generated.json", extracted)
            return extracted, manifest_path

        def on_success(result: object) -> None:
            extracted, manifest_path = result
            self._append_log(f"Extracted {len(extracted)} FSB file(s) to {output_dir}")
            self._append_log(f"Wrote manifest to {manifest_path}")
            self.task_status_var.set("Extraction complete.")
            self.status_var.set(f"Extracted {Path(csb_path).name}.")

        self._run_dl1_task(
            start_message=f"Extracting {Path(csb_path).name}...",
            error_title="Extract CSB failed",
            worker=worker,
            on_success=on_success,
        )

    def _save_csb_file(self) -> None:
        if not self._apply_selected_entry():
            return
        self._ensure_raw_builder_mode(notify=True)
        self._save_settings()

        target_path: Path | None = None
        overwrite_loaded = False
        if self.loaded_csb_path is not None:
            choice = messagebox.askyesnocancel(
                "Save CSB file",
                f"Overwrite the loaded CSB?\n\n{self.loaded_csb_path}\n\nChoose No to pick another .csb file.",
            )
            if choice is None:
                return
            if choice:
                target_path = self.loaded_csb_path
                overwrite_loaded = True

        if target_path is None:
            mods_root = self._resolve_mods_root(allow_discovery=True)
            initial_dir = str(
                self.loaded_csb_path.parent
                if self.loaded_csb_path is not None
                else (mods_root if mods_root is not None else Path.cwd())
            )
            initial_file = f"{Path(self.bundle_name_var.get().strip() or DEFAULT_BUNDLE_NAME).stem}.csb"
            selection = filedialog.asksaveasfilename(
                title="Save CSB file",
                defaultextension=".csb",
                initialdir=initial_dir,
                initialfile=initial_file,
                filetypes=[("CSB files", "*.csb"), ("All files", "*.*")],
            )
            if not selection:
                return
            target_path = Path(selection).resolve()
            if target_path.exists() and not messagebox.askyesno(
                "Overwrite CSB",
                f"Replace this file?\n\n{target_path}",
            ):
                return

        def worker(progress, log):
            return build_csb_file(
                entries=self.entries,
                output_path=target_path,
                builder_mode=self.builder_mode_var.get().strip() or "Raw Audio via DLDT",
                toolchain=self.current_toolchain,
                log=log,
                magic=self._effective_output_magic(),
                progress=progress,
            )

        def on_success(result: object) -> None:
            csb_result = result
            if overwrite_loaded:
                self._set_loaded_csb(csb_result.csb_path)
            self.task_status_var.set("Save complete.")
            self.status_var.set(f"Saved {csb_result.csb_path.name}.")
            self._append_log(f"Saved CSB file: {csb_result.csb_path}")
            messagebox.showinfo("Save complete", f"Saved CSB file:\n{csb_result.csb_path}")

        self._run_dl1_task(
            start_message=f"Saving {target_path.name}...",
            error_title="Save failed",
            worker=worker,
            on_success=on_success,
        )

    def _play_selected_entry(self) -> None:
        if not self._apply_selected_entry():
            return

        index = self._selected_index()
        if index is None:
            messagebox.showinfo("Preview audio", "Select an entry to preview first.")
            return

        entry = self.entries[index]
        try:
            preview_path = self.preview_player.play_entry(entry, self._append_log)
        except Exception as exc:
            messagebox.showerror("Preview failed", str(exc))
            self.status_var.set("Preview failed.")
            self._append_log(f"ERROR: {exc}")
            return

        self._begin_preview_progress(entry)
        self.status_var.set(f"Previewing '{entry.entry_name}'.")
        self._append_log(f"Previewing {entry.entry_name} from {preview_path}")

    def _stop_preview(self) -> None:
        self.preview_player.stop()
        self._reset_preview_progress("Preview stopped.")
        self.status_var.set("Preview stopped.")

    def _save_settings(self) -> None:
        settings = AppSettings()
        settings.dl1.mods_root = self.mods_root_var.get().strip()
        settings.dl1.dldt_root = self.dldt_root_var.get().strip()
        settings.dl1.builder_mode = self.builder_mode_var.get().strip() or "Raw Audio via DLDT"
        settings.dl1.mod_name = self.mod_name_var.get().strip() or DEFAULT_MOD_NAME
        settings.dl1.bundle_name = self.bundle_name_var.get().strip() or DEFAULT_BUNDLE_NAME
        settings.dl1.generate_audiodata = self.generate_script_var.get()
        settings.dl1.audio_proc_names = [line.strip() for line in self.proc_text.get("1.0", tk.END).splitlines() if line.strip()]
        settings.dl1.last_output_folder = str(self.last_built_mod_root or "")
        if self.experimental_frame is not None:
            settings.experimental = self.experimental_frame.build_settings()
        save_settings(settings)

    def _build_mod(self) -> None:
        if not self._apply_selected_entry():
            return
        self._ensure_raw_builder_mode(notify=True)
        mods_root = self._resolve_mods_root(allow_discovery=True)
        if mods_root is None:
            messagebox.showinfo(
                "Build mod",
                "Select the Dying Light Mods folder first, or click Browse to auto-find it.",
            )
            return
        if self.mods_root_var.get().strip() != str(mods_root):
            self.mods_root_var.set(str(mods_root))
        toolchain = self.current_toolchain
        if toolchain is None:
            dldt_root = self._resolve_dldt_root(allow_discovery=True)
            if dldt_root is not None:
                toolchain, _errors = discover_toolchain(dldt_root)
                if toolchain is not None:
                    self.current_toolchain = toolchain
                    if self.dldt_root_var.get().strip() != str(dldt_root):
                        self.dldt_root_var.set(str(dldt_root))
        self._save_settings()
        if self._has_raw_entries() and self.current_toolchain is None:
            messagebox.showinfo(
                "Build mod",
                "Select or auto-find the DLDT toolchain first.",
            )
            return

        def worker(progress, log):
            return build_mod(
                entries=self.entries,
                mods_root=mods_root,
                mod_name=self.mod_name_var.get().strip() or DEFAULT_MOD_NAME,
                bundle_name=self.bundle_name_var.get().strip() or DEFAULT_BUNDLE_NAME,
                generate_script=self.generate_script_var.get(),
                proc_names_text=self.proc_text.get("1.0", tk.END),
                builder_mode=self.builder_mode_var.get().strip() or "Raw Audio via DLDT",
                toolchain=self.current_toolchain,
                log=log,
                magic=self._effective_output_magic(),
                progress=progress,
            )

        def on_success(result: object) -> None:
            artifacts = result
            self.last_built_mod_root = artifacts.mod_root
            self.task_status_var.set("Build complete.")
            self.status_var.set(f"Built {artifacts.csb_path.name} in {artifacts.mod_root.name}.")
            self._append_log(f"Build complete: {artifacts.csb_path}")
            self._append_log(f"modinfo.ini: {artifacts.modinfo_path}")
            if artifacts.script_path is not None:
                self._append_log(f"audiodata.scr: {artifacts.script_path}")
            messagebox.showinfo("Build complete", f"Built mod folder:\n{artifacts.mod_root}")

        self._run_dl1_task(
            start_message=f"Building mod '{self.mod_name_var.get().strip() or DEFAULT_MOD_NAME}'...",
            error_title="Build failed",
            worker=worker,
            on_success=on_success,
        )

    def _open_mod_folder(self) -> None:
        mods_root = self._resolve_mods_root(allow_discovery=True)
        if self.last_built_mod_root is not None:
            target = self.last_built_mod_root
        elif mods_root is not None:
            target = mods_root / (self.mod_name_var.get().strip() or DEFAULT_MOD_NAME)
        else:
            messagebox.showinfo(
                "Open mod folder",
                "Select the Dying Light Mods folder first, or click Browse to auto-find it.",
            )
            return
        if not target.exists():
            messagebox.showinfo("Open mod folder", f"Folder does not exist yet:\n{target}")
            return
        os.startfile(str(target))

    def _on_close(self) -> None:
        self._save_settings()
        self.task_runner.cancel()
        self.task_runner.cancel_polling()
        self._close_loading_window()
        self.preview_player.close()
        if self.experimental_frame is not None:
            self.experimental_frame.shutdown()
        self._cancel_preview_progress_updates()
        self._cleanup_edit_session()
        self.destroy()


def main() -> None:
    app = DyingAudioApp()
    app.mainloop()
