from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import traceback
import time
import sys
import tkinter as tk
import webbrowser
from dataclasses import replace
from pathlib import Path
from tkinter import filedialog, font as tkfont, ttk
from tkinter.scrolledtext import ScrolledText
from typing import Callable

from dyingaudio.audio_info import probe_audio_metadata
from dyingaudio.background import BackgroundTaskRunner, TaskCancelled, TaskProgress
from dyingaudio.core.csb import WORKSHOP_MAGIC, extract_csb, parse_csb
from dyingaudio.core.dldt import DldtToolchain, compile_audio_to_fsb, discover_toolchain
from dyingaudio.core.localized_text import load_text_catalog, upsert_scr_text
from dyingaudio.core.manifest import load_manifest, write_manifest
from dyingaudio.core.media_tools import (
    AUDIO_EXPORT_FILETYPES,
    COMMON_AUDIO_FILETYPES,
    DL1_AUDIO_QUALITY_CHOICES,
    audio_quality_output_suffix,
    decode_audio_to_wav,
    discover_media_tools,
    export_audio_file,
    run_hidden,
)
from dyingaudio.core.mod_writer import build_csb_file, build_mod
from dyingaudio.core.windows_shell import register_csb_open_with, unregister_csb_open_with
from dyingaudio.core.preview import PreviewPlayer, preview_strategy_for_entry
from dyingaudio.core.scriptgen import generate_audiodata_scr
from dyingaudio.core.spb import SpeechBuildOptions
from dyingaudio.experimental_workspace import ExperimentalWwiseFrame
from dyingaudio.core.wwise_workspace import DL2_GAME, DLTB_GAME
from dyingaudio.models import AudioEntry, entry_type_from_channel_count, format_entry_type
from dyingaudio.other_workspace import OtherWorkspaceFrame
from dyingaudio.popups import (
    ask_string_dialog,
    ask_yes_no_cancel_dialog,
    ask_yes_no_dialog,
    show_error_dialog,
    show_info_dialog,
    show_warning_dialog,
)
from dyingaudio.settings import (
    AppSettings,
    DEFAULT_AUDIO_PROCS,
    DEFAULT_BUNDLE_NAME,
    DEFAULT_DL1_AUDIO_QUALITY,
    DEFAULT_EXPERIMENTAL_CACHE_ROOT,
    DEFAULT_MOD_NAME,
    DEFAULT_OTHER_CACHE_ROOT,
    ToolSettings,
    bundled_resource_root,
    discover_dldt_root,
    discover_game_root,
    discover_mods_root,
    is_windows_dark_mode,
    load_settings,
    save_settings,
)
from dyingaudio.ui_theme import (
    configure_dark_checkbutton_style,
    configure_high_contrast_checkbutton_style,
    dark_palette,
    high_contrast_palette,
    style_scrolled_text,
)


DL1_SOURCE_FILETYPES = [
    ("DL1 source files", f"*.fsb {COMMON_AUDIO_FILETYPES[0][1]}"),
    COMMON_AUDIO_FILETYPES[0],
    ("FSB files", "*.fsb"),
    COMMON_AUDIO_FILETYPES[-1],
]

SPEECH_INTENSITY_MIN = 0.0
SPEECH_INTENSITY_MAX = 2.0
DEFAULT_SPEECH_INTENSITY = 1.0
MIXED_DETAIL_VALUE = "[mixed]"
DEFAULT_LOCALIZED_TEXT_MESSAGE = (
    "No text entry found. Browse for your text source or add a valid string entry to your text source!"
)
GITHUB_REPOSITORY_URL = "https://github.com/BalancedLight/DyingLightAudioTools/wiki"


def _is_fsb_source(path: str | Path) -> bool:
    return Path(path).suffix.lower() == ".fsb"


def _normalize_dl1_entry_name(name: str) -> str:
    return name.lower()


def _clamp_speech_intensity(value: float) -> float:
    return max(SPEECH_INTENSITY_MIN, min(SPEECH_INTENSITY_MAX, float(value)))


def _format_speech_intensity(value: float) -> str:
    clamped = _clamp_speech_intensity(value)
    if clamped.is_integer():
        return str(int(clamped))
    return f"{clamped:.2f}".rstrip("0").rstrip(".")


def _collect_startup_csb_paths(argv: list[str]) -> list[Path]:
    paths: list[Path] = []
    for raw in argv:
        if raw.startswith("-"):
            continue
        candidate = Path(raw).expanduser()
        if candidate.suffix.lower() != ".csb":
            continue
        paths.append(candidate)
    return paths


class DyingAudioApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("DyingAudio")
        self._apply_window_icon()
        self._configure_appearance()
        self.geometry("1500x920")
        self.minsize(1200, 1100)

        self.settings = load_settings()
        self.experimental_frame: ExperimentalWwiseFrame | None = None
        self.other_frame: OtherWorkspaceFrame | None = None
        self.entries: list[AudioEntry] = []
        self.current_toolchain: DldtToolchain | None = None
        self.last_built_mod_root: Path | None = None
        self.loaded_csb_path: Path | None = None
        self.loaded_csb_magic: int | None = None
        self.loaded_csb_layout: str | None = None
        self.edit_session_dir: tempfile.TemporaryDirectory[str] | None = None
        self.preview_player = PreviewPlayer()
        self.preview_player.environment = discover_media_tools(self.settings.tools)
        self.task_runner = BackgroundTaskRunner(self)
        self._dark_theme_colors: dict[str, str] | None = None

        self.mod_name_var = tk.StringVar(value=self.settings.mod_name or DEFAULT_MOD_NAME)
        self.bundle_name_var = tk.StringVar(value=self.settings.bundle_name or DEFAULT_BUNDLE_NAME)
        self.mods_root_var = tk.StringVar(value=self.settings.mods_root)
        self.dldt_root_var = tk.StringVar(value=self.settings.dldt_root)
        self.dl2_root_var = tk.StringVar(value=self.settings.experimental.dl2_root)
        self.dltb_root_var = tk.StringVar(value=self.settings.experimental.dltb_root)
        self.other_root_var = tk.StringVar(value=self.settings.other.root)
        self.experimental_cache_root_var = tk.StringVar(value=self.settings.experimental.cache_root)
        self.other_cache_root_var = tk.StringVar(value=self.settings.other.cache_root)
        self.ffmpeg_root_var = tk.StringVar(value=self.settings.tools.ffmpeg_root)
        self.vgmstream_root_var = tk.StringVar(value=self.settings.tools.vgmstream_root)
        self.wwise_root_var = tk.StringVar(value=self.settings.tools.wwise_root)
        self.show_welcome_on_startup_var = tk.BooleanVar(value=self.settings.tools.show_welcome_on_startup)
        self.high_contrast_var = tk.BooleanVar(value=getattr(self.settings.tools, "high_contrast_mode", False))
        self.builder_mode_var = tk.StringVar(value=self.settings.builder_mode or "Raw Audio via DLDT")
        self.audio_quality_var = tk.StringVar(value=self.settings.audio_quality or DEFAULT_DL1_AUDIO_QUALITY)
        self.generate_script_var = tk.BooleanVar(value=self.settings.generate_audiodata)
        self.localized_bank_var = tk.BooleanVar(value=self.settings.dl1.localized_bank or self.settings.dl1.generate_spb)
        self.generate_spb_var = tk.BooleanVar(value=self.settings.dl1.generate_spb)
        self.speech_text_source_var = tk.StringVar(value=self.settings.dl1.speech_text_source)
        self.global_speech_intensity_var = tk.StringVar(
            value=_format_speech_intensity(getattr(self.settings.dl1, "speech_intensity", DEFAULT_SPEECH_INTENSITY))
        )
        self.global_speech_intensity_scale_var = tk.DoubleVar(
            value=_clamp_speech_intensity(getattr(self.settings.dl1, "speech_intensity", DEFAULT_SPEECH_INTENSITY))
        )
        self.speech_summary_var = tk.StringVar(value="Speech Data: disabled")
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
        self.loading_cancel_button: ttk.Button | None = None
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
        self.selected_speech_intensity_var = tk.StringVar(value=_format_speech_intensity(DEFAULT_SPEECH_INTENSITY))
        self.selected_speech_intensity_scale_var = tk.DoubleVar(value=DEFAULT_SPEECH_INTENSITY)
        self.selected_source_var = tk.StringVar(value="")
        self.selected_fsb_var = tk.StringVar(value="")
        self.selected_notes_var = tk.StringVar(value="")
        self._localized_text_catalog: dict[str, str] = {}
        self._localized_text_catalog_source: str = ""
        self._localized_text_dirty = False
        self._localized_text_snapshot = ""
        self._localized_text_entry_index: int | None = None
        self._localized_text_placeholder_active = False
        self._detail_form_loading = False
        self._detail_form_dirty = False
        self._detail_entry_index: int | None = None
        self._detail_entry_indices: tuple[int, ...] = ()
        self._detail_snapshot: dict[str, str] = {}
        self._suspend_tree_select = False
        self._preview_after_id: str | None = None
        self._preview_started_at: float | None = None
        self._preview_duration_ms = 0
        self._preview_playing = False
        self._preview_indeterminate = False
        self._preview_entry_name = ""
        self._preview_playback_kind: str | None = None
        self.console_frame: ttk.Frame | None = None
        self.console_notebook: ttk.Notebook | None = None
        self.console_tab: ttk.Frame | None = None
        self.log_text: ScrolledText | None = None
        self._console_visible = True
        self._welcome_window: tk.Toplevel | None = None
        self._welcome_configure_after_id: str | None = None
        self._skip_welcome_wizard = False
        self._menus: list[tk.Menu] = []
        self.file_menu: tk.Menu | None = None
        self._welcome_background_image: tk.PhotoImage | None = None
        self._localized_text_widget_font: tkfont.Font | None = None
        self._localized_text_widget_bold_font: tkfont.Font | None = None

        self._configure_high_contrast()
        self._build_ui()
        self._configure_dark_combobox_popdowns()
        self._apply_text_widget_theme()
        self._load_proc_names()
        self._update_sort_controls()
        self._refresh_tree()
        self._update_toolchain_status()
        self._update_speech_summary()
        self._update_script_preview()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        for traced_var in (
            self.bundle_name_var,
            self.generate_script_var,
            self.dldt_root_var,
            self.builder_mode_var,
            self.audio_quality_var,
            self.localized_bank_var,
            self.generate_spb_var,
            self.speech_text_source_var,
            self.global_speech_intensity_var,
        ):
            traced_var.trace_add("write", self._on_settings_changed)
        self.entry_search_var.trace_add("write", self._on_entry_filter_changed)
        self.sort_field_var.trace_add("write", self._on_entry_filter_changed)
        self.sort_descending_var.trace_add("write", self._on_entry_filter_changed)
        self.show_welcome_on_startup_var.trace_add("write", self._on_welcome_toggle_changed)
        self.high_contrast_var.trace_add("write", self._on_high_contrast_changed)
        for traced_var in (
            self.selected_name_var,
            self.selected_type_var,
            self.selected_sample_count_var,
            self.selected_duration_var,
            self.selected_speech_intensity_var,
        ):
            traced_var.trace_add("write", self._on_selected_detail_changed)
        self.after(250, self._maybe_show_welcome_wizard)

    def _apply_window_icon(self) -> None:
        icon_path = bundled_resource_root() / "assets" / "dyinglight_devtools.ico"
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

        colors = dark_palette()
        self._dark_theme_colors = colors
        background = colors["background"]
        panel_background = colors["panel_background"]
        field_background = colors["field_background"]
        foreground = colors["foreground"]
        border_color = colors["border_color"]
        selected_background = colors["selected_background"]
        selected_foreground = colors["selected_foreground"]

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
        self.option_add("*TCombobox*Listbox.background", field_background)
        self.option_add("*TCombobox*Listbox.foreground", foreground)
        self.option_add("*TCombobox*Listbox.selectBackground", selected_background)
        self.option_add("*TCombobox*Listbox.selectForeground", selected_foreground)

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
        configure_dark_checkbutton_style(self.style)
        self.style.map(
            "TCombobox",
            fieldbackground=[("readonly", field_background), ("disabled", panel_background)],
            foreground=[("readonly", foreground), ("disabled", "#777777")],
            background=[("readonly", field_background), ("disabled", panel_background)],
            selectbackground=[("readonly", selected_background)],
            selectforeground=[("readonly", selected_foreground)],
            arrowcolor=[("readonly", foreground), ("disabled", "#777777")],
        )
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

    def _configure_high_contrast(self) -> None:
        if not hasattr(self, "high_contrast_var"):
            return
        if not self.high_contrast_var.get():
            if is_windows_dark_mode():
                self._configure_appearance()
                self._apply_text_widget_theme()
            for menu in getattr(self, "_menus", []):
                self._apply_menu_colors(menu)
            if hasattr(self, "experimental_frame") and self.experimental_frame is not None:
                self.experimental_frame.apply_theme(high_contrast=False)
            if hasattr(self, "other_frame") and self.other_frame is not None:
                self.other_frame.apply_theme(high_contrast=False)
            return
        style = getattr(self, "style", ttk.Style(self))
        self.style = style
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        colors = high_contrast_palette()
        background = colors["background"]
        foreground = colors["foreground"]
        selected = colors["selected_background"]
        self.configure(bg=background)
        self.option_add("*Background", background)
        self.option_add("*foreground", foreground)
        self.option_add("*FieldBackground", background)
        self.option_add("*insertBackground", foreground)
        self.option_add("*selectColor", selected)
        self.option_add("*selectBackground", selected)
        self.option_add("*selectForeground", background)
        self.option_add("*Menu.background", background)
        self.option_add("*Menu.foreground", foreground)
        self.option_add("*Menu.activeBackground", selected)
        self.option_add("*Menu.activeForeground", background)
        style.configure(".", background=background, foreground=foreground)
        style.configure("TFrame", background=background)
        style.configure("TLabelframe", background=background, foreground=foreground, bordercolor=foreground)
        style.configure("TLabelframe.Label", background=background, foreground=foreground)
        style.configure("TLabel", background=background, foreground=foreground)
        style.configure("TEntry", fieldbackground=background, foreground=foreground, background=background, insertcolor=foreground)
        style.map(
            "TEntry",
            fieldbackground=[("disabled", "#202020"), ("readonly", background), ("!disabled", background)],
            foreground=[("disabled", "#808080"), ("readonly", foreground), ("!disabled", foreground)],
            selectbackground=[("!disabled", selected)],
            selectforeground=[("!disabled", background)],
        )
        style.configure("TCombobox", fieldbackground=background, foreground=foreground, background=background, arrowcolor=foreground)
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", background), ("disabled", "#202020"), ("!disabled", background)],
            foreground=[("readonly", foreground), ("disabled", "#808080"), ("!disabled", foreground)],
            background=[("readonly", background), ("disabled", "#202020"), ("!disabled", background)],
            selectbackground=[("readonly", selected), ("!disabled", selected)],
            selectforeground=[("readonly", background), ("!disabled", background)],
            arrowcolor=[("readonly", foreground), ("disabled", "#808080"), ("!disabled", foreground)],
        )
        style.configure(
            "TButton",
            background=background,
            foreground=foreground,
            bordercolor=foreground,
            lightcolor=foreground,
            darkcolor=foreground,
            focuscolor=selected,
        )
        style.map(
            "TButton",
            background=[("active", selected), ("pressed", selected), ("disabled", "#202020")],
            foreground=[("active", background), ("pressed", background), ("disabled", "#808080")],
        )
        configure_high_contrast_checkbutton_style(style)
        style.configure("TNotebook", background=background, bordercolor=foreground)
        style.configure("TNotebook.Tab", background=background, foreground=foreground, focuscolor=selected)
        style.map("TNotebook.Tab", background=[("selected", selected)], foreground=[("selected", background)])
        style.configure("Treeview", background=background, fieldbackground=background, foreground=foreground, bordercolor=foreground)
        style.configure("Treeview.Heading", background=background, foreground=foreground, bordercolor=foreground)
        style.map("Treeview", background=[("selected", selected)], foreground=[("selected", background)])
        style.configure("TProgressbar", troughcolor=background, background=selected, bordercolor=foreground)
        style.configure("Horizontal.TScale", background=background, troughcolor=foreground)
        style.configure("Vertical.TScale", background=background, troughcolor=foreground)
        for menu in getattr(self, "_menus", []):
            self._apply_menu_colors(menu)
        if hasattr(self, "notebook"):
            self._apply_high_contrast_to_widget(self)
            self._configure_high_contrast_combobox_popdowns()
        self._apply_text_widget_theme()
        if hasattr(self, "experimental_frame") and self.experimental_frame is not None:
            self.experimental_frame.apply_theme(high_contrast=True)
        if hasattr(self, "other_frame") and self.other_frame is not None:
            self.other_frame.apply_theme(high_contrast=True)

    def _apply_high_contrast_to_widget(self, widget: tk.Misc) -> None:
        if not self.high_contrast_var.get():
            return
        background = "#000000"
        foreground = "#ffffff"
        selected = "#ffff00"
        for child in self._iter_child_widgets(widget):
            try:
                if isinstance(child, tk.Text):
                    child.configure(
                        background=background,
                        foreground=foreground,
                        insertbackground=foreground,
                        selectbackground=selected,
                        selectforeground=background,
                        highlightbackground=foreground,
                        highlightcolor=selected,
                    )
                elif isinstance(child, tk.Entry):
                    child.configure(
                        background=background,
                        foreground=foreground,
                        insertbackground=foreground,
                        selectbackground=selected,
                        selectforeground=background,
                        highlightbackground=foreground,
                        highlightcolor=selected,
                    )
                elif isinstance(child, tk.Listbox):
                    child.configure(
                        background=background,
                        foreground=foreground,
                        selectbackground=selected,
                        selectforeground=background,
                        highlightbackground=foreground,
                        highlightcolor=selected,
                    )
                elif isinstance(child, tk.Canvas):
                    child.configure(background=background, highlightbackground=background)
                elif isinstance(child, tk.Menu):
                    self._apply_menu_colors(child)
            except tk.TclError:
                pass

    def _apply_text_widget_theme(self) -> None:
        if not hasattr(self, "proc_text") or self.proc_text is None:
            return
        if self.high_contrast_var.get():
            colors = high_contrast_palette()
        elif is_windows_dark_mode():
            colors = dark_palette()
        else:
            return
        for widget in (
            getattr(self, "proc_text", None),
            getattr(self, "preview_text", None),
            getattr(self, "log_text", None),
            getattr(self, "localized_text_display", None),
        ):
            if widget is not None:
                style_scrolled_text(widget, colors)

    def _configure_high_contrast_combobox_popdowns(self) -> None:
        for widget in self._iter_child_widgets(self):
            if not isinstance(widget, ttk.Combobox):
                continue
            try:
                popdown = self.tk.eval(f"ttk::combobox::PopdownWindow {widget}")
                listbox = f"{popdown}.f.l"
                self.tk.call(
                    listbox,
                    "configure",
                    "-background",
                    "#000000",
                    "-foreground",
                    "#ffffff",
                    "-selectbackground",
                    "#ffff00",
                    "-selectforeground",
                    "#000000",
                )
            except tk.TclError:
                pass

    def _apply_menu_colors(self, menu: tk.Menu) -> None:
        if self.high_contrast_var.get():
            menu.configure(
                background="#000000",
                foreground="#ffffff",
                activebackground="#ffff00",
                activeforeground="#000000",
                selectcolor="#ffff00",
            )
            return
        if is_windows_dark_mode():
            menu.configure(
                background="#1e1e1e",
                foreground="#d4d4d4",
                activebackground="#252526",
                activeforeground="#d4d4d4",
                selectcolor="#0a84ff",
            )

    def _iter_child_widgets(self, widget: tk.Misc) -> list[tk.Misc]:
        descendants: list[tk.Misc] = []
        for child in widget.winfo_children():
            descendants.append(child)
            descendants.extend(self._iter_child_widgets(child))
        return descendants

    def _configure_dark_combobox_popdowns(self) -> None:
        if self._dark_theme_colors is None:
            return
        for widget in self._iter_child_widgets(self):
            if isinstance(widget, ttk.Combobox):
                self._configure_combobox_popdown(widget)

    def _configure_combobox_popdown(self, combobox: ttk.Combobox) -> None:
        if self._dark_theme_colors is None:
            return
        colors = self._dark_theme_colors
        try:
            popdown = self.tk.eval(f"ttk::combobox::PopdownWindow {combobox}")
            listbox = f"{popdown}.f.l"
            self.tk.call(
                listbox,
                "configure",
                "-background",
                colors["field_background"],
                "-foreground",
                colors["foreground"],
                "-selectbackground",
                colors["selected_background"],
                "-selectforeground",
                colors["selected_foreground"],
            )
        except tk.TclError:
            pass

    def report_callback_exception(self, exc: type[BaseException], val: BaseException, tb: object) -> None:
        details = "".join(traceback.format_exception(exc, val, tb))
        self._append_log(details.rstrip())
        self.status_var.set("An unexpected error occurred.")
        self._show_error_window("Unexpected error", str(val))

    def _build_menu(self) -> None:
        menubar = tk.Menu(self)
        self._menus = [menubar]

        file_menu = tk.Menu(menubar, tearoff=False)
        self.file_menu = file_menu
        self._menus.append(file_menu)
        menubar.add_cascade(label="File", menu=file_menu)

        settings_menu = tk.Menu(menubar, tearoff=False)
        self._menus.append(settings_menu)
        settings_menu.add_command(label="Folders and Tools...", command=self._show_settings_window)
        settings_menu.add_command(label=".csb/.spb File Types...", command=self._open_csb_file_types_setup)
        settings_menu.add_command(label="Open Welcome Menu", command=lambda: self._show_welcome_wizard(force=True))
        settings_menu.add_checkbutton(
            label="Show Welcome On Startup",
            variable=self.show_welcome_on_startup_var,
            command=self._save_settings,
        )
        settings_menu.add_checkbutton(
            label="High Contrast Mode",
            variable=self.high_contrast_var,
            command=self._on_high_contrast_changed,
        )
        menubar.add_cascade(label="Settings", menu=settings_menu)

        tabs_menu = tk.Menu(menubar, tearoff=False)
        self._menus.append(tabs_menu)
        tabs_menu.add_command(label="Dying Light 1", command=lambda: self._select_main_tab(self.dl1_tab))
        tabs_menu.add_command(
            label="Dying Light 2 / The Beast",
            command=lambda: self._select_main_tab(self.experimental_frame),
        )
        tabs_menu.add_command(label="Other", command=lambda: self._select_main_tab(self.other_frame))
        tabs_menu.add_separator()
        tabs_menu.add_command(label="Open Console", command=self._show_console_panel)
        tabs_menu.add_command(label="Close Console", command=self._hide_console_panel)
        menubar.add_cascade(label="Tabs", menu=tabs_menu)

        help_menu = tk.Menu(menubar, tearoff=False)
        self._menus.append(help_menu)
        help_menu.add_command(label="Open Wiki", command=self._open_github_page)
        help_menu.add_separator()
        help_menu.add_command(label="Tool Status...", command=self._show_tool_status_window)
        menubar.add_cascade(label="Help", menu=help_menu)

        for menu in self._menus:
            self._apply_menu_colors(menu)
        self.config(menu=menubar)

    def _refresh_file_menu(self, _event: object | None = None) -> None:
        if self.file_menu is None or not hasattr(self, "notebook") or not hasattr(self, "dl1_tab"):
            return
        self.file_menu.delete(0, tk.END)
        selected = self.notebook.select() if hasattr(self, "notebook") else ""
        selected_widget = self.nametowidget(selected) if selected else None
        if selected_widget is self.dl1_tab:
            self.file_menu.add_command(label="New empty CSB", command=self._new_empty_csb)
            self.file_menu.add_command(label="Open CSB", command=self._open_csb_for_editing)
            self.file_menu.add_command(label="Save CSB", command=self._save_csb_file)
            self.file_menu.add_command(label="Extract CSB", command=self._extract_csb)
            self.file_menu.add_command(label="Inspect CSB", command=self._inspect_csb)
            self.file_menu.add_separator()
            self.file_menu.add_command(label="Add Audio / FSB", command=self._add_source_files)
            self.file_menu.add_command(label="Import Manifest", command=self._import_manifest)
            self.file_menu.add_separator()
            self.file_menu.add_command(label="Clear DL1 Cache", command=self._clear_dl1_cache)
        elif selected_widget is self.experimental_frame and self.experimental_frame is not None:
            workspace = self.experimental_frame
            self.file_menu.add_command(label="Build / Refresh Workspace", command=workspace._build_workspace)
            self.file_menu.add_command(label="Export Selected Media...", command=workspace._export_selected_media)
            self.file_menu.add_command(label="Export Mixed Audio...", command=workspace._export_selected_media_mixed)
            self.file_menu.add_command(label="Export Selected Event Folder...", command=workspace._export_selected_event)
            self.file_menu.add_command(label="Export Selected Bank Files...", command=workspace._export_selected_bank_files)
            self.file_menu.add_command(label="Export Workspace Dump...", command=workspace._export_workspace_dump)
            self.file_menu.add_separator()
            self.file_menu.add_command(label="Replace Selected Audio...", command=workspace._replace_selected_media_aesp)
            self.file_menu.add_command(label="Restore Original AESP...", command=workspace._restore_aesp_archive)
            self.file_menu.add_separator()
            self.file_menu.add_command(label="Open Cache Folder", command=workspace._open_cache_folder)
            self.file_menu.add_command(label="Clear Cache...", command=workspace._clear_cache)
        elif selected_widget is self.other_frame and self.other_frame is not None:
            workspace = self.other_frame
            self.file_menu.add_command(label="Build / Refresh Workspace", command=workspace._build_workspace)
            self.file_menu.add_command(label="Export Selected Media...", command=workspace._export_selected_media)
            self.file_menu.add_command(label="Export Mixed Audio...", command=workspace._export_selected_media_mixed)
            self.file_menu.add_separator()
            self.file_menu.add_command(label="Replace Selected Audio...", command=workspace._replace_selected_media)
            self.file_menu.add_separator()
            self.file_menu.add_command(label="Open Cache Folder", command=workspace._open_cache_folder)
            self.file_menu.add_command(label="Clear Cache...", command=workspace._clear_cache)
        self.file_menu.add_separator()
        self.file_menu.add_command(label="Exit", command=self._on_close)

    def _build_ui(self) -> None:
        self._build_menu()
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.notebook = ttk.Notebook(self)
        self.notebook.grid(row=0, column=0, sticky="nsew")
        self.notebook.bind("<<NotebookTabChanged>>", self._refresh_file_menu)

        self.dl1_tab = ttk.Frame(self.notebook)
        self.dl1_tab.columnconfigure(0, weight=1)
        self.dl1_tab.rowconfigure(0, weight=1)
        self.notebook.add(self.dl1_tab, text="Dying Light 1")

        self.experimental_frame = ExperimentalWwiseFrame(self.notebook, self.settings.experimental, self)
        self.notebook.add(self.experimental_frame, text="Dying Light 2 / The Beast")

        self.other_frame = OtherWorkspaceFrame(self.notebook, self.settings.other, self)
        self.notebook.add(self.other_frame, text="Other")
        self._refresh_file_menu()

        self.main_frame = ttk.Frame(self.dl1_tab)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(1, weight=3)
        self.main_frame.rowconfigure(2, weight=2)

        settings_frame = ttk.LabelFrame(self.main_frame, text="Project")
        settings_frame.grid(row=0, column=0, sticky="nsew", padx=12, pady=(12, 6))
        for column in range(8):
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
        ttk.Label(settings_frame, text="Audio Quality").grid(row=0, column=6, sticky="w", padx=6, pady=6)
        ttk.Combobox(
            settings_frame,
            textvariable=self.audio_quality_var,
            values=DL1_AUDIO_QUALITY_CHOICES,
            state="readonly",
        ).grid(row=0, column=7, sticky="ew", padx=6, pady=6)

        ttk.Checkbutton(
            settings_frame,
            text="Localized speech bank",
            variable=self.localized_bank_var,
            command=self._update_script_preview,
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=6, pady=6)
        ttk.Checkbutton(
            settings_frame,
            text="Generate SPB",
            variable=self.generate_spb_var,
            command=self._on_generate_spb_toggle,
        ).grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Label(settings_frame, text="Text Source").grid(row=1, column=3, sticky="w", padx=6, pady=6)
        ttk.Entry(settings_frame, textvariable=self.speech_text_source_var).grid(
            row=1,
            column=4,
            columnspan=3,
            sticky="ew",
            padx=6,
            pady=6,
        )
        ttk.Button(settings_frame, text="Browse", command=self._browse_speech_text_source).grid(
            row=1,
            column=7,
            sticky="ew",
            padx=6,
            pady=6,
        )
        ttk.Label(settings_frame, textvariable=self.speech_summary_var).grid(
            row=2,
            column=0,
            columnspan=8,
            sticky="w",
            padx=6,
            pady=(0, 6),
        )
        self.global_speech_intensity_frame = ttk.Frame(settings_frame)
        self.global_speech_intensity_frame.grid(row=3, column=0, columnspan=8, sticky="ew", padx=6, pady=(0, 6))
        self.global_speech_intensity_frame.columnconfigure(1, weight=1)
        ttk.Label(self.global_speech_intensity_frame, text="Global Speech Intensity").grid(
            row=0, column=0, sticky="w", padx=(0, 6)
        )
        self.global_speech_intensity_scale = ttk.Scale(
            self.global_speech_intensity_frame,
            from_=SPEECH_INTENSITY_MIN,
            to=SPEECH_INTENSITY_MAX,
            variable=self.global_speech_intensity_scale_var,
            command=self._on_global_speech_intensity_scale_changed,
        )
        self.global_speech_intensity_scale.grid(row=0, column=1, sticky="ew", padx=(0, 6))
        self.global_speech_intensity_entry = ttk.Entry(
            self.global_speech_intensity_frame,
            textvariable=self.global_speech_intensity_var,
            width=8,
        )
        self.global_speech_intensity_entry.grid(row=0, column=2, sticky="e")
        self.global_speech_intensity_entry.bind("<FocusOut>", self._sync_global_speech_intensity_from_entry)
        self.global_speech_intensity_entry.bind("<Return>", self._sync_global_speech_intensity_from_entry)
        ttk.Label(settings_frame, textvariable=self.toolchain_status_var).grid(
            row=4,
            column=0,
            columnspan=5,
            sticky="w",
            padx=6,
            pady=(0, 6),
        )
        ttk.Label(settings_frame, textvariable=self.status_var).grid(row=4, column=5, columnspan=3, sticky="e", padx=6, pady=(0, 6))

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
        self._configure_high_contrast()

    def _build_entries_frame(self, parent: ttk.Frame) -> None:
        entries_frame = ttk.LabelFrame(parent, text="Audio Entries")
        entries_frame.grid(row=0, column=0, sticky="nsew")
        entries_frame.columnconfigure(0, weight=1)
        entries_frame.rowconfigure(2, weight=1)

        toolbar = ttk.Frame(entries_frame)
        toolbar.grid(row=0, column=0, sticky="ew", padx=6, pady=6)
        for index in range(7):
            toolbar.columnconfigure(index, weight=1)

        self.add_source_button = ttk.Button(toolbar, text="Add Audio / FSB", command=self._add_source_files)
        self.add_source_button.grid(row=0, column=0, sticky="ew", padx=2)
        self.import_manifest_button = ttk.Button(toolbar, text="Import Manifest", command=self._import_manifest)
        self.import_manifest_button.grid(row=0, column=1, sticky="ew", padx=2)
        self.replace_source_button = ttk.Button(toolbar, text="Replace Audio / FSB", command=self._replace_selected_with_source)
        self.replace_source_button.grid(row=0, column=2, sticky="ew", padx=2)
        self.remove_entry_button = ttk.Button(toolbar, text="Remove", command=self._remove_selected)
        self.remove_entry_button.grid(row=0, column=3, sticky="ew", padx=2)
        self.move_up_button = ttk.Button(toolbar, text="Move Up", command=lambda: self._move_selected(-1))
        self.move_up_button.grid(row=0, column=4, sticky="ew", padx=2)
        self.move_down_button = ttk.Button(toolbar, text="Move Down", command=lambda: self._move_selected(1))
        self.move_down_button.grid(row=0, column=5, sticky="ew", padx=2)
        self.clear_entries_button = ttk.Button(toolbar, text="Clear", command=self._clear_entries)
        self.clear_entries_button.grid(row=0, column=6, sticky="ew", padx=2)

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
            values=("Original Order", "Name", "Mode", "Type", "Duration", "Samples"),
            state="readonly",
            width=18,
        )
        self.sort_combo.grid(row=0, column=3, sticky="ew", padx=(0, 8))
        self.sort_order_button = ttk.Button(filter_bar, textvariable=self.sort_button_var, command=self._toggle_sort_direction)
        self.sort_order_button.grid(row=0, column=4, sticky="ew", padx=(0, 8))
        ttk.Button(filter_bar, text="Clear Search", command=self._clear_search).grid(row=0, column=5, sticky="ew", padx=(0, 8))
        ttk.Label(filter_bar, textvariable=self.entry_count_var).grid(row=0, column=6, sticky="e")

        columns = ("name", "mode", "type", "duration", "samples")
        self.tree = ttk.Treeview(entries_frame, columns=columns, show="headings", height=14, selectmode="extended")
        self.tree.grid(row=2, column=0, sticky="nsew", padx=6, pady=(0, 6))
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Button-3>", self._show_tree_context_menu)
        self.tree.bind("<Shift-F10>", self._show_tree_context_menu)

        headings = {
            "name": ("Entry Name", 220),
            "mode": ("Mode", 110),
            "type": ("Type", 120),
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
        self.entry_context_menu.add_command(label="Replace Audio / FSB...", command=self._replace_selected_with_source)
        self.entry_context_menu.add_command(label="Open Source", command=self._open_selected_source)
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
        ttk.Label(detail_frame, text="Type / Channels").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        self.selected_type_entry = ttk.Entry(detail_frame, textvariable=self.selected_type_var)
        self.selected_type_entry.grid(row=1, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(
            detail_frame,
            text="1 = mono, 2 = stereo. Any other value is treated as a channel count.",
            wraplength=420,
        ).grid(row=2, column=0, columnspan=2, sticky="w", padx=6, pady=(0, 6))
        ttk.Label(detail_frame, text="Samples @ 48k").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        self.selected_sample_count_entry = ttk.Entry(detail_frame, textvariable=self.selected_sample_count_var)
        self.selected_sample_count_entry.grid(row=3, column=1, sticky="ew", padx=6, pady=6)
        ttk.Label(detail_frame, text="Duration (ms)").grid(row=4, column=0, sticky="w", padx=6, pady=6)
        self.selected_duration_entry = ttk.Entry(detail_frame, textvariable=self.selected_duration_var)
        self.selected_duration_entry.grid(row=4, column=1, sticky="ew", padx=6, pady=6)
        self.localized_text_frame = ttk.LabelFrame(detail_frame, text="Localized Text")
        self.localized_text_frame.columnconfigure(0, weight=1)
        self.localized_text_display = ScrolledText(self.localized_text_frame, height=6, wrap="word", state="disabled")
        self.localized_text_display.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        self.localized_text_display.configure(cursor="arrow")
        self.localized_text_display.bind("<<Modified>>", self._on_localized_text_modified)
        self.localized_text_display.bind("<FocusIn>", self._on_localized_text_focus_in)
        self._localized_text_widget_font = tkfont.Font(font=self.localized_text_display.cget("font"))
        self._localized_text_widget_bold_font = self._localized_text_widget_font.copy()
        self._localized_text_widget_bold_font.configure(weight="bold")
        self.localized_text_display.tag_configure("normal", font=self._localized_text_widget_font)
        self.localized_text_display.tag_configure("bold", font=self._localized_text_widget_bold_font)
        self.localized_text_frame.grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=6)
        self.localized_text_frame.grid_remove()
        ttk.Label(detail_frame, text="FSB").grid(row=6, column=0, sticky="nw", padx=6, pady=6)
        ttk.Label(detail_frame, textvariable=self.selected_fsb_var, wraplength=420).grid(row=6, column=1, sticky="w", padx=6, pady=6)
        ttk.Label(detail_frame, text="Notes").grid(row=7, column=0, sticky="nw", padx=6, pady=6)
        ttk.Label(detail_frame, textvariable=self.selected_notes_var, wraplength=420).grid(row=7, column=1, sticky="w", padx=6, pady=6)

        self.selected_speech_intensity_frame = ttk.Frame(detail_frame)
        self.selected_speech_intensity_frame.columnconfigure(1, weight=1)
        ttk.Label(self.selected_speech_intensity_frame, text="Speech Intensity").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.selected_speech_intensity_scale = ttk.Scale(
            self.selected_speech_intensity_frame,
            from_=SPEECH_INTENSITY_MIN,
            to=SPEECH_INTENSITY_MAX,
            variable=self.selected_speech_intensity_scale_var,
            command=self._on_selected_speech_intensity_scale_changed,
        )
        self.selected_speech_intensity_scale.grid(row=0, column=1, sticky="ew", padx=(0, 6))
        self.selected_speech_intensity_entry = ttk.Entry(
            self.selected_speech_intensity_frame,
            textvariable=self.selected_speech_intensity_var,
            width=8,
        )
        self.selected_speech_intensity_entry.grid(row=0, column=2, sticky="e")
        self.selected_speech_intensity_entry.bind("<FocusOut>", self._sync_selected_speech_intensity_from_entry)
        self.selected_speech_intensity_entry.bind("<Return>", self._sync_selected_speech_intensity_from_entry)

        preview_frame = ttk.LabelFrame(detail_frame, text="Preview")
        preview_frame.grid(row=9, column=0, columnspan=2, sticky="ew", padx=6, pady=(6, 6))
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

        self.apply_entry_button = ttk.Button(detail_frame, text="Apply Entry Changes", command=self._apply_selected_entry)
        self.apply_entry_button.grid(
            row=10, column=0, columnspan=2, sticky="ew", padx=6, pady=(6, 8)
        )

        for widget in (
            self.selected_name_entry,
            self.selected_type_entry,
            self.selected_sample_count_entry,
            self.selected_duration_entry,
            self.selected_speech_intensity_entry,
        ):
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
        self._apply_text_widget_theme()

    def _build_bottom_frame(self) -> None:
        self.console_frame = ttk.Frame(self.main_frame)
        self.console_frame.grid(row=2, column=0, sticky="nsew", padx=12, pady=(6, 12))
        self.console_frame.columnconfigure(0, weight=1)
        self.console_frame.rowconfigure(0, weight=1)

        self.console_notebook = ttk.Notebook(self.console_frame)
        self.console_notebook.grid(row=0, column=0, sticky="nsew")
        self.console_tab = ttk.Frame(self.console_notebook)
        self.console_tab.columnconfigure(0, weight=1)
        self.console_tab.rowconfigure(1, weight=1)
        self.console_notebook.add(self.console_tab, text="Console")

        console_toolbar = ttk.Frame(self.console_tab)
        console_toolbar.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 0))
        console_toolbar.columnconfigure(0, weight=1)
        ttk.Label(console_toolbar, text="DyingAudio console").grid(row=0, column=0, sticky="w")
        ttk.Button(console_toolbar, text="Close", command=self._hide_console_panel).grid(row=0, column=1, sticky="e")

        self.log_text = ScrolledText(self.console_tab, height=9, wrap="word")
        self.log_text.grid(row=1, column=0, sticky="nsew", padx=6, pady=6)
        self._apply_text_widget_theme()
        self._dl1_busy_widgets = [
            self.add_source_button,
            self.import_manifest_button,
            self.replace_source_button,
            self.remove_entry_button,
            self.move_up_button,
            self.move_down_button,
            self.clear_entries_button,
        ]

    def _load_proc_names(self) -> None:
        proc_names = self.settings.audio_proc_names or list(DEFAULT_AUDIO_PROCS)
        self.proc_text.delete("1.0", tk.END)
        self.proc_text.insert("1.0", "\n".join(proc_names))
        self.proc_text.edit_modified(False)

    def _on_settings_changed(self, *_args: object) -> None:
        self._update_speech_intensity_controls()
        self._update_toolchain_status()
        self._update_speech_summary()
        self._refresh_localized_text_catalog()
        self._update_localized_text_display()
        self._update_script_preview()

    def _on_welcome_toggle_changed(self, *_args: object) -> None:
        self._save_settings()

    def _on_high_contrast_changed(self, *_args: object) -> None:
        self._configure_high_contrast()
        if self._welcome_window is not None and self._welcome_window.winfo_exists():
            refresh = getattr(self._welcome_window, "_dyingaudio_refresh", None)
            if callable(refresh):
                refresh()
        self._save_settings()

    def _on_entry_filter_changed(self, *_args: object) -> None:
        self._update_sort_controls()
        self._refresh_tree()

    def _on_selected_detail_changed(self, *_args: object) -> None:
        if self._detail_form_loading or self._detail_entry_index is None:
            return
        self._detail_form_dirty = True
        self._update_apply_entry_button_state()

    def _populate_selected_entry_details(self, indices: int | list[int] | tuple[int, ...] | None) -> None:
        normalized = self._normalize_selection_indices(indices)
        self._detail_form_loading = True
        try:
            self._detail_entry_indices = normalized
            self._detail_entry_index = normalized[0] if normalized else None
            if not normalized:
                self.selected_name_var.set("")
                self.selected_type_var.set("2")
                self.selected_sample_count_var.set("0")
                self.selected_duration_var.set("0")
                self.selected_speech_intensity_var.set(_format_speech_intensity(DEFAULT_SPEECH_INTENSITY))
                self.selected_speech_intensity_scale_var.set(DEFAULT_SPEECH_INTENSITY)
                self.selected_fsb_var.set("")
                self.selected_notes_var.set("")
            elif len(normalized) == 1:
                entry = self.entries[normalized[0]]
                self.selected_name_var.set(entry.entry_name)
                self.selected_type_var.set(str(entry.entry_type))
                self.selected_sample_count_var.set(str(entry.sample_count))
                self.selected_duration_var.set(str(entry.duration_ms))
                self.selected_speech_intensity_var.set(_format_speech_intensity(entry.speech_intensity))
                self.selected_speech_intensity_scale_var.set(_clamp_speech_intensity(entry.speech_intensity))
                self.selected_fsb_var.set(entry.fsb_path)
                self.selected_notes_var.set(entry.notes)
            else:
                entries = [self.entries[index] for index in normalized]
                self.selected_name_var.set(MIXED_DETAIL_VALUE)
                self.selected_type_var.set(self._detail_display_value([entry.entry_type for entry in entries]))
                self.selected_sample_count_var.set(self._detail_display_value([entry.sample_count for entry in entries]))
                self.selected_duration_var.set(self._detail_display_value([entry.duration_ms for entry in entries]))
                self.selected_speech_intensity_var.set(
                    self._detail_display_value(
                        [entry.speech_intensity for entry in entries],
                        formatter=lambda value: _format_speech_intensity(float(value)),
                    )
                )
                self.selected_speech_intensity_scale_var.set(_clamp_speech_intensity(entries[0].speech_intensity))
                self.selected_fsb_var.set(self._detail_display_value([entry.fsb_path for entry in entries]))
                self.selected_notes_var.set(self._detail_display_value([entry.notes for entry in entries]))
        finally:
            self._detail_form_loading = False
        self._detail_form_dirty = False
        self._detail_snapshot = self._detail_values()
        self._update_selected_entry_controls()

    def _update_selected_entry_controls(self) -> None:
        has_selection = bool(self._detail_entry_indices)
        entry_state = "normal" if has_selection else "disabled"
        for widget in (
            self.selected_name_entry,
            self.selected_type_entry,
            self.selected_sample_count_entry,
            self.selected_duration_entry,
            self.selected_speech_intensity_entry,
        ):
            widget.configure(state=entry_state)
        self.selected_speech_intensity_scale.configure(state=entry_state)
        if has_selection and self._speech_controls_enabled():
            self.selected_speech_intensity_frame.grid(row=8, column=0, columnspan=2, sticky="ew", padx=6, pady=(0, 6))
        else:
            self.selected_speech_intensity_frame.grid_remove()
        if has_selection and self.localized_bank_var.get():
            self.localized_text_frame.grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=6)
        else:
            self.localized_text_frame.grid_remove()
        self._update_localized_text_display()
        self._update_apply_entry_button_state()

    def _update_apply_entry_button_state(self) -> None:
        has_selection = bool(self._detail_entry_indices)
        has_changes = self._detail_form_dirty or self._localized_text_dirty
        self.apply_entry_button.configure(state="normal" if has_selection and has_changes else "disabled")

    def _speech_controls_enabled(self) -> bool:
        return self.localized_bank_var.get() or self.generate_spb_var.get()

    def _current_loaded_csb_key(self) -> str | None:
        if self.loaded_csb_path is None:
            return None
        return str(self.loaded_csb_path.resolve()).casefold()

    def _refresh_localized_text_catalog(self) -> None:
        source = self.speech_text_source_var.get().strip()
        if source == self._localized_text_catalog_source:
            return
        self._localized_text_catalog_source = source
        if not source:
            self._localized_text_catalog = {}
            return
        try:
            self._localized_text_catalog = load_text_catalog(source)
        except OSError:
            self._localized_text_catalog = {}

    def _localized_text_for_selected_entry(self) -> str | None:
        if not self.localized_bank_var.get():
            return None
        indices = self._normalize_selection_indices(self._detail_entry_indices)
        if len(indices) != 1:
            return None
        if not indices[0] < len(self.entries):
            return None
        self._refresh_localized_text_catalog()
        entry = self.entries[indices[0]]
        value = self._localized_text_catalog.get(entry.entry_name.casefold())
        return value if value and value.strip() else None

    def _localized_text_is_editable(self) -> bool:
        indices = self._normalize_selection_indices(self._detail_entry_indices)
        return self.localized_bank_var.get() and len(indices) == 1 and indices[0] < len(self.entries)

    def _localized_text_editor_value(self) -> str:
        if not hasattr(self, "localized_text_display") or self.localized_text_display is None:
            return ""
        text = self.localized_text_display.get("1.0", "end-1c")
        if getattr(self, "_localized_text_placeholder_active", False) and text == DEFAULT_LOCALIZED_TEXT_MESSAGE:
            return ""
        return text

    def _set_localized_text_snapshot(self, text: str) -> None:
        indices = self._normalize_selection_indices(self._detail_entry_indices)
        self._localized_text_snapshot = text
        self._localized_text_entry_index = indices[0] if len(indices) == 1 else None
        self._localized_text_dirty = False

    def _on_localized_text_modified(self, _event: object) -> None:
        if not hasattr(self, "localized_text_display") or self.localized_text_display is None:
            return
        if not self.localized_text_display.edit_modified():
            return
        self.localized_text_display.edit_modified(False)
        if self._detail_form_loading:
            return
        if not DyingAudioApp._localized_text_is_editable(self):
            return
        text = DyingAudioApp._localized_text_editor_value(self)
        current_index = self._detail_entry_indices[0] if len(self._detail_entry_indices) == 1 else None
        self._localized_text_dirty = current_index == self._localized_text_entry_index and text != self._localized_text_snapshot
        self._update_apply_entry_button_state()

    def _on_localized_text_focus_in(self, _event: object) -> None:
        if not getattr(self, "_localized_text_placeholder_active", False):
            return
        if not DyingAudioApp._localized_text_is_editable(self):
            return
        self.localized_text_display.delete("1.0", tk.END)
        self.localized_text_display.edit_modified(False)
        self._localized_text_placeholder_active = False

    def _set_localized_text_display(self, text: str, *, bold: bool, editable: bool) -> None:
        if not hasattr(self, "localized_text_display") or self.localized_text_display is None:
            return
        widget = self.localized_text_display
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        if text:
            widget.insert("1.0", text, ("bold" if bold else "normal",))
        if hasattr(widget, "edit_modified"):
            widget.edit_modified(False)
        widget.configure(state="normal" if editable else "disabled", cursor="xterm" if editable else "arrow")
        self._localized_text_placeholder_active = bold and editable and text == DEFAULT_LOCALIZED_TEXT_MESSAGE

    def _update_localized_text_display(self) -> None:
        if not hasattr(self, "localized_text_display") or self.localized_text_display is None:
            return
        show_panel = bool(self._detail_entry_indices) and self.localized_bank_var.get()
        if not show_panel:
            self._set_localized_text_display("", bold=False, editable=False)
            DyingAudioApp._set_localized_text_snapshot(self, "")
            return
        if not DyingAudioApp._localized_text_is_editable(self):
            self._set_localized_text_display(DEFAULT_LOCALIZED_TEXT_MESSAGE, bold=True, editable=False)
            DyingAudioApp._set_localized_text_snapshot(self, "")
            return
        text = self._localized_text_for_selected_entry()
        if text is None:
            self._set_localized_text_display(DEFAULT_LOCALIZED_TEXT_MESSAGE, bold=True, editable=True)
            DyingAudioApp._set_localized_text_snapshot(self, "")
            return
        self._set_localized_text_display(text, bold=False, editable=True)
        DyingAudioApp._set_localized_text_snapshot(self, text)

    def _save_selected_localized_text(self) -> bool:
        if not self._localized_text_dirty:
            return True
        if not DyingAudioApp._localized_text_is_editable(self) or self._localized_text_entry_index is None:
            self._localized_text_dirty = False
            self._update_apply_entry_button_state()
            return True
        source = self.speech_text_source_var.get().strip()
        if not source:
            self._show_error_window(
                "Save localized text failed",
                "Set a text source .scr file first in Project > Text Source before saving localized text edits.",
            )
            self.status_var.set("Localized text update failed.")
            return False
        source_path = Path(source).expanduser().resolve()
        if source_path.suffix.lower() != ".scr":
            self._show_error_window(
                "Save localized text failed",
                "Localized text editing currently supports .scr files only. Choose a .scr text source.",
            )
            self.status_var.set("Localized text update failed.")
            return False

        entry = self.entries[self._localized_text_entry_index]
        text = DyingAudioApp._localized_text_editor_value(self)
        try:
            upsert_scr_text(source_path, entry.entry_name, text)
        except OSError as exc:
            self._show_error_window("Save localized text failed", str(exc))
            self.status_var.set("Localized text update failed.")
            return False

        self._localized_text_catalog_source = str(source_path)
        self._localized_text_catalog[entry.entry_name.casefold()] = text
        DyingAudioApp._set_localized_text_snapshot(self, text)
        self._append_log(f"Updated localized text in {source_path.name}: {entry.entry_name}")
        return True

    def _remember_current_bank_text_source(self) -> None:
        bank_key = self._current_loaded_csb_key()
        if bank_key is None:
            return
        text_source = self.speech_text_source_var.get().strip()
        if text_source:
            self.settings.dl1.bank_text_sources[bank_key] = text_source
        else:
            self.settings.dl1.bank_text_sources.pop(bank_key, None)

    def _apply_loaded_bank_text_source_state(self) -> None:
        bank_key = self._current_loaded_csb_key()
        remembered_source = self.settings.dl1.bank_text_sources.get(bank_key, "") if bank_key is not None else ""
        self.speech_text_source_var.set(remembered_source)
        has_source = bool(remembered_source)
        self.localized_bank_var.set(has_source)
        self.generate_spb_var.set(False)
        self._refresh_localized_text_catalog()
        self._update_speech_intensity_controls()
        self._update_speech_summary()
        self._update_script_preview()

    def _update_speech_intensity_controls(self) -> None:
        show_controls = self._speech_controls_enabled()
        if show_controls:
            self.global_speech_intensity_frame.grid()
        else:
            self.global_speech_intensity_frame.grid_remove()
        global_state = "normal" if show_controls else "disabled"
        self.global_speech_intensity_entry.configure(state=global_state)
        self.global_speech_intensity_scale.configure(state=global_state)
        self._update_selected_entry_controls()

    def _try_parse_speech_intensity(self, value: str, *, default: float) -> float:
        try:
            return _clamp_speech_intensity(float(value.strip() or default))
        except ValueError:
            return _clamp_speech_intensity(default)

    def _parse_speech_intensity(self, value: str, *, field_name: str) -> float:
        try:
            parsed = float(value.strip() or DEFAULT_SPEECH_INTENSITY)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a number between {SPEECH_INTENSITY_MIN:g} and {SPEECH_INTENSITY_MAX:g}.") from exc
        if parsed < SPEECH_INTENSITY_MIN or parsed > SPEECH_INTENSITY_MAX:
            raise ValueError(f"{field_name} must be between {SPEECH_INTENSITY_MIN:g} and {SPEECH_INTENSITY_MAX:g}.")
        return parsed

    def _on_global_speech_intensity_scale_changed(self, value: str) -> None:
        self.global_speech_intensity_var.set(_format_speech_intensity(float(value)))

    def _sync_global_speech_intensity_from_entry(self, _event: object | None = None) -> str | None:
        value = self._try_parse_speech_intensity(self.global_speech_intensity_var.get(), default=DEFAULT_SPEECH_INTENSITY)
        self.global_speech_intensity_scale_var.set(value)
        self.global_speech_intensity_var.set(_format_speech_intensity(value))
        return None

    def _on_selected_speech_intensity_scale_changed(self, value: str) -> None:
        self.selected_speech_intensity_var.set(_format_speech_intensity(float(value)))

    def _sync_selected_speech_intensity_from_entry(self, _event: object | None = None) -> str | None:
        value = self._try_parse_speech_intensity(self.selected_speech_intensity_var.get(), default=DEFAULT_SPEECH_INTENSITY)
        self.selected_speech_intensity_scale_var.set(value)
        self.selected_speech_intensity_var.set(_format_speech_intensity(value))
        return None

    def _clear_suspended_tree_select(self) -> None:
        self._suspend_tree_select = False

    def _normalize_selection_indices(self, indices: int | list[int] | tuple[int, ...] | None) -> tuple[int, ...]:
        if indices is None:
            return ()
        if isinstance(indices, int):
            return (indices,)
        return tuple(sorted(dict.fromkeys(int(index) for index in indices)))

    def _set_tree_selection(self, indices: int | list[int] | tuple[int, ...]) -> None:
        normalized = self._normalize_selection_indices(indices)
        children = set(self.tree.get_children())
        valid = [str(index) for index in normalized if str(index) in children]
        if not valid:
            return
        self._suspend_tree_select = True
        current_selection = tuple(self.tree.selection())
        if current_selection and hasattr(self.tree, "selection_remove"):
            self.tree.selection_remove(*current_selection)
        first = valid[0]
        self.tree.selection_set(first)
        if len(valid) > 1:
            if hasattr(self.tree, "selection_add"):
                for iid in valid[1:]:
                    self.tree.selection_add(iid)
            else:
                self.tree.selection_set(tuple(valid))
        self.tree.focus(first)
        self.tree.see(first)
        self.after_idle(self._clear_suspended_tree_select)

    def _restore_tree_selection(self, indices: int | list[int] | tuple[int, ...] | None) -> None:
        normalized = self._normalize_selection_indices(indices)
        if not normalized:
            return
        self._set_tree_selection(normalized)

    def _detail_values(self) -> dict[str, str]:
        return {
            "name": self.selected_name_var.get(),
            "type": self.selected_type_var.get(),
            "samples": self.selected_sample_count_var.get(),
            "duration": self.selected_duration_var.get(),
            "speech": self.selected_speech_intensity_var.get(),
        }

    def _detail_display_value(
        self,
        values: list[object],
        *,
        formatter: Callable[[object], str] | None = None,
        always_mixed_when_multiple: bool = False,
    ) -> str:
        if not values:
            return ""
        if len(values) > 1 and always_mixed_when_multiple:
            return MIXED_DETAIL_VALUE
        format_value = formatter or (lambda value: str(value))
        formatted = [format_value(value) for value in values]
        first = formatted[0]
        return first if all(value == first for value in formatted[1:]) else MIXED_DETAIL_VALUE

    def _selected_indices(self) -> list[int]:
        selected = self.tree.selection()
        if not selected:
            return []
        return sorted(int(iid) for iid in selected)

    def _update_sort_controls(self) -> None:
        is_manual_order = self.sort_field_var.get().strip() == "Original Order"
        self.sort_button_var.set("Manual Order" if is_manual_order else ("Descending" if self.sort_descending_var.get() else "Ascending"))
        self.sort_order_button.configure(state="disabled" if is_manual_order else "normal")

    def _set_dl1_busy(self, busy: bool) -> None:
        for widget in self._dl1_busy_widgets:
            widget.configure(state="disabled" if busy else "normal")

    def _select_main_tab(self, tab: tk.Widget | None) -> None:
        if tab is None:
            return
        self.notebook.select(tab)

    def _open_github_page(self) -> None:
        if webbrowser.open(GITHUB_REPOSITORY_URL, new=2):
            self.status_var.set("Opened GitHub page.")
            return
        self._show_error_window("Open GitHub page", f"Could not open:\n{GITHUB_REPOSITORY_URL}")

    def _show_console_panel(self) -> None:
        if self.console_frame is None:
            return
        if not self._console_visible:
            self.console_frame.grid(row=2, column=0, sticky="nsew", padx=12, pady=(6, 12))
            self.main_frame.rowconfigure(2, weight=2)
            self._console_visible = True
        if self.console_notebook is not None and self.console_tab is not None:
            self.console_notebook.select(self.console_tab)

    def _hide_console_panel(self) -> None:
        if self.console_frame is None:
            return
        self.console_frame.grid_remove()
        self.main_frame.rowconfigure(2, weight=0)
        self._console_visible = False

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
        container.rowconfigure(3, weight=0)

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

        cancel_button = ttk.Button(container, text="Cancel", command=self._cancel_current_task)
        cancel_button.grid(row=3, column=0, sticky="e", pady=(14, 0))

        self.loading_window = window
        self.loading_status_label = status_label
        self.loading_progress = progress
        self.loading_cancel_button = cancel_button
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
        self.loading_cancel_button = None
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

    def _show_info_window(self, title: str, message: str) -> None:
        show_info_dialog(self, title, message)

    def _show_warning_window(self, title: str, message: str) -> None:
        show_warning_dialog(self, title, message)

    def _show_error_window(self, title: str, message: str) -> None:
        show_error_dialog(self, title, message)

    def _ask_yes_no_window(self, title: str, message: str, *, kind: str = "warning") -> bool:
        return ask_yes_no_dialog(self, title, message, kind=kind)

    def _ask_yes_no_cancel_window(self, title: str, message: str, *, kind: str = "warning") -> bool | None:
        return ask_yes_no_cancel_dialog(self, title, message, kind=kind)

    def _ask_string_window(self, title: str, prompt: str, *, initialvalue: str = "") -> str | None:
        return ask_string_dialog(self, title, prompt, initialvalue=initialvalue)

    def _cancel_current_task(self) -> None:
        if not self.task_runner.is_running:
            self._close_loading_window()
            return
        self.task_status_var.set("Cancelling...")
        self.status_var.set("Cancelling task...")
        if self.loading_cancel_button is not None:
            self.loading_cancel_button.configure(state="disabled")
        self.task_runner.cancel()

    def _run_dl1_task(
        self,
        *,
        start_message: str,
        error_title: str,
        worker: callable,
        on_success: callable,
    ) -> None:
        if self.task_runner.is_running:
            self._show_info_window("Dying Light 1 workspace busy", "Wait for the current task to finish first.")
            return
        self._set_dl1_busy(True)
        self.task_status_var.set(start_message)
        self.status_var.set(start_message)
        self.task_progress_var.set(0.0)
        self._show_loading_window(start_message)

        def handle_error(exc: BaseException, details: str) -> None:
            if isinstance(exc, TaskCancelled):
                self._append_log("Task cancelled.")
                self.task_status_var.set("Task cancelled.")
                self.status_var.set("Task cancelled.")
                return
            self._append_log(details.rstrip())
            self._show_error_window(error_title, str(exc))
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
            self.toolchain_status_var.set("DLDT toolchain not selected. Use Settings > Folders and Tools.")
            return
        toolchain, errors = discover_toolchain(root)
        self.current_toolchain = toolchain
        if toolchain:
            self.toolchain_status_var.set(f"DLDT toolchain ready: {toolchain.fsb_dir}")
        elif self.builder_mode_var.get().strip() == "Existing FSB Files":
            self.toolchain_status_var.set("DLDT toolchain not ready, but Existing FSB Files mode can still build.")
        else:
            self.toolchain_status_var.set("DLDT toolchain not ready: " + "; ".join(errors))

    def _speech_load_mode(self) -> str:
        return "localised" if self.localized_bank_var.get() or self.generate_spb_var.get() else "audio"

    def _on_generate_spb_toggle(self) -> None:
        if self.generate_spb_var.get() and not self.localized_bank_var.get():
            self.localized_bank_var.set(True)
        self._update_speech_intensity_controls()
        self._update_speech_summary()
        self._update_script_preview()

    def _update_speech_summary(self) -> None:
        text_source = self.speech_text_source_var.get().strip()
        intensity_label = self.global_speech_intensity_var.get().strip() or _format_speech_intensity(DEFAULT_SPEECH_INTENSITY)
        if self.generate_spb_var.get():
            source_label = text_source or "auto-search data/texts_steam_workshop.scr"
            self.speech_summary_var.set(
                f"Speech Data: SPB generation enabled; text source: {source_label}; global intensity: {intensity_label}x"
            )
        elif self.localized_bank_var.get():
            self.speech_summary_var.set(
                f"Speech Data: localized load mode enabled; generated scripts use LoadLocalisedAudioBank; global intensity: {intensity_label}x"
            )
        else:
            self.speech_summary_var.set("Speech Data: disabled")

    def _speech_options(self, *, auto_text_root: str | Path, log: Callable[[str], None] | None = None) -> SpeechBuildOptions:
        text_source = self.speech_text_source_var.get().strip()
        return SpeechBuildOptions(
            text_source=text_source or None,
            auto_text_root=auto_text_root,
            log=log,
            global_intensity=self._try_parse_speech_intensity(
                self.global_speech_intensity_var.get(),
                default=getattr(self.settings.dl1, "speech_intensity", DEFAULT_SPEECH_INTENSITY),
            ),
        )

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
            preview = generate_audiodata_scr(
                self.bundle_name_var.get().strip(),
                self.proc_text.get("1.0", tk.END),
                load_mode=self._speech_load_mode(),
            )
        except ValueError:
            preview = "// Enter a bundle name to preview audiodata.scr.\n"
        self._set_preview_text(preview)

    def _append_log(self, message: str) -> None:
        if not message:
            return
        if self.log_text is None:
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
            self._show_warning_window(
                "DLDT toolchain required",
                "This project includes raw audio. Saving or building it will require a valid DLDT toolchain path.",
            )

    def _commit_selected_entry_from_focus(self, _event: object) -> str:
        self._apply_selected_entry()
        return "break"

    def _select_entries(self, indices: int | list[int] | tuple[int, ...]) -> None:
        normalized = self._normalize_selection_indices(indices)
        if not normalized:
            return
        visible_indices = self._visible_entry_indices()
        visible_index_set = set(visible_indices)
        visible_selection = [index for index in normalized if index in visible_index_set]
        if not visible_selection:
            if not visible_indices:
                return
            visible_selection = [visible_indices[0]]
        self._set_tree_selection(visible_selection)
        self._on_tree_select(None)

    def _select_entry(self, index: int) -> None:
        self._select_entries((index,))

    def _set_loaded_csb(self, path: str | Path | None) -> None:
        self.loaded_csb_path = Path(path).resolve() if path else None
        if self.loaded_csb_path is None:
            self.loaded_csb_var.set("Loaded CSB: none")
            self._append_log("Loaded CSB: none")
        else:
            self.loaded_csb_var.set(f"Loaded CSB: {self.loaded_csb_path}")
            self._append_log(f"Loaded CSB: {self.loaded_csb_path}")

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
        indices = self._selected_indices()
        if not indices:
            self.preview_info_var.set("Select an entry to preview it.")
            return
        if len(indices) > 1:
            first_entry = self.entries[indices[0]]
            self.preview_info_var.set(
                f"{len(indices)} entries selected. Preview uses '{first_entry.entry_name}'. "
                f"{preview_strategy_for_entry(first_entry, self.preview_player.environment)}"
            )
            return
        self.preview_info_var.set(preview_strategy_for_entry(self.entries[indices[0]], self.preview_player.environment))

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

    def _browse_speech_text_source(self) -> None:
        current = self.speech_text_source_var.get().strip()
        initialdir = None
        if current:
            candidate = Path(current).expanduser()
            initialdir = str(candidate.parent if candidate.is_file() else candidate)
        selection = filedialog.askopenfilename(
            title="Select localized text source",
            initialdir=initialdir,
            filetypes=[
                ("Localized text sources", "*.scr *.bin *.tsv *.txt *.csv"),
                ("Script files", "*.scr"),
                ("Binary text files", "*.bin"),
                ("Tab-separated values", "*.tsv"),
                ("All files", "*.*"),
            ],
        )
        if selection:
            self.speech_text_source_var.set(str(Path(selection).resolve()))
            self._remember_current_bank_text_source()
            self._save_settings()

    def _tool_settings_snapshot(self) -> ToolSettings:
        return ToolSettings(
            ffmpeg_root=self.ffmpeg_root_var.get().strip(),
            vgmstream_root=self.vgmstream_root_var.get().strip(),
            wwise_root=self.wwise_root_var.get().strip(),
            show_welcome_on_startup=self.show_welcome_on_startup_var.get(),
            high_contrast_mode=self.high_contrast_var.get(),
        )

    def _refresh_tool_discovery(self) -> None:
        self.preview_player.environment = discover_media_tools(self._tool_settings_snapshot())
        self.preview_tools_var.set(self.preview_player.environment.summary())
        self._update_preview_info()

    def _browse_folder_into_var(self, target_var: tk.StringVar, title: str, *, discovery: Callable[[], Path | None] | None = None) -> None:
        selection = discovery() if discovery is not None else None
        if selection is None:
            current = target_var.get().strip()
            initialdir = current if current and Path(current).expanduser().exists() else None
            chosen = filedialog.askdirectory(title=title, initialdir=initialdir)
            if chosen:
                selection = Path(chosen).resolve()
        if selection is not None:
            target_var.set(str(selection))
            self._apply_folder_settings()

    def _apply_folder_settings(self) -> None:
        if self.experimental_frame is not None:
            self.experimental_frame._game_roots[DL2_GAME] = self.dl2_root_var.get().strip()
            self.experimental_frame._game_roots[DLTB_GAME] = self.dltb_root_var.get().strip()
            if self.experimental_frame.game_var.get() == DL2_GAME:
                self.experimental_frame.install_root_var.set(self.dl2_root_var.get().strip())
            elif self.experimental_frame.game_var.get() == DLTB_GAME:
                self.experimental_frame.install_root_var.set(self.dltb_root_var.get().strip())
            self.experimental_frame.cache_root_var.set(self.experimental_cache_root_var.get().strip() or DEFAULT_EXPERIMENTAL_CACHE_ROOT)
            self.experimental_frame.preview_player.environment = discover_media_tools(self._tool_settings_snapshot())
            self.experimental_frame.preview_tools_var.set(self.experimental_frame.preview_player.environment.summary())
        if self.other_frame is not None:
            self.other_frame.root_var.set(self.other_root_var.get().strip())
            self.other_frame.cache_root_var.set(self.other_cache_root_var.get().strip() or DEFAULT_OTHER_CACHE_ROOT)
            self.other_frame.preview_player.environment = discover_media_tools(self._tool_settings_snapshot())
            self.other_frame.preview_tools_var.set(self.other_frame.preview_player.environment.summary())
        self._refresh_tool_discovery()
        self._update_toolchain_status()
        self._save_settings()

    def _show_settings_window(self) -> None:
        window = tk.Toplevel(self)
        window.title("Folders and Tools")
        window.transient(self)
        window.geometry("920x560")
        window.minsize(760, 460)
        if is_windows_dark_mode():
            window.configure(bg="#1e1e1e")
        window.columnconfigure(0, weight=1)
        window.rowconfigure(0, weight=1)

        container = ttk.Frame(window, padding=14)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        fields = ttk.Frame(container)
        fields.grid(row=0, column=0, sticky="nsew")
        fields.columnconfigure(1, weight=1)

        rows: list[tuple[str, tk.StringVar, str, Callable[[], Path | None] | None]] = [
            ("DLDT Root", self.dldt_root_var, "Select Dying Light Developer Tools root", discover_dldt_root),
            ("DL1 Mods Root", self.mods_root_var, "Select Dying Light Mods root", discover_mods_root),
            ("DL2 Root", self.dl2_root_var, "Select Dying Light 2 root", lambda: discover_game_root(DL2_GAME)),
            ("The Beast Root", self.dltb_root_var, "Select Dying Light The Beast root", lambda: discover_game_root(DLTB_GAME)),
            ("Other Pack Root", self.other_root_var, "Select pack root", None),
            ("DL2/TB Cache Root", self.experimental_cache_root_var, "Select DL2/TB cache root", None),
            ("Other Cache Root", self.other_cache_root_var, "Select Other cache root", None),
            ("FFmpeg Folder", self.ffmpeg_root_var, "Select FFmpeg folder", None),
            ("vgmstream Folder", self.vgmstream_root_var, "Select vgmstream folder", None),
            ("Wwise Folder", self.wwise_root_var, "Select Wwise Authoring folder", None),
        ]
        for row, (label, variable, title, discovery) in enumerate(rows):
            ttk.Label(fields, text=label).grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
            ttk.Entry(fields, textvariable=variable).grid(row=row, column=1, sticky="ew", padx=(0, 8), pady=4)
            ttk.Button(
                fields,
                text="Browse",
                command=lambda var=variable, browse_title=title, discover=discovery: self._browse_folder_into_var(
                    var,
                    browse_title,
                    discovery=discover,
                ),
            ).grid(row=row, column=2, sticky="ew", pady=4)

        ttk.Checkbutton(
            fields,
            text="Show welcome menu on startup",
            variable=self.show_welcome_on_startup_var,
        ).grid(row=len(rows), column=0, columnspan=3, sticky="w", pady=(12, 4))
        ttk.Checkbutton(
            fields,
            text="High contrast mode",
            variable=self.high_contrast_var,
            command=self._on_high_contrast_changed,
        ).grid(row=len(rows) + 1, column=0, columnspan=3, sticky="w", pady=(4, 4))

        actions = ttk.Frame(container)
        actions.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        actions.columnconfigure(0, weight=1)
        ttk.Button(actions, text="Open Welcome Menu", command=lambda: self._show_welcome_wizard(force=True)).grid(
            row=0,
            column=0,
            sticky="w",
        )
        ttk.Button(actions, text="Apply", command=self._apply_folder_settings).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(
            actions,
            text="OK",
            command=lambda: (self._apply_folder_settings(), window.destroy()),
        ).grid(row=0, column=2)
        self._center_child_window(window, 920, 560)
        window.lift()

    def _current_packaged_exe_path(self) -> Path:
        if not getattr(sys, "frozen", False):
            raise RuntimeError("CSB association registration is only available in the compiled DyingAudio executable.")
        return Path(sys.executable).resolve()

    def _show_csb_association_window(self) -> None:
        window = tk.Toplevel(self)
        window.title(".csb / .spb File Types")
        window.transient(self)
        window.geometry("680x300")
        window.minsize(560, 240)
        if is_windows_dark_mode():
            window.configure(bg="#1e1e1e")
        window.columnconfigure(0, weight=1)
        window.rowconfigure(0, weight=1)

        container = ttk.Frame(window, padding=16)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)

        ttk.Label(
            container,
            text="Register DyingAudio file types so Windows shows .csb as Compiled Sound Bank and .spb as Speech Pattern Bank.",
            wraplength=620,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            container,
            text="This only updates your current Windows user. .csb keeps the DyingAudio Open with entry, and .spb gets a friendly type name without a direct opener.",
            wraplength=620,
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))

        exe_text = str(Path(sys.executable).resolve()) if getattr(sys, "frozen", False) else "Available in the compiled DyingAudio executable only."
        ttk.Label(container, text=f"Current executable: {exe_text}", wraplength=620).grid(row=2, column=0, sticky="w", pady=(12, 0))

        button_row = ttk.Frame(container)
        button_row.grid(row=3, column=0, sticky="ew", pady=(18, 0))
        button_row.columnconfigure(0, weight=1)
        button_row.columnconfigure(1, weight=1)

        can_register = getattr(sys, "frozen", False)
        ttk.Button(
            button_row,
            text="Register File Types",
            command=self._register_csb_association,
            state="normal" if can_register else "disabled",
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(
            button_row,
            text="Remove File Types",
            command=self._remove_csb_association,
            state="normal" if can_register else "disabled",
        ).grid(row=0, column=1, sticky="ew", padx=(8, 0))

        ttk.Button(container, text="Close", command=window.destroy).grid(row=4, column=0, sticky="e", pady=(20, 0))
        self._center_child_window(window, 680, 300)
        window.lift()

    def _open_csb_file_types_setup(self) -> None:
        self._show_csb_association_window()

    def _register_csb_association(self) -> None:
        try:
            exe_path = self._current_packaged_exe_path()
            register_csb_open_with(exe_path)
        except Exception as exc:
            self._show_error_window(".csb / .spb File Types", str(exc))
            return

        self._show_info_window(
            ".csb / .spb File Types",
            "DyingAudio file types have been registered for this Windows user.\n\n"
            ".csb is labeled as Compiled Sound Bank and remains available in Explorer's Open with menu.\n"
            ".spb is labeled as Speech Pattern Bank without a direct open command.\n\n"
            f"Registered executable:\n{exe_path}",
        )

    def _remove_csb_association(self) -> None:
        try:
            exe_path = self._current_packaged_exe_path()
            unregister_csb_open_with()
        except Exception as exc:
            self._show_error_window(".csb / .spb File Types", str(exc))
            return

        self._show_info_window(
            ".csb / .spb File Types",
            "DyingAudio's .csb and .spb file-type registrations have been removed from this Windows user.\n\n"
            f"Last registered executable:\n{exe_path}",
        )

    def _tool_status_rows(self) -> list[tuple[str, bool, str, tk.StringVar | None]]:
        self._refresh_tool_discovery()
        self._update_toolchain_status()
        tools = self.preview_player.environment
        dldt_ready = self._resolve_dldt_root(allow_discovery=False) is not None and self.current_toolchain is not None
        return [
            ("FFmpeg", tools.ffmpeg_path is not None, str(tools.ffmpeg_path or "Missing"), self.ffmpeg_root_var),
            ("FFplay", tools.ffplay_path is not None, str(tools.ffplay_path or "Missing"), self.ffmpeg_root_var),
            ("FFprobe", tools.ffprobe_path is not None, str(tools.ffprobe_path or "Missing"), self.ffmpeg_root_var),
            ("vgmstream", tools.vgmstream_path is not None, str(tools.vgmstream_path or "Missing"), self.vgmstream_root_var),
            ("Wwise 2023.1.13.8732", tools.wwise_console_path is not None, str(tools.wwise_console_path or "Missing"), self.wwise_root_var),
            ("Dying Light Developer Tools", dldt_ready, self.toolchain_status_var.get(), self.dldt_root_var),
        ]

    def _all_welcome_requirements_ready(self) -> bool:
        return all(installed for _label, installed, _detail, _var in self._tool_status_rows())

    def _show_tool_status_window(self) -> None:
        window = tk.Toplevel(self)
        window.title("Tool Status")
        window.transient(self)
        window.geometry("760x420")
        window.minsize(620, 340)
        if is_windows_dark_mode():
            window.configure(bg="#1e1e1e")
        window.columnconfigure(0, weight=1)
        window.rowconfigure(0, weight=1)

        frame = ttk.Frame(window, padding=14)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(1, weight=1)

        def redraw() -> None:
            for child in frame.winfo_children():
                child.destroy()
            ttk.Label(frame, text="Tool Status", font=("TkDefaultFont", 14, "bold")).grid(
                row=0,
                column=0,
                columnspan=3,
                sticky="w",
                pady=(0, 12),
            )
            for index, (label, installed, detail, variable) in enumerate(self._tool_status_rows(), start=1):
                color = "#2e7d32" if installed else "#c62828"
                tk.Label(frame, text="Ready" if installed else "Missing", fg=color, anchor="w").grid(
                    row=index,
                    column=0,
                    sticky="w",
                    padx=(0, 10),
                    pady=4,
                )
                ttk.Label(frame, text=label).grid(row=index, column=1, sticky="w", pady=4)
                ttk.Label(frame, text=detail, wraplength=420).grid(row=index, column=2, sticky="w", pady=4)
                if not installed and variable is not None:
                    ttk.Button(
                        frame,
                        text="Browse",
                        command=lambda var=variable: (self._browse_folder_into_var(var, "Select tool folder"), redraw()),
                    ).grid(row=index, column=3, sticky="e", padx=(8, 0), pady=4)

        redraw()
        self._center_child_window(window, 760, 420)
        window.lift()

    def _maybe_show_welcome_wizard(self) -> None:
        if self._skip_welcome_wizard or not self.show_welcome_on_startup_var.get():
            return
        self._show_welcome_wizard(force=False)

    def _browse_welcome_requirement(self, variable: tk.StringVar, title: str, refresh: Callable[[], None]) -> None:
        self._browse_folder_into_var(variable, title)
        refresh()

    def _show_welcome_wizard(self, *, force: bool) -> None:
        if self._welcome_window is not None and self._welcome_window.winfo_exists():
            self._welcome_window.lift()
            return
        if not force and not self.show_welcome_on_startup_var.get():
            return

        start_screen = 2 if self._all_welcome_requirements_ready() else 1
        screen_var = tk.IntVar(value=start_screen)
        dont_show_var = tk.BooleanVar(value=not self.show_welcome_on_startup_var.get())

        window = tk.Toplevel(self)
        self._welcome_window = window
        window.title("Welcome to Dying Audio")
        window.transient(self)
        window.geometry("1280x720")
        window.minsize(960, 540)
        window.resizable(False, False)
        window.configure(bg="#000000")
        window.protocol("WM_DELETE_WINDOW", window.destroy)

        canvas = tk.Canvas(window, highlightthickness=0, bg="#000000")
        canvas.pack(fill="both", expand=True)
        landing_path = bundled_resource_root() / "assets" / "Landing.png"
        landing_image: tk.PhotoImage | None = None
        background_item = canvas.create_image(0, 0, anchor="nw")
        if landing_path.exists():
            try:
                landing_image = tk.PhotoImage(master=window, file=str(landing_path))
            except tk.TclError:
                landing_image = None
        if landing_image is not None:
            canvas.itemconfigure(background_item, image=landing_image, anchor="nw")
            canvas.image = landing_image

        def render_background(width: int, height: int) -> None:
            if self.high_contrast_var.get():
                canvas.configure(bg="#000000")
                canvas.itemconfigure(background_item, image="")
                return
            if landing_image is not None:
                canvas.itemconfigure(background_item, image=landing_image, anchor="nw")
            else:
                canvas.itemconfigure(background_item, image="")
            canvas.coords(background_item, 0, 0)

        panel = tk.Frame(canvas, bg="#000000", padx=18, pady=14, highlightthickness=0, borderwidth=0)
        panel_window = canvas.create_window(854, 0, window=panel, anchor="nw", width=426, height=720)

        def overlay_button(
            parent: tk.Misc,
            text: str,
            command: Callable[[], None],
            *,
            accent: bool = False,
        ) -> tk.Label:
            border = "#ffff00" if self.high_contrast_var.get() or accent else "#ffffff"
            foreground = "#ffff00" if accent else "#ffffff"
            label = tk.Label(
                parent,
                text=text,
                bg="#000000",
                fg=foreground,
                cursor="hand2",
                highlightbackground=border,
                highlightcolor="#ffff00",
                highlightthickness=1,
                relief="solid",
                borderwidth=1,
                padx=8,
                pady=6 if not accent else 4,
            )
            label.bind("<Button-1>", lambda _event: command())
            label.bind("<Return>", lambda _event: (command(), "break")[1])
            label.bind("<space>", lambda _event: (command(), "break")[1])
            label.bind(
                "<Enter>",
                lambda _event: label.configure(bg="#ffff00", fg="#000000", highlightbackground="#ffff00"),
            )
            label.bind(
                "<Leave>",
                lambda _event: label.configure(bg="#000000", fg=foreground, highlightbackground=border),
            )
            return label

        def welcome_checkbox(parent: tk.Misc, text: str, variable: tk.BooleanVar, command: Callable[[], None]) -> tk.Checkbutton:
            return tk.Checkbutton(
                parent,
                text=text,
                variable=variable,
                command=command,
                bg="#000000",
                fg="#ffffff",
                activebackground="#000000",
                activeforeground="#ffffff",
                selectcolor="#000000",
                highlightbackground="#000000",
                highlightcolor="#ffff00",
                relief="flat",
                borderwidth=0,
            )

        def apply_dont_show() -> None:
            self.show_welcome_on_startup_var.set(not dont_show_var.get())
            self._save_settings()

        def toggle_high_contrast() -> None:
            self.high_contrast_var.set(not self.high_contrast_var.get())
            self._on_high_contrast_changed()

        def close() -> None:
            apply_dont_show()
            window.destroy()

        def select_workspace(tab: tk.Widget | None) -> None:
            close()
            self._select_main_tab(tab)

        def add_header(text: str, row: int, size: int = 24) -> int:
            tk.Label(
                panel,
                text=text,
                fg="#ffffff",
                bg="#000000",
                font=("TkDefaultFont", size, "bold"),
                anchor="w",
                justify="left",
                wraplength=380,
            ).grid(
                row=row,
                column=0,
                columnspan=3,
                sticky="ew",
                pady=(0, 14),
            )
            return row + 1

        def add_status_row(row: int, label: str, installed: bool, detail: str, variable: tk.StringVar | None) -> int:
            color = "#67d36f" if installed else "#ff4d4d"
            tk.Label(panel, text=label, fg="#ffffff", bg="#000000", anchor="w", justify="left").grid(
                row=row,
                column=0,
                sticky="w",
                pady=3,
            )
            tk.Label(panel, text="Ready" if installed else "Missing", fg=color, bg="#000000", anchor="w").grid(
                row=row,
                column=1,
                sticky="w",
                padx=(8, 0),
                pady=3,
            )
            if not installed and variable is not None:
                overlay_button(
                    panel,
                    text="Browse",
                    command=lambda var=variable: self._browse_welcome_requirement(var, f"Select {label} folder", redraw),
                ).grid(row=row, column=2, sticky="e", padx=(8, 0), pady=3)
            elif detail and detail != "Missing":
                tk.Label(panel, text=Path(detail).name, fg="#b8b8b8", bg="#000000", anchor="e").grid(
                    row=row,
                    column=2,
                    sticky="e",
                    padx=(8, 0),
                    pady=3,
                )
            return row + 1

        def redraw() -> None:
            for child in panel.winfo_children():
                child.destroy()
            panel.columnconfigure(0, weight=1)
            panel.columnconfigure(1, weight=0)
            panel.columnconfigure(2, weight=0)
            panel.rowconfigure(99, weight=1)
            overlay_button(
                panel,
                f"High Contrast: {'On' if self.high_contrast_var.get() else 'Off'}",
                toggle_high_contrast,
                accent=True,
            ).grid(
                row=0,
                column=2,
                sticky="ne",
                pady=(0, 8),
            )

            row = 1
            if screen_var.get() == 1:
                row = add_header("Welcome to Dying Audio", row, size=22)
                tk.Label(
                    panel,
                    text="Before you start a project, please make sure you have the following tools installed:",
                    fg="#ffffff",
                    bg="#000000",
                    wraplength=360,
                    justify="left",
                    anchor="w",
                ).grid(row=row, column=0, columnspan=3, sticky="ew", pady=(0, 18))
                row += 1

                tk.Label(panel, text="Required for audio playback:", fg="#ffffff", bg="#000000", font=("TkDefaultFont", 11, "bold")).grid(
                    row=row,
                    column=0,
                    columnspan=3,
                    sticky="w",
                    pady=(0, 8),
                )
                row += 1
                status_rows = self._tool_status_rows()
                playback_names = {"FFmpeg", "FFplay", "FFprobe", "vgmstream"}
                editing_names = {"Wwise 2023.1.13.8732", "Dying Light Developer Tools"}
                for label, installed, detail, variable in status_rows:
                    if label in playback_names:
                        row = add_status_row(row, label, installed, detail, variable)

                row += 1
                tk.Label(panel, text="Required for audio editing:", fg="#ffffff", bg="#000000", font=("TkDefaultFont", 11, "bold")).grid(
                    row=row,
                    column=0,
                    columnspan=3,
                    sticky="w",
                    pady=(12, 8),
                )
                row += 1
                tk.Label(panel, text="DL2/TB Only:", fg="#f0a033", bg="#000000", font=("TkDefaultFont", 10, "bold")).grid(
                    row=row,
                    column=0,
                    columnspan=3,
                    sticky="w",
                )
                row += 1
                for label, installed, detail, variable in status_rows:
                    if label == "Wwise 2023.1.13.8732":
                        row = add_status_row(row, label, installed, detail, variable)
                tk.Label(panel, text="DL1 Only:", fg="#f0a033", bg="#000000", font=("TkDefaultFont", 10, "bold")).grid(
                    row=row,
                    column=0,
                    columnspan=3,
                    sticky="w",
                    pady=(8, 0),
                )
                row += 1
                for label, installed, detail, variable in status_rows:
                    if label in editing_names and label != "Wwise 2023.1.13.8732":
                        row = add_status_row(row, label, installed, detail, variable)

                tk.Label(
                    panel,
                    text="Optional first-time setup:",
                    fg="#ffffff",
                    bg="#000000",
                    font=("TkDefaultFont", 11, "bold"),
                    anchor="w",
                    justify="left",
                ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(12, 4))
                row += 1
                tk.Label(
                    panel,
                    text="Register .csb and .spb file types so they open with DyingAudio from Windows Explorer.",
                    fg="#ffffff",
                    bg="#000000",
                    wraplength=360,
                    justify="left",
                    anchor="w",
                ).grid(row=row, column=0, columnspan=3, sticky="ew", pady=(0, 8))
                row += 1
                overlay_button(panel, "Register File Types", self._open_csb_file_types_setup).grid(
                    row=row,
                    column=0,
                    columnspan=3,
                    sticky="ew",
                    pady=(0, 4),
                )
                row += 1

                overlay_button(panel, "Continue", lambda: (screen_var.set(2), redraw())).grid(
                    row=99,
                    column=0,
                    columnspan=3,
                    sticky="ew",
                    pady=(18, 0),
                )
                return

            row = add_header("MAIN MENU", row, size=24)
            panel.rowconfigure(20, weight=1)
            buttons = tk.Frame(panel, bg="#000000", highlightthickness=0, borderwidth=0)
            buttons.grid(row=20, column=0, columnspan=3, sticky="nsew")
            buttons.columnconfigure(0, weight=1)
            buttons.rowconfigure(0, weight=1)
            buttons.rowconfigure(3, weight=1)
            overlay_button(buttons, "Dying Light 1 Workspace", lambda: select_workspace(self.dl1_tab)).grid(
                row=1,
                column=0,
                sticky="ew",
                pady=(0, 12),
            )
            overlay_button(
                buttons,
                "Dying Light 2 / The Beast Workspace",
                lambda: select_workspace(self.experimental_frame),
            ).grid(
                row=2,
                column=0,
                sticky="ew"
            )
            welcome_checkbox(
                panel,
                "Don't show this again",
                dont_show_var,
                apply_dont_show,
            ).grid(
                row=99,
                column=0,
                columnspan=3,
                sticky="w",
                pady=(18, 0)
            )

        def on_configure(event: tk.Event) -> None:
            if event.widget is not window:
                return
            width = max(960, event.width)
            height = max(540, event.height)
            render_background(width, height)
            canvas.coords(panel_window, int(width * 2 / 3), 0)
            canvas.itemconfigure(panel_window, width=max(320, width // 3), height=height)
            for child in panel.winfo_children():
                if isinstance(child, tk.Label):
                    child.configure(wraplength=max(220, width // 3 - 42))
            if self._welcome_configure_after_id is not None:
                window.after_cancel(self._welcome_configure_after_id)
            target_height = max(540, int(width * 9 / 16))
            if abs(target_height - height) > 4:
                self._welcome_configure_after_id = window.after(
                    120,
                    lambda: window.geometry(f"{width}x{target_height}"),
                )

        window.bind("<Configure>", on_configure)
        window.bind("<Escape>", lambda _event: close())
        window._dyingaudio_refresh = lambda: (redraw(), render_background(window.winfo_width(), window.winfo_height()))
        redraw()
        self._center_child_window(window, 1280, 720)
        render_background(1280, 720)
        window.lift()

    def _refresh_tree(self) -> None:
        selected_indices = self._selected_indices()
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
                    format_entry_type(entry.entry_type),
                    entry.duration_ms,
                    entry.sample_count,
                ),
            )
        self.entry_count_var.set(f"{len(visible_indices)} shown / {len(self.entries)} total")
        visible_selection = [index for index in selected_indices if str(index) in self.tree.get_children()]
        if visible_selection:
            self._set_tree_selection(visible_selection)
        elif visible_indices:
            self._set_tree_selection((visible_indices[0],))
        self._on_tree_select(None)

    def _visible_entry_indices(self) -> list[int]:
        search_text = self.entry_search_var.get().strip().lower()
        indexed_entries = list(enumerate(self.entries))
        if search_text:
            indexed_entries = [
                (index, entry)
                for index, entry in indexed_entries
                if search_text in entry.entry_name.lower()
                or search_text in entry.notes.lower()
                or search_text in format_entry_type(entry.entry_type).lower()
            ]

        sort_field = self.sort_field_var.get().strip()
        if sort_field != "Original Order":
            key_map = {
                "Name": lambda pair: pair[1].entry_name.lower(),
                "Mode": lambda pair: pair[1].source_mode.lower(),
                "Type": lambda pair: pair[1].entry_type,
                "Duration": lambda pair: pair[1].duration_ms,
                "Samples": lambda pair: pair[1].sample_count,
            }
            indexed_entries.sort(key=key_map.get(sort_field, lambda pair: pair[0]), reverse=self.sort_descending_var.get())

        return [index for index, _entry in indexed_entries]

    def _selected_index(self) -> int | None:
        selected_indices = self._selected_indices()
        if not selected_indices:
            return None
        return selected_indices[0]

    def _selected_entry(self) -> AudioEntry | None:
        index = self._selected_index()
        if index is None:
            return None
        return self.entries[index]

    def _selected_entries(self) -> list[AudioEntry]:
        return [self.entries[index] for index in self._selected_indices() if 0 <= index < len(self.entries)]

    def _on_tree_select(self, _event: object) -> None:
        if self._suspend_tree_select:
            self._suspend_tree_select = False
            return

        selected_indices = tuple(self._selected_indices())
        current_indices = self._detail_entry_indices
        current_index = self._detail_entry_index
        if (self._detail_form_dirty or self._localized_text_dirty) and current_indices != selected_indices:
            current_name = "the current selection"
            if len(current_indices) == 1 and current_index is not None and 0 <= current_index < len(self.entries):
                current_name = f"'{self.entries[current_index].entry_name}'"
            elif current_indices:
                current_name = f"the {len(current_indices)} selected entries"
            choice = self._ask_yes_no_cancel_window(
                "Unsaved changes",
                (
                    f"Apply changes to {current_name} before switching selection?\n\n"
                    "Yes = apply changes\n"
                    "No = discard changes\n"
                    "Cancel = keep the current selection"
                ),
                kind="question",
            )
            if choice is None:
                self._restore_tree_selection(current_indices)
                return
            if choice:
                if not self._apply_selected_entry(target_indices=current_indices):
                    self._restore_tree_selection(current_indices)
                return
            self._detail_form_dirty = False
            self._localized_text_dirty = False
            self._update_localized_text_display()
            self._update_apply_entry_button_state()

        if selected_indices == current_indices and (self._detail_form_dirty or self._localized_text_dirty):
            self._update_apply_entry_button_state()
            self._update_preview_info()
            return

        self._populate_selected_entry_details(selected_indices)
        self._update_preview_info()

    def _show_tree_context_menu(self, event: object) -> str | None:
        if not hasattr(event, "x") or not hasattr(event, "y"):
            return None

        row_id = self.tree.identify_row(event.y)
        selected_rows = set(self.tree.selection())
        if row_id and row_id not in selected_rows:
            self.tree.selection_set(row_id)
            self.tree.focus(row_id)
            self._on_tree_select(None)

        selected_entries = self._selected_entries()
        has_selection = bool(selected_entries)
        can_open_source = False
        if len(selected_entries) == 1:
            entry = selected_entries[0]
            source = entry.resolved_source_path() if entry.source_mode == "raw" else entry.resolved_fsb_path()
            can_open_source = source is not None and source.exists()
        can_export_audio = has_selection
        can_export_fsb = has_selection and (
            self.current_toolchain is not None or all(entry.source_mode == "fsb" for entry in selected_entries)
        )

        self.entry_context_menu.entryconfigure("Replace Audio / FSB...", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Open Source", state="normal" if can_open_source else "disabled")
        self.entry_context_menu.entryconfigure("Export Audio...", state="normal" if can_export_audio else "disabled")
        self.entry_context_menu.entryconfigure("Export FSB...", state="normal" if can_export_fsb else "disabled")
        self.entry_context_menu.entryconfigure("Duplicate Entry", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Rename Entry...", state="normal" if has_selection else "disabled")
        self.entry_context_menu.entryconfigure("Remove Entry", state="normal" if has_selection else "disabled")

        if hasattr(event, "x_root") and hasattr(event, "y_root"):
            self.entry_context_menu.tk_popup(event.x_root, event.y_root)
            self.entry_context_menu.grab_release()
        return "break"

    def _apply_bulk_entry_name(self, indices: tuple[int, ...], base_name: str) -> list[str]:
        width = max(2, len(str(max(0, len(indices) - 1))))
        normalized_base_name = _normalize_dl1_entry_name(base_name)
        renamed: list[str] = []
        for offset, index in enumerate(indices):
            name = normalized_base_name if len(indices) == 1 else f"{normalized_base_name}_{offset:0{width}d}"
            self.entries[index].entry_name = name
            renamed.append(name)
        return renamed

    def _apply_selected_entry(self, *, target_indices: int | list[int] | tuple[int, ...] | None = None) -> bool:
        indices = self._normalize_selection_indices(self._detail_entry_indices if target_indices is None else target_indices)
        if not indices:
            return True
        localized_dirty = bool(getattr(self, "_localized_text_dirty", False))
        if localized_dirty and len(indices) != 1:
            localized_dirty = False
        displayed_values = self._detail_values()
        changed_fields = {
            key: displayed_values[key] != self._detail_snapshot.get(key, displayed_values[key]) for key in displayed_values
        }
        if not any(changed_fields.values()) and not localized_dirty:
            self._detail_form_dirty = False
            self._update_selected_entry_controls()
            return True
        is_multi_selection = len(indices) > 1

        try:
            entry_type = None
            sample_count = None
            duration_ms = None
            speech_intensity = None
            if not is_multi_selection or changed_fields["type"]:
                entry_type = int(self.selected_type_var.get() or 2)
            if not is_multi_selection or changed_fields["samples"]:
                sample_count = int(self.selected_sample_count_var.get() or 0)
            if not is_multi_selection or changed_fields["duration"]:
                duration_ms = int(self.selected_duration_var.get() or 0)
            if not is_multi_selection or changed_fields["speech"]:
                speech_intensity = self._parse_speech_intensity(
                    self.selected_speech_intensity_var.get(),
                    field_name="Speech intensity",
                )
        except ValueError:
            self._show_error_window(
                "Invalid entry values",
                "Type, Samples @ 48k, and Duration (ms) must be whole numbers, and speech intensity must be a number from 0 to 2.",
            )
            self.status_var.set("Entry update failed.")
            return False
        if entry_type is not None and entry_type <= 0:
            self._show_error_window("Invalid entry values", "Type must be 1, 2, or another positive channel count.")
            self.status_var.set("Entry update failed.")
            return False
        if (sample_count is not None and sample_count < 0) or (duration_ms is not None and duration_ms < 0):
            self._show_error_window("Invalid entry values", "Samples @ 48k and Duration (ms) cannot be negative.")
            self.status_var.set("Entry update failed.")
            return False

        renamed_entries: list[str] = []
        if changed_fields["name"]:
            base_name = self.selected_name_var.get().strip()
            if base_name:
                renamed_entries = self._apply_bulk_entry_name(indices, base_name)

        for index in indices:
            entry = self.entries[index]
            if entry_type is not None:
                entry.entry_type = entry_type
            if sample_count is not None:
                entry.sample_count = sample_count
            if duration_ms is not None:
                entry.duration_ms = duration_ms
            if speech_intensity is not None:
                entry.speech_intensity = speech_intensity

        if localized_dirty:
            self._localized_text_entry_index = indices[0]
            if not self._save_selected_localized_text():
                return False

        self._detail_form_dirty = False
        self._refresh_tree()
        if len(indices) == 1 and (any(changed_fields.values()) or localized_dirty):
            self.status_var.set(f"Updated entry '{self.entries[indices[0]].entry_name}'.")
        elif renamed_entries:
            self.status_var.set(f"Updated {len(indices)} entries with base name '{self.selected_name_var.get().strip()}'.")
        elif any(changed_fields.values()) or localized_dirty:
            self.status_var.set(f"Updated {len(indices)} entries.")
        return True

    def _build_entry_from_source_file(self, selection: str) -> tuple[AudioEntry, str]:
        path = Path(selection)
        if _is_fsb_source(path):
            return (
                AudioEntry(
                    entry_name=path.stem,
                    source_path=str(path),
                    source_mode="fsb",
                    fsb_path=str(path),
                    entry_type=2,
                    notes="Existing FSB file. Update Type if the bank is not stereo.",
                ),
                "fsb",
            )

        metadata = probe_audio_metadata(path)
        return (
            AudioEntry(
                entry_name=path.stem,
                source_path=str(path),
                source_mode="raw",
                entry_type=entry_type_from_channel_count(metadata.channel_count),
                sample_count=metadata.sample_count_48k,
                duration_ms=metadata.duration_ms,
                notes=metadata.notes,
            ),
            "raw",
        )

    def _apply_source_file_to_entry(self, entry: AudioEntry, selection: str) -> str:
        path = Path(selection)
        if _is_fsb_source(path):
            entry.source_path = str(path)
            entry.source_mode = "fsb"
            entry.fsb_path = str(path)
            entry.notes = f"Replacement FSB: {path.name}"
            return "fsb"

        metadata = probe_audio_metadata(path)
        entry.source_path = str(path)
        entry.source_mode = "raw"
        entry.fsb_path = ""
        entry.entry_type = entry_type_from_channel_count(metadata.channel_count)
        entry.sample_count = metadata.sample_count_48k
        entry.duration_ms = metadata.duration_ms
        entry.notes = f"Replacement audio: {metadata.notes or path.name}"
        return "raw"

    def _format_source_summary(self, raw_count: int, fsb_count: int) -> str:
        parts: list[str] = []
        if raw_count:
            parts.append(f"{raw_count} audio")
        if fsb_count:
            parts.append(f"{fsb_count} FSB")
        return ", ".join(parts) if parts else "0 files"

    def _add_source_files(self) -> None:
        selections = filedialog.askopenfilenames(
            title="Select audio or FSB files",
            filetypes=DL1_SOURCE_FILETYPES,
        )
        if not selections:
            self._update_preview_info()
            return

        raw_count = 0
        fsb_count = 0
        new_entries: list[AudioEntry] = []
        try:
            for selection in selections:
                entry, source_kind = self._build_entry_from_source_file(selection)
                new_entries.append(entry)
                if source_kind == "raw":
                    raw_count += 1
                else:
                    fsb_count += 1
        except Exception as exc:
            self._show_error_window("Add audio / FSB failed", str(exc))
            self.status_var.set("Add failed.")
            self._append_log(f"ERROR: {exc}")
            return

        start_index = len(self.entries)
        self.entries.extend(new_entries)
        if raw_count:
            self._ensure_raw_builder_mode()
        self._refresh_tree()
        self._select_entry(start_index)
        self.status_var.set(f"Added {self._format_source_summary(raw_count, fsb_count)}.")
        if raw_count:
            self._warn_if_raw_entries_need_toolchain()
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

    def _new_empty_csb(self) -> None:
        if not self._apply_selected_entry():
            return
        if self.entries and not self._ask_yes_no_window("New empty CSB", "Start a new empty CSB and clear the current entries?"):
            return
        self._save_settings()
        self.preview_player.stop()
        self._reset_preview_progress()
        self.preview_player.clear_cache()
        self._cleanup_edit_session()
        self.entries.clear()
        self._set_loaded_csb(None)
        self._set_loaded_csb_magic(None)
        self._set_loaded_csb_layout(None)
        self._refresh_tree()
        self._apply_loaded_bank_text_source_state()
        self._update_preview_info()
        self.status_var.set("Started a new empty CSB.")

    def _open_csb_for_editing(self, selection: str | Path | None = None) -> None:
        if selection is None:
            selection = filedialog.askopenfilename(
                title="Open CSB",
                filetypes=[("CSB files", "*.csb"), ("All files", "*.*")],
            )
        if not selection:
            return

        selection_path = Path(selection).expanduser().resolve()
        if not selection_path.exists():
            self._show_error_window("Open CSB failed", f"The selected file does not exist:\n{selection_path}")
            return

        self._save_settings()
        self._select_main_tab(self.dl1_tab)

        def worker(progress, _log):
            progress("Parsing CSB header...", None, None)
            parsed = parse_csb(selection_path)
            session_dir = tempfile.TemporaryDirectory(prefix="dyingaudio_edit_")
            extracted = extract_csb(selection_path, session_dir.name, progress=progress)
            return parsed, session_dir, extracted

        def on_success(result: object) -> None:
            parsed, session_dir, extracted = result
            self.preview_player.stop()
            self._reset_preview_progress()
            self.preview_player.clear_cache()
            self._cleanup_edit_session()
            self.edit_session_dir = session_dir
            self.entries = extracted
            self.bundle_name_var.set(selection_path.stem)
            self.builder_mode_var.set("Existing FSB Files")
            self._set_loaded_csb(selection_path)
            self._set_loaded_csb_magic(parsed.magic)
            self._set_loaded_csb_layout(parsed.layout)
            self._refresh_tree()
            if self.entries:
                self._select_entry(0)
            else:
                self._update_preview_info()
            self._apply_loaded_bank_text_source_state()
            self.task_status_var.set("Open complete.")
            self._append_log(
                f"Loaded {selection_path.name} for editing with {self._format_csb_variant(parsed.magic, parsed.layout)}."
            )
            self.status_var.set(f"Opened {selection_path.name} for editing.")

        self._run_dl1_task(
            start_message=f"Opening {selection_path.name}...",
            error_title="Open CSB failed",
            worker=worker,
            on_success=on_success,
        )

    def _replace_selected_with_source(self) -> None:
        if not self._apply_selected_entry():
            return

        indices = tuple(self._selected_indices())
        if not indices:
            self._show_info_window("Replace audio / FSB", "Select an entry to replace first.")
            return

        if len(indices) == 1:
            selection = filedialog.askopenfilename(title="Replace selected entry with audio or FSB", filetypes=DL1_SOURCE_FILETYPES)
            selections = (selection,) if selection else ()
        else:
            selections = filedialog.askopenfilenames(
                title="Replace selected entries with audio or FSB",
                filetypes=DL1_SOURCE_FILETYPES,
            )
        if not selections:
            return
        if len(selections) != len(indices):
            self._show_error_window(
                "Replace audio / FSB",
                f"Select exactly {len(indices)} replacement file(s) for the current selection.",
            )
            return

        raw_count = 0
        fsb_count = 0
        try:
            for index, selection in zip(indices, selections, strict=True):
                source_kind = self._apply_source_file_to_entry(self.entries[index], selection)
                if source_kind == "raw":
                    raw_count += 1
                else:
                    fsb_count += 1
        except Exception as exc:
            self._show_error_window("Replace audio / FSB failed", str(exc))
            self.status_var.set("Replace failed.")
            self._append_log(f"ERROR: {exc}")
            return

        self.preview_player.stop()
        self._reset_preview_progress()
        if raw_count:
            self._ensure_raw_builder_mode()
            self._warn_if_raw_entries_need_toolchain()
        self._refresh_tree()
        self._select_entries(indices)
        if len(indices) == 1:
            entry = self.entries[indices[0]]
            replacement_label = "audio" if raw_count else "FSB"
            self.status_var.set(f"Replaced '{entry.entry_name}' with new {replacement_label}.")
        else:
            self.status_var.set(f"Replaced {len(indices)} entries with {self._format_source_summary(raw_count, fsb_count)}.")

    def _open_selected_source(self) -> None:
        if not self._apply_selected_entry():
            return

        selected_entries = self._selected_entries()
        if len(selected_entries) != 1:
            self._show_info_window("Open source", "Select a single entry first.")
            return

        entry = selected_entries[0]
        source = entry.resolved_source_path() if entry.source_mode == "raw" else entry.resolved_fsb_path()
        if source is None:
            self._show_error_window("Open source failed", f"No source path is available for '{entry.entry_name}'.")
            return
        if not source.exists():
            self._show_error_window("Open source failed", f"The source file does not exist:\n{source}")
            return

        try:
            os.startfile(str(source))
        except OSError as exc:
            self._show_error_window("Open source failed", str(exc))
            return

        self.status_var.set(f"Opened source for '{entry.entry_name}'.")

    def _suggest_export_audio_name(self, entry: AudioEntry) -> str:
        return f"{entry.entry_name}{audio_quality_output_suffix(self.audio_quality_var.get())}"

    def _unique_export_destination(self, directory: Path, filename: str, reserved: set[Path]) -> Path:
        candidate = directory / filename
        stem = candidate.stem
        suffix = candidate.suffix
        counter = 1
        while candidate in reserved or candidate.exists():
            candidate = directory / f"{stem}_{counter:02d}{suffix}"
            counter += 1
        reserved.add(candidate)
        return candidate

    def _export_selected_audio(self) -> None:
        if not self._apply_selected_entry():
            return

        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            self._show_info_window("Export audio", "Select an entry to export first.")
            return
        selected_entries = [self.entries[index] for index in selected_indices]
        multiple = len(selected_entries) > 1

        if multiple:
            selection = filedialog.askdirectory(title="Export selected audio")
            if not selection:
                return
            destination_root = Path(selection).resolve()
        else:
            entry = selected_entries[0]
            selection = filedialog.asksaveasfilename(
                title="Export audio",
                defaultextension=Path(self._suggest_export_audio_name(entry)).suffix,
                initialfile=self._suggest_export_audio_name(entry),
                filetypes=AUDIO_EXPORT_FILETYPES,
            )
            if not selection:
                return
            destination_root = Path(selection).resolve()

        def worker(progress, log):
            total = len(selected_entries)
            exported: list[Path] = []
            reserved_paths: set[Path] = set()
            for offset, entry in enumerate(selected_entries, start=1):
                progress(f"Exporting audio for {entry.entry_name}...", offset - 1, total)
                source = entry.resolved_source_path() if entry.source_mode == "raw" else entry.resolved_fsb_path()
                if source is None or not source.exists():
                    raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")
                destination = (
                    self._unique_export_destination(destination_root, self._suggest_export_audio_name(entry), reserved_paths)
                    if multiple
                    else destination_root
                )
                export_audio_file(
                    source,
                    destination,
                    log=log,
                    audio_quality=self.audio_quality_var.get().strip() or DEFAULT_DL1_AUDIO_QUALITY,
                )
                exported.append(destination)
                progress(f"Exported {destination.name}.", offset, total)
            return exported

        def on_success(result: object) -> None:
            exported = result
            if not multiple:
                self.status_var.set(f"Exported audio for '{selected_entries[0].entry_name}'.")
            else:
                self.status_var.set(f"Exported audio for {len(selected_entries)} entries.")
            for path in exported:
                self._append_log(f"Exported audio to {path}")
            self.task_status_var.set("Audio export complete.")

        self._run_dl1_task(
            start_message=(
                f"Exporting audio for '{selected_entries[0].entry_name}'..."
                if not multiple
                else f"Exporting audio for {len(selected_entries)} entries..."
            ),
            error_title="Export audio failed",
            worker=worker,
            on_success=on_success,
        )

    def _export_selected_fsb(self) -> None:
        if not self._apply_selected_entry():
            return

        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            self._show_info_window("Export FSB", "Select an entry to export first.")
            return
        selected_entries = [self.entries[index] for index in selected_indices]
        multiple = len(selected_entries) > 1

        if multiple:
            selection = filedialog.askdirectory(title="Export selected FSB files")
            if not selection:
                return
            destination_root = Path(selection).resolve()
        else:
            entry = selected_entries[0]
            selection = filedialog.asksaveasfilename(
                title="Export FSB",
                defaultextension=".fsb",
                initialfile=f"{entry.entry_name}.fsb",
                filetypes=[("FSB files", "*.fsb"), ("All files", "*.*")],
            )
            if not selection:
                return
            destination_root = Path(selection).resolve()

        def worker(progress, log):
            total = len(selected_entries)
            exported: list[Path] = []
            reserved_paths: set[Path] = set()
            for offset, entry in enumerate(selected_entries, start=1):
                destination = (
                    self._unique_export_destination(destination_root, f"{entry.entry_name}.fsb", reserved_paths)
                    if multiple
                    else destination_root
                )
                progress(f"Exporting FSB for {entry.entry_name}...", offset - 1, total)
                if entry.source_mode == "fsb":
                    source = entry.resolved_fsb_path()
                    if source is None or not source.exists():
                        raise FileNotFoundError(f"Missing FSB file for '{entry.entry_name}'.")
                    shutil.copyfile(source, destination)
                    exported.append(destination)
                    progress(f"Copied {destination.name}.", offset, total)
                    continue

                if self.current_toolchain is None:
                    raise RuntimeError("A valid DLDT toolchain is required to export raw audio as FSB.")
                with tempfile.TemporaryDirectory(prefix="dyingaudio_export_fsb_") as temp_dir:
                    temp_root = Path(temp_dir)
                    source = entry.resolved_source_path()
                    if source is None or not source.exists():
                        raise FileNotFoundError(f"Missing source file for '{entry.entry_name}'.")
                    compile_source = source
                    if source.suffix.lower() != ".wav":
                        media_tools = discover_media_tools()
                        compile_source = temp_root / f"{entry.entry_name}.wav"
                        compile_source = decode_audio_to_wav(source, compile_source, log=log, tools=media_tools)
                    compile_result = compile_audio_to_fsb(
                        self.current_toolchain,
                        compile_source,
                        destination,
                        temp_root / "cache",
                        audio_quality=self.audio_quality_var.get().strip() or DEFAULT_DL1_AUDIO_QUALITY,
                    )
                    log(" ".join(compile_result.command))
                    if compile_result.stdout:
                        log(compile_result.stdout)
                    if compile_result.stderr:
                        log(compile_result.stderr)
                    if not compile_result.success:
                        raise RuntimeError(f"Could not compile '{entry.entry_name}' to FSB.")
                exported.append(destination)
                progress(f"Exported {destination.name}.", offset, total)
            return exported

        def on_success(result: object) -> None:
            exported = result
            if not multiple:
                self.status_var.set(f"Exported FSB for '{selected_entries[0].entry_name}'.")
            else:
                self.status_var.set(f"Exported FSB for {len(selected_entries)} entries.")
            for path in exported:
                self._append_log(f"Exported FSB to {path}")
            self.task_status_var.set("FSB export complete.")

        self._run_dl1_task(
            start_message=(
                f"Exporting FSB for '{selected_entries[0].entry_name}'..."
                if not multiple
                else f"Exporting FSB for {len(selected_entries)} entries..."
            ),
            error_title="Export FSB failed",
            worker=worker,
            on_success=on_success,
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

        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            self._show_info_window("Duplicate entry", "Select an entry to duplicate first.")
            return

        original_names = [self.entries[index].entry_name for index in selected_indices]
        inserted_indices: list[int] = []
        offset = 0
        for index in selected_indices:
            source_entry = self.entries[index + offset]
            duplicate = replace(source_entry, entry_name=self._make_duplicate_name(source_entry.entry_name))
            insert_at = index + offset + 1
            self.entries.insert(insert_at, duplicate)
            inserted_indices.append(insert_at)
            offset += 1
        self._refresh_tree()
        self._select_entries(inserted_indices)
        if len(selected_indices) == 1:
            self.status_var.set(f"Duplicated '{original_names[0]}'.")
        else:
            self.status_var.set(f"Duplicated {len(selected_indices)} entries.")

    def _rename_selected_entry(self) -> None:
        if not self._apply_selected_entry():
            return

        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            self._show_info_window("Rename entry", "Select an entry to rename first.")
            return

        initial_name = self.entries[selected_indices[0]].entry_name if len(selected_indices) == 1 else ""
        new_name = self._ask_string_window("Rename entry", "Entry name:", initialvalue=initial_name)
        if new_name is None:
            return

        cleaned_name = new_name.strip()
        if not cleaned_name:
            self._show_error_window("Rename entry", "Entry name cannot be empty.")
            return

        self._apply_bulk_entry_name(selected_indices, cleaned_name)
        self._refresh_tree()
        self._select_entries(selected_indices)
        if len(selected_indices) == 1:
            self.status_var.set(f"Renamed entry to '{cleaned_name}'.")
        else:
            self.status_var.set(f"Renamed {len(selected_indices)} entries to '{cleaned_name}_XX'.")

    def _remove_selected(self) -> None:
        if not self._apply_selected_entry():
            return
        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            return
        removed_names = [self.entries[index].entry_name for index in selected_indices]
        next_index = selected_indices[0]
        for index in reversed(selected_indices):
            self.entries.pop(index)
        self.preview_player.stop()
        self._reset_preview_progress()
        self._refresh_tree()
        if self.entries:
            self._select_entry(min(next_index, len(self.entries) - 1))
        else:
            self._update_preview_info()
        if len(removed_names) == 1:
            self.status_var.set(f"Removed entry '{removed_names[0]}'.")
        else:
            self.status_var.set(f"Removed {len(removed_names)} entries.")

    def _move_selected(self, delta: int) -> None:
        if not self._apply_selected_entry():
            return
        if self.sort_field_var.get().strip() != "Original Order":
            self._show_info_window("Reorder entries", "Switch sorting back to Original Order before moving entries manually.")
            return
        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            return
        if delta not in (-1, 1):
            return
        if delta < 0 and selected_indices[0] == 0:
            return
        if delta > 0 and selected_indices[-1] == len(self.entries) - 1:
            return

        if delta < 0:
            for index in selected_indices:
                self.entries[index - 1], self.entries[index] = self.entries[index], self.entries[index - 1]
            moved_indices = [index - 1 for index in selected_indices]
        else:
            for index in reversed(selected_indices):
                self.entries[index + 1], self.entries[index] = self.entries[index], self.entries[index + 1]
            moved_indices = [index + 1 for index in selected_indices]
        self._refresh_tree()
        self._select_entries(moved_indices)
        self.status_var.set("Reordered entries.")

    def _clear_entries(self) -> None:
        if not self._apply_selected_entry():
            return
        if not self.entries:
            return
        if not self._ask_yes_no_window("Clear entries", "Remove all current entries?"):
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

    def _clear_dl1_cache(self) -> None:
        if self.task_runner.is_running:
            self._show_info_window("Clear cache", "Wait for the current Dying Light 1 task to finish first.")
            return

        has_edit_session = self.edit_session_dir is not None
        prompt = (
            "Clear the Dying Light 1 temporary cache?\n\n"
            "This will clear preview files and unload the current extracted CSB edit session."
            if has_edit_session
            else "Clear the Dying Light 1 temporary cache?\n\nThis will clear preview files."
        )
        if not self._ask_yes_no_window("Clear cache", prompt):
            return

        self.preview_player.stop()
        self._reset_preview_progress()
        session_dir = self.edit_session_dir

        def worker(progress, log):
            progress("Clearing Dying Light 1 cache...")
            log("Clearing Dying Light 1 preview and edit-session cache.")
            if session_dir is not None:
                session_dir.cleanup()
            return True

        def on_success(_result: object) -> None:
            self.preview_player.clear_cache()
            self.edit_session_dir = None
            self.entries.clear()
            self._set_loaded_csb(None)
            self._set_loaded_csb_magic(None)
            self._set_loaded_csb_layout(None)
            self._refresh_tree()
            self._update_preview_info()
            self.status_var.set("Cleared Dying Light 1 cache.")
            self._append_log("Cleared Dying Light 1 preview and edit-session cache.")

        self._run_dl1_task(
            start_message="Clearing Dying Light 1 cache...",
            error_title="Clear cache failed",
            worker=worker,
            on_success=on_success,
        )

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
                "type": ("Type", 120),
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
                    values=(entry.entry_name, format_entry_type(entry.entry_type), entry.duration_ms, entry.sample_count, entry.notes),
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

        export_audio = self._ask_yes_no_cancel_window(
            "Extract CSB",
            "Export decoded audio instead of FSB files?\n\nYes: export audio files\nNo: export FSB files\nCancel: stop extraction.",
            kind="info",
        )
        if export_audio is None:
            return
        output_format = "audio" if export_audio else "fsb"

        def worker(progress, log):
            extracted = extract_csb(
                csb_path,
                output_dir,
                progress=progress,
                output_format=output_format,
                audio_quality=self.audio_quality_var.get().strip() or DEFAULT_DL1_AUDIO_QUALITY,
                log=log,
            )
            manifest_path = write_manifest(Path(output_dir) / "manifest.generated.json", extracted)
            return extracted, manifest_path

        def on_success(result: object) -> None:
            extracted, manifest_path = result
            output_label = "audio file(s)" if output_format == "audio" else "FSB file(s)"
            self._append_log(f"Extracted {len(extracted)} {output_label} to {output_dir}")
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
            choice = self._ask_yes_no_cancel_window(
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
            if target_path.exists() and not self._ask_yes_no_window("Overwrite CSB", f"Replace this file?\n\n{target_path}"):
                return

        def worker(progress, log):
            return build_csb_file(
                entries=self.entries,
                output_path=target_path,
                builder_mode=self.builder_mode_var.get().strip() or "Raw Audio via DLDT",
                toolchain=self.current_toolchain,
                log=log,
                audio_quality=self.audio_quality_var.get().strip() or DEFAULT_DL1_AUDIO_QUALITY,
                magic=self._effective_output_magic(),
                generate_spb=self.generate_spb_var.get(),
                speech_options=self._speech_options(auto_text_root=target_path.parent, log=log),
                progress=progress,
            )

        def on_success(result: object) -> None:
            csb_result = result
            if overwrite_loaded:
                self._set_loaded_csb(csb_result.csb_path)
            self.task_status_var.set("Save complete.")
            self.status_var.set(f"Saved {csb_result.csb_path.name}.")
            self._append_log(f"Saved CSB file: {csb_result.csb_path}")
            message = f"Saved CSB file:\n{csb_result.csb_path}"
            if csb_result.spb_path is not None:
                self._append_log(f"Saved SPB file: {csb_result.spb_path}")
                message += f"\n\nSaved SPB file:\n{csb_result.spb_path}"
            if csb_result.speech_result is not None:
                self.speech_summary_var.set(csb_result.speech_result.summary())
                self._append_log(csb_result.speech_result.summary())
            if self.localized_bank_var.get() or self.generate_spb_var.get():
                message += f'\n\nLoad this bank with LoadLocalisedAudioBank("{csb_result.csb_path.stem}") in audiodata.scr.'
            self._show_info_window("Save complete", message)

        self._run_dl1_task(
            start_message=f"Saving {target_path.name}...",
            error_title="Save failed",
            worker=worker,
            on_success=on_success,
        )

    def _play_selected_entry(self) -> None:
        if not self._apply_selected_entry():
            return

        selected_indices = tuple(self._selected_indices())
        if not selected_indices:
            self._show_info_window("Preview audio", "Select an entry to preview first.")
            return

        entry = self.entries[selected_indices[0]]
        try:
            preview_path = self.preview_player.play_entry(entry, self._append_log)
        except Exception as exc:
            self._show_error_window("Preview failed", str(exc))
            self.status_var.set("Preview failed.")
            self._append_log(f"ERROR: {exc}")
            return

        self._begin_preview_progress(entry)
        if len(selected_indices) == 1:
            self.status_var.set(f"Previewing '{entry.entry_name}'.")
        else:
            self.status_var.set(f"Previewing '{entry.entry_name}' from the current multi-selection.")
        self._append_log(f"Previewing {entry.entry_name} from {preview_path}")

    def _stop_preview(self) -> None:
        self.preview_player.stop()
        self._reset_preview_progress("Preview stopped.")
        self.status_var.set("Preview stopped.")

    def _save_settings(self) -> None:
        self._remember_current_bank_text_source()
        settings = AppSettings()
        settings.dl1.mods_root = self.mods_root_var.get().strip()
        settings.dl1.dldt_root = self.dldt_root_var.get().strip()
        settings.dl1.builder_mode = self.builder_mode_var.get().strip() or "Raw Audio via DLDT"
        settings.dl1.audio_quality = self.audio_quality_var.get().strip() or DEFAULT_DL1_AUDIO_QUALITY
        settings.dl1.mod_name = self.mod_name_var.get().strip() or DEFAULT_MOD_NAME
        settings.dl1.bundle_name = self.bundle_name_var.get().strip() or DEFAULT_BUNDLE_NAME
        settings.dl1.generate_audiodata = self.generate_script_var.get()
        settings.dl1.audio_proc_names = [line.strip() for line in self.proc_text.get("1.0", tk.END).splitlines() if line.strip()]
        settings.dl1.localized_bank = self.localized_bank_var.get() or self.generate_spb_var.get()
        settings.dl1.generate_spb = self.generate_spb_var.get()
        settings.dl1.speech_text_source = self.speech_text_source_var.get().strip()
        settings.dl1.bank_text_sources = dict(self.settings.dl1.bank_text_sources)
        settings.dl1.speech_intensity = self._try_parse_speech_intensity(
            self.global_speech_intensity_var.get(),
            default=getattr(settings.dl1, "speech_intensity", DEFAULT_SPEECH_INTENSITY),
        )
        self.global_speech_intensity_var.set(_format_speech_intensity(settings.dl1.speech_intensity))
        self.global_speech_intensity_scale_var.set(settings.dl1.speech_intensity)
        settings.dl1.last_output_folder = str(self.last_built_mod_root or "")
        if self.experimental_frame is not None:
            self.experimental_frame._game_roots[DL2_GAME] = self.dl2_root_var.get().strip()
            self.experimental_frame._game_roots[DLTB_GAME] = self.dltb_root_var.get().strip()
            settings.experimental = self.experimental_frame.build_settings()
            settings.experimental.dl2_root = self.dl2_root_var.get().strip()
            settings.experimental.dltb_root = self.dltb_root_var.get().strip()
            settings.experimental.cache_root = self.experimental_cache_root_var.get().strip() or DEFAULT_EXPERIMENTAL_CACHE_ROOT
        if self.other_frame is not None:
            settings.other = self.other_frame.build_settings()
            settings.other.root = self.other_root_var.get().strip()
            settings.other.cache_root = self.other_cache_root_var.get().strip() or DEFAULT_OTHER_CACHE_ROOT
        settings.tools = self._tool_settings_snapshot()
        save_settings(settings)

    def _build_mod(self) -> None:
        if not self._apply_selected_entry():
            return
        self._ensure_raw_builder_mode(notify=True)
        mods_root = self._resolve_mods_root(allow_discovery=True)
        if mods_root is None:
            self._show_info_window(
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
            self._show_info_window(
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
                audio_quality=self.audio_quality_var.get().strip() or DEFAULT_DL1_AUDIO_QUALITY,
                magic=self._effective_output_magic(),
                localized_bank=self.localized_bank_var.get() or self.generate_spb_var.get(),
                generate_spb=self.generate_spb_var.get(),
                speech_options=self._speech_options(
                    auto_text_root=mods_root / (self.mod_name_var.get().strip() or DEFAULT_MOD_NAME),
                    log=log,
                ),
                progress=progress,
            )

        def on_success(result: object) -> None:
            artifacts = result
            self.last_built_mod_root = artifacts.mod_root
            self.task_status_var.set("Build complete.")
            self.status_var.set(f"Built {artifacts.csb_path.name} in {artifacts.mod_root.name}.")
            self._append_log(f"Build complete: {artifacts.csb_path}")
            self._append_log(f"modinfo.ini: {artifacts.modinfo_path}")
            if artifacts.spb_path is not None:
                self._append_log(f"SPB file: {artifacts.spb_path}")
            if artifacts.speech_result is not None:
                self.speech_summary_var.set(artifacts.speech_result.summary())
                self._append_log(artifacts.speech_result.summary())
            if artifacts.script_path is not None:
                self._append_log(f"audiodata.scr: {artifacts.script_path}")
            message = f"Built mod folder:\n{artifacts.mod_root}"
            if (self.localized_bank_var.get() or self.generate_spb_var.get()) and artifacts.script_path is None:
                message += f'\n\nLoad this bank with LoadLocalisedAudioBank("{artifacts.csb_path.stem}") in audiodata.scr.'
            self._show_info_window("Build complete", message)

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
            self._show_info_window(
                "Open mod folder",
                "Select the Dying Light Mods folder first, or click Browse to auto-find it.",
            )
            return
        if not target.exists():
            self._show_info_window("Open mod folder", f"Folder does not exist yet:\n{target}")
            return
        os.startfile(str(target))

    def _on_close(self) -> None:
        if self._detail_form_dirty or self._localized_text_dirty:
            current_indices = self._detail_entry_indices
            current_index = self._detail_entry_index
            current_name = "the current selection"
            if len(current_indices) == 1 and current_index is not None and 0 <= current_index < len(self.entries):
                current_name = f"'{self.entries[current_index].entry_name}'"
            elif current_indices:
                current_name = f"the {len(current_indices)} selected entries"
            choice = self._ask_yes_no_cancel_window(
                "Unsaved changes",
                (
                    f"You have unsaved changes for {current_name}.\n\n"
                    "Yes = apply changes and exit\n"
                    "No = discard changes and exit\n"
                    "Cancel = keep editing"
                ),
                kind="question",
            )
            if choice is None:
                return
            if choice and not self._apply_selected_entry(target_indices=current_indices):
                return
        self._save_settings()
        self.task_runner.cancel()
        self.task_runner.cancel_polling()
        self._close_loading_window()
        self.preview_player.close()
        if self.experimental_frame is not None:
            self.experimental_frame.shutdown()
        if self.other_frame is not None:
            self.other_frame.shutdown()
        self._cancel_preview_progress_updates()
        self._cleanup_edit_session()
        self.destroy()


def main(argv: list[str] | None = None) -> None:
    startup_paths = _collect_startup_csb_paths(list(sys.argv[1:] if argv is None else argv))
    app = DyingAudioApp()
    if startup_paths:
        app._skip_welcome_wizard = True
        app.after(0, lambda path=startup_paths[0]: app._open_csb_for_editing(path))
    app.mainloop()
