from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Mapping


DARK_BACKGROUND = "#1e1e1e"
DARK_PANEL_BACKGROUND = "#252526"
DARK_FIELD_BACKGROUND = "#2d2d30"
DARK_FOREGROUND = "#d4d4d4"
DARK_BORDER = "#3f3f46"
DARK_SELECTED_BACKGROUND = "#0a84ff"
DARK_SELECTED_FOREGROUND = "#ffffff"

HIGH_CONTRAST_BACKGROUND = "#000000"
HIGH_CONTRAST_FOREGROUND = "#ffffff"
HIGH_CONTRAST_SELECTED = "#ffff00"


def dark_palette() -> dict[str, str]:
    return {
        "background": DARK_BACKGROUND,
        "panel_background": DARK_PANEL_BACKGROUND,
        "field_background": DARK_FIELD_BACKGROUND,
        "foreground": DARK_FOREGROUND,
        "border_color": DARK_BORDER,
        "selected_background": DARK_SELECTED_BACKGROUND,
        "selected_foreground": DARK_SELECTED_FOREGROUND,
        "highlight_background": DARK_BORDER,
        "highlight_color": DARK_SELECTED_BACKGROUND,
    }


def high_contrast_palette() -> dict[str, str]:
    return {
        "background": HIGH_CONTRAST_BACKGROUND,
        "panel_background": HIGH_CONTRAST_BACKGROUND,
        "field_background": HIGH_CONTRAST_BACKGROUND,
        "foreground": HIGH_CONTRAST_FOREGROUND,
        "border_color": HIGH_CONTRAST_FOREGROUND,
        "selected_background": HIGH_CONTRAST_SELECTED,
        "selected_foreground": HIGH_CONTRAST_BACKGROUND,
        "highlight_background": HIGH_CONTRAST_FOREGROUND,
        "highlight_color": HIGH_CONTRAST_SELECTED,
    }


def style_scrolled_text(widget: tk.Misc, palette: Mapping[str, str]) -> None:
    try:
        widget.configure(
            background=palette["field_background"],
            foreground=palette["foreground"],
            insertbackground=palette["foreground"],
            selectbackground=palette["selected_background"],
            selectforeground=palette["selected_foreground"],
            highlightbackground=palette["highlight_background"],
            highlightcolor=palette["highlight_color"],
        )
    except tk.TclError:
        pass
    scrollbar = getattr(widget, "vbar", None)
    if scrollbar is None:
        return
    try:
        scrollbar.configure(
            background=palette["panel_background"],
            troughcolor=palette["background"],
            activebackground=palette["selected_background"],
            highlightbackground=palette["highlight_background"],
        )
    except tk.TclError:
        pass


def configure_dark_checkbutton_style(style: ttk.Style) -> None:
    style.configure(
        "TCheckbutton",
        background=DARK_BACKGROUND,
        foreground=DARK_FOREGROUND,
        indicatorcolor=DARK_BACKGROUND,
    )
    style.map(
        "TCheckbutton",
        background=[("active", DARK_BACKGROUND), ("selected", DARK_BACKGROUND), ("focus", DARK_BACKGROUND)],
        foreground=[("active", DARK_FOREGROUND), ("selected", DARK_FOREGROUND), ("focus", DARK_FOREGROUND)],
        indicatorcolor=[("selected", DARK_SELECTED_BACKGROUND), ("!selected", DARK_BACKGROUND)],
    )


def configure_high_contrast_checkbutton_style(style: ttk.Style) -> None:
    style.configure(
        "TCheckbutton",
        background=HIGH_CONTRAST_BACKGROUND,
        foreground=HIGH_CONTRAST_FOREGROUND,
        indicatorcolor=HIGH_CONTRAST_BACKGROUND,
    )
    style.map(
        "TCheckbutton",
        background=[
            ("active", HIGH_CONTRAST_BACKGROUND),
            ("selected", HIGH_CONTRAST_BACKGROUND),
            ("focus", HIGH_CONTRAST_BACKGROUND),
        ],
        foreground=[
            ("active", HIGH_CONTRAST_FOREGROUND),
            ("selected", HIGH_CONTRAST_FOREGROUND),
            ("focus", HIGH_CONTRAST_FOREGROUND),
        ],
        indicatorcolor=[("selected", HIGH_CONTRAST_SELECTED), ("!selected", HIGH_CONTRAST_BACKGROUND)],
    )
