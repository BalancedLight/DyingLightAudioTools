from __future__ import annotations

from dataclasses import dataclass
import tkinter as tk
from tkinter import ttk

from dyingaudio.settings import bundled_resource_root, is_windows_dark_mode


@dataclass(frozen=True, slots=True)
class _ButtonSpec:
    label: str
    value: object
    default: bool = False


def _maybe_beep(kind: str) -> None:
    if kind not in {"warning", "error"}:
        return
    try:
        import winsound

        message_type = winsound.MB_ICONEXCLAMATION if kind == "warning" else winsound.MB_ICONHAND
        winsound.MessageBeep(message_type)
    except (ImportError, AttributeError, OSError):
        pass


def _load_icon(window: tk.Toplevel, kind: str) -> tk.PhotoImage | None:
    icon_path = bundled_resource_root() / "assets" / f"{kind}.png"
    if not icon_path.exists():
        return None
    try:
        icon = tk.PhotoImage(master=window, file=str(icon_path))
        max_size = 48
        if icon.width() > max_size or icon.height() > max_size:
            scale = max(icon.width(), icon.height()) / max_size
            subsample = max(1, int(scale))
            icon = icon.subsample(subsample, subsample)
        return icon
    except tk.TclError:
        return None


def _center_window(window: tk.Toplevel, parent: tk.Misc, width: int, height: int) -> None:
    try:
        parent.update_idletasks()
        window.update_idletasks()
        root = parent.winfo_toplevel()
        root_x = root.winfo_rootx()
        root_y = root.winfo_rooty()
        root_width = root.winfo_width()
        root_height = root.winfo_height()
    except tk.TclError:
        return

    x = root_x + max(0, (root_width - width) // 2)
    y = root_y + max(0, (root_height - height) // 2)
    window.geometry(f"{width}x{height}+{x}+{y}")


def _preferred_popup_width(title: str, message: str, prompt: str | None, button_count: int) -> int:
    lines = [title, *message.splitlines()]
    if prompt:
        lines.extend(prompt.splitlines())
    longest_line = max((len(line.strip()) for line in lines if line.strip()), default=0)
    width = 520
    if longest_line >= 90:
        width = 760
    elif longest_line >= 65:
        width = 680
    elif longest_line >= 45:
        width = 600
    if button_count >= 2:
        width = max(width, 580)
    if prompt is not None:
        width = max(width, 600)
    return width


class _ModalPopup:
    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str,
        message: str,
        kind: str,
        buttons: list[_ButtonSpec],
        close_value: object | None,
        prompt: str | None = None,
        initial_value: str = "",
    ) -> None:
        self.parent = parent
        self.kind = kind
        self.close_value = close_value
        self.result: object | None = None
        self.window = tk.Toplevel(parent.winfo_toplevel())
        self.window.withdraw()
        self.window.title(title)
        self.window.transient(parent.winfo_toplevel())
        preferred_width = _preferred_popup_width(title, message, prompt, len(buttons))
        preferred_height = 320 if prompt is None else 380
        self.window.minsize(preferred_width, preferred_height)
        self.window.resizable(True, True)
        self.window.protocol("WM_DELETE_WINDOW", self._close)
        if is_windows_dark_mode():
            self.window.configure(bg="#1e1e1e")

        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(0, weight=1)
        self._preferred_width = preferred_width
        self._preferred_height = preferred_height

        container = ttk.Frame(self.window, padding=18)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=0)
        container.rowconfigure(1, weight=0)
        container.rowconfigure(2, weight=0)
        container.rowconfigure(3, weight=1)
        container.rowconfigure(4, weight=0)

        title_label = ttk.Label(container, text=title, font=("TkDefaultFont", 12, "bold"), anchor="center")
        title_label.grid(row=0, column=0, sticky="ew", pady=(0, 10))

        icon_frame = ttk.Frame(container, width=64, height=64)
        icon_frame.grid(row=1, column=0, sticky="n", pady=(0, 10))
        icon_frame.grid_propagate(False)

        icon_label = ttk.Label(icon_frame, anchor="center")
        icon_label.place(relx=0.5, rely=0.5, anchor="center")
        icon = _load_icon(self.window, kind)
        if icon is not None:
            icon_label.configure(image=icon)
            icon_label.image = icon
        else:
            icon_label.configure(text=kind.upper())
            icon_label.image = None

        self.message_label: ttk.Label | None = None
        if message:
            message_frame = ttk.Frame(container)
            message_frame.grid(row=2, column=0, sticky="ew", pady=(0, 16))
            message_frame.columnconfigure(0, weight=1)
            self.message_label = ttk.Label(
                message_frame,
                text=message,
                anchor="center",
                justify="center",
                wraplength=max(360, preferred_width - 120),
            )
            self.message_label.grid(row=0, column=0, sticky="ew")

        self.entry_var: tk.StringVar | None = None
        self.entry_widget: ttk.Entry | None = None
        if prompt is not None:
            input_frame = ttk.Frame(container)
            input_frame.grid(row=3, column=0, sticky="ew", pady=(0, 16))
            input_frame.columnconfigure(0, weight=1)
            ttk.Label(
                input_frame,
                text=prompt,
                anchor="center",
                justify="center",
                wraplength=max(360, preferred_width - 120),
            ).grid(row=0, column=0, sticky="ew", pady=(0, 8))
            self.entry_var = tk.StringVar(value=initial_value)
            self.entry_widget = ttk.Entry(input_frame, textvariable=self.entry_var)
            self.entry_widget.grid(row=1, column=0, sticky="ew")
            self.entry_widget.bind("<Return>", self._accept_prompt)
            self.entry_widget.bind("<Escape>", self._cancel_prompt)

        buttons_frame = ttk.Frame(container)
        buttons_frame.grid(row=4, column=0, sticky="ew")
        buttons_frame.columnconfigure(0, weight=1)
        self.default_button: ttk.Button | None = None
        button_container = ttk.Frame(buttons_frame)
        button_container.grid(row=0, column=0, sticky="e")
        for index, spec in enumerate(buttons):
            button = ttk.Button(button_container, text=spec.label, command=lambda value=spec.value: self._close(value))
            button.grid(row=0, column=index, padx=(0, 8) if index < len(buttons) - 1 else 0)
            if spec.default:
                self.default_button = button

        self.window.bind("<Return>", self._accept_default)
        self.window.bind("<Escape>", self._cancel_prompt)
        self.window.bind("<Configure>", self._on_configure)

    def show(self) -> object | None:
        _maybe_beep(self.kind)
        width = max(self.window.winfo_reqwidth(), self._preferred_width)
        height = max(self.window.winfo_reqheight(), self._preferred_height)
        _center_window(self.window, self.parent, width, height)
        self.window.deiconify()
        try:
            self.window.grab_set()
        except tk.TclError:
            pass
        if self.entry_widget is not None:
            try:
                self.entry_widget.focus_set()
                self.entry_widget.select_range(0, tk.END)
                self.entry_widget.icursor(tk.END)
            except tk.TclError:
                self.window.focus_set()
        elif self.default_button is not None:
            try:
                self.default_button.focus_set()
            except tk.TclError:
                self.window.focus_set()
        else:
            try:
                self.window.focus_set()
            except tk.TclError:
                pass
        self.window.lift()
        self.window.wait_window()
        return self.result

    def _close(self, value: object | None = None) -> None:
        if not self.window.winfo_exists():
            return
        self.result = value
        self.window.destroy()

    def _accept_default(self, _event: object | None = None) -> str | None:
        if self.entry_widget is not None:
            return self._accept_prompt(_event)
        if self.default_button is not None:
            self.default_button.invoke()
            return "break"
        self._close(self.close_value)
        return "break"

    def _accept_prompt(self, _event: object | None = None) -> str:
        if self.entry_var is not None:
            self.result = self.entry_var.get()
        self._close(self.result)
        return "break"

    def _cancel_prompt(self, _event: object | None = None) -> str:
        self._close(self.close_value)
        return "break"

    def _on_configure(self, event: object) -> None:
        if self.message_label is None or not hasattr(event, "width"):
            return
        width = max(220, int(event.width) - 36)
        self.message_label.configure(wraplength=width)


def show_info_dialog(parent: tk.Misc, title: str, message: str) -> None:
    _ModalPopup(
        parent,
        title=title,
        message=message,
        kind="info",
        buttons=[_ButtonSpec("OK", None, default=True)],
        close_value=None,
    ).show()


def show_warning_dialog(parent: tk.Misc, title: str, message: str) -> None:
    _ModalPopup(
        parent,
        title=title,
        message=message,
        kind="warning",
        buttons=[_ButtonSpec("OK", None, default=True)],
        close_value=None,
    ).show()


def show_error_dialog(parent: tk.Misc, title: str, message: str) -> None:
    _ModalPopup(
        parent,
        title=title,
        message=message,
        kind="error",
        buttons=[_ButtonSpec("OK", None, default=True)],
        close_value=None,
    ).show()


def ask_yes_no_dialog(parent: tk.Misc, title: str, message: str, *, kind: str = "warning") -> bool:
    result = _ModalPopup(
        parent,
        title=title,
        message=message,
        kind=kind,
        buttons=[
            _ButtonSpec("Yes", True, default=True),
            _ButtonSpec("No", False),
        ],
        close_value=False,
    ).show()
    return bool(result)


def ask_yes_no_cancel_dialog(parent: tk.Misc, title: str, message: str, *, kind: str = "warning") -> bool | None:
    result = _ModalPopup(
        parent,
        title=title,
        message=message,
        kind=kind,
        buttons=[
            _ButtonSpec("Yes", True, default=True),
            _ButtonSpec("No", False),
            _ButtonSpec("Cancel", None),
        ],
        close_value=None,
    ).show()
    if result is None:
        return None
    return bool(result)


def ask_string_dialog(parent: tk.Misc, title: str, prompt: str, *, initialvalue: str = "") -> str | None:
    result = _ModalPopup(
        parent,
        title=title,
        message="",
        kind="info",
        buttons=[
            _ButtonSpec("OK", None, default=True),
            _ButtonSpec("Cancel", None),
        ],
        close_value=None,
        prompt=prompt,
        initial_value=initialvalue,
    ).show()
    if result is None:
        return None
    return str(result)
