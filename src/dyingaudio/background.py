from __future__ import annotations

import queue
import threading
import traceback
import tkinter as tk
from dataclasses import dataclass
from typing import Callable


class TaskCancelled(RuntimeError):
    pass


@dataclass(slots=True)
class TaskProgress:
    message: str = ""
    current: float | None = None
    total: float | None = None

    @property
    def is_determinate(self) -> bool:
        return self.current is not None and self.total is not None and self.total > 0

    @property
    def percent(self) -> float:
        if not self.is_determinate or self.current is None or self.total is None:
            return 0.0
        return min(100.0, max(0.0, (self.current * 100.0) / self.total))


class BackgroundTaskRunner:
    def __init__(self, widget: tk.Misc, poll_ms: int = 60, max_events_per_poll: int = 64) -> None:
        self.widget = widget
        self.poll_ms = poll_ms
        self.max_events_per_poll = max(1, max_events_per_poll)
        self._queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._progress_lock = threading.Lock()
        self._latest_progress: TaskProgress | None = None
        self._progress_queued = False
        self._thread: threading.Thread | None = None
        self._after_id: str | None = None
        self._running = False
        self._cancel_event = threading.Event()

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def cancel_event(self) -> threading.Event:
        return self._cancel_event

    def cancel(self) -> None:
        if self._running:
            self._cancel_event.set()

    def start(
        self,
        worker: Callable[[Callable[[str, float | None, float | None], None], Callable[[str], None]], object],
        *,
        on_progress: Callable[[TaskProgress], None] | None = None,
        on_log: Callable[[str], None] | None = None,
        on_success: Callable[[object], None] | None = None,
        on_error: Callable[[BaseException, str], None] | None = None,
        on_finally: Callable[[], None] | None = None,
    ) -> None:
        if self._running:
            raise RuntimeError("A background task is already running.")

        self._running = True
        self._cancel_event = threading.Event()
        self._queue = queue.Queue()
        with self._progress_lock:
            self._latest_progress = None
            self._progress_queued = False

        def emit_progress(message: str = "", current: float | None = None, total: float | None = None) -> None:
            if self._cancel_event.is_set():
                raise TaskCancelled("Task cancelled by user.")
            # Keep at most one progress notification queued. Workers can emit
            # thousands of ticks while indexing; Tk only needs the newest one.
            with self._progress_lock:
                self._latest_progress = TaskProgress(message=message, current=current, total=total)
                if self._progress_queued:
                    return
                self._progress_queued = True
                self._queue.put(("progress", None))

        def emit_log(message: str) -> None:
            if self._cancel_event.is_set():
                raise TaskCancelled("Task cancelled by user.")
            self._queue.put(("log", message))

        def run() -> None:
            try:
                result = worker(emit_progress, emit_log)
            except TaskCancelled as exc:
                self._queue.put(("error", (exc, traceback.format_exc())))
            except BaseException as exc:
                self._queue.put(("error", (exc, traceback.format_exc())))
            else:
                self._queue.put(("success", result))
            finally:
                self._queue.put(("finally", None))

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

        def invoke_callback(callback: Callable[..., object] | None, *args: object, report_errors: bool = True) -> None:
            if callback is None:
                return
            try:
                callback(*args)
            except BaseException as exc:
                if not report_errors:
                    traceback.print_exc()
                    return
                details = traceback.format_exc()
                if on_error is not None and callback is not on_error:
                    try:
                        on_error(exc, details)
                    except BaseException:
                        traceback.print_exc()
                else:
                    traceback.print_exc()

        def poll() -> None:
            self._after_id = None
            should_continue = self._running
            processed = 0
            pending_progress: TaskProgress | None = None

            def deliver_progress() -> None:
                nonlocal pending_progress
                if pending_progress is None:
                    return
                invoke_callback(on_progress, pending_progress)
                pending_progress = None

            while processed < self.max_events_per_poll:
                try:
                    event, payload = self._queue.get_nowait()
                except queue.Empty:
                    break

                processed += 1

                if event == "progress":
                    # A single queue token represents the latest worker update.
                    # Take it atomically so a new tick can enqueue the next token.
                    with self._progress_lock:
                        pending_progress = self._latest_progress
                        self._latest_progress = None
                        self._progress_queued = False
                elif event == "log" and isinstance(payload, str):
                    deliver_progress()
                    invoke_callback(on_log, payload)
                elif event == "success":
                    deliver_progress()
                    invoke_callback(on_success, payload)
                elif event == "error" and isinstance(payload, tuple):
                    deliver_progress()
                    exc, details = payload
                    if isinstance(exc, BaseException) and isinstance(details, str):
                        invoke_callback(on_error, exc, details, report_errors=False)
                elif event == "finally":
                    deliver_progress()
                    self._running = False
                    should_continue = False
                    invoke_callback(on_finally)

            deliver_progress()

            if should_continue:
                try:
                    # Yield immediately when there is still queued UI work, rather
                    # than draining an unbounded queue in one Tk callback.
                    next_delay = 0 if processed >= self.max_events_per_poll else self.poll_ms
                    self._after_id = self.widget.after(next_delay, poll)
                except tk.TclError:
                    self._after_id = None
                    self._running = False

        poll()

    def cancel_polling(self) -> None:
        if self._after_id is None:
            return
        try:
            self.widget.after_cancel(self._after_id)
        except tk.TclError:
            pass
        self._after_id = None
