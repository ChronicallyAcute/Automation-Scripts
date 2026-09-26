"""Stall diagnostics: find out what the app is doing when it hitches.

A frame-gap log tells you playback stuttered; it does not tell you why.  This
watches the GUI thread itself: a timer on the event loop stamps a heartbeat,
and a background thread notices when that heartbeat stops.  When it does, it
captures the MAIN thread's Python stack — so the log names the function that
was blocking, not just the moment it happened.

Everything is written to a file in the user's home directory rather than
stderr, because PowerShell wraps native stderr in NativeCommandError records
and shell redirection then behaves differently from cmd.  One env var, no
redirection, no shell quirks:

    GALLERY_DIAG=1        enable (threshold 120 ms)
    GALLERY_DIAG=250      enable with a custom threshold in ms
"""
from __future__ import annotations
import os
import sys
import threading
import time
import traceback

from . import config

LOG_PATH = os.path.join(config.HOME, ".gallery_py_qt_diag.log")

_last_beat = 0.0
_main_tid: "int | None" = None
_timer = None            # module-level ref: a local QTimer would be collected
_lock = threading.Lock()


def enabled() -> "int | None":
    """Threshold in ms if diagnostics are on, else None."""
    raw = (os.environ.get("GALLERY_DIAG") or "").strip()
    if not raw:
        return None
    if raw == "1":
        return 120
    try:
        return max(30, int(raw))
    except ValueError:
        return 120


def write(text: str) -> None:
    """Append a timestamped line/block to the diagnostic log."""
    stamp = time.strftime("%H:%M:%S")
    try:
        with _lock, open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{stamp}] {text}\n")
    except OSError:
        pass


# The watchdog inspects other threads' frames (sys._current_frames), which is
# only safe while those threads exist.  A daemon thread left running into
# interpreter shutdown can therefore crash the process, and every install()
# used to add another one that never stopped.  Track it so it can be replaced
# and stopped.
_thread: "threading.Thread | None" = None
_stop: "threading.Event | None" = None


def _new_stop_event() -> "threading.Event":
    global _stop
    uninstall()
    _stop = threading.Event()
    return _stop


def uninstall() -> None:
    """Stop the watchdog thread and wait briefly for it to notice."""
    global _thread, _stop
    if _stop is not None:
        _stop.set()
    t, _thread = _thread, None
    if t is not None and t.is_alive():
        t.join(timeout=1.0)


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()


def install(app, threshold_ms: int = 120) -> None:
    """Start the GUI-thread stall watchdog."""
    global _last_beat, _main_tid, _timer
    from PySide6.QtCore import QTimer

    _main_tid = threading.get_ident()
    _last_beat = time.perf_counter()

    _timer = QTimer(app)
    _timer.setInterval(50)

    def _beat() -> None:
        global _last_beat
        _last_beat = time.perf_counter()

    _timer.timeout.connect(_beat)
    _timer.start()

    stop = _new_stop_event()

    def _watch() -> None:
        while not stop.is_set():
            if stop.wait(0.02):
                return
            gap = (time.perf_counter() - _last_beat) * 1000.0
            if gap < threshold_ms:
                continue
            # The event loop is not running.  Grab the main thread's stack —
            # this is the whole point: it names what is blocking.
            frame = sys._current_frames().get(_main_tid)
            stack = ("".join(traceback.format_stack(frame))
                     if frame is not None else "  (stack unavailable)\n")
            write(f"GUI THREAD BLOCKED ~{gap:.0f}ms while running:\n{stack}")
            # Wait for recovery so one long stall isn't reported repeatedly.
            while (not stop.is_set()
                   and (time.perf_counter() - _last_beat) * 1000.0
                   >= threshold_ms):
                stop.wait(0.02)

    global _thread
    _thread = threading.Thread(target=_watch, name="stall-watchdog",
                               daemon=True)
    _thread.start()
    # The watchdog reads other threads' stacks, which is not a safe thing to be
    # doing while the interpreter tears those threads down — so it must stop
    # before shutdown, not merely be a daemon.
    try:
        app.aboutToQuit.connect(uninstall)
    except Exception:
        pass
    try:
        open(LOG_PATH, "a", encoding="utf-8").close()
    except OSError:
        pass
    write(f"=== diagnostics started (threshold {threshold_ms}ms) ===")
