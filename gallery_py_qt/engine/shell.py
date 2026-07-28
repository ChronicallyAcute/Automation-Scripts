"""Hand a path to the desktop environment: reveal it, or open it.

Qt's QDesktopServices can open a file or a folder, but it cannot ask a file
manager to open a folder with one entry *selected* — the behaviour every OS
offers natively and the one that actually answers "where did this come from?".
So each platform gets its native reveal command, with a plain "open the
containing folder" fallback whenever that command is missing or fails.

Kept free of Qt widget imports so it can be unit-tested headless.
"""
from __future__ import annotations
import os
import shutil
import subprocess
import sys


def _run(cmd: "list[str]") -> bool:
    try:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False


def open_path(path: str) -> bool:
    """Open `path` with the desktop's default handler."""
    if not path:
        return False
    try:
        from PySide6.QtCore import QUrl
        from PySide6.QtGui import QDesktopServices
        return bool(QDesktopServices.openUrl(QUrl.fromLocalFile(path)))
    except Exception:
        return False


def reveal_path(path: str) -> bool:
    """Show `path` in the OS file manager, selected within its folder.

    Falls back to opening the containing folder when no native reveal command
    is available.  Returns True if something was launched.
    """
    if not path:
        return False
    path = os.path.abspath(path)
    folder = path if os.path.isdir(path) else os.path.dirname(path)

    if sys.platform.startswith("win"):
        # explorer /select, <path> — returns non-zero even on success, so this
        # is fire-and-forget by design.
        if _run(["explorer", f"/select,{path}"]):
            return True
    elif sys.platform == "darwin":
        if _run(["open", "-R", path]):
            return True
    else:
        # Freedesktop: the file manager D-Bus interface is the portable way to
        # select an item; fall back to whatever can open the folder.
        if shutil.which("dbus-send") and _run([
                "dbus-send", "--session", "--print-reply",
                "--dest=org.freedesktop.FileManager1",
                "--type=method_call", "/org/freedesktop/FileManager1",
                "org.freedesktop.FileManager1.ShowItems",
                f"array:string:file://{path}", "string:"]):
            return True
        for tool in ("nautilus", "dolphin", "thunar", "nemo"):
            if shutil.which(tool) and _run([tool, path]):
                return True

    return open_path(folder)
