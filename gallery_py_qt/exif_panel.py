"""Lightweight file-info / EXIF dialog.

The dialog opens instantly with the cheap rows (name, path, size, date) and
fills dimensions / duration / EXIF in asynchronously.  Probing a video's size
takes a cv2.VideoCapture open under the process-global lock — while loader
threads hold that lock decoding video thumbnails, a synchronous probe made
the popup appear seconds late.
"""
from __future__ import annotations
import datetime
import os

from PySide6.QtCore import Qt, QObject, QRunnable, QThreadPool, Signal
from PySide6.QtWidgets import QDialog, QFormLayout, QLabel

from . import config
from .engine import media


def _fmt_size(nbytes: int) -> str:
    """Human-readable file size (B / KB / MB / GB)."""
    if nbytes < 1024:
        return f"{nbytes} B"
    kb = nbytes / 1024
    if kb < 1024:
        return f"{kb:.0f} KB"
    mb = kb / 1024
    if mb < 1024:
        return f"{mb:.1f} MB"
    return f"{mb / 1024:.2f} GB"


class _MetaSignals(QObject):
    ready = Signal(list)     # [(label, value), ...]


class _MetaJob(QRunnable):
    """Gather dimensions / duration / EXIF off the GUI thread."""

    def __init__(self, path: str, signals: _MetaSignals):
        super().__init__()
        self._path = path
        self._signals = signals

    def run(self) -> None:
        rows: list[tuple[str, str]] = []
        path = self._path
        try:
            w, h = media.peek_size(path)
            if w and h:
                rows.append(("Dimensions", f"{w} × {h}"))
            if media.is_video(path):
                dur = media.peek_duration(path)
                if dur:
                    m, s = divmod(int(round(dur)), 60)
                    rows.append(("Duration", f"{m}:{s:02d}  ({dur:.1f}s)"))
            else:
                rows.extend(self._exif_rows(path))
        except Exception:
            pass
        try:
            self._signals.ready.emit(rows)
        except RuntimeError:
            pass          # dialog already closed and deleted

    @staticmethod
    def _exif_rows(path: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        try:
            from PIL import Image
            from PIL.ExifTags import TAGS
            with Image.open(path) as im:
                raw = getattr(im, "_getexif", lambda: None)()
            if not raw:
                return out
            data = {TAGS.get(k, k): v for k, v in raw.items()}
            for tag in ("DateTimeOriginal", "Make", "Model", "LensModel",
                        "ExposureTime", "FNumber", "ISOSpeedRatings",
                        "FocalLength"):
                if tag in data:
                    out.append((tag, str(data[tag])[:40]))
        except Exception:
            pass
        return out


class InfoDialog(QDialog):
    def __init__(self, path: str, parent=None):
        super().__init__(parent)
        # Auto-destroy on close so repeated info popups don't accumulate as
        # (parented) children of the main window.
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        self.setWindowTitle("File info")
        self.setMinimumWidth(380)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())
        self._form = QFormLayout(self)
        self._form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        self._row("Name", os.path.basename(path))
        self._row("Path", path)
        try:
            st = os.stat(path)
            self._row("Size", _fmt_size(st.st_size))
            self._row("Modified", datetime.datetime.fromtimestamp(
                st.st_mtime).strftime("%Y-%m-%d %H:%M"))
        except OSError:
            pass
        self._pending = QLabel("Reading media details…")
        self._pending.setStyleSheet(f"color: {config.FG_DIM};")
        self._form.addRow(self._pending)

        self._sig = _MetaSignals(self)
        self._sig.ready.connect(self._on_meta)
        QThreadPool.globalInstance().start(_MetaJob(path, self._sig))

    def _row(self, k: str, v: str) -> None:
        lbl = QLabel(str(v))
        # Let the user select and copy any value (path, dimensions, EXIF).
        lbl.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        lbl.setWordWrap(True)
        lbl.setStyleSheet(f"color: {config.FG_BRIGHT};")
        kl = QLabel(k + ":")
        kl.setStyleSheet(f"color: {config.FG_DIM};")
        self._form.addRow(kl, lbl)

    def _on_meta(self, rows: list) -> None:
        self._pending.hide()
        self._form.removeRow(self._pending)
        self._pending = None
        for k, v in rows:
            self._row(k, v)
