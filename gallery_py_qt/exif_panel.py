"""Lightweight file-info / EXIF dialog."""
from __future__ import annotations
import datetime
import os

from PySide6.QtCore import Qt
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
        form = QFormLayout(self)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        def row(k, v):
            lbl = QLabel(str(v))
            # Let the user select and copy any value (path, dimensions, EXIF).
            lbl.setTextInteractionFlags(
                Qt.TextInteractionFlag.TextSelectableByMouse)
            lbl.setWordWrap(True)
            lbl.setStyleSheet(f"color: {config.FG_BRIGHT};")
            kl = QLabel(k + ":")
            kl.setStyleSheet(f"color: {config.FG_DIM};")
            form.addRow(kl, lbl)

        row("Name", os.path.basename(path))
        row("Path", path)
        try:
            st = os.stat(path)
            row("Size", _fmt_size(st.st_size))
            row("Modified", datetime.datetime.fromtimestamp(
                st.st_mtime).strftime("%Y-%m-%d %H:%M"))
        except OSError:
            pass

        w, h = media.peek_size(path)
        if w and h:
            row("Dimensions", f"{w} \u00d7 {h}")
        if media.is_video(path):
            dur = media.peek_duration(path)
            if dur:
                m, s = divmod(int(round(dur)), 60)
                row("Duration", f"{m}:{s:02d}  ({dur:.1f}s)")
        else:
            self._exif_rows(path, row)

    def _exif_rows(self, path: str, row) -> None:
        try:
            from PIL import Image
            from PIL.ExifTags import TAGS
            with Image.open(path) as im:
                raw = getattr(im, "_getexif", lambda: None)()
            if not raw:
                return
            data = {TAGS.get(k, k): v for k, v in raw.items()}
            for tag in ("DateTimeOriginal", "Make", "Model", "LensModel",
                        "ExposureTime", "FNumber", "ISOSpeedRatings",
                        "FocalLength"):
                if tag in data:
                    row(tag, str(data[tag])[:40])
        except Exception:
            pass
