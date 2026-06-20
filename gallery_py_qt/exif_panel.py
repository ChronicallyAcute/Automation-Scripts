"""Lightweight file-info / EXIF dialog."""
from __future__ import annotations
import datetime
import os

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDialog, QFormLayout, QLabel

from . import config
from .engine import media


class InfoDialog(QDialog):
    def __init__(self, path: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("File info")
        self.setMinimumWidth(360)
        form = QFormLayout(self)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        def row(k, v):
            lbl = QLabel(str(v))
            lbl.setTextInteractionByMouse = None
            lbl.setStyleSheet(f"color: {config.FG_BRIGHT};")
            kl = QLabel(k + ":")
            kl.setStyleSheet(f"color: {config.FG_DIM};")
            form.addRow(kl, lbl)

        row("Name", os.path.basename(path))
        try:
            st = os.stat(path)
            kb = st.st_size / 1024
            row("Size", f"{kb:.0f} KB" if kb < 1024 else f"{kb/1024:.1f} MB")
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
                row("Duration", f"{dur:.1f}s")
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
