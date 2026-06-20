"""SeekBar — a click-to-seek time tracker for video objects."""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal, QPoint
from PySide6.QtWidgets import QSlider, QToolTip

from . import config

_RESOLUTION = 1000


def fmt_time(ms: int) -> str:
    s = max(0, int(ms)) // 1000
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


class SeekBar(QSlider):
    seeked = Signal(float)

    def __init__(self, parent=None):
        super().__init__(Qt.Orientation.Horizontal, parent)
        self.setRange(0, _RESOLUTION)
        self.setMouseTracking(True)
        self._duration_ms = 0
        self._dragging = False
        self.setStyleSheet(f"""
            QSlider::groove:horizontal {{
                height: 5px; background: #2a2a2a; border-radius: 2px;
            }}
            QSlider::sub-page:horizontal {{
                height: 5px; background: {config.ACCENT}; border-radius: 2px;
            }}
            QSlider::handle:horizontal {{
                width: 11px; margin: -4px 0; border-radius: 6px;
                background: {config.FG_BRIGHT};
            }}
            QSlider::handle:horizontal:hover {{ background: #ffffff; }}
        """)

    def set_duration(self, ms: int) -> None:
        self._duration_ms = max(0, int(ms))

    def set_position(self, ms: int) -> None:
        if self._dragging or self._duration_ms <= 0:
            return
        frac = min(1.0, max(0.0, ms / self._duration_ms))
        self.blockSignals(True)
        self.setValue(round(frac * _RESOLUTION))
        self.blockSignals(False)

    def _fraction_at(self, x: float) -> float:
        w = max(1, self.width())
        return min(1.0, max(0.0, x / w))

    def _seek_to(self, x: float) -> None:
        frac = self._fraction_at(x)
        self.blockSignals(True)
        self.setValue(round(frac * _RESOLUTION))
        self.blockSignals(False)
        self.seeked.emit(frac)

    def mousePressEvent(self, e) -> None:
        if e.button() == Qt.MouseButton.LeftButton:
            self._dragging = True
            self._seek_to(e.position().x())
            e.accept()
        else:
            super().mousePressEvent(e)

    def mouseMoveEvent(self, e) -> None:
        if self._dragging:
            self._seek_to(e.position().x())
            e.accept()
            return
        if self._duration_ms > 0:
            t = self._fraction_at(e.position().x()) * self._duration_ms
            QToolTip.showText(e.globalPosition().toPoint(), fmt_time(int(t)), self)
        super().mouseMoveEvent(e)

    def mouseReleaseEvent(self, e) -> None:
        if self._dragging and e.button() == Qt.MouseButton.LeftButton:
            self._dragging = False
            e.accept()
        else:
            super().mouseReleaseEvent(e)

    def is_scrubbing(self) -> bool:
        return self._dragging
