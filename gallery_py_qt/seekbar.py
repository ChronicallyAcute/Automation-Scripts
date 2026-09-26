"""SeekBar \u2014 a click-to-seek time tracker for video objects.

Left-click / drag seeks.  Right-click sets an A\u2013B loop: the first right-click
drops the loop-in point (A), the second the loop-out point (B) \u2014 after which
the owning player replays only the A\u2013B span \u2014 and a third right-click clears it.
The selected span is painted as a translucent band over the groove.
"""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal, QPoint
from PySide6.QtGui import QColor, QPainter
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
    # (a, b) as fractions 0..1; a value < 0 means that point is unset.  Both < 0
    # means the loop was cleared.
    loopChanged = Signal(float, float)

    def __init__(self, parent=None):
        super().__init__(Qt.Orientation.Horizontal, parent)
        self.setRange(0, _RESOLUTION)
        self.setMouseTracking(True)
        self._duration_ms = 0
        self._dragging = False
        # A–B loop points as fractions (None = unset).
        self._loop_a: "float | None" = None
        self._loop_b: "float | None" = None
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
        elif e.button() == Qt.MouseButton.RightButton:
            self._cycle_loop(self._fraction_at(e.position().x()))
            e.accept()
        else:
            super().mousePressEvent(e)

    # -- A–B loop --------------------------------------------------------------
    def _cycle_loop(self, frac: float) -> None:
        """Right-click state machine: set A → set B → clear."""
        if self._loop_a is None:
            self._loop_a, self._loop_b = frac, None
        elif self._loop_b is None:
            a, b = self._loop_a, frac
            if b < a:                       # dropped B before A → swap
                a, b = b, a
            self._loop_a, self._loop_b = a, b
        else:
            self._loop_a = self._loop_b = None
        self.update()
        self._emit_loop()

    def _emit_loop(self) -> None:
        self.loopChanged.emit(
            self._loop_a if self._loop_a is not None else -1.0,
            self._loop_b if self._loop_b is not None else -1.0)

    def loop_points(self) -> "tuple[float, float] | None":
        """The active (a, b) loop as fractions, or None if not fully set."""
        if self._loop_a is not None and self._loop_b is not None:
            return self._loop_a, self._loop_b
        return None

    def clear_loop(self) -> None:
        """Drop any A–B loop (e.g. when the media changes).  Silent by default."""
        had = self._loop_a is not None or self._loop_b is not None
        self._loop_a = self._loop_b = None
        if had:
            self.update()

    def paintEvent(self, e) -> None:
        super().paintEvent(e)
        if self._loop_a is None:
            return
        w = max(1, self.width())
        h = self.height()
        p = QPainter(self)
        accent = QColor(config.ACCENT)
        xa = int(self._loop_a * w)
        if self._loop_b is not None:
            xb = int(self._loop_b * w)
            band = QColor(accent)
            band.setAlpha(70)
            p.fillRect(xa, 0, max(1, xb - xa), h, band)
            p.fillRect(xb - 1, 0, 2, h, accent)     # B marker
        p.fillRect(xa, 0, 2, h, accent)             # A marker
        p.end()

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
