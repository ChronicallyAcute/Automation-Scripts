"""Item delegate: paints each gallery cell (thumbnail, video badge, fav heart)."""
from __future__ import annotations

from PySide6.QtCore import QSize, Qt, QPointF
from PySide6.QtGui import QColor, QPainter, QPen, QPolygonF, QPixmap
from PySide6.QtWidgets import QStyledItemDelegate, QStyle

from . import config
from .model import IsVideoRole, FavRole


class CardDelegate(QStyledItemDelegate):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.cell = QSize(320, 320)

    def sizeHint(self, option, index) -> QSize:
        return self.cell

    def paint(self, painter: QPainter, option, index) -> None:
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        rect = option.rect   # full cell \u2014 no padding, no card background

        painter.fillRect(rect, QColor("#000"))   # black base \u2014 hides any sub-pixel seam

        pm = index.data(Qt.ItemDataRole.DecorationRole)
        if isinstance(pm, QPixmap) and not pm.isNull():
            # Crop-to-fill: scale up so the shorter axis fills the cell,
            # then centre-crop the excess along the longer axis.
            scaled = pm.scaled(rect.size(),
                               Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                               Qt.TransformationMode.SmoothTransformation)
            sx = (scaled.width()  - rect.width())  // 2
            sy = (scaled.height() - rect.height()) // 2
            painter.drawPixmap(rect.x(), rect.y(),
                               scaled, sx, sy, rect.width(), rect.height())
        else:
            painter.setPen(QPen(QColor(config.FG_DIM)))
            painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, "\u2026")

        if index.data(IsVideoRole):
            self._draw_play_badge(painter, rect)

        if index.data(FavRole):
            painter.setPen(QPen(QColor(config.RED)))
            f = painter.font(); f.setPointSize(12); f.setBold(True)
            painter.setFont(f)
            painter.drawText(rect.adjusted(0, 4, -6, 0),
                             Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight,
                             config.ICON_HEART_FULL)

        if option.state & QStyle.StateFlag.State_Selected:
            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.setPen(QPen(QColor(config.ACCENT), 2))
            painter.drawRect(rect)
        painter.restore()

    def _draw_play_badge(self, painter: QPainter, rect) -> None:
        cx, cy = rect.center().x(), rect.center().y()
        r = max(14, min(rect.width(), rect.height()) // 10)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(0, 0, 0, 130))
        painter.drawEllipse(QPointF(cx, cy), r, r)
        tri = QPolygonF([
            QPointF(cx - r * 0.35, cy - r * 0.5),
            QPointF(cx - r * 0.35, cy + r * 0.5),
            QPointF(cx + r * 0.55, cy),
        ])
        painter.setBrush(QColor(255, 255, 255, 230))
        painter.drawPolygon(tri)
