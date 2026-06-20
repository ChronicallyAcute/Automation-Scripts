"""Virtualized icon grid with a floating hover-overlay of action buttons."""
from __future__ import annotations

from PySide6.QtCore import Qt, QSize, Signal, QModelIndex, QTimer
from PySide6.QtWidgets import QListView, QWidget, QToolButton, QHBoxLayout

from . import config
from .delegate import CardDelegate
from .model import PathRole, IsVideoRole, FavRole


class _Overlay(QWidget):
    """Floating row of action buttons positioned over the hovered cell."""
    fav     = Signal(int)
    rotate  = Signal(int)
    trash   = Signal(int)
    enlarge = Signal(int)

    def __init__(self, parent: QWidget):
        super().__init__(parent)
        self.row = -1
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setStyleSheet("background: rgba(0,0,0,90); border-radius: 6px;")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(4, 2, 4, 2)
        lay.setSpacing(2)
        self._fav_btn = self._mk(config.ICON_HEART_EMPTY,
                                 lambda: self.fav.emit(self.row))
        self._mk(config.ICON_ENLARGE, lambda: self.enlarge.emit(self.row))
        self._mk(config.ICON_ROTATE_CW, lambda: self.rotate.emit(self.row))
        self._mk(config.ICON_TRASH, lambda: self.trash.emit(self.row))
        self.hide()

    def _mk(self, glyph: str, cb) -> QToolButton:
        b = QToolButton(self)
        b.setObjectName("Overlay")
        b.setText(glyph)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.clicked.connect(cb)
        self.layout().addWidget(b)
        return b

    def set_fav(self, is_fav: bool) -> None:
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"color: {config.RED};" if is_fav else f"color: {config.OVERLAY_FG};")


class GalleryView(QListView):
    openLightbox = Signal(int)
    favToggled   = Signal(int)
    rotateItem   = Signal(int)
    trashItem    = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setViewMode(QListView.ViewMode.IconMode)
        self.setMovement(QListView.Movement.Static)
        self.setResizeMode(QListView.ResizeMode.Adjust)
        self.setFlow(QListView.Flow.LeftToRight)
        self.setWrapping(True)
        self.setUniformItemSizes(True)
        self.setSpacing(config.GAP)
        self.setMouseTracking(True)
        self.setSelectionMode(QListView.SelectionMode.SingleSelection)
        self.setVerticalScrollMode(QListView.ScrollMode.ScrollPerPixel)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # Always-on vertical scrollbar: prevents the relayout oscillation that
        # occurs when the bar appears/disappears as content overflows, which
        # caused viewport-width jitter and blanked all thumbnails every frame.
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        self._last_cell = -1

        self._delegate = CardDelegate(self)
        self.setItemDelegate(self._delegate)
        self._cols = config.DEFAULT_COLS

        self._overlay = _Overlay(self.viewport())
        self._overlay.fav.connect(self.favToggled)
        self._overlay.rotate.connect(self.rotateItem)
        self._overlay.trash.connect(self.trashItem)
        self._overlay.enlarge.connect(self.openLightbox)

        self.entered.connect(self._on_entered)
        self.doubleClicked.connect(lambda idx: self.openLightbox.emit(idx.row()))
        self.clicked.connect(lambda idx: self.openLightbox.emit(idx.row()))
        self.verticalScrollBar().valueChanged.connect(
            lambda _: self._overlay.hide())

        self._hide_timer = QTimer(self)
        self._hide_timer.setSingleShot(True)
        self._hide_timer.setInterval(120)
        self._hide_timer.timeout.connect(self._overlay.hide)

    # ── columns / cell sizing ────────────────────────────────────────────────
    def set_columns(self, n: int) -> None:
        self._cols = max(config.MIN_COLS, min(config.MAX_COLS, n))
        self._last_cell = -1
        self._recompute_cell()

    def columns(self) -> int:
        return self._cols

    def _recompute_cell(self) -> None:
        vw = self.viewport().width()
        if vw <= 0:
            return
        gap = config.GAP
        cell = max(120, (vw - gap * (self._cols + 1)) // self._cols)
        if cell == self._last_cell:
            return
        self._last_cell = cell
        self._delegate.cell = QSize(cell, cell)
        self.setGridSize(QSize(cell + gap, cell + gap))
        m = self.model()
        if m is not None and hasattr(m, "set_thumb_px"):
            thumb = min(config.MAX_THUMB_PX, (cell // 32) * 32)
            m.set_thumb_px(max(128, thumb))
        self.scheduleDelayedItemsLayout()

    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        self._recompute_cell()

    # ── hover overlay ─────────────────────────────────────────────────────────
    def _on_entered(self, index: QModelIndex) -> None:
        if not index.isValid():
            return
        rect = self.visualRect(index)
        self._overlay.row = index.row()
        self._overlay.set_fav(bool(index.data(FavRole)))
        self._overlay.adjustSize()
        ow = self._overlay.width()
        x = rect.x() + (rect.width() - ow) // 2
        y = rect.y() + 6
        self._overlay.move(max(0, x), max(0, y))
        self._overlay.show()
        self._overlay.raise_()
        self._hide_timer.stop()

    def leaveEvent(self, e) -> None:
        super().leaveEvent(e)
        self._hide_timer.start()

    def refresh_overlay_fav(self, row: int, is_fav: bool) -> None:
        if self._overlay.row == row and self._overlay.isVisible():
            self._overlay.set_fav(is_fav)
