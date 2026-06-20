"""Virtualized icon grid with a floating hover-overlay of action buttons.

Changes vs original:
  - Single-click no longer opens the lightbox (double-click only).  Single-click
    was the root cause of accidental trash/fav collisions because the lightbox
    opened underneath every overlay action.
  - Overlay hides immediately after any action button is clicked, so the user
    sees the result (rotation, fav heart, etc.) straight away.
  - Video items that scroll into the viewport auto-play as looping muted previews;
    they pause when they leave the viewport.  A pool of up to 4 QMediaPlayers
    is shared across all visible video cells, evicting the least-recently-visible
    player when the pool is exhausted.
  - Arrow-key navigation: Left/Right move between items; Enter opens lightbox.
"""
from __future__ import annotations

from PySide6.QtCore import Qt, QSize, Signal, QModelIndex, QTimer, QPoint, QUrl
from PySide6.QtWidgets import QListView, QWidget, QToolButton, QHBoxLayout
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput, QVideoSink

from . import config
from .delegate import CardDelegate
from .model import PathRole, IsVideoRole, FavRole


# ---------------------------------------------------------------------------
# In-grid video preview pool
# ---------------------------------------------------------------------------

class _VideoPreviewPool(QWidget):
    """Pool of muted QMediaPlayers for grid-level video previews.

    frame_cb(path, QPixmap) is called on the GUI thread when a new frame
    arrives for a playing video.  Calls are throttled to ~20 fps by the
    GalleryView's flush timer, not here.
    """

    MAX = 4

    def __init__(self, frame_cb, parent=None):
        super().__init__(parent)
        self._frame_cb = frame_cb
        self._free: list[tuple[QMediaPlayer, QAudioOutput, QVideoSink]] = []
        self._used: dict[str, tuple[QMediaPlayer, QAudioOutput, QVideoSink]] = {}
        for _ in range(self.MAX):
            p = QMediaPlayer(self)
            a = QAudioOutput(self)
            a.setMuted(True)
            p.setAudioOutput(a)
            p.setLoops(QMediaPlayer.Loops.Infinite)
            s = QVideoSink(self)
            p.setVideoSink(s)
            self._free.append((p, a, s))

    def play(self, path: str) -> None:
        if path in self._used:
            return
        if not self._free:
            oldest = next(iter(self._used))
            self.stop(oldest)
        player, audio, sink = self._free.pop()
        sink.videoFrameChanged.connect(
            lambda frame, p=path: self._on_frame(p, frame))
        player.setSource(QUrl.fromLocalFile(path))
        player.play()
        self._used[path] = (player, audio, sink)

    def stop(self, path: str) -> None:
        if path not in self._used:
            return
        player, audio, sink = self._used.pop(path)
        player.stop()
        player.setSource(QUrl())
        try:
            sink.videoFrameChanged.disconnect()
        except RuntimeError:
            pass
        self._free.append((player, audio, sink))

    def stop_all(self) -> None:
        for path in list(self._used):
            self.stop(path)

    def _on_frame(self, path: str, frame) -> None:
        from PySide6.QtGui import QPixmap
        img = frame.toImage()
        if not img.isNull():
            self._frame_cb(path, QPixmap.fromImage(img))


# ---------------------------------------------------------------------------
# Hover overlay
# ---------------------------------------------------------------------------

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
        # fav stays visible after click so the updated heart icon is shown
        self._fav_btn = self._mk(config.ICON_HEART_EMPTY, self.fav, stay=True)
        self._mk(config.ICON_ENLARGE, self.enlarge)
        self._mk(config.ICON_ROTATE_CW, self.rotate)
        self._mk(config.ICON_TRASH, self.trash)
        self.hide()

    def _mk(self, glyph: str, signal: Signal, stay: bool = False) -> QToolButton:
        b = QToolButton(self)
        b.setObjectName("Overlay")
        b.setText(glyph)
        b.setCursor(Qt.CursorShape.PointingHandCursor)

        def _on_click():
            row = self.row          # snapshot row at click time
            if row >= 0:
                signal.emit(row)
            if not stay:
                self.hide()         # reveal result immediately (rotate, trash…)

        b.clicked.connect(_on_click)
        self.layout().addWidget(b)
        return b

    def set_fav(self, is_fav: bool) -> None:
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"color: {config.RED};" if is_fav else f"color: {config.OVERLAY_FG};")


# ---------------------------------------------------------------------------
# Main view
# ---------------------------------------------------------------------------

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
        self.setSpacing(0)
        self.setMouseTracking(True)
        self.setSelectionMode(QListView.SelectionMode.SingleSelection)
        self.setVerticalScrollMode(QListView.ScrollMode.ScrollPerPixel)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
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

        # Double-click opens lightbox; single-click no longer opens it
        # (single-click was the cause of accidental lightbox-then-trash scenarios).
        self.entered.connect(self._on_entered)
        self.doubleClicked.connect(lambda idx: self.openLightbox.emit(idx.row()))
        self.verticalScrollBar().valueChanged.connect(self._on_scroll)

        self._hide_timer = QTimer(self)
        self._hide_timer.setSingleShot(True)
        self._hide_timer.setInterval(120)
        self._hide_timer.timeout.connect(self._overlay.hide)

        # Video preview -------------------------------------------------
        self._vid_pool = _VideoPreviewPool(self._on_video_frame, self)
        self._pending_frames: dict[str, object] = {}   # path -> QPixmap

        self._frame_flush = QTimer(self)
        self._frame_flush.setInterval(50)              # 20 fps cap
        self._frame_flush.timeout.connect(self._flush_frames)

        self._vid_update = QTimer(self)
        self._vid_update.setSingleShot(True)
        self._vid_update.setInterval(300)              # debounce scroll
        self._vid_update.timeout.connect(self._sync_video_previews)

    # -- columns / cell sizing ------------------------------------------------
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
        cell = max(120, vw // self._cols)
        if cell == self._last_cell:
            return
        self._last_cell = cell
        self._delegate.cell = QSize(cell, cell)
        self.setGridSize(QSize(cell, cell))
        m = self.model()
        if m is not None and hasattr(m, "set_thumb_px"):
            thumb = min(config.MAX_THUMB_PX, (cell // 32) * 32)
            m.set_thumb_px(max(128, thumb))
        self.scheduleDelayedItemsLayout()

    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        self._recompute_cell()

    # -- model wiring ---------------------------------------------------------
    def setModel(self, model) -> None:
        old = self.model()
        if old is not None:
            try:
                old.modelReset.disconnect(self._on_model_reset)
            except RuntimeError:
                pass
        super().setModel(model)
        if model is not None:
            model.modelReset.connect(self._on_model_reset)

    def _on_model_reset(self) -> None:
        self._overlay.hide()
        self._vid_pool.stop_all()
        self._pending_frames.clear()

    # -- hover overlay --------------------------------------------------------
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

    # -- keyboard navigation --------------------------------------------------
    def keyPressEvent(self, e) -> None:
        m = self.model()
        if m is None:
            return super().keyPressEvent(e)
        row = self.currentIndex().row()
        total = m.rowCount()
        if e.key() in (Qt.Key.Key_Right, Qt.Key.Key_Down):
            if row + 1 < total:
                self.setCurrentIndex(m.index(row + 1))
            e.accept()
        elif e.key() in (Qt.Key.Key_Left, Qt.Key.Key_Up):
            if row > 0:
                self.setCurrentIndex(m.index(row - 1))
            e.accept()
        elif e.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            if 0 <= row < total:
                self.openLightbox.emit(row)
            e.accept()
        else:
            super().keyPressEvent(e)

    # -- video previews -------------------------------------------------------
    def _on_scroll(self, _) -> None:
        self._overlay.hide()
        self._vid_update.start()     # debounce: sync after scroll settles

    def _visible_rows(self) -> list[int]:
        m = self.model()
        if m is None or m.rowCount() == 0:
            return []
        vp = self.viewport().rect()
        first = self.indexAt(QPoint(0, 0))
        start = first.row() if first.isValid() else 0
        rows = []
        for row in range(start, m.rowCount()):
            vr = self.visualRect(m.index(row))
            if vr.top() > vp.bottom():
                break
            if vp.intersects(vr):
                rows.append(row)
        return rows

    def _sync_video_previews(self) -> None:
        m = self.model()
        if m is None:
            return
        want: set[str] = set()
        for row in self._visible_rows():
            idx = m.index(row)
            if m.data(idx, IsVideoRole):
                path = m.data(idx, PathRole)
                if path:
                    want.add(path)
        for path in list(self._vid_pool._used):
            if path not in want:
                self._vid_pool.stop(path)
        for path in want:
            self._vid_pool.play(path)
        if want:
            self._frame_flush.start()
        else:
            self._frame_flush.stop()

    def _on_video_frame(self, path: str, pm) -> None:
        self._pending_frames[path] = pm

    def _flush_frames(self) -> None:
        if not self._pending_frames:
            return
        m = self.model()
        if m is None:
            self._pending_frames.clear()
            return
        for path, pm in self._pending_frames.items():
            if hasattr(m, "update_video_frame"):
                m.update_video_frame(path, pm)
        self._pending_frames.clear()

    def showEvent(self, e) -> None:
        super().showEvent(e)
        self._vid_update.start()
