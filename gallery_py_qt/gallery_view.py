"""Masonry (waterfall) gallery grid.

Each column packs items greedily into the shortest column.  Cell width is
viewport_width / cols; cell height is proportional to the image aspect ratio
so portrait items are tall, landscape items are short, and nothing is cropped.

Dimensions are sourced from:
  1. model._dims (pre-loaded by the dimension scan in main_window)
  2. Loaded QPixmap size (extracted when each thumbnail arrives)
  3. 1:1 square fallback while neither is available yet

A 80 ms coalescing timer batches rapid dimension updates into one re-layout
so initial thumbnail-burst loading doesn't produce O(n²) re-layouts.
"""
from __future__ import annotations

from PySide6.QtCore import (Qt, Signal, QModelIndex, QTimer, QPoint, QUrl,
                             QRect, QPointF, QEvent)
from PySide6.QtGui import (QPalette, QColor, QPainter, QPen, QPixmap,
                            QPolygonF)
from PySide6.QtWidgets import (QAbstractScrollArea, QFrame, QWidget,
                               QToolButton, QHBoxLayout)
from PySide6.QtMultimedia import QMediaPlayer, QVideoSink

from . import config
from .model import PathRole, IsVideoRole, FavRole, LoadedRole


# ---------------------------------------------------------------------------
# In-grid video preview pool  (unchanged from icon-mode version)
# ---------------------------------------------------------------------------

class _VideoPreviewPool(QWidget):
    MAX = 4

    def __init__(self, frame_cb, parent=None):
        super().__init__(parent)
        self._frame_cb = frame_cb
        self._free: list[tuple[QMediaPlayer, QVideoSink]] = []
        self._used: dict[str, tuple[QMediaPlayer, QVideoSink]] = {}
        for _ in range(self.MAX):
            p = QMediaPlayer(self)
            # No audio output is attached: these previews only pull video frames
            # through the sink, so leaving audio unset skips audio-stream
            # decoding entirely (cheaper than decoding into a muted output) and
            # avoids holding open four audio device handles.
            p.setLoops(QMediaPlayer.Loops.Infinite)
            s = QVideoSink(self)
            p.setVideoSink(s)
            self._free.append((p, s))

    def play(self, path: str) -> None:
        if path in self._used:
            return
        if not self._free:
            self.stop(next(iter(self._used)))
        player, sink = self._free.pop()
        sink.videoFrameChanged.connect(
            lambda frame, p=path: self._on_frame(p, frame))
        player.setSource(QUrl.fromLocalFile(path))
        player.play()
        self._used[path] = (player, sink)

    def stop(self, path: str) -> None:
        if path not in self._used:
            return
        player, sink = self._used.pop(path)
        player.stop()
        player.setSource(QUrl())
        try:
            sink.videoFrameChanged.disconnect()
        except RuntimeError:
            pass
        self._free.append((player, sink))

    def stop_all(self) -> None:
        for path in list(self._used):
            self.stop(path)

    def _on_frame(self, path: str, frame) -> None:
        img = frame.toImage()
        if not img.isNull():
            self._frame_cb(path, QPixmap.fromImage(img))


# ---------------------------------------------------------------------------
# Hover overlay  (unchanged from icon-mode version)
# ---------------------------------------------------------------------------

class _Overlay(QWidget):
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
            row = self.row
            if row >= 0:
                signal.emit(row)
            if not stay:
                self.hide()

        b.clicked.connect(_on_click)
        self.layout().addWidget(b)
        return b

    def set_fav(self, is_fav: bool) -> None:
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"color: {config.RED};" if is_fav else f"color: {config.OVERLAY_FG};")


# ---------------------------------------------------------------------------
# Masonry gallery view
# ---------------------------------------------------------------------------

class GalleryView(QAbstractScrollArea):
    openLightbox = Signal(int)
    favToggled   = Signal(int)
    rotateItem   = Signal(int)
    trashItem    = Signal(int)

    # A portrait image taller than this multiple of the column width is capped
    # so pathologically narrow images don't dominate the layout.
    _MAX_RATIO = 3.0

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setContentsMargins(0, 0, 0, 0)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        vp = self.viewport()
        vp.setContentsMargins(0, 0, 0, 0)
        vp.setMouseTracking(True)
        p = vp.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        p.setColor(QPalette.ColorRole.Base,   QColor("#000"))
        vp.setPalette(p)
        vp.setAutoFillBackground(True)

        self._model = None
        self._cols = config.DEFAULT_COLS
        # Masonry layout: (x, y, w, h) per model row, in screen coords before scroll.
        self._cells: list[tuple[int, int, int, int]] = []
        self._total_h = 0
        # Pixmap-derived dims: populated as thumbnails arrive.
        self._pm_dims: dict[str, tuple[int, int]] = {}
        self._cur_row = -1

        self._overlay = _Overlay(vp)
        self._overlay.fav.connect(self.favToggled)
        self._overlay.rotate.connect(self.rotateItem)
        self._overlay.trash.connect(self.trashItem)
        self._overlay.enlarge.connect(self.openLightbox)

        self._hide_timer = QTimer(self)
        self._hide_timer.setSingleShot(True)
        self._hide_timer.setInterval(120)
        self._hide_timer.timeout.connect(self._overlay.hide)

        # Coalesces rapid dimension updates (thumbnail burst) into one re-layout.
        self._layout_timer = QTimer(self)
        self._layout_timer.setSingleShot(True)
        self._layout_timer.setInterval(80)
        self._layout_timer.timeout.connect(self._relayout)

        self._vid_pool = _VideoPreviewPool(self._on_video_frame, self)
        self._pending_frames: dict[str, object] = {}

        self._frame_flush = QTimer(self)
        self._frame_flush.setInterval(50)
        self._frame_flush.timeout.connect(self._flush_frames)

        self._vid_update = QTimer(self)
        self._vid_update.setSingleShot(True)
        self._vid_update.setInterval(300)
        self._vid_update.timeout.connect(self._sync_video_previews)

        self.verticalScrollBar().setSingleStep(config.SCROLL_DEF)
        self.verticalScrollBar().valueChanged.connect(self._on_scroll)

    # -- columns / sizing ------------------------------------------------------
    def set_columns(self, n: int) -> None:
        self._cols = max(config.MIN_COLS, min(config.MAX_COLS, n))
        self._relayout()

    def columns(self) -> int:
        return self._cols

    # -- model wiring ----------------------------------------------------------
    def setModel(self, model) -> None:
        old = self._model
        if old is not None:
            for sig, slot in (
                (old.modelReset,    self._on_model_reset),
                (old.dataChanged,   self._on_data_changed),
                (old.rowsInserted,  self._relayout),
                (old.rowsRemoved,   self._on_model_reset),
                (old.dimsChanged,   self._on_dims_changed),
            ):
                try:
                    sig.disconnect(slot)
                except RuntimeError:
                    pass
        self._model = model
        if model is not None:
            model.modelReset.connect(self._on_model_reset)
            model.dataChanged.connect(self._on_data_changed)
            model.rowsInserted.connect(self._relayout)
            model.rowsRemoved.connect(self._on_model_reset)
            model.dimsChanged.connect(self._on_dims_changed)
        self._on_model_reset()

    def _on_dims_changed(self) -> None:
        """The background dimension scan finished; reflow with true heights."""
        if not self._layout_timer.isActive():
            self._layout_timer.start()

    def model(self):
        return self._model

    def currentIndex(self) -> QModelIndex:
        m = self._model
        if m and 0 <= self._cur_row < m.rowCount():
            return m.index(self._cur_row)
        return QModelIndex()

    def refresh_overlay_fav(self, row: int, is_fav: bool) -> None:
        if self._overlay.row == row and self._overlay.isVisible():
            self._overlay.set_fav(is_fav)

    def _on_model_reset(self) -> None:
        self._overlay.hide()
        self._vid_pool.stop_all()
        self._pending_frames.clear()
        # _pm_dims is NOT cleared — path→aspect-ratio never changes for a given
        # file, so stale entries are always correct and save a re-layout cycle.
        self._cur_row = -1
        # Seed dims from thumbnails already in the model's in-memory pixmap cache
        # so the very first relayout after a modelReset uses the correct heights
        # instead of falling back to square cells.
        m = self._model
        if m is not None:
            for i in range(m.rowCount()):
                idx = m.index(i)
                path = idx.data(PathRole)
                if path and path not in self._pm_dims and idx.data(LoadedRole):
                    pm = idx.data(Qt.ItemDataRole.DecorationRole)
                    if isinstance(pm, QPixmap) and not pm.isNull():
                        self._pm_dims[path] = (pm.width(), pm.height())
        self._relayout()

    def _on_data_changed(self, top: QModelIndex, bottom: QModelIndex,
                         roles=None) -> None:
        if roles and Qt.ItemDataRole.DecorationRole not in roles:
            self.viewport().update()
            return
        # Extract pixel dimensions from newly loaded pixmaps so the masonry
        # layout can use the real aspect ratio without waiting for the full
        # dimension pre-scan.
        m = self._model
        if m is None:
            return
        changed = False
        for row in range(top.row(), bottom.row() + 1):
            idx = m.index(row)
            # Only inspect pixmaps already in the model's cache.  Calling
            # data(DecorationRole) on an unloaded row queues a thumbnail
            # request, and set_thumb_px() re-emits dataChanged for EVERY row —
            # without this guard that would stampede the loader with the whole
            # gallery at once instead of just the visible cells.
            if not idx.data(LoadedRole):
                continue
            pm = idx.data(Qt.ItemDataRole.DecorationRole)
            if isinstance(pm, QPixmap) and not pm.isNull():
                path = idx.data(PathRole)
                if path and path not in self._pm_dims:
                    self._pm_dims[path] = (pm.width(), pm.height())
                    changed = True
        if changed:
            if not self._layout_timer.isActive():
                self._layout_timer.start()
        else:
            self.viewport().update()

    # -- masonry layout --------------------------------------------------------
    def _cell_h(self, path: str, cell_w: int) -> int:
        """Height for one cell, capped at MAX_RATIO * cell_w."""
        m = self._model
        dims = (m._dims.get(path) if m else None) or self._pm_dims.get(path)
        if dims and dims[0] > 0 and dims[1] > 0:
            h = int(cell_w * dims[1] / dims[0])
            return max(60, min(h, int(cell_w * self._MAX_RATIO)))
        return cell_w  # square fallback while dimensions unknown

    def _relayout(self) -> None:
        m = self._model
        vw = self.viewport().width()
        if vw <= 0 or m is None:
            self._cells = []
            self._total_h = 0
            self.verticalScrollBar().setRange(0, 0)
            self.viewport().update()
            return

        cell_w = max(120, vw // self._cols)
        col_h = [0] * self._cols
        cells: list[tuple[int, int, int, int]] = []

        for i in range(m.rowCount()):
            path = m.path_at(i) or ""
            h = self._cell_h(path, cell_w)
            col = min(range(self._cols), key=lambda c: col_h[c])
            cells.append((col * cell_w, col_h[col], cell_w, h))
            col_h[col] += h

        self._cells = cells
        self._total_h = max(col_h) if col_h else 0

        vh = self.viewport().height()
        self.verticalScrollBar().setRange(0, max(0, self._total_h - vh))
        self.verticalScrollBar().setPageStep(vh)

        # Tell the model what thumbnail resolution to request.
        thumb = min(config.MAX_THUMB_PX, (cell_w // 32) * 32)
        if hasattr(m, "set_thumb_px"):
            m.set_thumb_px(max(128, thumb))

        self.viewport().update()

    # -- painting --------------------------------------------------------------
    def viewportEvent(self, event: QEvent) -> bool:
        if event.type() == QEvent.Type.Paint:
            self._paint(QPainter(self.viewport()))
            return True
        return super().viewportEvent(event)

    def _paint(self, painter: QPainter) -> None:
        vp_rect = self.viewport().rect()
        painter.fillRect(vp_rect, QColor("#000"))

        m = self._model
        if m is None or not self._cells:
            return

        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        scroll_y = self.verticalScrollBar().value()
        vp_h = vp_rect.height()

        for i, (cx, cy, cw, ch) in enumerate(self._cells):
            ry = cy - scroll_y
            if ry + ch < 0 or ry > vp_h:
                continue
            rect = QRect(cx, ry, cw, ch)
            idx = m.index(i)

            pm = idx.data(Qt.ItemDataRole.DecorationRole)
            if isinstance(pm, QPixmap) and not pm.isNull():
                # Cell already has the correct aspect ratio; KeepAspectRatio
                # gives a pixel-perfect fit with no letterboxing or cropping.
                scaled = pm.scaled(rect.size(),
                                   Qt.AspectRatioMode.KeepAspectRatio,
                                   Qt.TransformationMode.SmoothTransformation)
                ox = (cw - scaled.width())  // 2
                oy = (ch - scaled.height()) // 2
                painter.drawPixmap(rect.x() + ox, rect.y() + oy, scaled)
            else:
                painter.fillRect(rect, QColor("#000"))
                painter.setPen(QPen(QColor(config.FG_DIM)))
                painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, "…")

            if idx.data(IsVideoRole):
                self._draw_badge(painter, rect)

            if idx.data(FavRole):
                painter.setPen(QPen(QColor(config.RED)))
                f = painter.font()
                f.setPointSize(12)
                f.setBold(True)
                painter.setFont(f)
                painter.drawText(
                    rect.adjusted(0, 4, -6, 0),
                    Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight,
                    config.ICON_HEART_FULL)

            if i == self._cur_row:
                painter.setBrush(Qt.BrushStyle.NoBrush)
                painter.setPen(QPen(QColor(config.ACCENT), 2))
                painter.drawRect(rect)

    def _draw_badge(self, painter: QPainter, rect: QRect) -> None:
        cx = rect.center().x()
        cy = rect.center().y()
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

    # -- hit testing -----------------------------------------------------------
    def _row_at(self, pos: QPoint) -> int:
        scroll_y = self.verticalScrollBar().value()
        py = pos.y() + scroll_y
        px = pos.x()
        for i, (x, y, w, h) in enumerate(self._cells):
            if x <= px < x + w and y <= py < y + h:
                return i
        return -1

    def _rect_for(self, row: int) -> QRect | None:
        if 0 <= row < len(self._cells):
            x, y, w, h = self._cells[row]
            return QRect(x, y - self.verticalScrollBar().value(), w, h)
        return None

    # -- mouse -----------------------------------------------------------------
    def mouseMoveEvent(self, e) -> None:
        self._update_hover(e.position().toPoint())
        super().mouseMoveEvent(e)

    def mouseDoubleClickEvent(self, e) -> None:
        row = self._row_at(e.position().toPoint())
        if row >= 0:
            self._cur_row = row
            self.openLightbox.emit(row)
            self.viewport().update()

    def leaveEvent(self, e) -> None:
        super().leaveEvent(e)
        self._hide_timer.start()

    def _update_hover(self, pos: QPoint) -> None:
        row = self._row_at(pos)
        if row < 0:
            self._hide_timer.start()
            return
        self._hide_timer.stop()
        rect = self._rect_for(row)
        if rect is None:
            return
        m = self._model
        self._overlay.row = row
        is_fav = bool(m.data(m.index(row), FavRole)) if m else False
        self._overlay.set_fav(is_fav)
        self._overlay.adjustSize()
        ow = self._overlay.width()
        ox = rect.x() + (rect.width() - ow) // 2
        oy = rect.y() + 6
        self._overlay.move(max(0, ox), max(0, oy))
        self._overlay.show()
        self._overlay.raise_()

    # -- keyboard --------------------------------------------------------------
    def keyPressEvent(self, e) -> None:
        m = self._model
        if m is None:
            return super().keyPressEvent(e)
        total = m.rowCount()
        row = self._cur_row
        if e.key() in (Qt.Key.Key_Right, Qt.Key.Key_Down):
            if row + 1 < total:
                self._cur_row = row + 1
                self._scroll_to(self._cur_row)
                self.viewport().update()
            e.accept()
        elif e.key() in (Qt.Key.Key_Left, Qt.Key.Key_Up):
            if row > 0:
                self._cur_row = row - 1
                self._scroll_to(self._cur_row)
                self.viewport().update()
            e.accept()
        elif e.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            if 0 <= row < total:
                self.openLightbox.emit(row)
            e.accept()
        else:
            super().keyPressEvent(e)

    def _scroll_to(self, row: int) -> None:
        if 0 <= row < len(self._cells):
            x, y, w, h = self._cells[row]
            sb = self.verticalScrollBar()
            vh = self.viewport().height()
            if y < sb.value():
                sb.setValue(y)
            elif y + h > sb.value() + vh:
                sb.setValue(y + h - vh)

    # -- resize / scroll -------------------------------------------------------
    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        self._relayout()

    def _on_scroll(self, _) -> None:
        self._overlay.hide()
        self._vid_update.start()
        self.viewport().update()

    # -- video previews --------------------------------------------------------
    def _visible_rows(self) -> list[int]:
        if not self._cells:
            return []
        scroll_y = self.verticalScrollBar().value()
        vh = self.viewport().height()
        return [i for i, (x, y, w, h) in enumerate(self._cells)
                if y + h > scroll_y and y < scroll_y + vh]

    def _sync_video_previews(self) -> None:
        m = self._model
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
        m = self._model
        if m is None:
            self._pending_frames.clear()
            return
        for path, pm in self._pending_frames.items():
            if hasattr(m, "update_video_frame"):
                m.update_video_frame(path, pm)
        self._pending_frames.clear()

    def showEvent(self, e) -> None:
        super().showEvent(e)
        # The first _relayout (during setModel) often runs before the viewport
        # has a real width and bails out; relayout again now that we're visible.
        self._relayout()
        self._vid_update.start()
