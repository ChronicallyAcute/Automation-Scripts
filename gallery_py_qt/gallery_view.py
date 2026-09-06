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
import bisect

from PySide6.QtCore import (Qt, Signal, QModelIndex, QTimer, QPoint, QUrl,
                             QRect, QPointF, QEvent)
from PySide6.QtGui import (QPalette, QColor, QPainter, QPen, QPixmap,
                            QPolygonF)
from PySide6.QtWidgets import (QAbstractScrollArea, QFrame, QWidget,
                               QToolButton, QHBoxLayout)
from PySide6.QtMultimedia import QMediaPlayer, QVideoSink

from . import config
from .model import PathRole, IsVideoRole, FavRole, LoadedRole, FailedRole


# ---------------------------------------------------------------------------
# In-grid video preview pool  (unchanged from icon-mode version)
# ---------------------------------------------------------------------------

class _VideoPreviewPool(QWidget):
    MAX = 4

    def __init__(self, frame_cb, parent=None):
        super().__init__(parent)
        self._frame_cb = frame_cb
        # Preview frames are downscaled to this long side before caching.
        # Updated from the view's relayout to track the current thumb size.
        self._max_px = 640
        self._free: list[tuple[QMediaPlayer, QVideoSink]] = []
        self._used: dict[str, tuple[QMediaPlayer, QVideoSink]] = {}
        # Players are created lazily up to MAX on first use: constructing them
        # eagerly opened media pipelines for every gallery even when no video
        # was ever previewed (a real cost, and headless it can stall).
        self._created = 0

    def _acquire(self) -> "tuple[QMediaPlayer, QVideoSink]":
        if self._free:
            return self._free.pop()
        if self._created < self.MAX:
            p = QMediaPlayer(self)
            # No audio output: previews only pull video frames through the sink,
            # skipping audio-stream decoding and avoiding open audio handles.
            p.setLoops(QMediaPlayer.Loops.Infinite)
            p.errorOccurred.connect(
                lambda err, msg="", pl=p: self._on_error(pl, err))
            s = QVideoSink(self)
            p.setVideoSink(s)
            self._created += 1
            return (p, s)
        # Pool exhausted — recycle the oldest in-use player.
        self.stop(next(iter(self._used)))
        return self._free.pop()

    def play(self, path: str) -> None:
        if path in self._used:
            return
        from .engine import media
        if media.is_bad_video(path):        # known-unplayable — don't churn
            return
        player, sink = self._acquire()
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

    def _on_error(self, player, error) -> None:
        """Release a preview that errored; only blacklist one that never played.

        A file that already reported a duration is decodable, so a later error
        is a transient glitch — remembering it as unplayable would also kill its
        thumbnail and dimensions, long after the hover ended.
        """
        if error == QMediaPlayer.Error.NoError:
            return
        from .engine import media
        for path, (pl, _sink) in list(self._used.items()):
            if pl is player:
                if pl.duration() <= 0:      # never got going: genuinely bad
                    media._mark_bad_video(path)
                self.stop(path)
                break

    def scrub(self, path: str, frac: float) -> bool:
        """Seek the preview for `path` to frac (0..1) of its duration and pause
        there, so hovering across the thumbnail scrubs the timeline.  Returns
        False if the video isn't previewing yet or its duration is unknown."""
        pair = self._used.get(path)
        if pair is None:
            return False
        player, _sink = pair
        dur = player.duration()
        if dur <= 0:
            return False                      # not loaded yet — caller retries
        if player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            player.pause()
        player.setPosition(int(max(0.0, min(1.0, frac)) * dur))
        return True

    def resume(self, path: str) -> None:
        """Resume looping playback of a previously-scrubbed preview."""
        pair = self._used.get(path)
        if pair is not None:
            pair[0].play()

    def _on_frame(self, path: str, frame) -> None:
        img = frame.toImage()
        if img.isNull():
            return
        # Downscale before caching: a raw 4K frame is ~33 MB, and the model's
        # pixmap LRU caps by item count assuming thumb-sized entries — full
        # frames silently blew the byte budget on video folders.  Fast
        # transform: these are ephemeral preview frames, not stills.
        if max(img.width(), img.height()) > self._max_px:
            img = img.scaled(self._max_px, self._max_px,
                             Qt.AspectRatioMode.KeepAspectRatio,
                             Qt.TransformationMode.FastTransformation)
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
    favBatch     = Signal(list)   # rows — toggle favourite on a multi-selection
    trashBatch   = Signal(list)   # rows — trash a multi-selection
    selectionChanged = Signal(int)   # current selection count
    columnsZoom  = Signal(int)    # +1 = more columns (smaller), -1 = fewer
    contextMenu  = Signal(object)    # global QPoint — right-click on the grid

    # A portrait image taller than this multiple of the column width is capped
    # so pathologically narrow images don't dominate the layout.
    _MAX_RATIO = 3.0

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setContentsMargins(0, 0, 0, 0)
        # Accept keyboard focus so multi-select keys (Ctrl+A, F, Delete, Esc)
        # reach keyPressEvent after the user clicks into the grid.
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        vp = self.viewport()
        vp.setContentsMargins(0, 0, 0, 0)
        vp.setMouseTracking(True)
        vp.setAutoFillBackground(True)
        self._apply_canvas_palette()

        self._model = None
        self._cols = config.DEFAULT_COLS
        # Masonry layout: (x, y, w, h) per model row, in screen coords before scroll.
        self._cells: list[tuple[int, int, int, int]] = []
        self._cell_w = 0
        self._col_ystart: list[list[int]] = []      # per-column cell y-starts
        self._col_data: list[list[tuple[int, int]]] = []  # per-column (y_end, row)
        self._total_h = 0
        # Centred hint shown when the grid is empty (no folder / scanning / none found).
        self._empty_primary = "Open a folder to begin"
        self._empty_secondary = "Click ➕ Open in the bar, or press Ctrl+O"
        # Pixmap-derived dims: populated as thumbnails arrive.
        self._pm_dims: dict[str, tuple[int, int]] = {}
        self._cur_row = -1
        # Multi-selection: set of selected rows + a shift-range anchor.
        self._selection: set[int] = set()
        self._anchor = -1

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
        self._layout_timer.timeout.connect(self._on_layout_timer)
        # When True the next layout pass must rebuild everything (dims or cell
        # width changed); when False and rows were merely appended, the pass
        # extends the existing masonry instead of recomputing all n cells.
        self._full_relayout_needed = False
        self._col_h: list[int] = []

        self._vid_pool = _VideoPreviewPool(self._on_video_frame, self)
        self._pending_frames: dict[str, object] = {}
        self._previews_suspended = False
        self._scrub_path = None            # video currently being hover-scrubbed

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

    def _apply_canvas_palette(self) -> None:
        vp = self.viewport()
        p = vp.palette()
        p.setColor(QPalette.ColorRole.Window, QColor(config.CANVAS))
        p.setColor(QPalette.ColorRole.Base,   QColor(config.CANVAS))
        vp.setPalette(p)

    def apply_theme(self) -> None:
        """Re-read themed canvas colours and repaint (live theme switch)."""
        self._apply_canvas_palette()
        self.viewport().update()

    def release_video(self, path: str) -> None:
        """Release the in-grid preview player's handle on `path` (no-op if
        the path isn't previewing) so the file can be moved to trash."""
        self._vid_pool.stop(path)
        self._pending_frames.pop(path, None)

    def suspend_video_previews(self) -> None:
        """Stop all in-grid preview playback and frame processing.

        Called when another panel (multi-view, lightbox) covers the gallery:
        the hidden previews kept decoding AND converting every frame on the
        GUI thread, which starved input handling — 4 visible multi-view
        videos plus 4 invisible previews made every click lag.

        The explicit flag matters: spurious Show events (native-window
        creation elsewhere, stack switches) hit showEvent and would silently
        restart the timer otherwise.
        """
        self._previews_suspended = True
        self._scrub_path = None
        self._vid_update.stop()
        self._frame_flush.stop()
        self._vid_pool.stop_all()
        self._pending_frames.clear()

    def resume_video_previews(self) -> None:
        """Restart previews for the currently visible cells (debounced)."""
        self._previews_suspended = False
        self._vid_update.start()

    def forget_path_dims(self, path: str) -> None:
        """Drop a cached pixmap-derived aspect ratio (e.g. after a rotation)
        so the masonry layout re-measures the cell from fresh data."""
        if self._pm_dims.pop(path, None) is not None:
            self._full_relayout_needed = True
            if not self._layout_timer.isActive():
                self._layout_timer.start()

    def set_empty_hint(self, primary: str, secondary: str = "") -> None:
        """Set the message shown in the centre of an empty gallery."""
        self._empty_primary = primary
        self._empty_secondary = secondary
        if not self._cells:
            self.viewport().update()

    # -- model wiring ----------------------------------------------------------
    def setModel(self, model) -> None:
        old = self._model
        if old is not None:
            for sig, slot in (
                (old.modelReset,    self._on_model_reset),
                (old.dataChanged,   self._on_data_changed),
                (old.rowsInserted,  self._on_rows_inserted),
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
            model.rowsInserted.connect(self._on_rows_inserted)
            model.rowsRemoved.connect(self._on_model_reset)
            model.dimsChanged.connect(self._on_dims_changed)
        self._on_model_reset()

    def _on_rows_inserted(self, *args) -> None:
        """Coalesce a streaming scan's per-batch insertions into one layout
        pass every ~80 ms; appends extend the masonry incrementally."""
        if not self._layout_timer.isActive():
            self._layout_timer.start()

    def _on_dims_changed(self) -> None:
        """The background dimension scan finished; reflow with true heights."""
        self._full_relayout_needed = True
        if not self._layout_timer.isActive():
            self._layout_timer.start()

    def _on_layout_timer(self) -> None:
        """Pick incremental append vs full rebuild when the coalesce timer
        fires.  Appends (streaming scan batches) continue from the existing
        column heights — O(new) — so a 100k-file scan lays out O(n) total
        instead of O(n²) across its batches."""
        if not self.isVisible():
            self._full_relayout_needed = True   # settle on next showEvent
            return
        m = self._model
        if (not self._full_relayout_needed
                and m is not None
                and self._cells
                and len(self._col_ystart) == self._cols
                and m.rowCount() > len(self._cells)
                and max(120, self.viewport().width() // self._cols) == self._cell_w):
            self._append_cells(m.rowCount())
        else:
            self._relayout()

    def _append_cells(self, up_to: int) -> None:
        """Extend the masonry with rows [len(_cells), up_to) in place."""
        m = self._model
        cell_w = self._cell_w
        col_h = self._col_h
        for i in range(len(self._cells), up_to):
            path = m.path_at(i) or ""
            h = self._cell_h(path, cell_w)
            col = min(range(self._cols), key=col_h.__getitem__)
            y = col_h[col]
            self._cells.append((col * cell_w, y, cell_w, h))
            self._col_ystart[col].append(y)
            self._col_data[col].append((y + h, i))
            col_h[col] += h
        self._total_h = max(col_h) if col_h else 0
        vh = self.viewport().height()
        self.verticalScrollBar().setRange(0, max(0, self._total_h - vh))
        self.verticalScrollBar().setPageStep(vh)
        self.viewport().update()

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
        # Row indices are invalidated by a reset (folder/filter/sort change).
        if self._selection or self._anchor >= 0:
            self._selection.clear()
            self._anchor = -1
            self.selectionChanged.emit(0)
        # Seed dims from thumbnails already in the model's in-memory pixmap
        # cache so the first relayout after a modelReset uses correct heights.
        # Iterate the cache directly (bounded by the pixmap LRU cap, ~hundreds)
        # rather than every model row — the per-row data() loop was O(n) of
        # Python/Qt calls on EVERY reset, a real stall at 100k files now that
        # dims chunks each trigger a reset.
        m = self._model
        if m is not None:
            for path, pm in list(getattr(m, "_pixmaps", {}).items()):
                if path not in self._pm_dims and not pm.isNull():
                    self._pm_dims[path] = (pm.width(), pm.height())
        # While another panel covers the gallery, a full relayout per model
        # reset (one per dims batch) is pure GUI-thread waste — defer it to
        # showEvent, which relayouts unconditionally.
        if self.isVisible():
            self._relayout()
        else:
            self._full_relayout_needed = True

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
        # A span covering (nearly) the whole model is a broadcast invalidation
        # — set_thumb_px() just cleared the pixmap cache, so LoadedRole is
        # False everywhere and the per-row probe below would be a pure-waste
        # O(n) loop on the GUI thread.  Repaint and let visible cells reload.
        if bottom.row() - top.row() > 500:
            self.viewport().update()
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
            self._full_relayout_needed = True   # existing cell heights change
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
            self._cell_w = 0
            self._col_ystart = []
            self._col_data = []
            self._total_h = 0
            self.verticalScrollBar().setRange(0, 0)
            self.viewport().update()
            return

        cell_w = max(120, vw // self._cols)
        col_h = [0] * self._cols
        cells: list[tuple[int, int, int, int]] = []
        # Per-column y-start / (y-end, row) indexes so hit-testing is O(log n)
        # per mouse move instead of an O(n) scan over every cell.
        col_ystart: list[list[int]] = [[] for _ in range(self._cols)]
        col_data:   list[list[tuple[int, int]]] = [[] for _ in range(self._cols)]

        for i in range(m.rowCount()):
            path = m.path_at(i) or ""
            h = self._cell_h(path, cell_w)
            col = min(range(self._cols), key=lambda c: col_h[c])
            y = col_h[col]
            cells.append((col * cell_w, y, cell_w, h))
            col_ystart[col].append(y)
            col_data[col].append((y + h, i))
            col_h[col] += h

        self._cells = cells
        self._cell_w = cell_w
        self._col_ystart = col_ystart
        self._col_data = col_data
        self._col_h = col_h
        self._full_relayout_needed = False
        self._total_h = max(col_h) if col_h else 0

        vh = self.viewport().height()
        self.verticalScrollBar().setRange(0, max(0, self._total_h - vh))
        self.verticalScrollBar().setPageStep(vh)

        # Tell the model what thumbnail resolution to request.
        thumb = min(config.MAX_THUMB_PX, (cell_w // 32) * 32)
        if hasattr(m, "set_thumb_px"):
            m.set_thumb_px(max(128, thumb))
        # Video preview frames should match the cell size, not the source.
        self._vid_pool._max_px = max(320, min(960, thumb))

        self.viewport().update()

    # -- painting --------------------------------------------------------------
    def viewportEvent(self, event: QEvent) -> bool:
        if event.type() == QEvent.Type.Paint:
            self._paint(QPainter(self.viewport()))
            return True
        return super().viewportEvent(event)

    def _visible_cell_rows(self) -> list[int]:
        """Rows whose cells intersect the viewport, found by binary-searching
        each column's y-index — O(visible + log n) instead of scanning every
        cell, which mattered at 100k items on every paint/scroll tick."""
        if not self._col_ystart:
            return []
        scroll_y = self.verticalScrollBar().value()
        y_max = scroll_y + self.viewport().height()
        out: list[int] = []
        for ys, data in zip(self._col_ystart, self._col_data):
            if not ys:
                continue
            k = max(0, bisect.bisect_right(ys, scroll_y) - 1)
            while k < len(ys) and ys[k] < y_max:
                y_end, row = data[k]
                if y_end > scroll_y:
                    out.append(row)
                k += 1
        out.sort()
        return out

    def _paint(self, painter: QPainter) -> None:
        vp_rect = self.viewport().rect()
        painter.fillRect(vp_rect, QColor(config.CANVAS))

        m = self._model
        if m is None or not self._cells:
            self._paint_empty_hint(painter, vp_rect)
            return

        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        scroll_y = self.verticalScrollBar().value()

        for i in self._visible_cell_rows():
            cx, cy, cw, ch = self._cells[i]
            ry = cy - scroll_y
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
            elif idx.data(FailedRole):
                painter.fillRect(rect, QColor(config.CELL_BG))
                painter.setPen(QPen(QColor(config.RED_DIM)))
                painter.drawText(rect, Qt.AlignmentFlag.AlignCenter,
                                 "⚠\nunreadable")
            else:
                painter.fillRect(rect, QColor(config.CELL_BG))
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

            if i in self._selection:
                tint = QColor(config.ACCENT)
                tint.setAlpha(70)
                painter.fillRect(rect, tint)
                painter.setBrush(Qt.BrushStyle.NoBrush)
                painter.setPen(QPen(QColor(config.ACCENT), 3))
                painter.drawRect(rect.adjusted(1, 1, -2, -2))
            elif i == self._cur_row:
                painter.setBrush(Qt.BrushStyle.NoBrush)
                painter.setPen(QPen(QColor(config.ACCENT), 2))
                painter.drawRect(rect)

    def _paint_empty_hint(self, painter: QPainter, vp_rect: QRect) -> None:
        """Centred guidance text for an empty gallery."""
        if not self._empty_primary:
            return
        cx = vp_rect.center().x()
        cy = vp_rect.center().y()
        painter.setPen(QPen(QColor(config.FG_MID)))
        f = painter.font()
        f.setPointSize(16)
        f.setBold(True)
        painter.setFont(f)
        primary_rect = QRect(vp_rect.x(), cy - 36, vp_rect.width(), 32)
        painter.drawText(primary_rect, Qt.AlignmentFlag.AlignCenter,
                         self._empty_primary)
        if self._empty_secondary:
            painter.setPen(QPen(QColor(config.FG_DIM)))
            f.setPointSize(11)
            f.setBold(False)
            painter.setFont(f)
            secondary_rect = QRect(vp_rect.x(), cy + 4, vp_rect.width(), 24)
            painter.drawText(secondary_rect, Qt.AlignmentFlag.AlignCenter,
                             self._empty_secondary)

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
        cw = getattr(self, "_cell_w", 0)
        if cw <= 0 or not self._col_ystart:
            return -1
        py = pos.y() + self.verticalScrollBar().value()
        col = int(pos.x() // cw)
        if col < 0 or col >= len(self._col_ystart):
            return -1
        ys = self._col_ystart[col]
        # Rightmost cell whose y-start <= py, then check it actually contains py.
        k = bisect.bisect_right(ys, py) - 1
        if 0 <= k < len(ys):
            y_end, row = self._col_data[col][k]
            if py < y_end:
                return row
        return -1

    def _rect_for(self, row: int) -> QRect | None:
        if 0 <= row < len(self._cells):
            x, y, w, h = self._cells[row]
            return QRect(x, y - self.verticalScrollBar().value(), w, h)
        return None

    # -- selection -------------------------------------------------------------
    def selected_rows(self) -> list[int]:
        """Sorted list of currently selected rows."""
        return sorted(self._selection)

    def clear_selection(self) -> None:
        if self._selection:
            self._selection.clear()
            self._anchor = -1
            self.selectionChanged.emit(0)
            self.viewport().update()

    def reveal_row(self, row: int) -> None:
        """Make *row* the sole selection and scroll it into view."""
        m = self._model
        if not m or not (0 <= row < m.rowCount()):
            return
        self._selection = {row}
        self._anchor = row
        self._cur_row = row
        self._scroll_to(row)
        self.selectionChanged.emit(1)
        self.viewport().update()

    def select_all(self) -> None:
        m = self._model
        if m and m.rowCount():
            self._selection = set(range(m.rowCount()))
            self.selectionChanged.emit(len(self._selection))
            self.viewport().update()

    def contextMenuEvent(self, e) -> None:
        # Right-click acts on the selection; if the clicked cell isn't part of
        # it, make it the (single) selection first — standard file-manager UX.
        row = self._row_at(e.pos())
        if row >= 0 and row not in self._selection:
            self._set_single_selection(row)
            self.viewport().update()
        self.contextMenu.emit(e.globalPos())

    def _set_single_selection(self, row: int) -> None:
        self._selection = {row}
        self._anchor = row
        self.selectionChanged.emit(1)

    def _toggle_in_selection(self, row: int) -> None:
        if row in self._selection:
            self._selection.discard(row)
        else:
            self._selection.add(row)
        self._anchor = row
        self.selectionChanged.emit(len(self._selection))

    def _extend_selection_to(self, row: int) -> None:
        if self._anchor < 0:
            self._anchor = row
        lo, hi = sorted((self._anchor, row))
        self._selection |= set(range(lo, hi + 1))
        self.selectionChanged.emit(len(self._selection))

    # -- mouse -----------------------------------------------------------------
    def mousePressEvent(self, e) -> None:
        if e.button() != Qt.MouseButton.LeftButton:
            return super().mousePressEvent(e)
        row = self._row_at(e.position().toPoint())
        mods = e.modifiers()
        ctrl = bool(mods & Qt.KeyboardModifier.ControlModifier)
        shift = bool(mods & Qt.KeyboardModifier.ShiftModifier)
        if row < 0:
            if not (ctrl or shift):
                self.clear_selection()
            return super().mousePressEvent(e)
        if shift:
            self._extend_selection_to(row)
        elif ctrl:
            self._toggle_in_selection(row)
        else:
            self._set_single_selection(row)
        self._cur_row = row
        self.viewport().update()
        super().mousePressEvent(e)

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
        if self._scrub_path is not None:
            self._vid_pool.resume(self._scrub_path)
            self._scrub_path = None

    def _update_hover(self, pos: QPoint) -> None:
        row = self._row_at(pos)
        if row < 0:
            self._hide_timer.start()
            self._scrub_at(None, pos)
            return
        self._hide_timer.stop()
        rect = self._rect_for(row)
        if rect is None:
            return
        m = self._model
        # Hover-scrub: moving the mouse across a video preview seeks its timeline.
        if m is not None and m.data(m.index(row), IsVideoRole):
            self._scrub_at(m.data(m.index(row), PathRole), pos, rect)
        else:
            self._scrub_at(None, pos)
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

    def _scrub_at(self, path, pos, rect=None) -> None:
        """Seek `path`'s preview to the mouse x-position within `rect`.  When
        `path` is None (off a video), resume the previously-scrubbed preview."""
        if self._previews_suspended:
            return
        if path != self._scrub_path and self._scrub_path is not None:
            self._vid_pool.resume(self._scrub_path)   # released — play again
            self._scrub_path = None
        if path is None or rect is None or rect.width() <= 0:
            return
        # Ensure the hovered video is loaded in the pool, then scrub it.
        self._vid_pool.play(path)
        frac = (pos.x() - rect.x()) / rect.width()
        if self._vid_pool.scrub(path, frac):
            self._scrub_path = path

    # -- keyboard --------------------------------------------------------------
    def keyPressEvent(self, e) -> None:
        m = self._model
        if m is None:
            return super().keyPressEvent(e)
        total = m.rowCount()
        row = self._cur_row
        key = e.key()
        ctrl = bool(e.modifiers() & Qt.KeyboardModifier.ControlModifier)
        shift = bool(e.modifiers() & Qt.KeyboardModifier.ShiftModifier)
        if ctrl and key == Qt.Key.Key_A:
            self.select_all()
            e.accept()
        elif key == Qt.Key.Key_Escape:
            self.clear_selection()
            e.accept()
        elif key in (Qt.Key.Key_Right, Qt.Key.Key_Down):
            if row + 1 < total:
                self._cur_row = row + 1
                self._move_selection(self._cur_row, shift)
                self._scroll_to(self._cur_row)
                self.viewport().update()
            e.accept()
        elif key in (Qt.Key.Key_Left, Qt.Key.Key_Up):
            if row > 0:
                self._cur_row = row - 1
                self._move_selection(self._cur_row, shift)
                self._scroll_to(self._cur_row)
                self.viewport().update()
            e.accept()
        elif key in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            if 0 <= row < total:
                self.openLightbox.emit(row)
            e.accept()
        elif key == Qt.Key.Key_F:
            rows = self._action_rows()
            if rows:
                self.favBatch.emit(rows)
            e.accept()
        elif key in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            rows = self._action_rows()
            if rows:
                self.trashBatch.emit(rows)
            e.accept()
        else:
            super().keyPressEvent(e)

    def _move_selection(self, row: int, shift: bool) -> None:
        """Arrow-key navigation: extend selection with Shift, else select one."""
        if shift:
            self._extend_selection_to(row)
        else:
            self._set_single_selection(row)

    def _action_rows(self) -> list[int]:
        """Rows a batch keyboard action targets: the selection, else the cursor."""
        if self._selection:
            return sorted(self._selection)
        if 0 <= self._cur_row < (self._model.rowCount() if self._model else 0):
            return [self._cur_row]
        return []

    def _scroll_to(self, row: int) -> None:
        if 0 <= row < len(self._cells):
            x, y, w, h = self._cells[row]
            sb = self.verticalScrollBar()
            vh = self.viewport().height()
            if y < sb.value():
                sb.setValue(y)
            elif y + h > sb.value() + vh:
                sb.setValue(y + h - vh)

    def wheelEvent(self, e) -> None:
        # Ctrl+wheel zooms the grid (fewer columns = larger thumbnails),
        # matching every mainstream gallery/browser convention.
        if e.modifiers() & Qt.KeyboardModifier.ControlModifier:
            d = e.angleDelta().y()
            if d:
                self.columnsZoom.emit(-1 if d > 0 else 1)
            e.accept()
            return
        super().wheelEvent(e)

    # -- resize / scroll -------------------------------------------------------
    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        self._relayout()

    def _on_scroll(self, _) -> None:
        self._overlay.hide()
        if not self._previews_suspended:
            self._vid_update.start()
        self.viewport().update()

    # -- video previews --------------------------------------------------------
    def _visible_rows(self) -> list[int]:
        return self._visible_cell_rows()

    def _sync_video_previews(self) -> None:
        m = self._model
        if m is None:
            return
        # Never run previews while suspended or covered/hidden.
        if self._previews_suspended or not self.isVisible():
            self._vid_pool.stop_all()
            self._frame_flush.stop()
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
        if not self._previews_suspended:
            self._vid_update.start()
