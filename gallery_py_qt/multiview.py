"""Multi-view: adaptive 3-up (portrait) / 2x2 (landscape) grid of slots.

Each slot shows an image or a native-playback video with its own scrubber and
a hover overlay (pin, enlarge, rotate, favourite, trash).  Pinned slots keep
their content while paging through the rest of the collection.

Fixes vs previous version:
  - _Slot stores _path directly and emits it from signals -- no second
    path_at(row) lookup that could race with a model update mid-trash.
  - _Slot uses QStackedWidget for img/video; each page fills the full slot
    geometry so _AspectLabel.scaled() never sees a zero or half height.
  - Pin button visual driven by an explicit stylesheet swap on click rather
    than relying on QSS :checked, which was silently overridden by the
    parent widget's background: #000 cascade.
  - Grid margins/spacing zeroed so media fills the window edge-to-edge.
"""
from __future__ import annotations
import os

from PySide6.QtCore import Qt, QUrl, Signal, QSizeF
from PySide6.QtWidgets import (QDialog, QWidget, QGridLayout, QVBoxLayout,
                               QHBoxLayout, QLabel, QToolButton, QStackedWidget,
                               QGraphicsScene, QGraphicsView, QSizePolicy)
from PySide6.QtGui import QPixmap, QKeySequence, QShortcut, QPalette, QColor
from PySide6.QtMultimedia import QMediaPlayer
from PySide6.QtMultimediaWidgets import QGraphicsVideoItem

from . import config
from .engine import media
from .seekbar import SeekBar, fmt_time


class _AspectLabel(QLabel):
    """QLabel that keeps a source pixmap and re-fits it on every resize."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self._src = None
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setMinimumSize(1, 1)
        p = self.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        self.setPalette(p)
        self.setAutoFillBackground(True)

    def set_source(self, pm) -> None:
        self._src = pm
        self._apply()

    def clear_source(self) -> None:
        self._src = None
        self.clear()

    def _apply(self) -> None:
        if self._src is not None and not self._src.isNull():
            sz = self.size()
            if sz.width() > 0 and sz.height() > 0:
                self.setPixmap(self._src.scaled(
                    sz, Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation))

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._apply()


class _Slot(QWidget):
    """One pane in the multi-view grid.

    Signals emit the file *path* (not a row index) so callers never need a
    second model lookup that could race with a concurrent remove_path.
    """
    favToggled = Signal(str)   # path
    trashed    = Signal(str)   # path
    enlarge    = Signal(int)   # model row
    pinned     = Signal(int)   # slot index

    # Stylesheet templates for the pin button — same visual language as fav:
    # OFF = dim / outline look, ON = highlighted red (matches filled heart).
    _PIN_OFF = (f"QToolButton {{ color: {config.FG_DIM};"
                " background: rgba(0,0,0,90);"
                " border-radius: 4px; font-size: 15px; padding: 3px 6px; }}")
    _PIN_ON  = (f"QToolButton {{ color: {config.RED};"
                " background: rgba(180,40,40,110);"
                f" border: 1px solid {config.RED};"
                " border-radius: 4px; font-size: 15px; padding: 3px 6px; }}")

    def __init__(self, slot_index: int, favorites, parent=None):
        super().__init__(parent)
        self._idx = slot_index
        self._favs = favorites
        self._row  = -1
        self._path = ""
        self._rotation = 0
        self._dur_ms = 0
        self._is_pinned = False

        # Use QPalette for background so child widget stylesheets aren't
        # shadowed by an inherited background: #000 from setStyleSheet.
        p = self.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        self.setPalette(p)
        self.setAutoFillBackground(True)

        self._is_video = False
        self._disp = QSizeF(0, 0)
        self._pm: QPixmap = QPixmap()

        # Image page
        self._img = _AspectLabel()

        # Video page
        self._scene = QGraphicsScene(self)
        self._gview = QGraphicsView(self._scene)
        self._gview.setFrameStyle(0)
        self._gview.setStyleSheet("background: #000; border: none;")
        self._gview.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._gview.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._video_item = QGraphicsVideoItem()
        self._video_item.setAspectRatioMode(Qt.AspectRatioMode.KeepAspectRatio)
        self._scene.addItem(self._video_item)
        self._video_item.nativeSizeChanged.connect(lambda *_: self._fit_video())

        self._player = QMediaPlayer(self)
        # Multi-view tiles play silently by design (several tiles run at once),
        # so no audio output is attached — this skips audio-stream decoding
        # rather than decoding into a muted sink.
        self._player.setVideoOutput(self._video_item)
        self._player.setLoops(QMediaPlayer.Loops.Infinite)
        self._player.mediaStatusChanged.connect(self._on_status)
        self._player.positionChanged.connect(self._on_pos)
        self._player.durationChanged.connect(self._on_dur)

        # Stack: page 0 = image, page 1 = video.  QStackedWidget gives each
        # page the full slot geometry, fixing the half-height bug from a VBox.
        self._stack = QStackedWidget()
        self._stack.addWidget(self._img)
        self._stack.addWidget(self._gview)
        self._stack.setCurrentIndex(0)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        lay.addWidget(self._stack)

        # Floating button bar (manually positioned in resizeEvent)
        self._btnbar = QWidget(self)
        self._btnbar.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        ol = QHBoxLayout(self._btnbar)
        ol.setContentsMargins(6, 6, 6, 6)
        ol.setSpacing(4)

        self._pin_btn = QToolButton()
        self._pin_btn.setText(config.ICON_PIN_OFF)
        self._pin_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._pin_btn.setStyleSheet(self._PIN_OFF)
        self._pin_btn.clicked.connect(self._toggle_pin)
        ol.addWidget(self._pin_btn)
        ol.addStretch(1)

        self._fav_btn = self._mk(config.ICON_HEART_EMPTY, self._toggle_fav)
        ol.addWidget(self._fav_btn)
        self._mk(config.ICON_ENLARGE,    lambda: self.enlarge.emit(self._row), ol)
        self._mk(config.ICON_ROTATE_CW,  self._rotate, ol)
        self._mk(config.ICON_TRASH,      self._trash, ol)
        self._btnbar.raise_()

        # Seek bar (floating, shown only for video)
        self._seekwrap = QWidget(self)
        self._seekwrap.setStyleSheet(
            "background: rgba(0,0,0,90); border-radius: 4px;")
        sl = QHBoxLayout(self._seekwrap)
        sl.setContentsMargins(8, 1, 8, 1)
        sl.setSpacing(6)
        self._scrub = SeekBar()
        sl.addWidget(self._scrub, 1)
        self._time = QLabel("0:00")
        self._time.setStyleSheet(
            f"color: {config.FG_BRIGHT}; font-size: 10px;")
        sl.addWidget(self._time)
        self._scrub.seeked.connect(self._on_seek)
        self._seekwrap.hide()

    def resizeEvent(self, e):
        super().resizeEvent(e)
        if self._is_video:
            self._fit_video()
        else:
            self._position_overlays()

    def _fit_video(self) -> None:
        vw, vh = self._gview.width(), self._gview.height()
        if vw <= 0 or vh <= 0:
            return
        ns = self._video_item.nativeSize()
        if ns.isEmpty() or ns.width() <= 0 or ns.height() <= 0:
            disp_w, disp_h = float(vw), float(vh)
        else:
            scale = min(vw / ns.width(), vh / ns.height())
            disp_w, disp_h = ns.width() * scale, ns.height() * scale
        self._video_item.setSize(QSizeF(disp_w, disp_h))
        self._video_item.setPos((vw - disp_w) / 2.0, (vh - disp_h) / 2.0)
        self._scene.setSceneRect(0, 0, vw, vh)
        self._disp = QSizeF(disp_w, disp_h)
        self._position_overlays()

    def _img_displayed_rect(self) -> tuple[int, int, int, int]:
        """Pixel bounds (x, y, w, h) of the scaled image within the slot."""
        cw = self._img.width() or self.width()
        ch = self._img.height() or self.height()
        if self._pm.isNull() or cw <= 0 or ch <= 0:
            return 0, 0, max(cw, 1), max(ch, 1)
        pw, ph = self._pm.width(), self._pm.height()
        if pw <= 0 or ph <= 0:
            return 0, 0, cw, ch
        scale = min(cw / pw, ch / ph)
        dw = max(1, int(pw * scale))
        dh = max(1, int(ph * scale))
        return (cw - dw) // 2, (ch - dh) // 2, dw, dh

    def _position_overlays(self) -> None:
        """Constrain btnbar (and seekbar for video) to displayed media bounds."""
        sw, sh = self.width(), self.height()
        bh = self._btnbar.sizeHint().height()
        if self._is_video:
            dw = int(self._disp.width()) or sw
            dh = int(self._disp.height()) or sh
            x = int((sw - dw) / 2)
            y = int((sh - dh) / 2)
        else:
            x, y, dw, dh = self._img_displayed_rect()
        self._btnbar.setGeometry(max(0, x), max(0, y), max(1, dw), bh)
        self._btnbar.raise_()
        if self._is_video:
            self._position_seek()

    def _position_seek(self) -> None:
        if not self._is_video:
            return
        sw, sh = self.width(), self.height()
        disp_w = self._disp.width() or sw
        disp_h = self._disp.height() or sh
        barh = max(20, self._seekwrap.sizeHint().height())
        x = int((sw - disp_w) / 2)
        y = int((sh + disp_h) / 2 - barh - 4)
        self._seekwrap.setGeometry(max(0, x), max(0, y), int(disp_w), barh)
        self._seekwrap.raise_()

    def _mk(self, glyph, cb, layout=None):
        b = QToolButton()
        b.setText(glyph)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius: 4px; font-size: 15px; padding: 3px 6px; }}")
        b.clicked.connect(cb)
        if layout is not None:
            layout.addWidget(b)
        return b

    @property
    def is_pinned(self) -> bool:
        return self._is_pinned

    @property
    def row(self) -> int:
        return self._row

    def clear(self) -> None:
        self._player.stop()
        self._img.clear_source()
        self._seekwrap.hide()
        self._is_video = False
        self._row  = -1
        self._path = ""
        self._pm   = QPixmap()
        self._position_overlays()

    def show_item(self, row: int, path: str) -> None:
        self._row  = row
        self._path = path
        self._rotation = 0
        self._dur_ms   = 0
        is_fav = self._favs.is_fav(path)
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"QToolButton {{ color: {config.RED}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px; }}" if is_fav else
            f"QToolButton {{ color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px; }}")
        if media.is_video(path):
            self._is_video = True
            self._pm = QPixmap()
            self._stack.setCurrentIndex(1)
            self._seekwrap.show()
            self._player.setSource(QUrl.fromLocalFile(path))
            self._player.play()
            self._fit_video()
        else:
            self._is_video = False
            self._player.stop()
            self._stack.setCurrentIndex(0)
            self._seekwrap.hide()
            qim = media.load_full_qimage(path, max_px=2000)
            self._pm = QPixmap.fromImage(qim) if (qim and not qim.isNull()) else QPixmap()
            self._img.set_source(self._pm)
            self._position_overlays()

    def _toggle_pin(self) -> None:
        self._is_pinned = not self._is_pinned
        self._pin_btn.setText(
            config.ICON_PIN_ON if self._is_pinned else config.ICON_PIN_OFF)
        self._pin_btn.setStyleSheet(
            self._PIN_ON if self._is_pinned else self._PIN_OFF)
        self.pinned.emit(self._idx)

    def _rotate(self) -> None:
        if self._pm.isNull():
            return
        from PySide6.QtGui import QTransform
        self._rotation = (self._rotation + 90) % 360
        self._pm = self._pm.transformed(
            QTransform().rotate(90),
            Qt.TransformationMode.SmoothTransformation)
        self._img.set_source(self._pm)

    def _toggle_fav(self) -> None:
        if self._path:
            self.favToggled.emit(self._path)

    def _trash(self) -> None:
        if self._path:
            self._player.stop()
            self.trashed.emit(self._path)

    def _on_status(self, status):
        if status == QMediaPlayer.MediaStatus.EndOfMedia:
            self._player.setPosition(0)
            self._player.play()

    def _on_pos(self, pos):
        self._scrub.set_position(pos)
        self._time.setText(fmt_time(pos))

    def _on_dur(self, dur):
        self._dur_ms = dur
        self._scrub.set_duration(dur)

    def _on_seek(self, frac):
        dur = self._dur_ms or self._player.duration()
        if dur > 0:
            self._player.setPosition(int(frac * dur))


class MultiView(QDialog):
    favToggled   = Signal(str)   # path
    trashed      = Signal(str)   # path
    openLightbox = Signal(int)   # row

    def __init__(self, model, favorites, start_row=0, parent=None):
        super().__init__(parent, Qt.WindowType.Window)
        self._model = model
        self._favs  = favorites
        self._start = start_row
        self._layout_slots = 3
        p = self.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        self.setPalette(p)
        self.setAutoFillBackground(True)
        self.setWindowTitle("Multi-view")
        self.resize(1200, 850)

        self._root = QVBoxLayout(self)
        self._root.setContentsMargins(0, 0, 0, 0)
        self._root.setSpacing(0)

        chrome = QHBoxLayout()
        chrome.setContentsMargins(8, 6, 8, 6)
        chrome.setSpacing(6)
        self._prev_btn = self._chrome_btn(config.ICON_PREV, self.prev_page,
                                          "Previous page (←)", big=True)
        self._next_btn = self._chrome_btn(config.ICON_NEXT, self.next_page,
                                          "Next page (→)", big=True)
        self._counter = QLabel("")
        self._counter.setStyleSheet(f"color:{config.FG_MID};")
        self._fs_btn = self._chrome_btn(config.ICON_FULLSCREEN, self._toggle_fs,
                                        "Toggle full screen (F11)")
        close_btn = self._chrome_btn(config.ICON_CLOSE, self.close, "Close (Esc)")
        chrome.addWidget(self._prev_btn)
        chrome.addWidget(self._next_btn)
        chrome.addStretch(1)
        chrome.addWidget(self._counter)
        chrome.addStretch(1)
        chrome.addWidget(self._fs_btn)
        chrome.addWidget(close_btn)
        self._root.addLayout(chrome)

        self._grid_host = QWidget()
        self._grid = QGridLayout(self._grid_host)
        self._grid.setContentsMargins(0, 0, 0, 0)
        self._grid.setSpacing(2)        # 2 px hairline between slots
        self._root.addWidget(self._grid_host, 1)

        self._slots: list[_Slot] = []
        self._build_slots(3)

        QShortcut(QKeySequence(Qt.Key.Key_Escape), self, activated=self.close)
        QShortcut(QKeySequence(Qt.Key.Key_Left),   self, activated=self.prev_page)
        QShortcut(QKeySequence(Qt.Key.Key_Right),  self, activated=self.next_page)
        QShortcut(QKeySequence(Qt.Key.Key_F11),    self, activated=self._toggle_fs)

        self._render(start_row)

    def _chrome_btn(self, glyph, cb, tip="", big=False) -> QToolButton:
        b = QToolButton()
        b.setText(glyph)
        b.setToolTip(tip)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        size   = 26 if big else 18
        weight = "bold" if big else "normal"
        b.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG}; font-size: {size}px;"
            f" font-weight: {weight}; background: transparent; border: none;"
            " padding: 2px 10px; }}"
            " QToolButton:hover { color: #ffffff;"
            " background: rgba(255,255,255,30); border-radius: 5px; }")
        b.clicked.connect(cb)
        return b

    def _toggle_fs(self) -> None:
        if self.isFullScreen():
            self.showNormal()
        else:
            self.showFullScreen()
            self.raise_()
            self.activateWindow()

    def _build_slots(self, n: int) -> None:
        for s in self._slots:
            s.clear()
            s.setParent(None)
        self._slots.clear()
        while self._grid.count():
            self._grid.takeAt(0)
        for k in range(4):
            self._grid.setColumnStretch(k, 0)
            self._grid.setRowStretch(k, 0)
        self._layout_slots = n
        positions = ([(0, 0), (0, 1), (0, 2)] if n == 3
                     else [(0, 0), (0, 1), (1, 0), (1, 1)])
        for i, (r, c) in enumerate(positions):
            slot = _Slot(i, self._favs, self)
            slot.setSizePolicy(QSizePolicy.Policy.Ignored,
                               QSizePolicy.Policy.Ignored)
            slot.enlarge.connect(self._on_enlarge)
            slot.favToggled.connect(self._on_slot_fav)
            slot.trashed.connect(self._on_slot_trash)
            slot.pinned.connect(lambda *_: None)     # visual-only; is_pinned drives nav
            self._grid.addWidget(slot, r, c)
            self._slots.append(slot)
        used_cols = 3 if n == 3 else 2
        used_rows = 1 if n == 3 else 2
        for cc in range(used_cols):
            self._grid.setColumnStretch(cc, 1)
        for rr in range(used_rows):
            self._grid.setRowStretch(rr, 1)

    def _detect_layout(self, row: int) -> int:
        path = self._model.path_at(row)
        if not path:
            return 3
        w, h = media.peek_size(path)
        return 4 if (w and h and w > h) else 3

    def _render(self, start: int) -> None:
        total = self._model.rowCount()
        if total == 0:
            return
        want = self._detect_layout(start)
        if want != self._layout_slots and not any(s.is_pinned for s in self._slots):
            self._build_slots(want)
        self._start = max(0, min(start, total - 1))
        row = self._start
        for slot in self._slots:
            if slot.is_pinned and slot.row >= 0:
                continue
            if row < total:
                slot.show_item(row, self._model.path_at(row))
                row += 1
            else:
                slot.clear()
        self._counter.setText(
            f"{self._start + 1}–{min(row, total)} of {total}")

    def next_page(self) -> None:
        step  = sum(1 for s in self._slots if not s.is_pinned) or 1
        total = self._model.rowCount()
        nxt   = self._start + step
        self._render(0 if nxt >= total else nxt)

    def prev_page(self) -> None:
        step = sum(1 for s in self._slots if not s.is_pinned) or 1
        self._render(max(0, self._start - step))

    def _on_enlarge(self, row: int) -> None:
        self.openLightbox.emit(row)
        self.close()

    # Slots now emit PATH strings directly -- no model lookup needed here.
    def _on_slot_fav(self, path: str) -> None:
        if path:
            self.favToggled.emit(path)
            self._render(self._start)

    def _on_slot_trash(self, path: str) -> None:
        if path:
            self.trashed.emit(path)
            # Re-render after model has been updated by the trash signal handler.
            self._render(self._start)
