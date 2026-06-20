"""Multi-view: adaptive 3-up (portrait) / 2\u00d72 (landscape) grid of slots.

Each slot shows an image or a native-playback video with its own scrubber and
a hover overlay (pin, enlarge, rotate, favourite, trash).  Pinned slots keep
their content while paging through the rest of the collection.
"""
from __future__ import annotations
import os

from PySide6.QtCore import Qt, QUrl, Signal, QSizeF
from PySide6.QtWidgets import (QDialog, QWidget, QGridLayout, QVBoxLayout,
                               QHBoxLayout, QLabel, QToolButton, QStackedLayout,
                               QGraphicsScene, QGraphicsView, QSizePolicy)
from PySide6.QtGui import QPixmap, QKeySequence, QShortcut
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput
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
        self.setStyleSheet("background: #000;")

    def set_source(self, pm) -> None:
        self._src = pm
        self._apply()

    def clear_source(self) -> None:
        self._src = None
        self.clear()

    def _apply(self) -> None:
        if self._src is not None and not self._src.isNull():
            self.setPixmap(self._src.scaled(
                self.size(), Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation))

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._apply()


class _Slot(QWidget):
    favToggled = Signal(str)
    trashed    = Signal(str)
    enlarge    = Signal(int)
    pinned     = Signal(int)

    def __init__(self, slot_index: int, favorites, parent=None):
        super().__init__(parent)
        self._idx = slot_index
        self._favs = favorites
        self._row = -1
        self._rotation = 0
        self._dur_ms = 0
        self.setStyleSheet("background: #000;")

        self._is_video = False
        self._disp = QSizeF(0, 0)

        self._img = _AspectLabel()

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
        self._audio = QAudioOutput(self)
        self._audio.setMuted(True)
        self._player.setAudioOutput(self._audio)
        self._player.setVideoOutput(self._video_item)
        self._player.setLoops(QMediaPlayer.Loops.Infinite)
        self._player.mediaStatusChanged.connect(self._on_status)
        self._player.positionChanged.connect(self._on_pos)
        self._player.durationChanged.connect(self._on_dur)

        self._content = QWidget()
        cl = QVBoxLayout(self._content)
        cl.setContentsMargins(0, 0, 0, 0)
        cl.addWidget(self._img)
        cl.addWidget(self._gview)
        self._img.show(); self._gview.hide()

        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addWidget(self._content)

        self._btnbar = QWidget(self)
        self._btnbar.setStyleSheet("background: transparent;")
        ol = QHBoxLayout(self._btnbar)
        ol.setContentsMargins(6, 6, 6, 6)
        self._pin_btn = self._mk(config.ICON_PIN,
                                 lambda: self.pinned.emit(self._idx),
                                 checkable=True)
        ol.addWidget(self._pin_btn)
        ol.addStretch(1)
        self._fav_btn = self._mk(config.ICON_HEART_EMPTY, self._toggle_fav)
        ol.addWidget(self._fav_btn)
        self._mk(config.ICON_ENLARGE, lambda: self.enlarge.emit(self._row), ol)
        self._mk(config.ICON_ROTATE_CW, self._rotate, ol)
        self._mk(config.ICON_TRASH, self._trash, ol)
        self._btnbar.raise_()

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
        h = self._btnbar.sizeHint().height()
        self._btnbar.setGeometry(0, 0, self.width(), h)
        self._btnbar.raise_()
        if self._is_video:
            self._fit_video()

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

    def _mk(self, glyph, cb, layout=None, checkable=False):
        b = QToolButton()
        b.setText(glyph)
        b.setCheckable(checkable)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setStyleSheet(
            f"color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius: 4px; font-size: 15px; padding: 3px 6px;")
        b.clicked.connect(cb)
        if layout is not None:
            layout.addWidget(b)
        return b

    @property
    def is_pinned(self) -> bool:
        return self._pin_btn.isChecked()

    @property
    def row(self) -> int:
        return self._row

    def clear(self) -> None:
        self._player.stop()
        self._img.clear_source()
        self._seekwrap.hide()
        self._is_video = False
        self._row = -1

    def show_item(self, row: int, path: str) -> None:
        self._row = row
        self._rotation = 0
        self._dur_ms = 0
        is_fav = self._favs.is_fav(path)
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"color: {config.RED}; background: rgba(0,0,0,90); border-radius:4px;"
            " font-size:15px; padding:3px 6px;" if is_fav else
            f"color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px;")
        if media.is_video(path):
            self._is_video = True
            self._img.hide(); self._gview.show(); self._seekwrap.show()
            self._player.setSource(QUrl.fromLocalFile(path))
            self._player.play()
            self._fit_video()
        else:
            self._is_video = False
            self._player.stop()
            self._gview.hide(); self._seekwrap.hide(); self._img.show()
            qim = media.load_full_qimage(path, max_px=2000)
            self._pm = QPixmap.fromImage(qim) if qim else QPixmap()
            self._img.set_source(self._pm)

    def _rotate(self):
        if getattr(self, "_pm", None) and not self._pm.isNull():
            from PySide6.QtGui import QTransform
            self._rotation = (self._rotation + 90) % 360
            self._pm = self._pm.transformed(
                QTransform().rotate(90),
                Qt.TransformationMode.SmoothTransformation)
            self._img.set_source(self._pm)

    def _toggle_fav(self):
        self.favToggled.emit(str(self._row))

    def _trash(self):
        self._player.stop()
        self.trashed.emit(str(self._row))

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
    favToggled   = Signal(str)
    trashed      = Signal(str)
    openLightbox = Signal(int)

    def __init__(self, model, favorites, start_row=0, parent=None):
        super().__init__(parent)
        self._model = model
        self._favs = favorites
        self._start = start_row
        self._layout_slots = 3
        self.setStyleSheet("background: #000;")
        self.setWindowTitle("Multi-view")
        self.resize(1200, 850)

        self._root = QVBoxLayout(self)
        self._root.setContentsMargins(0, 0, 0, 0)
        self._root.setSpacing(0)

        chrome = QHBoxLayout()
        chrome.setContentsMargins(8, 6, 8, 6)
        chrome.setSpacing(6)
        self._prev_btn = self._chrome_btn(config.ICON_PREV, self.prev_page,
                                          "Previous page (\u2190)", big=True)
        self._next_btn = self._chrome_btn(config.ICON_NEXT, self.next_page,
                                          "Next page (\u2192)", big=True)
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
        self._grid.setContentsMargins(4, 4, 4, 4)
        self._grid.setSpacing(4)
        self._root.addWidget(self._grid_host, 1)

        self._slots: list[_Slot] = []
        self._build_slots(3)

        QShortcut(QKeySequence(Qt.Key.Key_Escape), self, activated=self.close)
        QShortcut(QKeySequence(Qt.Key.Key_Left), self, activated=self.prev_page)
        QShortcut(QKeySequence(Qt.Key.Key_Right), self, activated=self.next_page)
        QShortcut(QKeySequence(Qt.Key.Key_F11), self, activated=self._toggle_fs)
        QShortcut(QKeySequence(Qt.Key.Key_F), self, activated=self._toggle_fs)

        self._render(start_row)

    def _chrome_btn(self, glyph, cb, tip="", big=False) -> QToolButton:
        b = QToolButton()
        b.setText(glyph)
        b.setToolTip(tip)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        size = 26 if big else 18
        weight = "bold" if big else "normal"
        b.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG}; font-size: {size}px;"
            f" font-weight: {weight}; background: transparent; border: none;"
            " padding: 2px 10px; }"
            " QToolButton:hover { color: #ffffff;"
            " background: rgba(255,255,255,30); border-radius: 5px; }")
        b.clicked.connect(cb)
        return b

    def _toggle_fs(self) -> None:
        self.showNormal() if self.isFullScreen() else self.showFullScreen()

    def _build_slots(self, n: int) -> None:
        for s in self._slots:
            s.clear(); s.setParent(None)
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
            slot.pinned.connect(lambda *_: None)
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
        if want != self._layout_slots:
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
            f"{self._start + 1}\u2013{min(row, total)} of {total}")

    def next_page(self) -> None:
        step = sum(1 for s in self._slots if not s.is_pinned) or 1
        total = self._model.rowCount()
        nxt = self._start + step
        self._render(0 if nxt >= total else nxt)

    def prev_page(self) -> None:
        step = sum(1 for s in self._slots if not s.is_pinned) or 1
        self._render(max(0, self._start - step))

    def _on_enlarge(self, row: int) -> None:
        self.openLightbox.emit(row)
        self.close()

    def _on_slot_fav(self, row_str: str) -> None:
        p = self._model.path_at(int(row_str))
        if p:
            self.favToggled.emit(p)
            self._render(self._start)

    def _on_slot_trash(self, row_str: str) -> None:
        p = self._model.path_at(int(row_str))
        if p:
            self.trashed.emit(p)
            self._render(self._start)
