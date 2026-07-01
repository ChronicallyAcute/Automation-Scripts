"""Multi-view: adaptive 3×1 (portrait) / 2×2 (landscape) grid of slots.

Embedded as a panel inside the main window — not a separate dialog.  Shown via
MainWindow._open_multiview(), hidden via the closeRequested signal.

Layout rules
  • Portrait content  → 3 tiles in a single row (3×1)
  • Landscape / square → 2×2 grid
  • Media orientation groups never mix in the same view.
  • Orientation switch button lets the user jump to the other group.
  • Switching layout (3×1 ↔ 2×2) preserves pinned media.

Auto-slideshow (two switchable styles, via the mode button / S key)
  • Set         — holds the current 3×1 / 2×2 grid for N seconds, then jumps
                  to the next set (paged advance, wrapping at the end).
  • Side-scroll — a smooth continuous left-scroll: tiles glide across the
                  screen at a configurable speed and wrap seamlessly.
  • The spin-box adapts to the active style (seconds for Set, speed for
    Side-scroll).  Prev / Next stop the slideshow and page manually.

Audio
  • Each slot has its own QAudioOutput, muted by default.
  • Ctrl+M unmutes every currently-displayed slot in one key press.
"""
from __future__ import annotations
import os

from PySide6.QtCore import (Qt, QUrl, Signal, QSizeF, QSize, QRectF, QTimer,
                            QMimeData, QObject, QRunnable, QThreadPool)
from PySide6.QtWidgets import (QWidget, QGridLayout, QVBoxLayout,
                               QHBoxLayout, QLabel, QToolButton, QStackedWidget,
                               QGraphicsScene, QGraphicsView, QSizePolicy,
                               QSpinBox, QSlider, QApplication)
from PySide6.QtGui import (QPixmap, QImage, QKeySequence, QShortcut, QPalette,
                           QColor, QPainter, QIcon, QDrag)
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput, QtAudio
from PySide6.QtMultimediaWidgets import QGraphicsVideoItem

from . import config
from .engine import media
from .seekbar import SeekBar, fmt_time

# MIME type carrying a dragged tile's file path between slots.
_SLOT_MIME = "application/x-gallery-slot-path"


class _ImgSignals(QObject):
    ready = Signal(int, QImage)   # (generation, decoded image)


class _ImgJob(QRunnable):
    """Decode a tile's still off the GUI thread (mirrors the lightbox).

    A generation token lets the slot ignore results that land after it has
    been re-used for a different item, so paging never shows a stale image.
    gen_now is re-checked when the job actually starts, so decodes queued
    behind others are skipped entirely once superseded — rapid paging through
    large files no longer burns a full decode per discarded page.
    """
    def __init__(self, gen: int, path: str, max_px: int, signals: _ImgSignals,
                 gen_now=None):
        super().__init__()
        self._gen = gen
        self._path = path
        self._max_px = max_px
        self._signals = signals
        self._gen_now = gen_now

    def run(self) -> None:
        if self._gen_now is not None:
            try:
                if self._gen_now() != self._gen:
                    return              # superseded while waiting in the queue
            except Exception:
                return                  # owner gone — nothing to deliver to
        try:
            qim = media.load_full_qimage(self._path, max_px=self._max_px)
        except Exception:
            qim = None
        self._signals.ready.emit(self._gen, qim if qim is not None else QImage())


def _make_pause_icon(color: QColor, px: int = 32) -> QIcon:
    """Render a two-bar pause glyph as a QIcon."""
    pm = QPixmap(px, px)
    pm.fill(Qt.GlobalColor.transparent)
    p = QPainter(pm)
    p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    p.setPen(Qt.PenStyle.NoPen)
    p.setBrush(color)
    bar_w = px * 0.24
    gap   = px * 0.16
    bar_h = px * 0.66
    y     = (px - bar_h) / 2.0
    x1    = px / 2.0 - gap / 2.0 - bar_w
    x2    = px / 2.0 + gap / 2.0
    rad   = bar_w * 0.35
    p.drawRoundedRect(QRectF(x1, y, bar_w, bar_h), rad, rad)
    p.drawRoundedRect(QRectF(x2, y, bar_w, bar_h), rad, rad)
    p.end()
    return QIcon(pm)


class _AspectLabel(QLabel):
    """QLabel that keeps a source pixmap and re-fits it on every resize.

    Fit mode (default) scales to keep the whole image visible (letterboxed);
    fill mode scales to cover the whole label and crops the overflow.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._src = None
        self._fill = False
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setMinimumSize(1, 1)
        # Clip an oversized (cover-scaled) pixmap to the label bounds.
        self.setScaledContents(False)
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

    def set_fill(self, fill: bool) -> None:
        self._fill = fill
        self._apply()

    def _apply(self) -> None:
        if self._src is not None and not self._src.isNull():
            sz = self.size()
            if sz.width() > 0 and sz.height() > 0:
                mode = (Qt.AspectRatioMode.KeepAspectRatioByExpanding
                        if self._fill else Qt.AspectRatioMode.KeepAspectRatio)
                scaled = self._src.scaled(
                    sz, mode, Qt.TransformationMode.SmoothTransformation)
                if self._fill and (scaled.width() > sz.width()
                                   or scaled.height() > sz.height()):
                    # Centre-crop the overflow so the cover fill is exact.
                    x = max(0, (scaled.width() - sz.width()) // 2)
                    y = max(0, (scaled.height() - sz.height()) // 2)
                    scaled = scaled.copy(x, y, sz.width(), sz.height())
                self.setPixmap(scaled)

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._apply()


class _Slot(QWidget):
    """One pane in the multi-view grid."""
    favToggled = Signal(str)        # path
    trashed    = Signal(str)        # path
    enlarge    = Signal(int)        # model row
    pinned     = Signal(int)        # slot index
    reordered  = Signal(str, str)   # (dragged path, drop-target path)
    rotated    = Signal(str)        # path — request permanent rotation

    _SPEEDS = (0.25, 0.5, 1.0, 1.25, 1.5, 1.75, 2.0)

    _PIN_OFF = ("QToolButton { background: transparent; border: none;"
                " border-radius: 4px; padding: 3px 5px; }")
    _PIN_ON  = (f"QToolButton {{ background: rgba(180,40,40,120);"
                f" border: 1px solid {config.RED};"
                " border-radius: 4px; padding: 3px 5px; }")

    def __init__(self, slot_index: int, favorites, parent=None):
        super().__init__(parent)
        self._idx = slot_index
        self._favs = favorites
        self._row  = -1
        self._path = ""
        self._rotation = 0
        self._dur_ms = 0
        self._is_pinned = False
        self._speed_idx = 2          # index into _SPEEDS → 1.0×
        self._drag_start = None
        self._fill = False           # False = fit (letterbox), True = cover-crop
        # Off-thread still decoding (set by MultiView after construction).
        self._img_pool: "QThreadPool | None" = None
        self._img_gen = 0
        self._img_sig = _ImgSignals(self)
        self._img_sig.ready.connect(self._on_img_decoded)
        self.setAcceptDrops(True)

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

        # Media body passes mouse events through so slot can start a reorder drag.
        for w in (self._img, self._gview, self._gview.viewport()):
            w.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self._video_item = QGraphicsVideoItem()
        self._video_item.setAspectRatioMode(Qt.AspectRatioMode.KeepAspectRatio)
        self._scene.addItem(self._video_item)
        self._video_item.nativeSizeChanged.connect(lambda *_: self._fit_video())

        self._player = QMediaPlayer(self)
        self._player.setVideoOutput(self._video_item)
        self._player.setLoops(QMediaPlayer.Loops.Infinite)
        self._player.mediaStatusChanged.connect(self._on_status)
        self._player.positionChanged.connect(self._on_pos)
        self._player.durationChanged.connect(self._on_dur)

        # Per-tile audio output — muted by default; user enables per tile.
        self._audio = QAudioOutput(self)
        self._audio.setMuted(True)
        self._audio.setVolume(1.0)
        self._player.setAudioOutput(self._audio)
        self._muted = True

        # Stack: page 0 = image, page 1 = video.
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
        ol.setContentsMargins(4, 3, 4, 3)
        ol.setSpacing(4)

        red = QColor(config.RED)
        self._pause_icon_off = _make_pause_icon(QColor(255, 255, 255, 70))
        self._pause_icon_on  = _make_pause_icon(QColor(red.red(), red.green(),
                                                       red.blue(), 255))
        self._pin_btn = QToolButton()
        self._pin_btn.setIcon(self._pause_icon_off)
        self._pin_btn.setIconSize(QSize(16, 16))
        self._pin_btn.setToolTip("Hold this tile (pause paging)")
        self._pin_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._pin_btn.setStyleSheet(self._PIN_OFF)
        self._pin_btn.clicked.connect(self._toggle_pin)
        ol.addWidget(self._pin_btn)
        ol.addStretch(1)

        self._fav_btn = self._mk(config.ICON_HEART_EMPTY, self._toggle_fav)
        ol.addWidget(self._fav_btn)
        self._mk(config.ICON_ENLARGE,   lambda: self.enlarge.emit(self._row), ol)
        self._mk(config.ICON_ROTATE_CW, self._rotate, ol)
        self._mk(config.ICON_TRASH,     self._trash, ol)

        self._bar_hide_timer = QTimer(self)
        self._bar_hide_timer.setSingleShot(True)
        self._bar_hide_timer.setInterval(400)
        self._bar_hide_timer.timeout.connect(self._maybe_hide_bar)
        self._btnbar.hide()

        # Seek bar (floating, video only)
        self._seekwrap = QWidget(self)
        self._seekwrap.setStyleSheet(
            "background: rgba(0,0,0,90); border-radius: 4px;")
        sl = QHBoxLayout(self._seekwrap)
        sl.setContentsMargins(8, 1, 8, 1)
        sl.setSpacing(6)

        self._speed_btn = QToolButton()
        self._speed_btn.setText("1×")
        self._speed_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._speed_btn.setToolTip("Playback speed")
        self._speed_btn.setStyleSheet(
            f"QToolButton {{ color: {config.FG_BRIGHT}; background: transparent;"
            " border: none; font-size: 10px; font-weight: bold; padding: 0 2px; }")
        self._speed_btn.clicked.connect(self._cycle_speed)
        sl.addWidget(self._speed_btn)

        self._scrub = SeekBar()
        sl.addWidget(self._scrub, 1)

        self._time = QLabel("0:00")
        self._time.setStyleSheet(
            f"color: {config.FG_BRIGHT}; font-size: 10px;")
        sl.addWidget(self._time)

        # Mute / unmute toggle
        self._mute_btn = QToolButton()
        self._mute_btn.setText(config.ICON_MUTE)
        self._mute_btn.setToolTip("Unmute / mute (click); volume slider appears on unmute")
        self._mute_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._mute_btn.setStyleSheet(
            f"QToolButton {{ color: {config.FG_DIM}; background: transparent;"
            " border: none; font-size: 13px; padding: 0 2px; }")
        self._mute_btn.clicked.connect(self._toggle_mute)
        sl.addWidget(self._mute_btn)

        self._scrub.seeked.connect(self._on_seek)
        self._seekwrap.hide()

        # Volume popup — floating child, shown when unmuted
        self._vol_popup = QWidget(self)
        self._vol_popup.setStyleSheet(
            "QWidget { background: rgba(0,0,0,210); border-radius: 4px; }")
        vl = QVBoxLayout(self._vol_popup)
        vl.setContentsMargins(4, 8, 4, 4)
        vl.setSpacing(2)
        self._vol_slider = QSlider(Qt.Orientation.Vertical)
        self._vol_slider.setRange(0, 100)
        self._vol_slider.setValue(100)
        self._vol_slider.setToolTip("Volume")
        self._vol_slider.valueChanged.connect(self._on_vol_changed)
        self._vol_label = QLabel("100%")
        self._vol_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._vol_label.setStyleSheet(
            f"color: {config.FG_BRIGHT}; font-size: 9px; background: transparent;")
        vl.addWidget(self._vol_slider, 1)
        vl.addWidget(self._vol_label)
        self._vol_popup.setFixedSize(32, 96)
        self._vol_popup.hide()

    # -- event handling --------------------------------------------------------
    def resizeEvent(self, e):
        super().resizeEvent(e)
        if self._is_video:
            self._fit_video()
        else:
            self._position_overlays()

    def enterEvent(self, e):
        super().enterEvent(e)
        self._bar_hide_timer.stop()
        self._btnbar.show()
        self._btnbar.raise_()

    def leaveEvent(self, e):
        super().leaveEvent(e)
        self._bar_hide_timer.start()

    def _maybe_hide_bar(self) -> None:
        if not self._is_pinned:
            self._btnbar.hide()

    # -- video sizing ----------------------------------------------------------
    def _fit_video(self) -> None:
        vw, vh = self._gview.width(), self._gview.height()
        if vw <= 0 or vh <= 0:
            return
        ns = self._video_item.nativeSize()
        if ns.isEmpty() or ns.width() <= 0 or ns.height() <= 0:
            disp_w, disp_h = float(vw), float(vh)
        else:
            # Fill = cover (scale up, crop overflow); fit = contain (letterbox).
            if self._fill:
                scale = max(vw / ns.width(), vh / ns.height())
            else:
                scale = min(vw / ns.width(), vh / ns.height())
            disp_w, disp_h = ns.width() * scale, ns.height() * scale
        self._video_item.setSize(QSizeF(disp_w, disp_h))
        self._video_item.setPos((vw - disp_w) / 2.0, (vh - disp_h) / 2.0)
        self._scene.setSceneRect(0, 0, vw, vh)   # clips any cover overflow
        # Overlays sit inside the visible area — the whole tile when filling.
        self._disp = QSizeF(min(disp_w, float(vw)), min(disp_h, float(vh)))
        self._position_overlays()

    def _img_displayed_rect(self) -> tuple[int, int, int, int]:
        """Pixel bounds (x, y, w, h) of the scaled image within the slot."""
        cw = self._img.width() or self.width()
        ch = self._img.height() or self.height()
        # Fill mode covers the whole tile — overlays span the full tile.
        if self._fill:
            return 0, 0, max(cw, 1), max(ch, 1)
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
        """Constrain btnbar and seekbar to the displayed-media bounds."""
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

    # -- helper ----------------------------------------------------------------
    def _mk(self, glyph, cb, layout=None):
        b = QToolButton()
        b.setText(glyph)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius: 4px; font-size: 15px; padding: 3px 6px; }")
        b.clicked.connect(cb)
        if layout is not None:
            layout.addWidget(b)
        return b

    # -- properties ------------------------------------------------------------
    @property
    def is_pinned(self) -> bool:
        return self._is_pinned

    @property
    def row(self) -> int:
        return self._row

    def set_fill(self, fill: bool) -> None:
        """Toggle cover-fill (crop) vs fit (letterbox) for this tile's media."""
        self._fill = fill
        self._img.set_fill(fill)
        if self._is_video:
            self._fit_video()
        self._position_overlays()

    # -- content ---------------------------------------------------------------
    def clear(self) -> None:
        self._img_gen += 1                      # drop any in-flight decode
        self._player.stop()
        self._img.clear_source()
        self._seekwrap.hide()
        self._vol_popup.hide()
        self._is_video = False
        self._row  = -1
        self._path = ""
        self._pm   = QPixmap()
        self._reset_audio()
        self._position_overlays()

    def show_item(self, row: int, path: str) -> None:
        self._row  = row
        self._path = path
        self._rotation = 0
        self._dur_ms   = 0
        self._reset_audio()
        is_fav = self._favs.is_fav(path)
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"QToolButton {{ color: {config.RED}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px; }" if is_fav else
            f"QToolButton {{ color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px; }")
        self._img_gen += 1                      # invalidate any pending decode
        if media.is_video(path):
            self._is_video = True
            self._pm = QPixmap()
            self._stack.setCurrentIndex(1)
            self._seekwrap.show()
            self._player.setSource(QUrl.fromLocalFile(path))
            self._player.play()
            self._player.setPlaybackRate(self._SPEEDS[self._speed_idx])
            self._fit_video()
        else:
            self._is_video = False
            self._player.stop()
            self._stack.setCurrentIndex(0)
            self._seekwrap.hide()
            self._pm = QPixmap()
            self._img.clear_source()            # blank while decoding off-thread
            # Decode only to the tile's pixel size, not the source resolution.
            tile_px = max(self.width(), self.height()) or 1280
            tile_px = max(640, min(tile_px, 2048))
            if self._img_pool is not None:
                self._img_pool.start(
                    _ImgJob(self._img_gen, path, tile_px, self._img_sig,
                            gen_now=lambda: self._img_gen))
            else:
                qim = media.load_full_qimage(path, max_px=tile_px)
                self._on_img_decoded(self._img_gen,
                                     qim if qim is not None else QImage())
            self._position_overlays()

    def _on_img_decoded(self, gen: int, qim: QImage) -> None:
        if gen != self._img_gen or self._is_video:
            return                              # superseded by a later show_item
        self._pm = (QPixmap.fromImage(qim)
                    if (qim is not None and not qim.isNull()) else QPixmap())
        self._img.set_source(self._pm)
        self._position_overlays()

    def _reset_audio(self) -> None:
        self._muted = True
        self._audio.setMuted(True)
        self._vol_popup.hide()
        self._mute_btn.setText(config.ICON_MUTE)
        self._mute_btn.setStyleSheet(
            f"QToolButton {{ color: {config.FG_DIM}; background: transparent;"
            " border: none; font-size: 13px; padding: 0 2px; }")

    def unmute(self) -> None:
        """Unmute this slot. No-op if already unmuted or not playing video."""
        if self._is_video and self._muted:
            self._toggle_mute()

    # -- overlay button actions ------------------------------------------------
    def _toggle_pin(self) -> None:
        self._is_pinned = not self._is_pinned
        self._pin_btn.setIcon(
            self._pause_icon_on if self._is_pinned else self._pause_icon_off)
        self._pin_btn.setStyleSheet(
            self._PIN_ON if self._is_pinned else self._PIN_OFF)
        if self._is_pinned:
            self._bar_hide_timer.stop()
            self._btnbar.show()
            self._btnbar.raise_()
        else:
            self._bar_hide_timer.start()
        self.pinned.emit(self._idx)

    def _rotate(self) -> None:
        # Permanent rotation is handled by the main window (rewrites the file
        # and reloads the slot); video tiles have no still to rotate.
        if self._path and not self._is_video:
            self.rotated.emit(self._path)

    def _toggle_fav(self) -> None:
        if self._path:
            self.favToggled.emit(self._path)

    def _trash(self) -> None:
        if self._path:
            self._player.stop()
            self.trashed.emit(self._path)

    # -- media player callbacks ------------------------------------------------
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

    def _cycle_speed(self) -> None:
        self._speed_idx = (self._speed_idx + 1) % len(self._SPEEDS)
        rate = self._SPEEDS[self._speed_idx]
        self._player.setPlaybackRate(rate)
        self._speed_btn.setText(f"{rate:g}×")

    # -- audio -----------------------------------------------------------------
    def _toggle_mute(self) -> None:
        self._muted = not self._muted
        self._audio.setMuted(self._muted)
        if self._muted:
            self._mute_btn.setText(config.ICON_MUTE)
            self._mute_btn.setStyleSheet(
                f"QToolButton {{ color: {config.FG_DIM}; background: transparent;"
                " border: none; font-size: 13px; padding: 0 2px; }")
            self._vol_popup.hide()
        else:
            self._mute_btn.setText(config.ICON_UNMUTE)
            self._mute_btn.setStyleSheet(
                f"QToolButton {{ color: {config.FG_BRIGHT}; background: transparent;"
                " border: none; font-size: 13px; padding: 0 2px; }")
            self._show_vol_popup()

    def _on_vol_changed(self, val: int) -> None:
        self._vol_label.setText(f"{val}%")
        # Standard log→linear remap so the slider feels uniform end to end
        # (matches the lightbox transport's volume behaviour).
        self._audio.setVolume(QtAudio.convertVolume(
            val / 100.0,
            QtAudio.VolumeScale.LogarithmicVolumeScale,
            QtAudio.VolumeScale.LinearVolumeScale))

    def _show_vol_popup(self) -> None:
        btn_pos = self._mute_btn.mapTo(self, self._mute_btn.rect().topLeft())
        pw = self._vol_popup.width()
        ph = self._vol_popup.height()
        x = btn_pos.x() + (self._mute_btn.width() - pw) // 2
        y = btn_pos.y() - ph - 4
        x = max(0, min(x, self.width() - pw))
        y = max(0, y)
        self._vol_popup.setGeometry(x, y, pw, ph)
        self._vol_popup.show()
        self._vol_popup.raise_()

    # -- drag-and-drop reordering ----------------------------------------------
    def mousePressEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton and self._path:
            self._drag_start = e.position().toPoint()
        super().mousePressEvent(e)

    def mouseMoveEvent(self, e):
        if (self._drag_start is not None
                and (e.buttons() & Qt.MouseButton.LeftButton)
                and self._path
                and (e.position().toPoint() - self._drag_start).manhattanLength()
                >= QApplication.startDragDistance()):
            self._begin_drag()
        super().mouseMoveEvent(e)

    def mouseReleaseEvent(self, e):
        self._drag_start = None
        super().mouseReleaseEvent(e)

    def _begin_drag(self) -> None:
        self._drag_start = None
        drag = QDrag(self)
        mime = QMimeData()
        mime.setData(_SLOT_MIME, self._path.encode("utf-8"))
        drag.setMimeData(mime)
        if not self._pm.isNull():
            drag.setPixmap(self._pm.scaled(
                160, 160, Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation))
        drag.exec(Qt.DropAction.MoveAction)

    def dragEnterEvent(self, e):
        if e.mimeData().hasFormat(_SLOT_MIME) and self._path:
            e.acceptProposedAction()
        else:
            e.ignore()

    def dragMoveEvent(self, e):
        if e.mimeData().hasFormat(_SLOT_MIME) and self._path:
            e.acceptProposedAction()
        else:
            e.ignore()

    def dropEvent(self, e):
        if not e.mimeData().hasFormat(_SLOT_MIME):
            e.ignore()
            return
        src = bytes(e.mimeData().data(_SLOT_MIME)).decode("utf-8")
        if src and self._path and src != self._path:
            self.reordered.emit(src, self._path)
        e.acceptProposedAction()


class MultiView(QWidget):
    """Embedded multi-view panel (not a dialog).

    Shown/hidden by switching a QStackedWidget in MainWindow.
    Communicates back via signals rather than close().
    """
    favToggled     = Signal(str)   # path
    trashed        = Signal(str)   # path
    rotated        = Signal(str)   # path — request permanent rotation
    openLightbox   = Signal(int)   # row
    closeRequested = Signal()      # user wants to go back to gallery

    def __init__(self, model, favorites, parent=None):
        super().__init__(parent)
        self._model = model
        self._favs  = favorites
        self._layout_slots = 3
        # Layout override: None = auto by orientation, else forced slot count
        # (3 = 3×1, 4 = 2×2).  Fill mode crops media to cover each tile.
        self._forced_layout = None
        self._fill_mode = False

        # Shared, bounded pool for off-thread tile decoding (all tiles share it,
        # so at most 2 full decodes run at once — never one-per-tile on the GUI).
        self._img_pool = QThreadPool(self)
        self._img_pool.setMaxThreadCount(2)
        # Re-partition orientation groups when the async dims job reports sizes.
        model.dimsChanged.connect(self._on_model_dims_changed)

        # Orientation-partitioned path lists.
        self._portrait_paths:  list[str] = []
        self._landscape_paths: list[str] = []
        self._current_paths:   list[str] = []
        self._start = 0

        # Auto-slideshow state.  Two styles share one play/pause button and one
        # spin-box: "set" pages the whole grid on a timer; "scroll" side-scrolls.
        self._slideshow_mode = "set"     # "set" | "scroll"
        self._set_interval_s = 4         # remembered seconds-per-set
        self._scroll_level   = 3         # remembered side-scroll speed level

        # Side-scroll state
        self._ss_slot_order: list[_Slot] = []   # non-empty only when scrolling
        self._ss_head_idx = 0     # index in _current_paths of the leftmost slot
        self._ss_px = 0.0         # fractional px scrolled into current position
        self._ss_speed = self._scroll_level * 0.5   # px per timer tick
        self._ss_buf: "_Slot | None" = None
        self._slot_grid_pos: list[tuple[int, int]] = []

        p = self.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        self.setPalette(p)
        self.setAutoFillBackground(True)

        # ------------------------------------------------------------------ #
        # Root layout: [chrome bar] / [grid] / [bottom bar]                  #
        # The bars are IN the layout (not floating), so slot controls are     #
        # never obscured by a higher-z sibling.                               #
        # ------------------------------------------------------------------ #
        self._root = QVBoxLayout(self)
        self._root.setContentsMargins(0, 0, 0, 0)
        self._root.setSpacing(0)

        # Top chrome bar
        self._chrome_widget = QWidget()
        self._chrome_widget.setStyleSheet(
            "background: rgba(10,10,10,210); border: none;")
        chrome = QHBoxLayout(self._chrome_widget)
        chrome.setContentsMargins(8, 6, 8, 6)
        chrome.setSpacing(6)
        back_btn = self._chrome_btn(
            f"{config.ICON_BACK} Gallery",
            self.closeRequested.emit,
            "Back to gallery (Esc)")
        self._fs_btn = self._chrome_btn(
            config.ICON_FULLSCREEN, self._toggle_fs, "Toggle full screen")
        chrome.addWidget(back_btn)
        chrome.addStretch(1)
        chrome.addWidget(self._fs_btn)
        self._root.addWidget(self._chrome_widget)

        # Grid host (takes all remaining space)
        self._grid_host = QWidget()
        self._grid_host.setStyleSheet("background: #000; border: none;")
        self._grid = QGridLayout(self._grid_host)
        self._grid.setContentsMargins(0, 0, 0, 0)
        self._grid.setSpacing(1)
        self._root.addWidget(self._grid_host, 1)

        self._slots: list[_Slot] = []
        self._build_slots(3)

        # Side-scroll 60 fps timer
        self._ss_timer = QTimer(self)
        self._ss_timer.setInterval(16)
        self._ss_timer.timeout.connect(self._ss_tick)

        # Set-slideshow paged-advance timer (interval set on start).
        self._set_timer = QTimer(self)
        self._set_timer.timeout.connect(self._advance_set)

        # Bottom navigation / autoscroll bar
        self._autoscroll_widget = QWidget()
        self._autoscroll_widget.setStyleSheet(
            "background: rgba(10,10,10,210); border: none;")
        asl = QHBoxLayout(self._autoscroll_widget)
        asl.setContentsMargins(8, 6, 8, 6)
        asl.setSpacing(6)

        # Slideshow-style toggle (Set ⇄ Side-scroll)
        self._mode_btn = self._chrome_btn(
            "", self._toggle_mode, "Slideshow style (S)")
        self._mode_btn.setFixedWidth(86)

        self._prev_btn = self._chrome_btn(
            config.ICON_PREV, self.prev_page, "Previous page (←)", big=True)
        self._autoscroll_btn = self._chrome_btn(
            config.ICON_PLAY, self._toggle_autoscroll, "Play / pause slideshow (A)")
        self._autoscroll_spin = QSpinBox()
        self._autoscroll_spin.setFixedWidth(58)
        self._autoscroll_spin.setStyleSheet(
            f"QSpinBox {{ color: {config.FG_BRIGHT}; background: rgba(0,0,0,90);"
            f" border: 1px solid {config.FG_DIM}; border-radius: 4px;"
            " padding: 2px 4px; font-size: 12px; }"
            " QSpinBox::up-button, QSpinBox::down-button"
            f" {{ background: rgba(0,0,0,60); border: none; width: 14px; }}")
        self._autoscroll_spin.valueChanged.connect(self._on_autoscroll_spin_changed)
        self._apply_mode_to_spin()
        self._update_mode_btn()

        self._next_btn = self._chrome_btn(
            config.ICON_NEXT, self.next_page, "Next page (→)", big=True)
        self._counter = QLabel("")
        self._counter.setStyleSheet(
            f"color: {config.FG_MID}; background: rgba(0,0,0,90);"
            " border-radius: 4px; padding: 2px 8px;")

        # Layout picker (Auto / 3×1 / 2×2) and Fit/Fill toggle
        self._layout_btn = self._chrome_btn(
            "Auto", self._cycle_layout,
            "Tile layout: Auto / 3×1 / 2×2 (L)")
        self._layout_btn.setFixedWidth(56)
        self._fill_btn = self._chrome_btn(
            "Fit", self._toggle_fill, "Fit (letterbox) / Fill (crop) tiles (F)")
        self._fill_btn.setFixedWidth(46)

        # Orientation toggle: switch between portrait and landscape groups
        self._orient_btn = self._chrome_btn(
            "↔", self._switch_orientation,
            "Switch orientation group (⇔ portrait / landscape)")
        self._orient_btn.setFixedWidth(46)

        asl.addStretch(1)
        asl.addWidget(self._mode_btn)
        asl.addWidget(self._prev_btn)
        asl.addWidget(self._autoscroll_btn)
        asl.addWidget(self._autoscroll_spin)
        asl.addWidget(self._next_btn)
        asl.addWidget(self._counter)
        asl.addStretch(1)
        asl.addWidget(self._layout_btn)
        asl.addWidget(self._fill_btn)
        asl.addWidget(self._orient_btn)
        self._root.addWidget(self._autoscroll_widget)

        # Keyboard shortcuts
        QShortcut(QKeySequence(Qt.Key.Key_Escape), self,
                  activated=self.closeRequested.emit)
        QShortcut(QKeySequence(Qt.Key.Key_Left),  self, activated=self.prev_page)
        QShortcut(QKeySequence(Qt.Key.Key_Right), self, activated=self.next_page)
        QShortcut(QKeySequence(Qt.Key.Key_A),     self,
                  activated=self._toggle_autoscroll)
        QShortcut(QKeySequence(Qt.Key.Key_S),     self,
                  activated=self._toggle_mode)
        QShortcut(QKeySequence(Qt.Key.Key_L),     self,
                  activated=self._cycle_layout)
        QShortcut(QKeySequence(Qt.Key.Key_F),     self,
                  activated=self._toggle_fill)
        QShortcut(QKeySequence("Ctrl+M"),          self,
                  activated=self._unmute_all)

    # -- public API ------------------------------------------------------------

    def open(self, start_row: int) -> None:
        """Activate the panel from start_row's orientation group.

        Clears any previous scroll / pin state so each gallery→multiview
        transition starts fresh.
        """
        self._stop_slideshow()

        for s in self._slots:
            if s.is_pinned:
                s._is_pinned = False
                s._pin_btn.setIcon(s._pause_icon_off)
                s._pin_btn.setStyleSheet(s._PIN_OFF)
            s.clear()

        # Bias the partition toward the start item so the opening grid matches
        # its real orientation immediately (a single cheap peek — never the
        # whole folder).  The rest self-corrects via _on_model_dims_changed.
        path = self._model.path_at(start_row)
        self._refresh_orientation_lists(priority_path=path)

        if path and path in self._portrait_paths:
            self._current_paths = self._portrait_paths
            start_in_list = self._portrait_paths.index(path)
        elif path and path in self._landscape_paths:
            self._current_paths = self._landscape_paths
            start_in_list = self._landscape_paths.index(path)
        else:
            self._current_paths = self._landscape_paths or self._portrait_paths
            start_in_list = 0

        self._update_orient_btn()
        self._render(start_in_list)

    def stop_autoscroll(self) -> None:
        """Called by main window when navigating away from multi-view."""
        self._stop_slideshow()

    # -- orientation lists -----------------------------------------------------

    def _refresh_orientation_lists(self, priority_path: str | None = None) -> None:
        # Partition by dimensions ALREADY cached in the model.  Never peek_size
        # for the whole folder here — for videos that opens a cv2.VideoCapture
        # under a global lock, and doing it for thousands of files on the GUI
        # thread froze the UI.  As a bounded exception, a single priority_path
        # (the item multi-view is opening on) is peeked synchronously so the
        # opening grid matches its real orientation; everything else that isn't
        # measured yet buckets as landscape and is corrected by the async dims
        # job via _on_model_dims_changed.
        was_portrait = self._current_paths is self._portrait_paths
        portrait, landscape = [], []
        for i in range(self._model.rowCount()):
            path = self._model.path_at(i)
            if not path:
                continue
            w, h = self._model.dim_at(path)
            if (w <= 0 or h <= 0) and path == priority_path:
                try:
                    w, h = media.peek_size(path)
                except Exception:
                    w, h = 0, 0
            if w > 0 and h > 0 and w / h < 0.95:
                portrait.append(path)
            else:
                landscape.append(path)
        self._portrait_paths  = portrait
        self._landscape_paths = landscape
        self._current_paths = portrait if was_portrait else landscape

    def _on_model_dims_changed(self) -> None:
        """Dimensions arrived from the background job — re-partition and, in
        auto mode, flip the grid (3×1 ⇄ 2×2) to match the now-known orientation.

        The focused tile follows its item into whichever orientation group the
        real dimensions place it in, so vertical media that was provisionally
        shown as 2×2 (before its size was measured) switches to 3×1.
        """
        if not self.isVisible():
            return
        cur = (self._current_paths[self._start]
               if self._current_paths and self._start < len(self._current_paths)
               else None)
        self._refresh_orientation_lists()
        # Follow the focused item into its true orientation group.
        if cur is not None and cur in self._portrait_paths:
            self._current_paths = self._portrait_paths
            self._start = self._portrait_paths.index(cur)
        elif cur is not None and cur in self._landscape_paths:
            self._current_paths = self._landscape_paths
            self._start = self._landscape_paths.index(cur)
        else:
            self._start = min(self._start, max(0, len(self._current_paths) - 1))
        self._update_orient_btn()
        # Auto layout: re-render so the grid matches the corrected orientation.
        if self._forced_layout is None and not self._slideshow_active():
            self._render(self._start)

    def _detect_layout(self) -> int:
        """Forced layout if set, else 3 for portrait 3×1 / 4 for landscape 2×2."""
        if self._forced_layout in (3, 4):
            return self._forced_layout
        return 3 if (self._current_paths is self._portrait_paths) else 4

    # -- layout / slot management ----------------------------------------------

    def _build_slots(self, n: int) -> None:
        # Clean up any side-scroll state without re-rendering
        if self._ss_slot_order:
            self._ss_timer.stop()
            self._ss_slot_order.clear()
        for s in self._slots:
            s.clear()
            s.setParent(None)
        if self._ss_buf is not None:
            self._ss_buf.clear()
            self._ss_buf.setParent(None)
            self._ss_buf = None
        self._slots.clear()
        while self._grid.count():
            self._grid.takeAt(0)
        for k in range(4):
            self._grid.setColumnStretch(k, 0)
            self._grid.setRowStretch(k, 0)
        self._layout_slots = n
        positions = ([(0, 0), (0, 1), (0, 2)] if n == 3
                     else [(0, 0), (0, 1), (1, 0), (1, 1)])
        self._slot_grid_pos = list(positions)
        for i, (r, c) in enumerate(positions):
            slot = _Slot(i, self._favs, self)
            slot.setSizePolicy(QSizePolicy.Policy.Ignored,
                               QSizePolicy.Policy.Ignored)
            slot.enlarge.connect(self._on_enlarge)
            slot.favToggled.connect(self._on_slot_fav)
            slot.trashed.connect(self._on_slot_trash)
            slot.reordered.connect(self._on_reorder)
            slot.rotated.connect(self._on_slot_rotate)
            slot.pinned.connect(lambda *_: None)
            slot.set_fill(self._fill_mode)
            slot._img_pool = self._img_pool
            self._grid.addWidget(slot, r, c)
            self._slots.append(slot)
        # Extra slot for the side-scroll right-edge buffer (not in grid layout)
        self._ss_buf = _Slot(n, self._favs, self._grid_host)
        self._ss_buf.setSizePolicy(QSizePolicy.Policy.Ignored,
                                   QSizePolicy.Policy.Ignored)
        self._ss_buf.enlarge.connect(self._on_enlarge)
        self._ss_buf.favToggled.connect(self._on_slot_fav)
        self._ss_buf.trashed.connect(self._on_slot_trash)
        self._ss_buf.reordered.connect(self._on_reorder)
        self._ss_buf.rotated.connect(self._on_slot_rotate)
        self._ss_buf.set_fill(self._fill_mode)
        self._ss_buf._img_pool = self._img_pool
        self._ss_buf.hide()
        used_cols = 3 if n == 3 else 2
        used_rows = 1 if n == 3 else 2
        for cc in range(used_cols):
            self._grid.setColumnStretch(cc, 1)
        for rr in range(used_rows):
            self._grid.setRowStretch(rr, 1)

    def _switch_layout(self, n: int) -> None:
        """Rebuild grid for n slots, restoring pinned media to first slots."""
        saved = [(s._path, s._row, s._speed_idx)
                 for s in self._slots if s.is_pinned]
        self._build_slots(n)
        for (path, row, speed), slot in zip(saved[:n], self._slots):
            if path:
                slot._speed_idx = speed
                slot._speed_btn.setText(f"{slot._SPEEDS[speed]:g}×")
                slot.show_item(row if row >= 0 else 0, path)
                slot._is_pinned = True
                slot._pin_btn.setIcon(slot._pause_icon_on)
                slot._pin_btn.setStyleSheet(slot._PIN_ON)
                slot._bar_hide_timer.stop()
                slot._btnbar.show()

    # -- side-scroll -----------------------------------------------------------

    def _ss_n_visible(self) -> int:
        """Tiles visible simultaneously during side-scroll.

        Landscape uses 2 rather than 4 so each tile has a sensible width.
        """
        return 2 if (self._current_paths is self._landscape_paths) else 3

    def _cleanup_sidescroll(self) -> None:
        """Stop the scroll timer and restore all slots to the grid layout.

        Safe to call when side-scroll is not active (idempotent).
        """
        self._ss_timer.stop()
        if not self._ss_slot_order:
            return
        n_vis = self._ss_n_visible()
        # Re-add the slots that were removed from the grid
        for i in range(min(n_vis, len(self._slots))):
            r, c = self._slot_grid_pos[i]
            self._grid.addWidget(self._slots[i], r, c)
        # Show all grid slots; hide buffer
        for slot in self._slots:
            slot.show()
        if self._ss_buf is not None:
            self._ss_buf.hide()
        self._ss_slot_order.clear()

    def _start_sidescroll(self) -> None:
        n_paths = len(self._current_paths)
        if n_paths == 0:
            return
        n_vis = self._ss_n_visible()

        # Remove the visible slots from the grid layout so we can position them
        # freely; extra grid slots (n_vis..n) are just hidden.
        for i in range(n_vis):
            self._grid.removeWidget(self._slots[i])
        for i in range(n_vis, len(self._slots)):
            self._slots[i].hide()

        self._ss_buf.show()
        self._ss_slot_order = list(self._slots[:n_vis]) + [self._ss_buf]

        # Load initial content into all n_vis + 1 slots
        self._ss_head_idx = self._start
        self._ss_px = 0.0
        for i, slot in enumerate(self._ss_slot_order):
            path = self._current_paths[(self._ss_head_idx + i) % n_paths]
            row = self._model.row_for_path(path)
            slot.show_item(row if row >= 0 else 0, path)

        self._ss_reposition()
        self._ss_timer.start()

    def _ss_reposition(self) -> None:
        """Set slot geometries based on current scroll offset."""
        n_vis = self._ss_n_visible()
        gw = max(1, self._grid_host.width())
        gh = max(1, self._grid_host.height())
        slot_w = gw / n_vis
        for i, slot in enumerate(self._ss_slot_order):
            x = int(round(i * slot_w - self._ss_px))
            slot.setGeometry(x, 0, int(round(slot_w)), gh)

    def _ss_tick(self) -> None:
        n_paths = len(self._current_paths)
        if n_paths == 0:
            return
        n_vis = self._ss_n_visible()
        slot_w = max(1, self._grid_host.width()) / n_vis

        self._ss_px += self._ss_speed

        # When the leftmost slot has fully exited, recycle it to the right
        if self._ss_px >= slot_w:
            self._ss_px -= slot_w
            self._ss_head_idx = (self._ss_head_idx + 1) % n_paths
            exiting = self._ss_slot_order.pop(0)
            new_idx = (self._ss_head_idx + n_vis) % n_paths
            path = self._current_paths[new_idx]
            row = self._model.row_for_path(path)
            exiting.show_item(row if row >= 0 else 0, path)
            self._ss_slot_order.append(exiting)

        self._ss_reposition()

    # -- rendering -------------------------------------------------------------

    def _render(self, start: int) -> None:
        paths = self._current_paths
        if not paths:
            for s in self._slots:
                if not s.is_pinned:
                    s.clear()
            self._counter.setText("0 of 0")
            return

        want = self._detect_layout()
        if want != self._layout_slots:
            self._switch_layout(want)

        self._start = max(0, min(start, len(paths) - 1))
        pinned_set = {s._path for s in self._slots if s.is_pinned and s._path}

        idx = self._start
        shown_end = self._start
        for slot in self._slots:
            if slot.is_pinned:
                continue
            while idx < len(paths) and paths[idx] in pinned_set:
                idx += 1
            if idx < len(paths):
                path = paths[idx]
                row = self._model.row_for_path(path)
                slot.show_item(row if row >= 0 else idx, path)
                shown_end = idx + 1
                idx += 1
            elif paths:
                path = paths[idx % len(paths)]
                row = self._model.row_for_path(path)
                slot.show_item(row if row >= 0 else 0, path)
                shown_end = len(paths)
                idx += 1
            else:
                slot.clear()

        total = len(paths)
        self._counter.setText(
            f"{self._start + 1}–{min(shown_end, total)} of {total}")

    # -- navigation ------------------------------------------------------------

    def next_page(self) -> None:
        self._stop_slideshow()
        if not self._current_paths:
            return
        step = sum(1 for s in self._slots if not s.is_pinned) or 1
        nxt = self._start + step
        self._render(0 if nxt >= len(self._current_paths) else nxt)

    def prev_page(self) -> None:
        self._stop_slideshow()
        if not self._current_paths:
            return
        step = sum(1 for s in self._slots if not s.is_pinned) or 1
        self._render(max(0, self._start - step))

    # -- chrome / UI helpers ---------------------------------------------------

    def _chrome_btn(self, glyph, cb, tip="", big=False) -> QToolButton:
        b = QToolButton()
        b.setText(glyph)
        b.setToolTip(tip)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        size   = 26 if big else 18
        weight = "bold" if big else "normal"
        b.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG}; font-size: {size}px;"
            f" font-weight: {weight}; background: rgba(0,0,0,90); border: none;"
            " border-radius: 4px; padding: 2px 10px; }"
            " QToolButton:hover { color: #ffffff;"
            " background: rgba(0,0,0,160); border-radius: 4px; }")
        b.clicked.connect(cb)
        return b

    def _toggle_fs(self) -> None:
        w = self.window()
        if w.isFullScreen():
            w.showNormal()
        else:
            w.showFullScreen()
            w.raise_()
            w.activateWindow()

    # -- auto-slideshow (set / side-scroll) ------------------------------------

    def _slideshow_active(self) -> bool:
        return self._ss_timer.isActive() or self._set_timer.isActive()

    def _toggle_autoscroll(self) -> None:
        """Play / pause the slideshow in whichever style is selected."""
        if self._slideshow_active():
            self._stop_slideshow()
        else:
            self._start_slideshow()

    def _start_slideshow(self) -> None:
        if not self._current_paths:
            return
        if self._slideshow_mode == "scroll":
            self._start_sidescroll()
            active = self._ss_timer.isActive()
        else:
            self._set_timer.start(self._set_interval_s * 1000)
            active = True
        if active:
            self._autoscroll_btn.setText(config.ICON_PAUSE)

    def _stop_slideshow(self) -> None:
        """Stop both slideshow styles and restore the play icon (idempotent)."""
        self._set_timer.stop()
        if self._ss_timer.isActive():
            self._cleanup_sidescroll()
            self._render(self._ss_head_idx)
        self._autoscroll_btn.setText(config.ICON_PLAY)

    def _advance_set(self) -> None:
        """Paged advance for the 'set' slideshow style (wraps at the end)."""
        if not self._current_paths:
            self._set_timer.stop()
            return
        step = sum(1 for s in self._slots if not s.is_pinned) or 1
        nxt = self._start + step
        self._render(0 if nxt >= len(self._current_paths) else nxt)

    def _cycle_layout(self) -> None:
        """Cycle the tile layout: Auto → 3×1 → 2×2 → Auto."""
        order = [None, 3, 4]
        cur = order.index(self._forced_layout) if self._forced_layout in order else 0
        self._forced_layout = order[(cur + 1) % len(order)]
        self._update_layout_btn()
        self._stop_slideshow()
        self._render(self._start)

    def _update_layout_btn(self) -> None:
        self._layout_btn.setText(
            {None: "Auto", 3: "3×1", 4: "2×2"}[self._forced_layout])

    def _toggle_fill(self) -> None:
        """Toggle cover-fill (crop to fill tiles) vs fit (letterbox)."""
        self._fill_mode = not self._fill_mode
        self._fill_btn.setText("Fill" if self._fill_mode else "Fit")
        for slot in self._slots:
            slot.set_fill(self._fill_mode)
        if self._ss_buf is not None:
            self._ss_buf.set_fill(self._fill_mode)

    def _toggle_mode(self) -> None:
        """Switch slideshow style (Set ⇄ Side-scroll); stops any active run."""
        self._stop_slideshow()
        self._slideshow_mode = (
            "scroll" if self._slideshow_mode == "set" else "set")
        self._apply_mode_to_spin()
        self._update_mode_btn()

    def _update_mode_btn(self) -> None:
        if self._slideshow_mode == "set":
            self._mode_btn.setText("⊞ Set")
            self._mode_btn.setToolTip(
                "Style: paged sets (3×1 / 2×2) — click for side-scroll (S)")
        else:
            self._mode_btn.setText("⇆ Scroll")
            self._mode_btn.setToolTip(
                "Style: smooth side-scroll — click for paged sets (S)")

    def _apply_mode_to_spin(self) -> None:
        """Reconfigure the spin-box for the active style's units."""
        sp = self._autoscroll_spin
        sp.blockSignals(True)
        if self._slideshow_mode == "set":
            sp.setRange(1, 60)
            sp.setSuffix(" s")
            sp.setValue(self._set_interval_s)
            sp.setToolTip("Seconds each set is shown")
        else:
            sp.setRange(1, 10)
            sp.setSuffix("×")
            sp.setValue(self._scroll_level)
            sp.setToolTip("Side-scroll speed (1×–10×)")
        sp.blockSignals(False)

    def _on_autoscroll_spin_changed(self, val: int) -> None:
        if self._slideshow_mode == "set":
            self._set_interval_s = val
            if self._set_timer.isActive():
                self._set_timer.start(val * 1000)
        else:
            self._scroll_level = val
            self._ss_speed = val * 0.5   # 0.5–5.0 px per 16 ms tick

    def _unmute_all(self) -> None:
        """Unmute every video slot currently displayed (Ctrl+M)."""
        for slot in self._slots:
            slot.unmute()
        if self._ss_buf is not None:
            self._ss_buf.unmute()

    def _switch_orientation(self) -> None:
        """Toggle between portrait and landscape orientation groups."""
        self._stop_slideshow()
        if self._current_paths is self._portrait_paths:
            if self._landscape_paths:
                self._current_paths = self._landscape_paths
        else:
            if self._portrait_paths:
                self._current_paths = self._portrait_paths
        self._update_orient_btn()
        self._render(0)

    def _update_orient_btn(self) -> None:
        if self._current_paths is self._portrait_paths:
            other_n = len(self._landscape_paths)
            self._orient_btn.setText("↔")
            self._orient_btn.setToolTip(
                f"Switch to landscape media ({other_n} items)")
            self._orient_btn.setEnabled(bool(self._landscape_paths))
        else:
            other_n = len(self._portrait_paths)
            self._orient_btn.setText("↕")
            self._orient_btn.setToolTip(
                f"Switch to portrait media ({other_n} items)")
            self._orient_btn.setEnabled(bool(self._portrait_paths))

    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        if self._ss_timer.isActive():
            self._ss_reposition()

    # -- slot signal handlers --------------------------------------------------

    def _on_enlarge(self, row: int) -> None:
        self.openLightbox.emit(row)
        self.closeRequested.emit()

    def _on_slot_fav(self, path: str) -> None:
        if path:
            self.favToggled.emit(path)
            if not self._ss_timer.isActive():
                self._render(self._start)

    def _on_slot_rotate(self, path: str) -> None:
        if path:
            self.rotated.emit(path)

    def reload_path(self, path: str) -> None:
        """Reload any slot showing `path` after its pixels changed on disk.

        Re-partitions orientation groups (a 90° turn flips portrait↔landscape)
        and re-decodes the rotated file into every slot currently showing it.
        """
        self._refresh_orientation_lists()
        self._update_orient_btn()
        targets = list(self._slots)
        if self._ss_buf is not None:
            targets.append(self._ss_buf)
        for slot in targets:
            if slot._path == path:
                slot.show_item(slot._row, path)

    def _on_slot_trash(self, path: str) -> None:
        if path:
            self.trashed.emit(path)
            self._stop_slideshow()
            self._refresh_orientation_lists()
            self._update_orient_btn()
            start = min(self._start, max(0, len(self._current_paths) - 1))
            self._render(start)

    def _on_reorder(self, src_path: str, dst_path: str) -> None:
        self._stop_slideshow()
        cur_path = (self._current_paths[self._start]
                    if self._current_paths and self._start < len(self._current_paths)
                    else None)
        self._model.swap_paths(src_path, dst_path)
        self._refresh_orientation_lists()
        self._update_orient_btn()
        if cur_path and cur_path in self._current_paths:
            self._start = self._current_paths.index(cur_path)
        self._render(self._start)
