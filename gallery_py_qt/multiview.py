"""Multi-view: adaptive 3×1 (portrait) / 2×2 (landscape) grid of slots.

Embedded as a panel inside the main window — not a separate dialog.  Shown via
MainWindow._open_multiview(), hidden via the closeRequested signal.

Layout rules
  • Portrait content  → 3 tiles in a single row (3×1)
  • Landscape / square → 2×2 grid
  • Media orientation groups never mix in the same view.
  • Orientation switch button lets the user jump to the other group.
  • Switching layout (3×1 ↔ 2×2) preserves pinned media.
  • Tiles are positioned by _layout_tiles(), not a uniform grid: in Fit mode
    each tile takes exactly its media's aspect box (justified rows, 1 px
    seams, no internal letterboxing); Fill mode covers the panel edge-to-edge
    with centre-cropped tiles.  The top/bottom bars auto-hide after idle
    (mouse move or H restores them) so media gets the full panel.

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
import sys

from PySide6.QtCore import (Qt, QUrl, Signal, QSizeF, QSize, QRectF, QTimer,
                            QMimeData, QObject, QRunnable, QThreadPool, QEvent)
from PySide6.QtWidgets import (QWidget, QVBoxLayout,
                               QHBoxLayout, QLabel, QToolButton, QStackedWidget,
                               QGraphicsScene, QGraphicsView, QMenu,
                               QGraphicsDropShadowEffect,
                               QSpinBox, QSlider, QApplication)
from PySide6.QtGui import (QPixmap, QImage, QKeySequence, QShortcut, QPalette,
                           QColor, QPainter, QIcon, QDrag)
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput, QtAudio
from PySide6.QtMultimediaWidgets import QGraphicsVideoItem

from . import config
from .engine import media, shell, tags
from .gifplayer import GifPlayer
from .help_overlay import make_help_panel, toggle_help_panel
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
    A zoom factor scales the fit/fill target box, so the user can dial the
    media's coverage of its tile up or down without changing the mode.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._src = None
        self._fill = False
        self._zoom = 1.0
        self._ox = 0.5           # horizontal pan (0..1) when overflowing
        self._oy = 0.5           # vertical pan
        self._ovx = False        # currently overflows horizontally / vertically
        self._ovy = False
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

    def show_message(self, text: str) -> None:
        """Drop any pixmap and show centred text (e.g. an unplayable-file note)."""
        self._src = None
        self.setText(text)

    def set_fill(self, fill: bool) -> None:
        self._fill = fill
        self._apply()

    def set_zoom(self, zoom: float) -> None:
        self._zoom = max(0.05, float(zoom))
        self._apply()

    def set_offset(self, ox: float, oy: float) -> None:
        """Which part of an overflowing image is shown: 0..1 per axis
        (0 = left/top edge, 0.5 = centred, 1 = right/bottom edge)."""
        self._ox = min(1.0, max(0.0, float(ox)))
        self._oy = min(1.0, max(0.0, float(oy)))
        self._apply()

    def overflow(self) -> "tuple[bool, bool]":
        """(horizontal, vertical) — whether the fitted image exceeds the label
        on each axis, i.e. whether panning that axis does anything."""
        return self._ovx, self._ovy

    def _apply(self) -> None:
        if self._src is not None and not self._src.isNull():
            sz = self.size()
            if sz.width() > 0 and sz.height() > 0:
                mode = (Qt.AspectRatioMode.KeepAspectRatioByExpanding
                        if self._fill else Qt.AspectRatioMode.KeepAspectRatio)
                # Zoom scales the target box; >100% therefore overflows the
                # label and is cropped, <100% letterboxes further.
                target = QSize(max(1, int(round(sz.width() * self._zoom))),
                               max(1, int(round(sz.height() * self._zoom))))
                scaled = self._src.scaled(
                    target, mode, Qt.TransformationMode.SmoothTransformation)
                self._ovx = scaled.width() > sz.width()
                self._ovy = scaled.height() > sz.height()
                if self._ovx or self._ovy:
                    # Crop the overflow, offset by _ox/_oy (centre by default).
                    cw = min(scaled.width(), sz.width())
                    ch = min(scaled.height(), sz.height())
                    x = int(round((scaled.width() - cw) * self._ox))
                    y = int(round((scaled.height() - ch) * self._oy))
                    scaled = scaled.copy(x, y, cw, ch)
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
    rotated    = Signal(str, int)   # (path, degrees clockwise) — permanent
    tagSetChanged = Signal()        # a new tag was coined inline

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
        self._zoom = 1.0             # media coverage of the tile (±10% steps)
        self._ox = 0.5               # pan offset (0..1) when media overflows
        self._oy = 0.5
        self._vid_ovx = False        # video overflows horizontally / vertically
        self._vid_ovy = False
        # Off-thread still decoding (set by MultiView after construction).
        self._img_pool: "QThreadPool | None" = None
        self._img_gen = 0
        self._img_sig = _ImgSignals(self)
        self._img_sig.ready.connect(self._on_img_decoded)
        self.setAcceptDrops(True)

        # Tiny tag buttons along the bottom of the media: one click toggles
        # the descriptor on the file's tags metadata (JSON store + best-effort
        # embed).  Rebuilt when the user customises the tag set.
        # The bar itself never paints — it is a bare positioning container, so
        # the media shows through between the tag chips (matching _btnbar).
        self._tagbar = QWidget(self)
        self._tagbar.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        # With no bar behind them the chips would vanish on bright media, so a
        # tight dark shadow gives every glyph its own contrast instead.
        _tagshadow = QGraphicsDropShadowEffect(self._tagbar)
        _tagshadow.setBlurRadius(4)
        _tagshadow.setOffset(0, 1)
        _tagshadow.setColor(QColor(0, 0, 0, 230))
        self._tagbar.setGraphicsEffect(_tagshadow)
        self._taglay = QHBoxLayout(self._tagbar)
        self._taglay.setContentsMargins(2, 1, 2, 1)
        self._taglay.setSpacing(2)
        self._tag_btns: dict[str, QToolButton] = {}
        # Resting state shows only the tags this file HAS — that is the part
        # worth reading while scanning a grid.  The full, editable chip row
        # (every tag plus ditto / new-tag / per-tile zoom) appears on hover,
        # matching how _btnbar already behaves, so a 3x2 page paints a handful
        # of coloured labels instead of ~78 permanent controls.
        self._tag_editing = False
        self.rebuild_tag_buttons()

        p = self.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        self.setPalette(p)
        self.setAutoFillBackground(True)

        self._is_video = False
        self._last_vid_geom = None      # last geometry applied to _video_item
        self._last_shown_sec = -1       # last whole second painted in _time
        self._disp = QSizeF(0, 0)
        self._pm: QPixmap = QPixmap()

        # Image page
        self._img = _AspectLabel()

        # Video page
        self._scene = QGraphicsScene(self)
        # One item, no spatial queries needed — skip the BSP index churn.
        self._scene.setItemIndexMethod(QGraphicsScene.ItemIndexMethod.NoIndex)
        self._gview = QGraphicsView(self._scene)
        self._gview.setFrameStyle(0)
        self._gview.setStyleSheet("background: #000; border: none;")
        self._gview.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._gview.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # Render video on the GPU: with the default raster viewport,
        # QGraphicsVideoItem converts every frame YUV->RGB on the CPU — four
        # simultaneous videos saturated the GUI thread and every click lagged.
        # An OpenGL viewport uploads frames as textures instead.  Set the env
        # var GALLERY_NO_GL=1 to force the raster path if GL misbehaves.
        # The GL viewport is installed LAZILY, on the first video this slot
        # shows.  Creating one per slot up front meant a page of six tiles held
        # six OpenGL contexts even when every tile was a still image — real
        # cost on Windows, where per-context switching and swap-chain work is a
        # known source of periodic stutter.
        self._gl_viewport = False
        # Full-viewport updates are cheaper than per-frame dirty-region math
        # for a constantly-changing video (and required for the GL path).
        self._gview.setViewportUpdateMode(
            QGraphicsView.ViewportUpdateMode.FullViewportUpdate)

        # Media body passes mouse events through so slot can start a reorder
        # drag.  NOTE: viewport() must be fetched AFTER any setViewport above.
        for w in (self._img, self._gview, self._gview.viewport()):
            w.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self._video_item = QGraphicsVideoItem()
        self._video_item.setAspectRatioMode(Qt.AspectRatioMode.KeepAspectRatio)
        self._scene.addItem(self._video_item)
        self._video_item.nativeSizeChanged.connect(lambda *_: self._fit_video())

        # Playback stall diagnostic.  Set GALLERY_VIDEO_DIAG=1 to log every
        # gap between presented frames that exceeds twice the expected spacing,
        # with the media position it happened at.  That distinguishes a stall
        # tied to a point IN THE FILE from one tied to wall-clock (a timer, a
        # background pass), which is the thing worth knowing when a hitch can't
        # be reproduced on another machine.
        self._diag = os.environ.get("GALLERY_VIDEO_DIAG") == "1"
        self._diag_last = 0.0
        if self._diag:
            try:
                self._video_item.videoSink().videoFrameChanged.connect(
                    self._on_diag_frame)
            except Exception:
                self._diag = False

        self._player = QMediaPlayer(self)
        self._player.setVideoOutput(self._video_item)
        self._player.setLoops(QMediaPlayer.Loops.Infinite)
        self._player.mediaStatusChanged.connect(self._on_status)
        self._player.errorOccurred.connect(self._on_media_error)
        self._player.positionChanged.connect(self._on_pos)
        self._player.durationChanged.connect(self._on_dur)

        # Per-tile audio output is created LAZILY on first unmute: opening the
        # audio device per slot (5+ per multi-view) is slow and, headless,
        # can stall.  Tiles are muted by default, so most never need one.
        self._audio = None
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
        # Rotate: a click still does the common 90°, but the drop-down offers
        # 180°/270° too, so a tile no longer has to be maximised to be
        # straightened by an arbitrary right angle.
        self._rot_btn = self._mk(config.ICON_ROTATE_CW,
                                 lambda: self._rotate(90), ol)
        self._rot_menu = QMenu(self._rot_btn)
        for _deg in (90, 180, 270):
            act = self._rot_menu.addAction(f"Rotate {_deg}°")
            act.setToolTip(f"Rotate {_deg}° clockwise (permanent)")
            act.triggered.connect(lambda _=False, d=_deg: self._rotate(d))
        self._rot_btn.setMenu(self._rot_menu)
        self._rot_btn.setPopupMode(
            QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        self._rot_btn.setToolTip(
            "Rotate 90° clockwise — use the arrow for 180° / 270°")
        self._mk(config.ICON_TRASH,     self._trash, ol)

        self._bar_hide_timer = QTimer(self)
        self._bar_hide_timer.setSingleShot(True)
        self._bar_hide_timer.setInterval(400)
        self._bar_hide_timer.timeout.connect(self._maybe_hide_bar)
        self._btnbar.hide()

        # Source header: a minimal, always-visible hyperlink naming the file,
        # which reveals it in the OS file manager.  Deliberately chrome-free —
        # no bar, no box — so it reads as part of the media, not over it.
        # Side-scroll reuses these same slots, so this covers both views.
        self._srclink = QLabel(self)
        self._srclink.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self._srclink.setTextFormat(Qt.TextFormat.RichText)
        self._srclink.setOpenExternalLinks(False)
        self._srclink.setTextInteractionFlags(
            Qt.TextInteractionFlag.LinksAccessibleByMouse)
        self._srclink.setCursor(Qt.CursorShape.PointingHandCursor)
        self._srclink.setStyleSheet(
            "QLabel { background: transparent; padding: 1px 4px; }")
        self._srclink.linkActivated.connect(self._open_source)
        self._srclink.hide()

        # Pan arrow buttons — shown (on hover) only when the media, scaled to
        # Fill or zoomed past 100%, overflows the tile, so the user can choose
        # which part of an over-scaled image OR video is visible.  Arrows (a
        # fixed nudge per click) are used instead of a slider so the crop is
        # re-rendered a handful of times, not continuously as a slider is dragged.
        self._pan_btns: "list[QToolButton]" = []
        self._pan_left  = self._mk_pan("◀", -self._PAN_STEP, 0.0)
        self._pan_right = self._mk_pan("▶",  self._PAN_STEP, 0.0)
        self._pan_up    = self._mk_pan("▲", 0.0, -self._PAN_STEP)
        self._pan_down  = self._mk_pan("▼", 0.0,  self._PAN_STEP)

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
        self._scrub.loopChanged.connect(self._on_loop_changed)
        # Active A–B loop bounds in ms (None = not looping).
        self._loop_a_ms: "int | None" = None
        self._loop_b_ms: "int | None" = None
        # Animated-GIF playback (frames pushed into _img).
        self._gif = GifPlayer(self)
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
        # Auto-dismiss the volume slider a moment after the last adjustment, so
        # it doesn't linger over the media once the level has been set.
        self._vol_hide_timer = QTimer(self)
        self._vol_hide_timer.setSingleShot(True)
        self._vol_hide_timer.setInterval(1400)
        self._vol_hide_timer.timeout.connect(self._vol_popup.hide)

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
        self.set_tag_editing(True)       # expand to the full, editable chip row
        self._position_overlays()        # reveal pan sliders if media overflows

    def leaveEvent(self, e):
        super().leaveEvent(e)
        self._bar_hide_timer.start()

    def _maybe_hide_bar(self) -> None:
        # Opening the rotate drop-down moves the pointer off the tile, which
        # would otherwise hide the very bar the popup belongs to.  Keep the bar
        # while any popup is up and re-arm (mirrors MultiView._maybe_hide_bars).
        if QApplication.activePopupWidget() is not None:
            self._bar_hide_timer.start()
            return
        if not self._is_pinned:
            self._btnbar.hide()
            for b in self._pan_btns:     # pan arrows follow the button bar
                b.hide()
            self.set_tag_editing(False)  # collapse back to just the set tags
            self._position_overlays()

    # -- video sizing ----------------------------------------------------------
    def _on_diag_frame(self, frame) -> None:
        """Log frame-to-frame stalls during playback (GALLERY_VIDEO_DIAG=1)."""
        import time as _t
        now = _t.perf_counter()
        prev, self._diag_last = self._diag_last, now
        if not prev or not self._is_video:
            return
        gap_ms = (now - prev) * 1000.0
        # 16fps clips space frames ~62ms apart; flag anything over ~2x a
        # 30fps budget so the log stays short and only real hitches appear.
        if gap_ms >= 70.0:
            pos = self._player.position() / 1000.0 if self._player else -1
            print(f"[video-diag] slot {self._idx} "
                  f"{os.path.basename(self._path or '?')}: "
                  f"{gap_ms:6.1f}ms gap at media {pos:5.2f}s", file=sys.stderr)

    def _ensure_gl_viewport(self) -> None:
        """Install the OpenGL viewport the first time this slot plays a video.

        With the raster viewport QGraphicsVideoItem converts every frame
        YUV->RGB on the CPU, which saturates the GUI thread with several videos
        running; a GL viewport uploads frames as textures instead.  Doing it on
        demand keeps image-only pages free of GL contexts entirely.
        Set GALLERY_NO_GL=1 to stay on the raster path.
        """
        if self._gl_viewport or os.environ.get("GALLERY_NO_GL") == "1":
            return
        try:
            from PySide6.QtOpenGLWidgets import QOpenGLWidget
            self._gview.setViewport(QOpenGLWidget())
            self._gl_viewport = True
            # viewport() is a NEW widget after the swap — re-apply the
            # click-through flag or the tile stops starting reorder drags.
            self._gview.viewport().setAttribute(
                Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        except Exception:
            self._gl_viewport = False

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
            scale *= self._zoom          # ±10% coverage steps
            disp_w, disp_h = ns.width() * scale, ns.height() * scale
        # Offset by _ox/_oy on whichever axis overflows (centre = 0.5); a
        # letterboxed axis stays centred.
        self._vid_ovx = disp_w > vw + 0.5
        self._vid_ovy = disp_h > vh + 0.5
        px = (vw - disp_w) * self._ox if self._vid_ovx else (vw - disp_w) / 2.0
        py = (vh - disp_h) * self._oy if self._vid_ovy else (vh - disp_h) / 2.0
        # Resizing a QGraphicsVideoItem makes the backend rebuild its output
        # surface — a visible hitch mid-playback.  This runs on every layout
        # pass (hover, pan, tag refresh, the chrome auto-hide, zoom…), almost
        # always with identical numbers, so apply it only when it actually
        # changed.  Rounded to 0.01px: sub-pixel float noise is not a resize.
        geom = (round(disp_w, 2), round(disp_h, 2),
                round(px, 2), round(py, 2), vw, vh)
        if geom != self._last_vid_geom:
            self._last_vid_geom = geom
            self._video_item.setSize(QSizeF(disp_w, disp_h))
            self._video_item.setPos(px, py)
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
        # Read the pixmap the label actually rendered rather than recomputing
        # the scale: duplicating the maths drifted a pixel against _apply()'s
        # rounding, leaving the overlays slightly wider than the media.
        shown = self._img.pixmap()
        if shown is not None and not shown.isNull():
            dw = min(max(1, shown.width()), cw)
            dh = min(max(1, shown.height()), ch)
            return (cw - dw) // 2, (ch - dh) // 2, dw, dh
        pw, ph = self._pm.width(), self._pm.height()
        if pw <= 0 or ph <= 0:
            return 0, 0, cw, ch
        scale = min(cw / pw, ch / ph) * self._zoom
        dw = min(max(1, int(pw * scale)), cw)
        dh = min(max(1, int(ph * scale)), ch)
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
        # Source link sits just under the button bar, left-aligned to the media.
        if self._srclink.isVisible():
            lh = self._srclink.sizeHint().height()
            lw = min(self._srclink.sizeHint().width(), max(1, dw))
            self._srclink.setGeometry(max(0, x), max(0, y + bh), lw, lh)
            self._srclink.raise_()
        th = self._tagbar.sizeHint().height()
        seek_h = (max(20, self._seekwrap.sizeHint().height()) + 6
                  if self._is_video else 0)
        self._tagbar.setGeometry(max(0, x),
                                 max(0, y + dh - th - seek_h),
                                 max(1, dw), th)
        self._tagbar.raise_()
        if self._is_video:
            self._position_seek()
        self._position_pan(x, y, dw, dh)

    _PAN_BTN = 20                       # arrow button size (px)

    def _position_pan(self, x: int, y: int, dw: int, dh: int) -> None:
        """Show pan arrows (on hover) for whichever axis the media overflows."""
        ovx, ovy = self._media_overflow()
        # Follow the button bar's hover state so they aren't permanent chrome; a
        # pinned tile keeps them like it keeps the bar.  Use isHidden() (the
        # bar's own state) not isVisible() (which also needs the tile shown).
        active = not self._btnbar.isHidden()
        s = self._PAN_BTN
        cx = max(0, x) + (dw - s) // 2
        cy = max(0, y) + (dh - s) // 2
        # Horizontal arrows hug the left/right edges, centred vertically.
        show_h = ovx and active
        if show_h:
            self._pan_left.setGeometry(max(0, x) + 2, cy, s, s)
            self._pan_right.setGeometry(max(0, x) + dw - s - 2, cy, s, s)
            self._pan_left.raise_(); self._pan_right.raise_()
        self._pan_left.setVisible(show_h)
        self._pan_right.setVisible(show_h)
        # Vertical arrows hug the top/bottom edges, centred horizontally.
        show_v = ovy and active
        if show_v:
            self._pan_up.setGeometry(cx, max(0, y) + 2, s, s)
            self._pan_down.setGeometry(cx, max(0, y) + dh - s - 2, s, s)
            self._pan_up.raise_(); self._pan_down.raise_()
        self._pan_up.setVisible(show_v)
        self._pan_down.setVisible(show_v)

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

    def set_zoom(self, zoom: float) -> None:
        """Scale how much of the tile the media covers (1.0 = plain fit/fill)."""
        self._zoom = max(0.05, float(zoom))
        self._img.set_zoom(self._zoom)
        if self._is_video:
            self._fit_video()
        self._position_overlays()
        self._sync_tile_zoom_btns()

    # -- per-tile zoom (independent of the multi-view-wide ±) -------------------
    _TILE_ZOOM_STEP = 0.10
    _TILE_ZOOM_MIN  = 0.50
    _TILE_ZOOM_MAX  = 2.00

    def _tile_zoom(self, delta: float) -> None:
        """Resize just THIS tile's media in 10% steps.

        Independent of MultiView's global ±; note that using the global control
        afterwards re-applies its value to every tile, overriding this.
        """
        z = round(self._zoom + delta, 2)
        z = round(max(self._TILE_ZOOM_MIN, min(self._TILE_ZOOM_MAX, z)), 2)
        self.set_zoom(z)

    def _sync_tile_zoom_btns(self) -> None:
        out = getattr(self, "_tile_zoom_out_btn", None)
        if out is None:
            return
        out.setEnabled(self._zoom > self._TILE_ZOOM_MIN + 1e-9)
        self._tile_zoom_in_btn.setEnabled(self._zoom < self._TILE_ZOOM_MAX - 1e-9)

    _PAN_STEP = 0.12                    # fraction of the overflow nudged per click

    def _mk_pan(self, glyph: str, dox: float, doy: float) -> "QToolButton":
        b = QToolButton(self)
        b.setText(glyph)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setToolTip("Reposition media")
        b.setAutoRepeat(True)          # press-and-hold nudges at a steady rate
        b.setAutoRepeatDelay(300)
        b.setAutoRepeatInterval(120)
        b.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG};"
            " background: rgba(0,0,0,120); border: none; border-radius: 3px;"
            " font-size: 11px; padding: 0; }"
            " QToolButton:hover { background: rgba(0,0,0,180); }")
        b.clicked.connect(lambda _=False: self._nudge_pan(dox, doy))
        b.hide()
        self._pan_btns.append(b)
        return b

    def _nudge_pan(self, dox: float, doy: float) -> None:
        self._set_pan(self._ox + dox, self._oy + doy)

    def _set_pan(self, ox: "float | None", oy: "float | None") -> None:
        """Reposition over-scaled media; None leaves that axis unchanged."""
        if ox is not None:
            self._ox = min(1.0, max(0.0, ox))
        if oy is not None:
            self._oy = min(1.0, max(0.0, oy))
        self._img.set_offset(self._ox, self._oy)
        if self._is_video:
            self._fit_video()          # re-lays overlays (and arrows) itself
        else:
            self._position_overlays()

    def _reset_pan(self) -> None:
        self._ox = self._oy = 0.5
        self._img.set_offset(0.5, 0.5)

    def _media_overflow(self) -> "tuple[bool, bool]":
        """(horizontal, vertical) overflow of the current media past the tile."""
        if self._is_video:
            return self._vid_ovx, self._vid_ovy
        return self._img.overflow()

    # -- content ---------------------------------------------------------------
    def release_media(self) -> None:
        """Stop playback AND release the file handle.

        stop() alone keeps the source open — on Windows that locks the file
        and makes deleting a video that is (or recently was) playing fail.
        """
        self._gif.stop()                        # release the GIF's file handle too
        self._player.stop()
        self._player.setSource(QUrl())

    def clear(self) -> None:
        self._img_gen += 1                      # drop any in-flight decode
        self.release_media()
        self._img.clear_source()
        self._seekwrap.hide()
        self._vol_popup.hide()
        for b in self._pan_btns:
            b.hide()
        self._clear_loop()
        self._reset_pan()
        self._last_vid_geom = None
        self._is_video = False
        self._row  = -1
        self._path = ""
        self._pm   = QPixmap()
        self._reset_audio()
        self._refresh_srclink()
        self._sync_rotate_btn()
        self._position_overlays()

    # Tag chips float directly on the media: both states have a fully
    # transparent background, so the row of dark pills that used to read as a
    # bar across the bottom of every tile is gone.  State is carried by colour
    # and weight instead of by a fill — accent + bold + a hairline outline when
    # set, plain overlay-white when not.  Legibility on bright media comes from
    # the drop shadow installed on _tagbar, not from a backing plate.
    _TAG_CSS_ON  = ("QToolButton { color: %s; background: transparent;"
                    " border: 1px solid %s; border-radius: 2px;"
                    " font-size: 8px; font-weight: bold; padding: 0 3px; }"
                    " QToolButton:hover { background: rgba(0,0,0,90); }")
    _TAG_CSS_OFF = ("QToolButton { color: %s; background: transparent;"
                    " border: 1px solid transparent; border-radius: 2px;"
                    " font-size: 8px; padding: 0 3px; }"
                    " QToolButton:hover { background: rgba(0,0,0,90); }")

    def rebuild_tag_buttons(self) -> None:
        """(Re)create the tag buttons from the current tag set."""
        while self._taglay.count():
            item = self._taglay.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()
        self._tag_btns = {}
        # Ditto button: stamp the most-recently-applied tag set onto this file.
        self._repeat_btn = QToolButton()
        self._repeat_btn.setText("〃")
        self._repeat_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._repeat_btn.setToolTip("Apply the most recently used tags")
        self._repeat_btn.clicked.connect(self._apply_recent_tags)
        self._taglay.addWidget(self._repeat_btn)
        # Inline tag creation: coin a new tag and apply it without a dialog trip.
        self._new_tag_btn = QToolButton()
        self._new_tag_btn.setText("＋#")
        self._new_tag_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._new_tag_btn.setToolTip("Create a new tag and apply it to this file")
        self._new_tag_btn.setStyleSheet(self._TAG_CSS_OFF % config.OVERLAY_FG)
        self._new_tag_btn.clicked.connect(self._create_tag_inline)
        self._taglay.addWidget(self._new_tag_btn)
        for t in tags.get_tags():
            b = QToolButton()
            b.setText(t)
            b.setCursor(Qt.CursorShape.PointingHandCursor)
            b.setToolTip(f'Toggle tag "{t}"')
            b.clicked.connect(lambda _=False, tg=t: self._toggle_tag(tg))
            self._taglay.addWidget(b)
            self._tag_btns[t] = b
        self._taglay.addStretch(1)
        # Far-right: per-tile resize (±10%), sized like the tag chips.
        self._tile_zoom_out_btn = self._mk_tile_zoom("−", -self._TILE_ZOOM_STEP,
                                                     "Shrink this media 10%")
        self._tile_zoom_in_btn = self._mk_tile_zoom("+", self._TILE_ZOOM_STEP,
                                                    "Grow this media 10%")
        self._taglay.addWidget(self._tile_zoom_out_btn)
        self._taglay.addWidget(self._tile_zoom_in_btn)
        self._refresh_tag_styles()       # also re-applies the collapsed/expanded state
        self._sync_tile_zoom_btns()

    _TILE_ZOOM_CSS = (
        "QToolButton { color: %s; background: transparent;"
        " border: 1px solid transparent; border-radius: 2px;"
        " font-size: 11px; font-weight: bold; padding: 0 4px; }"
        " QToolButton:hover { background: rgba(0,0,0,90); }"
        " QToolButton:disabled { color: %s; }")

    def _mk_tile_zoom(self, glyph: str, delta: float, tip: str) -> QToolButton:
        b = QToolButton()
        b.setText(glyph)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setToolTip(tip)
        b.setStyleSheet(self._TILE_ZOOM_CSS % (config.OVERLAY_FG, config.FG_DIM))
        b.clicked.connect(lambda _=False, d=delta: self._tile_zoom(d))
        return b

    def _apply_recent_tags(self) -> None:
        if not self._path:
            return
        if tags.apply_recent(self._path):
            tags.sync_tag_folders(self._path, self._favs.is_fav(self._path))
            self._refresh_tag_styles()

    def _create_tag_inline(self) -> None:
        """Prompt for a new tag, add it to the set, and apply it to this file."""
        from PySide6.QtWidgets import QInputDialog
        name, ok = QInputDialog.getText(self, "New tag", "Tag name:")
        if not ok:
            return
        name = name.strip()
        if not name:
            return
        created = tags.add_tag(name)          # False if it already existed
        if self._path and name not in tags.tags_for(self._path):
            tags.toggle_tag(self._path, name)
            tags.sync_tag_folders(self._path, self._favs.is_fav(self._path))
        if created:
            # New descriptor: rebuild every tile's buttons + the filter menu.
            self.tagSetChanged.emit()
        else:
            self._refresh_tag_styles()

    # -- source link -----------------------------------------------------------
    _SRC_MAX_CHARS = 44

    def _refresh_srclink(self) -> None:
        """Point the header at the current file (hidden when the tile is empty)."""
        if not self._path:
            self._srclink.hide()
            return
        name = os.path.basename(self._path)
        if len(name) > self._SRC_MAX_CHARS:
            name = name[:self._SRC_MAX_CHARS - 1] + "…"
        # Escape so a filename containing & or < can't corrupt the rich text.
        safe = (name.replace("&", "&amp;").replace("<", "&lt;")
                    .replace(">", "&gt;"))
        self._srclink.setText(
            f'<a href="reveal" style="color:{config.OVERLAY_FG};'
            ' text-decoration:none; font-size:10px;">'
            f'↗︎ {safe}</a>')
        self._srclink.setToolTip(f"Show in file manager:\n{self._path}")
        self._srclink.adjustSize()
        self._srclink.show()
        self._srclink.raise_()

    def _open_source(self, _href: str = "") -> None:
        if self._path:
            shell.reveal_path(self._path)

    def _toggle_tag(self, tag: str) -> None:
        if self._path:
            tags.toggle_tag(self._path, tag)
            # A tagged + favourited item is copied into the tag's subfolder
            # under the Gallery Favorites folder (removed when untagged).
            tags.sync_tag_folders(self._path, self._favs.is_fav(self._path))
            self._refresh_tag_styles()

    def set_tag_editing(self, on: bool) -> None:
        """Expand the tag row to the full editable set (hover), or collapse it."""
        self._tag_editing = bool(on)
        self._apply_tag_visibility()

    def _apply_tag_visibility(self) -> None:
        """Show every chip while editing; otherwise only the tags that are set."""
        cur = set(tags.tags_for(self._path)) if self._path else set()
        on = self._tag_editing
        for t, b in self._tag_btns.items():
            b.setVisible(on or t in cur)
        for b in (getattr(self, "_repeat_btn", None),
                  getattr(self, "_new_tag_btn", None),
                  getattr(self, "_tile_zoom_out_btn", None),
                  getattr(self, "_tile_zoom_in_btn", None)):
            if b is not None:
                b.setVisible(on)
        # With nothing to show the bar would still cast its drop shadow, so
        # hide the container outright rather than leave an empty smudge.
        self._tagbar.setVisible(bool(on or cur))
        if self._tagbar.isVisible():
            self._tagbar.raise_()

    def _refresh_tag_styles(self) -> None:
        self._apply_tag_visibility()
        cur = set(tags.tags_for(self._path)) if self._path else set()
        for t, b in self._tag_btns.items():
            # A set tag wears its own colour (falling back to the accent), so a
            # dense chip row is scannable without reading every label.
            hue = tags.color_of(t) or config.ACCENT
            b.setStyleSheet(
                (self._TAG_CSS_ON % (hue, hue)) if t in cur
                else (self._TAG_CSS_OFF % config.OVERLAY_FG))
        # The ditto button lights up only when there are remembered tags to add.
        recent = tags.recent_tags()
        pending = bool(self._path) and any(
            t not in cur for t in recent if t in tags.TAGS)
        self._repeat_btn.setEnabled(pending)
        self._repeat_btn.setStyleSheet(
            (self._TAG_CSS_ON % (config.ACCENT, config.ACCENT)) if pending
            else (self._TAG_CSS_OFF % config.FG_DIM))
        if recent:
            self._repeat_btn.setToolTip(
                "Apply the most recently used tags: " + ", ".join(recent))

    def refresh_fav(self) -> None:
        """Sync the heart button with the item's current favourite state.

        Separate from show_item(): favourite toggles must update the icon
        WITHOUT reloading the tile (a reload restarts playing videos, and
        _render deliberately skips unchanged tiles anyway).
        """
        is_fav = bool(self._path) and self._favs.is_fav(self._path)
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"QToolButton {{ color: {config.RED}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px; }" if is_fav else
            f"QToolButton {{ color: {config.OVERLAY_FG}; background: rgba(0,0,0,90);"
            " border-radius:4px; font-size:15px; padding:3px 6px; }")

    def show_item(self, row: int, path: str) -> None:
        self._row  = row
        self._path = path
        self._rotation = 0
        self._dur_ms   = 0
        self._clear_loop()
        self._reset_pan()               # each item starts centred
        self._reset_audio()
        self.refresh_fav()
        self._refresh_tag_styles()
        self._refresh_srclink()
        self._img_gen += 1                      # invalidate any pending decode
        self._last_vid_geom = None              # new media: force a re-fit
        if media.is_video(path):
            self._gif.stop()
            self._ensure_gl_viewport()
            self._is_video = True
            self._pm = QPixmap()
            self._stack.setCurrentIndex(1)
            self._seekwrap.show()
            self._player.setSource(QUrl.fromLocalFile(path))
            self._player.play()
            self._player.setPlaybackRate(self._SPEEDS[self._speed_idx])
            self._sync_rotate_btn()
            self._fit_video()
        else:
            self._is_video = False
            self._player.stop()
            self._stack.setCurrentIndex(0)
            self._seekwrap.hide()
            self._pm = QPixmap()
            if media.is_gif(path) and self._gif.play(path, self._on_gif_frame):
                # Animated GIF: QMovie drives frames into _img.
                pass
            else:
                self._gif.stop()
                self._img.clear_source()        # blank while decoding off-thread
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
            self._sync_rotate_btn()
            self._position_overlays()

    def _on_gif_frame(self, pm: QPixmap, _first: bool) -> None:
        # Frames flow through the normal image path, so fit/fill/zoom all apply.
        self._pm = pm
        self._img.set_source(pm)
        self._position_overlays()

    def _on_img_decoded(self, gen: int, qim: QImage) -> None:
        if gen != self._img_gen or self._is_video:
            return                              # superseded by a later show_item
        self._pm = (QPixmap.fromImage(qim)
                    if (qim is not None and not qim.isNull()) else QPixmap())
        self._img.set_source(self._pm)
        self._position_overlays()

    def _ensure_audio(self):
        """Create the audio device on demand (see __init__ note)."""
        if self._audio is None:
            self._audio = QAudioOutput(self)
            self._audio.setVolume(1.0)
            self._player.setAudioOutput(self._audio)
        return self._audio

    def _reset_audio(self) -> None:
        self._muted = True
        if self._audio is not None:
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

    def _rotate(self, degrees: int = 90) -> None:
        # Permanent rotation is handled by the main window (rewrites the file
        # and reloads the slot); video tiles have no still to rotate.
        if self._path and not self._is_video:
            self.rotated.emit(self._path, int(degrees) % 360)

    def _sync_rotate_btn(self) -> None:
        """Rotation is image-only — don't offer a menu that can't do anything."""
        ok = bool(self._path) and not self._is_video
        self._rot_btn.setEnabled(ok)
        self._rot_btn.setToolTip(
            "Rotate 90° clockwise — use the arrow for 180° / 270°" if ok
            else "Rotating video files isn't supported")

    def _toggle_fav(self) -> None:
        if self._path:
            self.favToggled.emit(self._path)

    def _trash(self) -> None:
        if self._path:
            # Release this slot's handle up front; MultiView.release_path()
            # releases any OTHER slot showing the same file (duplicate-filled
            # pages and the side-scroll buffer) before the move happens.
            self.release_media()
            self.trashed.emit(self._path)

    # -- media player callbacks ------------------------------------------------
    def _on_status(self, status):
        # The player already loops natively (setLoops(Infinite)).  Seeking back
        # to 0 here as well meant TWO restarts raced at every loop boundary, and
        # the extra seek landed on a non-keyframe — the stutter and the
        # "co located POCs unavailable" decoder complaints on short clips, which
        # come round every few seconds on a 5-second file.
        if status == QMediaPlayer.MediaStatus.InvalidMedia:
            self._fail_video()

    def _on_media_error(self, error, msg: str = "") -> None:
        """Give up on a file that never played; ride out a mid-playback hiccup.

        A file that has already produced a duration is decodable, so an error
        arriving later is a transient decode glitch (a damaged GOP, a seek onto
        a non-keyframe).  Tearing the tile down and blacklisting the file for
        thumbnails and dimensions over that would turn a momentary stutter into
        permanent breakage.
        """
        if error == QMediaPlayer.Error.NoError:
            return
        if self._player.duration() > 0 or self._dur_ms > 0:
            print(f"[video] transient decode error on "
                  f"{os.path.basename(self._path or '?')}: {msg}",
                  file=sys.stderr)
            return
        self._fail_video()

    def _fail_video(self) -> None:
        if not getattr(self, "_is_video", False):
            return
        path = self._path
        if path:
            media._mark_bad_video(path)
        # Release the handle so FFmpeg stops retrying, then fall back to a
        # centred "can't play" note on the image page.
        self._player.stop()
        self._player.setSource(QUrl())
        self._is_video = False
        self._seekwrap.hide()
        self._stack.setCurrentIndex(0)
        self._img.show_message("⚠  This video can't be played\n(file may be "
                               "truncated or use an unsupported codec)")

    def _on_pos(self, pos):
        # A–B loop: jump back to A the moment playback reaches B.
        if (self._loop_a_ms is not None and self._loop_b_ms is not None
                and pos >= self._loop_b_ms):
            self._player.setPosition(self._loop_a_ms)
            pos = self._loop_a_ms
        self._scrub.set_position(pos)
        # positionChanged fires many times a second per tile; setText relayouts
        # the transport row, so only touch it when the displayed second — the
        # only thing fmt_time shows — actually changes.
        secs = pos // 1000
        if secs != self._last_shown_sec:
            self._last_shown_sec = secs
            self._time.setText(fmt_time(pos))

    def _on_loop_changed(self, a_frac: float, b_frac: float) -> None:
        dur = self._dur_ms or self._player.duration()
        self._loop_a_ms = int(a_frac * dur) if a_frac >= 0 and dur > 0 else None
        self._loop_b_ms = int(b_frac * dur) if b_frac >= 0 and dur > 0 else None
        if (self._loop_a_ms is not None and self._loop_b_ms is not None
                and self._player.position() >= self._loop_b_ms):
            self._player.setPosition(self._loop_a_ms)

    def _clear_loop(self) -> None:
        self._loop_a_ms = self._loop_b_ms = None
        self._scrub.clear_loop()

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
        self._ensure_audio().setMuted(self._muted)
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
        self._ensure_audio().setVolume(QtAudio.convertVolume(
            val / 100.0,
            QtAudio.VolumeScale.LogarithmicVolumeScale,
            QtAudio.VolumeScale.LinearVolumeScale))
        # Each adjustment (re)arms the auto-hide; dragging keeps it open, and it
        # slides away shortly after the user stops changing the level.
        if not self._muted:
            self._vol_hide_timer.start()

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

    def mouseDoubleClickEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton and self._path:
            self.enlarge.emit(self._row)     # open this tile in the lightbox
        super().mouseDoubleClickEvent(e)

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
    rotated        = Signal(str, int)  # (path, degrees) — permanent rotation
    openLightbox   = Signal(int)   # row
    closeRequested = Signal()      # user wants to go back to gallery
    barsVisibleChanged = Signal(bool)   # chrome bars shown/hidden (auto-hide)
    tagSetChanged  = Signal()      # a tile coined a new tag inline

    def __init__(self, model, favorites, parent=None):
        super().__init__(parent)
        self._model = model
        self._favs  = favorites
        self._layout_slots = 3
        # Layout override: None = auto by orientation, else forced slot count
        # (3 = 3×1, 4 = 2×2).  Fill mode crops media to cover each tile.
        self._forced_layout = None
        self._fill_mode = False
        # Media coverage of each tile, adjustable in 10% steps by the ± buttons.
        self._zoom = 1.0

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

        # Tile currently under the mouse — the Delete key's target.
        self._hover_slot: "_Slot | None" = None
        # Dimension probing still running (counter shows "measuring…").
        self._measuring = False
        self._counter_base = ""

        # Side-scroll state
        self._ss_slot_order: list[_Slot] = []   # non-empty only when scrolling
        self._ss_head_idx = 0     # index in _current_paths of the leftmost slot
        self._ss_px = 0.0         # fractional px scrolled into current position
        self._ss_speed = self._scroll_level * 0.5   # px per timer tick
        self._ss_buf: "_Slot | None" = None

        p = self.palette()
        p.setColor(QPalette.ColorRole.Window, QColor("#000"))
        self.setPalette(p)
        self.setAutoFillBackground(True)

        # ------------------------------------------------------------------ #
        # Root layout: [chrome bar] / [tile host] / [bottom bar]              #
        # The bars are IN the layout (not floating), so slot controls are     #
        # never obscured by a higher-z sibling.  Both bars auto-hide after a  #
        # short idle so the tiles get the full panel; hiding a widget in a    #
        # QVBoxLayout releases its space, so there is still no overlap.       #
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
        self._help_btn = self._chrome_btn(
            "?", self.toggle_help, "Keyboard shortcuts (?)")
        self._fs_btn = self._chrome_btn(
            config.ICON_FULLSCREEN, self._toggle_fs, "Toggle full screen")
        chrome.addWidget(back_btn)
        chrome.addStretch(1)
        chrome.addWidget(self._help_btn)
        chrome.addWidget(self._fs_btn)
        self._root.addWidget(self._chrome_widget)

        # Reserved strip for the main window's floating settings bar.  The bar
        # used to overlay the top edge of the tiles, which covered each tile's
        # icon-button row AND swallowed its clicks (the rotate/fav/pin buttons
        # "stopped working").  Giving the bar real layout space pushes the
        # tiles below it instead.
        self._top_inset = QWidget()
        self._top_inset.setFixedHeight(0)
        self._top_inset.setStyleSheet("background: #000; border: none;")
        self._root.addWidget(self._top_inset)

        # Tile host (takes all remaining space).  Slots are positioned manually
        # by _layout_tiles() — a justified layout where each tile takes exactly
        # its media's aspect box, so media meets media with no internal
        # letterboxing (QGridLayout's uniform cells wasted large black bars on
        # any tile whose media didn't match the cell shape).
        self._grid_host = QWidget()
        self._grid_host.setStyleSheet("background: #000; border: none;")
        self._grid_host.setMouseTracking(True)
        self._grid_host.installEventFilter(self)
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

        # Zoom: grow / shrink how much of each tile the media covers, 10% a step.
        self._zoom_out_btn = self._chrome_btn(
            "−", self.zoom_out, "Shrink media coverage 10% (−)")
        self._zoom_out_btn.setFixedWidth(30)
        # Readout doubles as the reset control (click → back to 100%).
        self._zoom_lbl = self._chrome_btn(
            "100%", self.reset_zoom,
            "Media coverage of each tile — click to reset to 100% (0)")
        self._zoom_lbl.setFixedWidth(52)
        self._zoom_lbl.setStyleSheet(
            f"QToolButton {{ color: {config.FG_MID}; font-size: 12px;"
            " background: rgba(0,0,0,90); border: none; border-radius: 4px;"
            " padding: 2px 4px; }"
            " QToolButton:hover { color: #ffffff; background: rgba(0,0,0,160); }")
        self._zoom_in_btn = self._chrome_btn(
            "+", self.zoom_in, "Grow media coverage 10% (+)")
        self._zoom_in_btn.setFixedWidth(30)
        self._update_zoom_lbl()

        # Orientation toggle: switch between portrait and landscape groups.
        # Its label carries the count of the OTHER group (e.g. "↔ 128") so the
        # size of the alternative layout is visible without hovering.
        self._orient_btn = self._chrome_btn(
            "↔ 0", self._switch_orientation,
            "Switch orientation group (⇔ portrait / landscape)")
        self._orient_btn.setMinimumWidth(62)

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
        asl.addWidget(self._zoom_out_btn)
        asl.addWidget(self._zoom_lbl)
        asl.addWidget(self._zoom_in_btn)
        asl.addWidget(self._orient_btn)
        # Tag every tile on this page in one go — the natural companion to the
        # per-tile chips when a whole screenful shares a descriptor.
        self._tagall_btn = QToolButton()
        self._tagall_btn.setText("Tag page ▾")
        self._tagall_btn.setToolTip(
            "Apply (or remove) a tag across every item shown on this page")
        self._tagall_btn.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup)
        self._tagall_btn.setStyleSheet(
            f"QToolButton {{ color: {config.OVERLAY_FG}; font-size: 18px;"
            " background: rgba(0,0,0,90); border: none;"
            " border-radius: 4px; padding: 2px 10px; }"
            " QToolButton:hover { color: #ffffff;"
            " background: rgba(0,0,0,160); border-radius: 4px; }")
        self._tagall_menu = QMenu(self._tagall_btn)
        self._tagall_btn.setMenu(self._tagall_menu)
        self._tagall_menu.aboutToShow.connect(self._build_tagall_menu)
        asl.addWidget(self._tagall_btn)
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
        for _k in (Qt.Key.Key_Plus, Qt.Key.Key_Equal):
            QShortcut(QKeySequence(_k), self, activated=self.zoom_in)
        QShortcut(QKeySequence(Qt.Key.Key_Minus), self, activated=self.zoom_out)
        QShortcut(QKeySequence(Qt.Key.Key_0),     self, activated=self.reset_zoom)
        # NOTE: 'H' is deliberately NOT bound here.  MainWindow also binds H
        # (gallery bar toggle); two window-context shortcuts on the same key
        # make it ambiguous and Qt fires NEITHER.  MainWindow dispatches H to
        # whichever panel is active instead.
        QShortcut(QKeySequence("Ctrl+M"),          self,
                  activated=self._unmute_all)
        QShortcut(QKeySequence(Qt.Key.Key_Delete), self,
                  activated=self._trash_hovered)
        QShortcut(QKeySequence(Qt.Key.Key_Backspace), self,
                  activated=self._trash_hovered)

        # Auto-hide the chrome/bottom bars after idle so tiles get the full
        # panel height; any mouse move (H toggles manually) brings them back.
        self.setMouseTracking(True)
        self._bars_hide_timer = QTimer(self)
        self._bars_hide_timer.setSingleShot(True)
        self._bars_hide_timer.setInterval(2500)
        self._bars_hide_timer.timeout.connect(self._maybe_hide_bars)

        # Keyboard/mouse help (?/F1 dispatched here by MainWindow — binding
        # them locally too would make the shortcuts ambiguous, like H was).
        self._help = make_help_panel(self, [
            ("← / →  ·  wheel", "Previous / next set"),
            ("A", "Play / pause slideshow"),
            ("S", "Slideshow style (Set ⇄ Scroll)"),
            ("L", "Layout: Auto / 3×1 / 2×2"),
            ("F", "Fit (no crop) / Fill (cover)"),
            ("+  /  −  /  0", "Media coverage: bigger / smaller / reset"),
            ("Edge arrows", "Reposition media that overflows its tile"),
            ("H", "Show / hide the bars"),
            ("Ctrl+M", "Unmute all visible videos"),
            ("Delete", "Trash the hovered tile (undoable)"),
            ("Double-click", "Open tile in the viewer"),
            ("Right-click seek bar", "Loop between two points (A → B → clear)"),
            ("Drag tile → tile", "Swap positions"),
            ("Esc", "Back to gallery"),
            ("?  /  F1", "Show / hide this help"),
        ])

    def toggle_help(self) -> None:
        toggle_help_panel(self._help, self)

    def set_top_inset(self, h: int) -> None:
        """Reserve `h` px below the chrome strip for the floating settings
        bar so it never overlaps (or click-blocks) the tiles."""
        self._top_inset.setFixedHeight(max(0, int(h)))

    # -- public API ------------------------------------------------------------

    def open(self, start_row: int) -> None:
        """Activate the panel from start_row's orientation group.

        Clears any previous scroll / pin / forced-layout state so each
        gallery→multiview transition starts fresh.
        """
        self._stop_slideshow()
        # Start in Auto layout: a manual 3×1/2×2 override from a previous
        # visit must not stop portrait media from getting its 3×1 grid.
        self._forced_layout = None
        self._update_layout_btn()
        self.reset_zoom()          # a previous visit's zoom shouldn't linger

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
        self._show_bars()          # visible on entry; auto-hide after idle
        self._render(start_in_list)

    def reopen(self) -> None:
        """Return to the panel exactly as it was left (same orientation group,
        page, forced layout and pins) — used by the lightbox's back button.

        Unlike open(), nothing is reset; the lists are refreshed against the
        model in case items were deleted/rotated while in the viewer.
        """
        self._refresh_orientation_lists()
        self._start = min(self._start, max(0, len(self._current_paths) - 1))
        self._update_orient_btn()
        self._show_bars()
        self._render(self._start)

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
        else:
            # Even without a re-render, newly-known aspects refine tile boxes.
            self._layout_tiles()

    def _detect_layout(self) -> int:
        """Forced layout if set, else 3 for portrait 3×1 / 4 for landscape 2×2."""
        if self._forced_layout in (3, 4):
            return self._forced_layout
        return 3 if (self._current_paths is self._portrait_paths) else 4

    # -- layout / slot management ----------------------------------------------

    _TILE_GAP = 1     # px between adjacent tiles — minimal visible seam

    def _make_slot(self, index: int) -> _Slot:
        slot = _Slot(index, self._favs, self._grid_host)
        slot.enlarge.connect(self._on_enlarge)
        slot.favToggled.connect(self._on_slot_fav)
        slot.trashed.connect(self._on_slot_trash)
        slot.reordered.connect(self._on_reorder)
        slot.rotated.connect(self._on_slot_rotate)
        slot.tagSetChanged.connect(self.tagSetChanged)
        slot.pinned.connect(lambda *_: None)
        slot.set_fill(self._fill_mode)
        slot.set_zoom(self._zoom)
        slot._img_pool = self._img_pool
        slot.setMouseTracking(True)
        slot.installEventFilter(self)     # mouse activity re-shows the bars
        return slot

    def _build_slots(self, n: int) -> None:
        """Activate `n` of the persistent slots (3 = 3×1, 4 = 2×2).

        Slots (and their media players / GL viewports) are created ONCE and
        reused: the old rebuild destroyed and recreated five QMediaPlayer +
        QAudioOutput pipelines on every 3×1⇄2×2 switch — a visible stall and
        audio-device churn each time the orientation flipped.
        """
        self._hover_slot = None
        # Clean up any side-scroll state without re-rendering
        if self._ss_slot_order:
            self._ss_timer.stop()
            self._ss_slot_order.clear()
        if not self._slots:                       # first call: create 4 + buffer
            for i in range(4):
                self._slots.append(self._make_slot(i))
            self._ss_buf = self._make_slot(4)
            self._ss_buf.hide()
        prev = self._layout_slots
        self._layout_slots = n
        # A pinned slot beyond the new active range keeps its media: relocate
        # it into the first non-pinned active slot before deactivating.
        for idx in range(n, len(self._slots)):
            src = self._slots[idx]
            if src.is_pinned and src._path:
                for dst in self._slots[:n]:
                    if not dst.is_pinned:
                        dst._speed_idx = src._speed_idx
                        dst._speed_btn.setText(
                            f"{dst._SPEEDS[dst._speed_idx]:g}×")
                        dst.show_item(src._row, src._path)
                        dst._is_pinned = True
                        dst._pin_btn.setIcon(dst._pause_icon_on)
                        dst._pin_btn.setStyleSheet(dst._PIN_ON)
                        dst._bar_hide_timer.stop()
                        dst._btnbar.show()
                        break
            src._is_pinned = False
            src._pin_btn.setIcon(src._pause_icon_off)
            src._pin_btn.setStyleSheet(src._PIN_OFF)
            src.clear()
            src.hide()
        for slot in self._slots[:n]:
            slot.show()
        if prev != n or not self._slots[0].isVisible():
            self._layout_tiles()

    def _active_slots(self) -> list["_Slot"]:
        """The slots participating in the current layout (3 or 4)."""
        return self._slots[:self._layout_slots]

    # -- tile geometry -----------------------------------------------------------

    def _slot_aspect(self, slot: "_Slot") -> float:
        """Best-known width/height ratio for the media in `slot`."""
        path = slot._path
        if path:
            w, h = self._model.dim_at(path)
            if w > 0 and h > 0:
                return w / h
            if slot._is_video:
                ns = slot._video_item.nativeSize()
                if ns.width() > 0 and ns.height() > 0:
                    return ns.width() / ns.height()
            elif not slot._pm.isNull():
                return slot._pm.width() / slot._pm.height()
        # Unknown: assume the active group's typical shape.
        return 0.6 if (self._current_paths is self._portrait_paths) else 16 / 9

    def _layout_tiles(self) -> None:
        """Position the visible slots as a UNIFORM grid.

        The 3×1 / 2×2 viewing windows are fixed, equal cells that never change
        with the media on show or the zoom level — only the media *within* each
        window scales (Fit contains it, Fill covers it, and ± zoom grows or
        shrinks it inside the fixed cell).  This keeps the layout steady while
        zooming instead of re-justifying tiles to each item's aspect ratio.
        """
        if self._ss_slot_order:            # side-scroll owns tile geometry
            return
        W = self._grid_host.width()
        H = self._grid_host.height()
        if W <= 2 or H <= 2 or not self._slots:
            return
        gap = self._TILE_GAP
        rows = ([self._slots[:3]] if self._layout_slots == 3
                else [self._slots[:2], self._slots[2:4]])
        rows = [r for r in rows if r]
        nrows = len(rows)
        rh = (H - gap * (nrows - 1)) / nrows
        for r, row in enumerate(rows):
            cw = (W - gap * (len(row) - 1)) / len(row)
            y = round(r * (rh + gap))
            y2 = round((r + 1) * rh + r * gap)
            for c, slot in enumerate(row):
                x = round(c * (cw + gap))
                x2 = round((c + 1) * cw + c * gap)
                slot.setGeometry(x, y, max(1, x2 - x), max(1, y2 - y))

    def _switch_layout(self, n: int) -> None:
        """Activate n slots; pinned media survives in place (persistent slots
        mean no player teardown — see _build_slots)."""
        self._build_slots(n)

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
        # Show the active tile slots; hide the buffer; restore geometry.
        for slot in self._active_slots():
            slot.show()
        if self._ss_buf is not None:
            self._ss_buf.hide()
        self._ss_slot_order.clear()
        self._layout_tiles()

    def _start_sidescroll(self) -> None:
        n_paths = len(self._current_paths)
        if n_paths == 0:
            return
        n_vis = self._ss_n_visible()

        # Slots beyond the visible strip are just hidden (all slots are
        # manually positioned children of the tile host already).
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
            self._counter_base = "0 of 0"
            self._apply_counter()
            return

        want = self._detect_layout()
        if want != self._layout_slots:
            self._switch_layout(want)

        self._start = max(0, min(start, len(paths) - 1))
        active = self._active_slots()
        pinned_set = {s._path for s in active if s.is_pinned and s._path}

        idx = self._start
        shown_end = self._start
        for slot in active:
            if slot.is_pinned:
                continue
            while idx < len(paths) and paths[idx] in pinned_set:
                idx += 1
            if idx < len(paths):
                path = paths[idx]
                shown_end = idx + 1
                idx += 1
            elif paths:
                path = paths[idx % len(paths)]
                shown_end = len(paths)
                idx += 1
            else:
                slot.clear()
                continue
            # Skip reloading a slot that already shows this file — dims-driven
            # re-partitions re-render frequently, and an unconditional
            # show_item() restarted every playing video each time.
            row = self._model.row_for_path(path)
            if slot._path != path:
                slot.show_item(row if row >= 0 else max(0, idx - 1), path)
            elif row >= 0:
                slot._row = row     # keep enlarge/lightbox on the right item

        total = len(paths)
        self._counter_base = (
            f"{self._start + 1}–{min(shown_end, total)} of {total}")
        self._apply_counter()
        # Content changed → tile aspect boxes may have changed.
        self._layout_tiles()

    def _apply_counter(self) -> None:
        suffix = "  ·  measuring…" if self._measuring else ""
        self._counter.setText(self._counter_base + suffix)

    def set_measuring(self, on: bool) -> None:
        """Dimension probing is running: the orientation groups can still grow.

        Shown in the counter so a partially-measured video folder doesn't
        falsely read as \"these are the only items\".
        """
        on = bool(on)
        if on == self._measuring:
            return
        self._measuring = on
        self._apply_counter()
        # Unmeasured items provisionally bucket as landscape, so the alternative
        # group's count is only trustworthy once probing stops — refresh it.
        self._update_orient_btn()
        if not on and self.isVisible():
            # Final sizes are in — settle the groups one last time.
            self._on_model_dims_changed()

    # -- navigation ------------------------------------------------------------

    def next_page(self) -> None:
        self._stop_slideshow()
        if not self._current_paths:
            return
        step = sum(1 for s in self._active_slots() if not s.is_pinned) or 1
        nxt = self._start + step
        self._render(0 if nxt >= len(self._current_paths) else nxt)

    def prev_page(self) -> None:
        self._stop_slideshow()
        if not self._current_paths:
            return
        step = sum(1 for s in self._active_slots() if not s.is_pinned) or 1
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
        step = sum(1 for s in self._active_slots() if not s.is_pinned) or 1
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
        """Toggle cover-fill (uniform grid, crop) vs fit (justified, no crop)."""
        self._fill_mode = not self._fill_mode
        self._fill_btn.setText("Fill" if self._fill_mode else "Fit")
        for slot in self._slots:
            slot.set_fill(self._fill_mode)
        if self._ss_buf is not None:
            self._ss_buf.set_fill(self._fill_mode)
        self._layout_tiles()      # geometry philosophy changes with the mode

    # -- zoom (media coverage of each tile) ------------------------------------

    ZOOM_STEP = 0.10
    ZOOM_MIN  = 0.50
    # Capped at 200%: tiles are decoded to tile size (show_item's tile_px), so
    # zooming further only upsamples — it would promise detail that isn't there.
    ZOOM_MAX  = 2.00

    def zoom_in(self) -> None:
        self.set_zoom(self._zoom + self.ZOOM_STEP)

    def zoom_out(self) -> None:
        self.set_zoom(self._zoom - self.ZOOM_STEP)

    def reset_zoom(self) -> None:
        self.set_zoom(1.0)

    def set_zoom(self, zoom: float) -> None:
        """Apply a clamped zoom to every tile (and the side-scroll buffer).

        Rounded to whole 10% steps so repeated ± clicks can't drift off the
        grid through float accumulation.
        """
        zoom = round(max(self.ZOOM_MIN, min(self.ZOOM_MAX, float(zoom))), 2)
        zoom = round(round(zoom / self.ZOOM_STEP) * self.ZOOM_STEP, 2)
        if zoom == self._zoom:
            self._update_zoom_lbl()
            return
        self._zoom = zoom
        for slot in self._slots:
            slot.set_zoom(zoom)
        if self._ss_buf is not None:
            self._ss_buf.set_zoom(zoom)
        self._update_zoom_lbl()

    def _update_zoom_lbl(self) -> None:
        self._zoom_lbl.setText(f"{round(self._zoom * 100):d}%")
        self._zoom_out_btn.setEnabled(self._zoom > self.ZOOM_MIN + 1e-9)
        self._zoom_in_btn.setEnabled(self._zoom < self.ZOOM_MAX - 1e-9)

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
        """Label the orientation toggle with how much media the OTHER group
        holds, so the count is visible without hovering for a tooltip."""
        if self._current_paths is self._portrait_paths:
            other_n, other_name, glyph = (
                len(self._landscape_paths), "landscape", "↔")
        else:
            other_n, other_name, glyph = (
                len(self._portrait_paths), "portrait", "↕")
        # While dimensions are still being probed, unmeasured items provisionally
        # bucket as landscape, so mark the count as not yet final.
        pending = "~" if self._measuring else ""
        self._orient_btn.setText(f"{glyph} {pending}{other_n}")
        if other_n:
            tip = f"Switch to {other_name} media ({other_n} items)"
        else:
            tip = f"No {other_name} media in this set"
        if self._measuring:
            tip += "  ·  still measuring, count may change"
        self._orient_btn.setToolTip(tip)
        self._orient_btn.setEnabled(bool(other_n))

    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        if self._ss_timer.isActive():
            self._ss_reposition()

    def eventFilter(self, obj, event):
        et = event.type()
        if obj is self._grid_host and et == QEvent.Type.Resize:
            # Host resizes when the panel resizes AND when the bars hide/show.
            if self._ss_slot_order:
                self._ss_reposition()
            else:
                self._layout_tiles()
        elif et in (QEvent.Type.MouseMove, QEvent.Type.Enter):
            if isinstance(obj, _Slot):
                self._hover_slot = obj      # target for the Delete key
            self._show_bars()
        elif et == QEvent.Type.Leave and obj is self._hover_slot:
            self._hover_slot = None
        return super().eventFilter(obj, event)

    def mouseMoveEvent(self, e):
        self._show_bars()
        super().mouseMoveEvent(e)

    def wheelEvent(self, e):
        # Wheel pages the view (down = next set) — slots don't consume wheel
        # events, so this works anywhere over the tiles.
        d = e.angleDelta().y()
        if d < 0:
            self.next_page()
        elif d > 0:
            self.prev_page()
        e.accept()

    # -- bar auto-hide -----------------------------------------------------------

    # The FOOTER (page navigation, layout, zoom, orientation, tag-page) stays
    # put: auto-hiding it resized the grid every couple of seconds, and each
    # resize re-fits every video — the periodic jitter mid-playback.  It is also
    # the strip you actually reach for, so it should not move under the cursor.
    # Only the header auto-hides; per-tile overlays keep their own hover rule.
    def _show_bars(self) -> None:
        if not self._chrome_widget.isVisible():
            self._chrome_widget.show()
            self._autoscroll_widget.show()
            self._bars_hide_timer.start()
            self.barsVisibleChanged.emit(True)
        elif self._bars_hide_timer.remainingTime() < 2200:
            # Mouse-move events arrive in the hundreds per second; restarting
            # the timer at most ~every 300 ms keeps the handler near-free.
            self._bars_hide_timer.start()

    def _maybe_hide_bars(self) -> None:
        # Keep the bars while the user is interacting with them.
        if (self._chrome_widget.underMouse()
                or self._autoscroll_widget.underMouse()
                or QApplication.activePopupWidget() is not None):
            self._bars_hide_timer.start()
            return
        self._chrome_widget.hide()      # footer deliberately stays visible
        self.barsVisibleChanged.emit(False)

    def _toggle_bars(self) -> None:
        if self._chrome_widget.isVisible():
            self._bars_hide_timer.stop()
            self._chrome_widget.hide()
            self._autoscroll_widget.hide()
            self.barsVisibleChanged.emit(False)
        else:
            self._show_bars()

    # -- slot signal handlers --------------------------------------------------

    def _on_enlarge(self, row: int) -> None:
        self.openLightbox.emit(row)
        self.closeRequested.emit()

    def _on_slot_fav(self, path: str) -> None:
        if path:
            # MainWindow toggles the favourite and calls refresh_fav(path)
            # back on us synchronously — no re-render needed (and a re-render
            # wouldn't refresh hearts anyway, since unchanged tiles are
            # deliberately not reloaded).
            self.favToggled.emit(path)

    def refresh_fav(self, path: str) -> None:
        """Update the heart on every tile showing `path` (duplicate-filled
        pages and the side-scroll buffer can all display the same file)."""
        for slot in self._all_slots():
            if slot._path == path:
                slot.refresh_fav()

    def refresh_tag(self, path: str) -> None:
        """Update the tag-button highlights on every tile showing `path`."""
        for slot in self._all_slots():
            if slot._path == path:
                slot._refresh_tag_styles()

    def rebuild_tag_buttons(self) -> None:
        """Rebuild every tile's tag buttons after the tag set changed."""
        for slot in self._all_slots():
            slot.rebuild_tag_buttons()
            slot._position_overlays()

    # -- tag the whole page ----------------------------------------------------
    def page_paths(self) -> "list[str]":
        """Distinct paths currently shown in the visible tiles."""
        seen: "list[str]" = []
        for slot in self._slots:
            p = getattr(slot, "_path", "")
            if p and p not in seen:
                seen.append(p)
        return seen

    def _build_tagall_menu(self) -> None:
        """Rebuilt on each open so it tracks the tag set and the current page."""
        self._tagall_menu.clear()
        paths = self.page_paths()
        if not paths:
            self._tagall_menu.addAction("(nothing on this page)").setEnabled(False)
            return
        for t in tags.get_tags():
            have = sum(1 for p in paths if t in tags.tags_for(p))
            if have == len(paths):
                label = f"Remove “{t}” from all {len(paths)}"
                add = False
            else:
                label = f"Apply “{t}” to {len(paths)}"
                if have:
                    label += f"  ({have} already tagged)"
                add = True
            self._tagall_menu.addAction(
                label,
                lambda _=False, tg=t, a=add: self._tag_page(tg, a))

    def _tag_page(self, tag: str, add: bool) -> None:
        """Add or remove `tag` across every item on this page in one store write."""
        paths = self.page_paths()
        if not paths:
            return
        changed = tags.apply_tag_to_paths(paths, tag, add)
        for p in changed:
            tags.sync_tag_folders(p, self._favs.is_fav(p))
        for slot in self._all_slots():
            slot._refresh_tag_styles()

    def _on_slot_rotate(self, path: str, degrees: int = 90) -> None:
        if path:
            self.rotated.emit(path, int(degrees) % 360)

    def _all_slots(self) -> list["_Slot"]:
        return self._slots + ([self._ss_buf] if self._ss_buf is not None else [])

    def reload_path(self, path: str) -> None:
        """Reload any slot showing `path` after its pixels changed on disk.

        Re-partitions orientation groups (a 90° turn flips portrait↔landscape)
        and re-decodes the rotated file into every slot currently showing it.
        """
        self._refresh_orientation_lists()
        self._update_orient_btn()
        for slot in self._all_slots():
            if slot._path == path:
                slot.show_item(slot._row, path)

    def release_path(self, path: str) -> None:
        """Release every player handle on `path` so the file can be moved.

        Duplicate-filled pages and the side-scroll buffer can all hold the
        same video open; a single locked handle makes deletion fail on
        Windows.  Called by the main window before trashing a file.
        """
        for slot in self._all_slots():
            if slot._path == path:
                slot.release_media()

    def release_all_media(self) -> None:
        """Stop playback and release every file handle (panel deactivated).

        Without this, tiles kept decoding (and locking) their videos while
        the gallery was shown — burning CPU and blocking deletion of any
        file last seen in multi-view.
        """
        for slot in self._all_slots():
            slot.clear()

    def _trash_hovered(self) -> None:
        """Delete key: trash the tile currently under the mouse."""
        slot = self._hover_slot
        if slot is not None and slot._path:
            slot._trash()

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
