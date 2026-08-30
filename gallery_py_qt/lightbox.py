"""Full-screen lightbox: image zoom/pan/rotate + native video playback.

Changes vs original:
  - Qt.WindowType.Window flag ensures the dialog covers the OS taskbar when
    shown full-screen on Windows (QDialog without this flag is constrained
    by its parent's geometry on some platforms).
  - Volume slider added to the transport bar.
  - 'F' key shortcut toggles favourite for the current item.
  - Full-screen toggle calls raise_() + activateWindow() after showFullScreen()
    so the window reliably takes focus and sits above system trays.
"""
from __future__ import annotations
import os
import sys

from PySide6.QtCore import (Qt, QUrl, Signal, QTimer, QObject, QRunnable,
                            QThreadPool, QEvent, QSize)
from PySide6.QtGui import (QPixmap, QImage, QKeySequence, QShortcut,
                           QStandardItem, QStandardItemModel)
from PySide6.QtWidgets import (QDialog, QGraphicsView, QGraphicsScene,
                               QGraphicsPixmapItem, QVBoxLayout, QHBoxLayout,
                               QToolButton, QLabel, QStackedWidget, QWidget,
                               QSlider, QListView, QAbstractItemView, QMenu,
                               QApplication)
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput, QtAudio
from PySide6.QtMultimediaWidgets import QVideoWidget

from . import config
from .engine import media
from .seekbar import SeekBar, fmt_time


class _FullImageSignals(QObject):
    ready = Signal(int, str, QImage)   # (generation, path, decoded image)


class _FullImageJob(QRunnable):
    """Decode a full-resolution still off the GUI thread.

    A generation token lets the lightbox ignore results that arrive after the
    user has already navigated on, so rapid arrow-key paging never flashes a
    stale image.
    """
    def __init__(self, gen: int, path: str, max_px: int,
                 signals: _FullImageSignals, gen_now=None):
        super().__init__()
        self._gen = gen
        self._path = path
        self._max_px = max_px
        self._signals = signals
        self._gen_now = gen_now

    def run(self) -> None:
        # Re-check the generation when the job actually starts: rapid arrow-key
        # paging queues decodes whose results would be discarded anyway — skip
        # them instead of running a full large-image decode per skipped page.
        if self._gen_now is not None:
            try:
                if self._gen_now() != self._gen:
                    return
            except Exception:
                return
        try:
            qim = media.load_full_qimage(self._path, max_px=self._max_px)
        except Exception:
            qim = None
        self._signals.ready.emit(self._gen, self._path,
                                 qim if qim is not None else QImage())


class _StripSignals(QObject):
    ready = Signal(str, QImage)        # (path, thumbnail)


class _StripThumbJob(QRunnable):
    """Decode one filmstrip thumbnail off the GUI thread (disk-cache backed)."""
    def __init__(self, path: str, px: int, signals: "_StripSignals"):
        super().__init__()
        self._path = path
        self._px = px
        self._signals = signals

    def run(self) -> None:
        from .engine import cache
        try:
            qim = cache.get_thumbnail(self._path, self._px)
        except Exception:
            qim = None
        if qim is not None and not qim.isNull():
            self._signals.ready.emit(self._path, qim)


class _ImageView(QGraphicsView):
    """Graphics view with wheel-zoom, drag-pan and rotation."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self._scene = QGraphicsScene(self)
        self.setScene(self._scene)
        self._item = QGraphicsPixmapItem()
        self._scene.addItem(self._item)
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setRenderHints(self.renderHints())
        self.setStyleSheet("background: #000; border: none;")
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._zoom = 1.0

    def set_pixmap(self, pm: QPixmap) -> None:
        self._item.setPixmap(pm)
        self._scene.setSceneRect(self._item.boundingRect())
        self.reset_view()

    def set_frame(self, pm: QPixmap) -> None:
        """Swap the pixmap WITHOUT refitting — for animation frames, so any
        zoom/pan set on the first frame is preserved across the animation."""
        self._item.setPixmap(pm)

    def reset_view(self) -> None:
        self._zoom = 1.0
        self.resetTransform()
        if not self._item.pixmap().isNull():
            self.fitInView(self._item, Qt.AspectRatioMode.KeepAspectRatio)

    def zoom_by(self, factor: float) -> None:
        self._zoom = max(0.1, min(12.0, self._zoom * factor))
        self.scale(factor, factor)

    def rotate_by(self, deg: int) -> None:
        self.rotate(deg)

    def wheelEvent(self, e) -> None:
        self.zoom_by(1.15 if e.angleDelta().y() > 0 else 1 / 1.15)


class Lightbox(QDialog):
    favToggled  = Signal(str)
    trashed     = Signal(str)
    requestInfo = Signal(str)
    openMulti   = Signal(int)
    returnToMulti = Signal()      # back to the multi-view page we came from
    rotateRequested = Signal(int)   # degrees clockwise (+90 / -90)

    _SPEEDS = (0.25, 0.5, 1.0, 1.25, 1.5, 1.75, 2.0)

    def __init__(self, model, favorites, parent=None):
        # Qt.WindowType.Window is required so showFullScreen() covers the OS
        # taskbar on Windows (QDialog is otherwise constrained by parent geometry).
        super().__init__(parent, Qt.WindowType.Window)
        self._model = model
        self._favs = favorites
        self._row = 0
        self._dur_ms = 0
        self.setWindowTitle("Viewer")
        self.setStyleSheet(f"background: #000; color: {config.FG_BRIGHT};")

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Media stack fills the full window — overlays are positioned on top.
        # The video pipeline (QMediaPlayer + audio-device open + QVideoWidget)
        # is built LAZILY on the first video shown: constructing it eagerly
        # made every viewer popup slow, even for plain images — and with
        # WA_DeleteOnClose each open rebuilt it from scratch.
        self._stack = QStackedWidget()
        self._img = _ImageView()
        self._video_panel = QWidget()
        vlay = QVBoxLayout(self._video_panel)
        vlay.setContentsMargins(0, 0, 0, 0)
        self._video = None
        self._player = None
        self._audio = None
        self._stack.addWidget(self._img)
        self._stack.addWidget(self._video_panel)
        root.addWidget(self._stack, 1)

        # Filmstrip: a horizontal thumbnail rail docked under the media, so the
        # set can be scanned and jumped through without leaving the viewer.
        # Hidden by default (it costs vertical room); toggled from the top bar.
        self._STRIP_PX = 76
        self._strip_model = QStandardItemModel(self)
        self._strip = QListView()
        self._strip.setModel(self._strip_model)
        self._strip.setViewMode(QListView.ViewMode.IconMode)
        self._strip.setFlow(QListView.Flow.LeftToRight)
        self._strip.setWrapping(False)
        self._strip.setMovement(QListView.Movement.Static)
        self._strip.setResizeMode(QListView.ResizeMode.Adjust)
        self._strip.setIconSize(QSize(self._STRIP_PX, self._STRIP_PX))
        self._strip.setFixedHeight(self._STRIP_PX + 26)
        self._strip.setUniformItemSizes(True)
        self._strip.setHorizontalScrollMode(
            QAbstractItemView.ScrollMode.ScrollPerPixel)
        self._strip.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._strip.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._strip.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self._strip.setStyleSheet(
            "QListView { background: #0a0a0a; border: none; color: %s; }"
            "QListView::item:selected { background: rgba(255,255,255,40);"
            " border: 1px solid %s; }" % (config.FG_MID, config.ACCENT))
        self._strip.clicked.connect(self._on_strip_clicked)
        self._strip.hide()
        root.addWidget(self._strip)

        self._strip_items: "dict[str, QStandardItem]" = {}
        self._strip_built_for = -1          # model row-count the strip was built for
        self._strip_pool = QThreadPool(self)
        self._strip_pool.setMaxThreadCount(2)
        self._strip_sig = _StripSignals(self)
        self._strip_sig.ready.connect(self._on_strip_thumb)

        # Floating action bar — overlays the top edge of the media.
        self._bar_widget = QWidget(self)
        self._bar_widget.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        bar = QHBoxLayout(self._bar_widget)
        bar.setContentsMargins(8, 6, 8, 6)
        # Leftmost: return to whatever this viewer was opened from — the
        # multi-view set, or the gallery.  Always present and labelled, so
        # full-screening one item is never a one-way trip.
        self._back_btn = self._tb(f"{config.ICON_BACK} Back", self._go_back, bar)
        # Legacy alias: earlier code addressed this button as _back_mv_btn.
        self._back_mv_btn = self._back_btn
        self._return_kind = "gallery"
        self._apply_return_kind()
        # Leave OS full screen without leaving the viewer (F11 equivalent).
        self._exit_fs_btn = self._tb(
            config.ICON_CLOSE + " Full screen", self._toggle_fs, bar)
        self._exit_fs_btn.setToolTip("Leave full screen (F11)")
        self._exit_fs_btn.hide()
        self._hold = self._tb("HOLD", self._toggle_hold, checkable=True)
        bar.addWidget(self._hold)
        bar.addStretch(1)
        self._counter = QLabel("")
        self._counter.setStyleSheet(
            f"color: {config.FG_MID}; background: rgba(0,0,0,90);"
            " border-radius: 4px; padding: 2px 8px;")
        bar.addWidget(self._counter)
        bar.addStretch(1)
        # Right-side action group, added left-to-right in final order.
        self._fav_btn = self._tb(config.ICON_HEART_EMPTY, self._toggle_fav, bar)
        # Star rating (0–5): click the Nth star to set N, or clear if already N.
        self._star_btns = []
        for n in range(1, 6):
            b = self._tb("☆", lambda _=False, k=n: self._set_rating(k), bar)
            b.setToolTip(f"Rate {n} star(s)")
            self._star_btns.append(b)
        # Permanent rotation, collapsed into one split button (click = 90°,
        # arrow = the rest) — five separate degree buttons were a third of this
        # bar.  Mirrors the multi-view tile's rotate control.
        self._rot_btn = self._tb(
            config.ICON_ROTATE_CW,
            lambda: self.rotateRequested.emit(90), bar)
        self._rot_menu = QMenu(self._rot_btn)
        for deg in (90, 135, 180, 225, 270):
            act = self._rot_menu.addAction(f"Rotate {deg}°")
            act.setToolTip(f"Rotate {deg}° clockwise (permanent)")
            act.triggered.connect(
                lambda _=False, d=deg: self.rotateRequested.emit(d))
        self._rot_btn.setMenu(self._rot_menu)
        self._rot_btn.setPopupMode(
            QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        self._rot_btn.setToolTip(
            "Rotate 90° clockwise (permanent) — use the arrow for 135°–270°")
        self._slide_btn = self._tb("▶", self._toggle_slideshow, bar,
                                   checkable=True)
        self._slide_btn.setToolTip(
            "Slideshow — auto-advance (videos play through first)\n"
            "Right-click to set the interval")
        self._slide_btn.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self._slide_btn.customContextMenuRequested.connect(
            self._slide_interval_menu)
        self._strip_btn = self._tb("▤", self._toggle_strip, bar, checkable=True)
        self._strip_btn.setToolTip("Filmstrip — thumbnails of the whole set (T)")
        self._tb(config.ICON_INFO, lambda: self.requestInfo.emit(self._path()), bar)
        self._tb(config.ICON_GRID, lambda: self.openMulti.emit(self._row), bar)
        self._tb("?", self._toggle_help, bar).setToolTip("Keyboard shortcuts (?)")
        self._tb(config.ICON_TRASH, self._trash, bar)
        self._tb(config.ICON_CLOSE, self.close, bar)

        # Keyboard-shortcuts help overlay (hidden until toggled).
        self._help = self._build_help_overlay()

        # Floating transport bar (seek + volume) — overlays the bottom edge of the media.
        self._transport = QWidget(self)
        self._transport.setStyleSheet(f"background: {config.BAR_BG};")
        self._transport.hide()
        tlay = QHBoxLayout(self._transport)
        tlay.setContentsMargins(10, 4, 10, 8)
        tlay.setSpacing(8)
        self._play_btn = self._tb(config.ICON_PAUSE, self._toggle_play)
        tlay.addWidget(self._play_btn)
        self._scrub = SeekBar()
        tlay.addWidget(self._scrub, 1)
        self._time = QLabel("0:00 / 0:00")
        self._time.setStyleSheet(f"color: {config.FG_MID};")
        tlay.addWidget(self._time)

        self._speed_idx = 2   # 1.0x
        self._speed_btn = self._tb("1×", self._cycle_speed)
        self._speed_btn.setFixedWidth(46)
        tlay.addWidget(self._speed_btn)

        # Save the frame on screen as a still (decoded from the source, so it
        # is full resolution rather than the scaled-to-widget view).
        self._frame_btn = self._tb(config.ICON_CAMERA, self._save_frame)
        self._frame_btn.setToolTip("Save this frame as an image")
        tlay.addWidget(self._frame_btn)

        # Volume control
        vol_icon = QLabel("\U0001f50a︎")
        vol_icon.setStyleSheet(f"color: {config.FG_MID}; font-size: 13px;")
        tlay.addWidget(vol_icon)
        self._vol_slider = QSlider(Qt.Orientation.Horizontal)
        self._vol_slider.setRange(0, 100)
        self._vol_slider.setValue(80)
        self._vol_slider.setFixedWidth(80)
        self._vol_slider.setStyleSheet(
            f"QSlider::groove:horizontal {{ height:3px; background:{config.FG_DIM};"
            " border-radius:2px; }"
            f"QSlider::sub-page:horizontal {{ background:{config.ACCENT};"
            " border-radius:2px; }"
            f"QSlider::handle:horizontal {{ width:10px; margin:-4px 0;"
            f" border-radius:5px; background:{config.FG_BRIGHT}; }}")
        self._vol_slider.valueChanged.connect(self._set_volume_pct)
        tlay.addWidget(self._vol_slider)

        self._scrub.seeked.connect(self._on_seek)
        self._scrub.loopChanged.connect(self._on_loop_changed)
        # Active A–B loop bounds in ms (None = not looping).
        self._loop_a_ms: "int | None" = None
        self._loop_b_ms: "int | None" = None
        # Animated-GIF playback (frames pushed into _img).
        from .gifplayer import GifPlayer
        self._gif = GifPlayer(self)

        # Free everything (dialog, players, thread pool, retained pixmap) as
        # soon as the viewer closes — otherwise every open leaked a full
        # QMediaPlayer + pixmap that accumulated across a session.
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

        # Async full-image decoding so large images don't freeze the UI.
        # Decode only to screen resolution, not the source's.
        self._img_pool = QThreadPool(self)
        self._img_pool.setMaxThreadCount(2)
        self._decode_px = media.screen_max_px()
        self._img_gen = 0
        self._loaded_img_path: str | None = None
        self._img_sig = _FullImageSignals(self)
        self._img_sig.ready.connect(self._on_full_image)

        # "Loading…" overlay, shown only if a decode takes a noticeable moment.
        self._loading_lbl = QLabel("Loading…", self)
        self._loading_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._loading_lbl.setStyleSheet(
            f"color: {config.FG_MID}; background: rgba(0,0,0,140);"
            " border-radius: 8px; padding: 10px 18px; font-size: 14px;")
        self._loading_lbl.hide()
        self._loading_timer = QTimer(self)
        self._loading_timer.setSingleShot(True)
        self._loading_timer.setInterval(150)
        self._loading_timer.timeout.connect(self._show_loading)

        # Chrome auto-hide — the same rule multi-view already uses, so the whole
        # app behaves one way: bars idle out after a pause and wake on any mouse
        # movement, and never vanish while the pointer is on them or a popup is
        # open.  Applied to the top bar and (for video) the transport.
        self._chrome_hide_timer = QTimer(self)
        self._chrome_hide_timer.setSingleShot(True)
        self._chrome_hide_timer.setInterval(2500)
        self._chrome_hide_timer.timeout.connect(self._maybe_hide_chrome)
        self.setMouseTracking(True)
        for w in (self._stack, self._img, self._video_panel, self._strip):
            w.setMouseTracking(True)
            w.installEventFilter(self)
        self._chrome_hide_timer.start()

        # Slideshow: auto-advance images/GIFs on a timer; a video is allowed to
        # play through once and then advances on EndOfMedia.  The per-image
        # interval is user-configurable (right-click the ▶ button) and persisted.
        self._slideshow_on = False
        self._slide_ms = self._load_slide_ms()
        self._slide_timer = QTimer(self)
        self._slide_timer.setSingleShot(True)
        self._slide_timer.timeout.connect(self._slide_advance)

        self._install_shortcuts()

    def _ensure_player(self) -> None:
        """Build the video pipeline on first use (see __init__ note)."""
        if self._player is not None:
            return
        self._video = QVideoWidget()
        self._video_panel.layout().addWidget(self._video, 1)
        self._player = QMediaPlayer(self)
        self._audio = QAudioOutput(self)
        self._player.setAudioOutput(self._audio)
        self._player.setVideoOutput(self._video)
        self._player.setLoops(QMediaPlayer.Loops.Infinite)
        self._player.mediaStatusChanged.connect(self._on_media_status)
        self._player.errorOccurred.connect(self._on_media_error)
        self._player.positionChanged.connect(self._on_pos)
        self._player.durationChanged.connect(self._on_dur)
        self._set_volume_pct(self._vol_slider.value())

    # -- returning to where the viewer was opened from -------------------------
    def set_return_kind(self, kind: str) -> None:
        """Say what the Back button returns to: "multiview" or "gallery"."""
        self._return_kind = "multiview" if kind == "multiview" else "gallery"
        self._apply_return_kind()

    def _apply_return_kind(self) -> None:
        if self._return_kind == "multiview":
            self._back_btn.setText(f"{config.ICON_BACK} Multi-view")
            self._back_btn.setToolTip(
                "Back to the multi-view set you came from (Esc)")
        else:
            self._back_btn.setText(f"{config.ICON_BACK} Gallery")
            self._back_btn.setToolTip("Back to the gallery (Esc)")

    def _go_back(self) -> None:
        """Leave the viewer, restoring the set it was opened from."""
        if self._return_kind == "multiview":
            self.returnToMulti.emit()
        self.close()

    def _sync_fs_btn(self) -> None:
        """The exit-full-screen button only makes sense while full screen."""
        self._exit_fs_btn.setVisible(self.isFullScreen())

    def changeEvent(self, e) -> None:
        super().changeEvent(e)
        if e.type() == QEvent.Type.WindowStateChange:
            self._sync_fs_btn()

    def _tb(self, glyph, cb, layout=None, checkable=False) -> QToolButton:
        b = QToolButton()
        b.setText(glyph)
        b.setCheckable(checkable)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setStyleSheet(
            f"QToolButton {{ color: {config.FG_MID}; font-size: 15px; border: none;"
            " background: rgba(0,0,0,90); border-radius: 4px; padding: 4px 8px; }"
            " QToolButton:hover { color: #ffffff; background: rgba(0,0,0,160); }")
        b.clicked.connect(cb)
        if layout is not None:
            layout.addWidget(b)
        return b

    _HELP_ROWS = [
        ("←  /  →", "Previous / next item"),
        ("Space", "Play / pause video"),
        ("Right-click bar", "Set A–B loop (A → B → clear)"),
        ("+  /  −", "Zoom in / out (image)"),
        (",  /  .", "Step 1 s back / forward"),
        ("Shift+← / →", "Skip 5 s"),
        ("Ctrl+← / →", "Skip 15 s"),
        ("[  /  ]", "Volume down / up"),
        ("F", "Toggle favourite"),
        ("Delete", "Move to trash"),
        ("F11", "Toggle full screen"),
        ("?  /  F1", "Show / hide this help"),
        ("Esc", "Close help, then viewer"),
    ]

    def _build_help_overlay(self) -> QWidget:
        panel = QWidget(self)
        panel.setStyleSheet(
            "background: rgba(0,0,0,235); border: 1px solid #333;"
            " border-radius: 10px;")
        lay = QVBoxLayout(panel)
        lay.setContentsMargins(22, 18, 22, 18)
        lay.setSpacing(4)
        title = QLabel("Keyboard shortcuts")
        title.setStyleSheet(
            f"color: {config.FG_BRIGHT}; font-size: 15px; font-weight: bold;"
            " background: transparent;")
        lay.addWidget(title)
        rows = "".join(
            f"<tr><td style='color:{config.ACCENT};padding:2px 16px 2px 0;"
            f"white-space:nowrap;'>{k}</td>"
            f"<td style='color:{config.FG_MID};'>{v}</td></tr>"
            for k, v in self._HELP_ROWS)
        body = QLabel(f"<table>{rows}</table>")
        body.setTextFormat(Qt.TextFormat.RichText)
        body.setStyleSheet("background: transparent;")
        lay.addWidget(body)
        panel.hide()
        return panel

    def _toggle_help(self) -> None:
        if self._help.isVisible():
            self._help.hide()
        else:
            self._help.adjustSize()
            self._position_overlays()
            self._help.show()
            self._help.raise_()

    def _on_escape(self) -> None:
        """Esc dismisses the help overlay first, then leaves the viewer.

        Leaving goes back the same way the Back button does, so Esc from a
        multi-view-launched viewer lands on that same set rather than the
        gallery.
        """
        if self._help.isVisible():
            self._help.hide()
        else:
            self._go_back()

    def _install_shortcuts(self) -> None:
        for keys, fn in [
            (QKeySequence(Qt.Key.Key_Escape), self._on_escape),
            (QKeySequence(Qt.Key.Key_Question), self._toggle_help),
            (QKeySequence(Qt.Key.Key_F1),     self._toggle_help),
            (QKeySequence(Qt.Key.Key_Left),   self.prev),
            (QKeySequence(Qt.Key.Key_Right),  self.next),
            (QKeySequence(Qt.Key.Key_T),      self._kb_toggle_strip),
            (QKeySequence(Qt.Key.Key_Plus),   lambda: self._img.zoom_by(1.25)),
            (QKeySequence(Qt.Key.Key_Equal),  lambda: self._img.zoom_by(1.25)),
            (QKeySequence(Qt.Key.Key_Minus),  lambda: self._img.zoom_by(0.8)),
            (QKeySequence(Qt.Key.Key_Space),  self._toggle_play),
            (QKeySequence(Qt.Key.Key_Delete), self._trash),
            (QKeySequence(Qt.Key.Key_F),      self._toggle_fav),
            (QKeySequence(Qt.Key.Key_F11),    self._toggle_fs),
            (QKeySequence(Qt.Key.Key_Comma),  lambda: self._seek_relative(-1000)),
            (QKeySequence(Qt.Key.Key_Period), lambda: self._seek_relative(1000)),
            (QKeySequence("Shift+Left"),      lambda: self._seek_relative(-5000)),
            (QKeySequence("Shift+Right"),     lambda: self._seek_relative(5000)),
            (QKeySequence("Ctrl+Left"),       lambda: self._seek_relative(-15000)),
            (QKeySequence("Ctrl+Right"),      lambda: self._seek_relative(15000)),
            (QKeySequence(Qt.Key.Key_BracketLeft),
             lambda: self._vol_slider.setValue(
                 max(0, self._vol_slider.value() - 10))),
            (QKeySequence(Qt.Key.Key_BracketRight),
             lambda: self._vol_slider.setValue(
                 min(100, self._vol_slider.value() + 10))),
        ]:
            QShortcut(keys, self, activated=fn)

    def show_row(self, row: int) -> None:
        self._row = row
        self._dur_ms = 0
        # A fresh item starts unlooped.
        self._loop_a_ms = self._loop_b_ms = None
        self._scrub.clear_loop()
        self._time.setToolTip("")
        path = self._path()
        if not path:
            return
        n, total = row + 1, self._model.rowCount()
        self._counter.setText(f"{n} / {total}   ·   {os.path.basename(path)}")
        is_fav = self._favs.is_fav(path)
        self._fav_btn.setText(
            config.ICON_HEART_FULL if is_fav else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"QToolButton {{ color: {config.RED}; font-size: 15px; border: none;"
            " background: rgba(0,0,0,90); border-radius: 4px; padding: 4px 8px; }"
            if is_fav else
            f"QToolButton {{ color: {config.FG_MID}; font-size: 15px; border: none;"
            " background: rgba(0,0,0,90); border-radius: 4px; padding: 4px 8px; }")
        self._refresh_rating()
        if media.is_video(path):
            self._gif.stop()
            # Cancel any in-flight image decode and clear its loading state.
            self._img_gen += 1
            self._loaded_img_path = None
            self._loading_timer.stop()
            self._loading_lbl.hide()
            self._stack.setCurrentIndex(1)
            self._transport.setVisible(True)
            self._ensure_player()
            self._player.setSource(QUrl.fromLocalFile(path))
            self._player.play()
            self._player.setPlaybackRate(self._SPEEDS[self._speed_idx])
            self._play_btn.setText(config.ICON_PAUSE)
        elif media.is_gif(path) and self._gif.play(path, self._on_gif_frame):
            # Animated GIF: QMovie drives frames into _img (with a static
            # fallback below if the file isn't a decodable animation).
            if self._player is not None:
                self._player.stop()
            self._img_gen += 1
            self._loaded_img_path = None
            self._loading_timer.stop()
            self._loading_lbl.hide()
            self._stack.setCurrentIndex(0)
            self._transport.setVisible(False)
        else:
            self._gif.stop()
            if self._player is not None:
                self._player.stop()
            self._stack.setCurrentIndex(0)
            self._transport.setVisible(False)
            # Skip re-decoding when re-showing the same image (e.g. after a
            # favourite toggle); otherwise decode off-thread.
            if path != self._loaded_img_path:
                self._img_gen += 1
                self._loaded_img_path = path
                self._loading_timer.start()
                self._img_pool.start(
                    _FullImageJob(self._img_gen, path, self._decode_px,
                                  self._img_sig,
                                  gen_now=lambda: self._img_gen))
        self._position_overlays()
        self._wake_chrome()
        self._slide_kick()
        self._sync_strip()

    def _on_gif_frame(self, pm: QPixmap, first: bool) -> None:
        # Fit-to-view on the first frame; later frames just swap the pixmap so
        # the fit (and any user zoom) is preserved.
        if first:
            self._img.set_pixmap(pm)
        else:
            self._img.set_frame(pm)

    def _on_full_image(self, gen: int, path: str, qim: QImage) -> None:
        if gen != self._img_gen:
            return        # superseded by a later navigation
        self._loading_timer.stop()
        self._loading_lbl.hide()
        if qim is not None and not qim.isNull():
            self._img.set_pixmap(QPixmap.fromImage(qim))

    def _show_loading(self) -> None:
        if self._stack.currentIndex() != 0:
            return
        self._loading_lbl.adjustSize()
        self._position_loading()
        self._loading_lbl.show()
        self._loading_lbl.raise_()

    def _position_loading(self) -> None:
        self._loading_lbl.adjustSize()
        lw, lh = self._loading_lbl.width(), self._loading_lbl.height()
        self._loading_lbl.move(max(0, (self.width() - lw) // 2),
                               max(0, (self.height() - lh) // 2))

    def resizeEvent(self, e) -> None:
        super().resizeEvent(e)
        self._position_overlays()

    def _position_overlays(self) -> None:
        w, h = self.width(), self.height()
        bh = self._bar_widget.sizeHint().height()
        self._bar_widget.setGeometry(0, 0, w, max(bh, 1))
        self._bar_widget.raise_()
        if self._transport.isVisible():
            th = self._transport.sizeHint().height()
            self._transport.setGeometry(0, h - th, w, max(th, 1))
            self._transport.raise_()
        if self._loading_lbl.isVisible():
            self._position_loading()
        if self._help.isVisible():
            self._help.adjustSize()
            hw, hh = self._help.width(), self._help.height()
            self._help.move(max(0, (w - hw) // 2), max(0, (h - hh) // 2))
            self._help.raise_()

    def _path(self) -> str | None:
        return self._model.path_at(self._row)

    def current_path(self) -> str | None:
        return self._path()

    def reload_current(self) -> None:
        """Force a fresh decode of the current item (e.g. after disk rotation)."""
        self._loaded_img_path = None
        self.show_row(self._row)

    def prev(self) -> None:
        if self._hold.isChecked():
            return
        if self._model.rowCount():
            self.show_row((self._row - 1) % self._model.rowCount())

    def next(self) -> None:
        if self._hold.isChecked():
            return
        if self._model.rowCount():
            self.show_row((self._row + 1) % self._model.rowCount())

    # -- chrome auto-hide ------------------------------------------------------
    def _wake_chrome(self) -> None:
        """Show the bars and re-arm the idle timer (mirrors MultiView)."""
        if self._bar_widget.isHidden():
            self._bar_widget.show()
            self._bar_widget.raise_()
            if self._stack.currentIndex() == 1:
                self._transport.show()
                self._transport.raise_()
            self._position_overlays()
        # Mouse-move events arrive in the hundreds per second; only restart the
        # timer as it nears expiry so the handler stays near-free.
        if self._chrome_hide_timer.remainingTime() < 2200:
            self._chrome_hide_timer.start()

    def _maybe_hide_chrome(self) -> None:
        # Keep the chrome while the user is on it, a menu is open, or the
        # keyboard-help overlay is up.
        # isHidden(), not isVisible(): the latter is False for every child of a
        # window that hasn't been shown, which would let the chrome hide out
        # from under an open help overlay in exactly that case.
        if (self._bar_widget.underMouse() or self._transport.underMouse()
                or QApplication.activePopupWidget() is not None
                or not self._help.isHidden()):
            self._chrome_hide_timer.start()
            return
        self._bar_widget.hide()
        self._transport.hide()

    def eventFilter(self, obj, event):
        if event.type() in (QEvent.Type.MouseMove, QEvent.Type.Enter):
            self._wake_chrome()
        return super().eventFilter(obj, event)

    def mouseMoveEvent(self, e):
        self._wake_chrome()
        super().mouseMoveEvent(e)

    # -- save a video frame ----------------------------------------------------
    def _save_frame(self) -> None:
        """Write the frame currently on screen to a PNG/JPEG the user picks."""
        from PySide6.QtWidgets import QFileDialog, QMessageBox
        path = self._path()
        if not path or not media.is_video(path) or self._player is None:
            return
        pos = self._player.position()
        stem = os.path.splitext(os.path.basename(path))[0]
        default = os.path.join(
            os.path.dirname(path), f"{stem}_{int(pos)}ms.png")
        dest, _ = QFileDialog.getSaveFileName(
            self, "Save frame as image", default,
            "PNG (*.png);;JPEG (*.jpg *.jpeg)")
        if not dest:
            return
        was_playing = (self._player.playbackState()
                       == QMediaPlayer.PlaybackState.PlayingState)
        if was_playing:
            self._player.pause()          # hold the picture the user chose
        ok = media.save_frame(path, pos, dest)
        if was_playing:
            self._player.play()
        if ok:
            self._status_note(f"Saved frame to {os.path.basename(dest)}")
        else:
            QMessageBox.warning(self, "Save frame",
                                "Couldn't decode that frame.")

    def _status_note(self, text: str) -> None:
        """Brief on-media confirmation, reusing the loading label's styling."""
        self._loading_timer.stop()
        self._loading_lbl.setText(text)
        self._loading_lbl.adjustSize()
        self._position_loading()
        self._loading_lbl.show()
        QTimer.singleShot(1800, self._loading_lbl.hide)

    # -- filmstrip -------------------------------------------------------------
    def _kb_toggle_strip(self) -> None:
        """Keyboard path: setChecked doesn't emit clicked, so drive it by hand."""
        self._strip_btn.setChecked(not self._strip_btn.isChecked())
        self._toggle_strip()

    def _toggle_strip(self) -> None:
        on = self._strip_btn.isChecked()
        self._strip.setVisible(on)
        if on:
            self._build_strip()
            self._sync_strip()
        self._position_overlays()

    def _build_strip(self) -> None:
        """(Re)populate the rail — cheap placeholders now, thumbs off-thread."""
        total = self._model.rowCount()
        if self._strip_built_for == total and self._strip_model.rowCount():
            return
        self._strip_built_for = total
        self._strip_model.clear()
        self._strip_items.clear()
        for row in range(total):
            path = self._model.path_at(row)
            if not path:
                continue
            item = QStandardItem(os.path.basename(path)[:14])
            item.setEditable(False)
            item.setToolTip(path)
            item.setData(row, Qt.ItemDataRole.UserRole)
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter)
            self._strip_model.appendRow(item)
            self._strip_items[path] = item

    def _sync_strip(self) -> None:
        """Highlight + scroll to the current item and fetch nearby thumbnails."""
        if self._strip.isHidden() or not self._strip_model.rowCount():
            return
        idx = self._strip_model.index(self._row, 0)
        if idx.isValid():
            self._strip.setCurrentIndex(idx)
            self._strip.scrollTo(
                idx, QAbstractItemView.ScrollHint.PositionAtCenter)
        # Only decode a window around the current item: a 50k-item set must not
        # queue 50k decodes just because the rail became visible.
        lo = max(0, self._row - 12)
        hi = min(self._model.rowCount(), self._row + 13)
        for row in range(lo, hi):
            path = self._model.path_at(row)
            item = self._strip_items.get(path or "")
            if item is None or not item.icon().isNull():
                continue
            self._strip_pool.start(
                _StripThumbJob(path, self._STRIP_PX, self._strip_sig))

    def _on_strip_thumb(self, path: str, qim: QImage) -> None:
        item = self._strip_items.get(path)
        if item is not None and not qim.isNull():
            item.setIcon(QPixmap.fromImage(qim))

    def _on_strip_clicked(self, index) -> None:
        row = index.data(Qt.ItemDataRole.UserRole)
        if isinstance(row, int) and row != self._row:
            self.show_row(row)

    # -- slideshow -------------------------------------------------------------
    def _toggle_slideshow(self) -> None:
        self._slideshow_on = self._slide_btn.isChecked()
        if self._slideshow_on:
            self._slide_kick()
        else:
            self._slide_timer.stop()
            if self._player is not None:      # restore normal video looping
                self._player.setLoops(QMediaPlayer.Loops.Infinite)

    def _slide_kick(self) -> None:
        """Arm the next auto-advance for the item now on screen."""
        self._slide_timer.stop()
        if not self._slideshow_on:
            return
        path = self._path()
        if path and media.is_video(path):
            # Play the video through once, then _on_media_status advances.
            if self._player is not None:
                self._player.setLoops(1)
        else:
            self._slide_timer.start(self._slide_ms)

    def _slide_advance(self) -> None:
        if self._slideshow_on and not self._hold.isChecked():
            self.next()

    # Presets offered on right-click, in seconds.
    _SLIDE_PRESETS = (2, 3, 4, 5, 8, 10, 15, 30, 60)
    _SLIDE_MIN_MS = 1000
    _SLIDE_MAX_MS = 600_000

    @staticmethod
    def _load_slide_ms() -> int:
        from .engine import prefs
        try:
            ms = int(prefs.load_prefs().get("slideshow_ms", 4000))
        except (TypeError, ValueError):
            ms = 4000
        return max(Lightbox._SLIDE_MIN_MS, min(Lightbox._SLIDE_MAX_MS, ms))

    def _set_slide_ms(self, ms: int) -> None:
        self._slide_ms = max(self._SLIDE_MIN_MS, min(self._SLIDE_MAX_MS, int(ms)))
        from .engine import prefs
        p = prefs.load_prefs()
        p["slideshow_ms"] = self._slide_ms
        prefs.save_prefs(p)
        # If a show is running on an image, re-arm at the new cadence.
        if self._slideshow_on:
            path = self._path()
            if not (path and media.is_video(path)):
                self._slide_timer.start(self._slide_ms)

    def _slide_interval_menu(self, pos) -> None:
        from PySide6.QtWidgets import QMenu
        menu = QMenu(self)
        cur = self._slide_ms
        for secs in self._SLIDE_PRESETS:
            act = menu.addAction(f"{secs} seconds")
            act.setCheckable(True)
            act.setChecked(abs(secs * 1000 - cur) < 1)
            act.triggered.connect(
                lambda _=False, s=secs: self._set_slide_ms(s * 1000))
        menu.exec(self._slide_btn.mapToGlobal(pos))

    def _toggle_hold(self) -> None:
        pass

    def _set_rating(self, stars: int) -> None:
        from .engine import ratings
        p = self._path()
        if p:
            ratings.cycle_rating(p, stars)
            self._refresh_rating()

    def _refresh_rating(self) -> None:
        from .engine import ratings
        cur = ratings.rating_of(self._path()) if self._path() else 0
        for i, b in enumerate(self._star_btns, start=1):
            b.setText("★" if i <= cur else "☆")
            b.setStyleSheet(
                "QToolButton { border: none; background: rgba(0,0,0,90);"
                " border-radius: 4px; padding: 4px 4px; font-size: 15px; color: "
                + (config.ACCENT if i <= cur else config.FG_MID) + "; }")

    def _toggle_fav(self) -> None:
        p = self._path()
        if p:
            self.favToggled.emit(p)
            self.show_row(self._row)

    def _trash(self) -> None:
        p = self._path()
        if p:
            # Release the handle, not just stop — a stopped QMediaPlayer still
            # holds the file open, which blocks the trash move on Windows.
            if self._player is not None:
                self._player.stop()
                self._player.setSource(QUrl())
            self.trashed.emit(p)

    def _toggle_fs(self) -> None:
        if self.isFullScreen():
            self.showNormal()
        else:
            self.showFullScreen()
            self.raise_()
            self.activateWindow()

    def _cycle_speed(self) -> None:
        self._speed_idx = (self._speed_idx + 1) % len(self._SPEEDS)
        rate = self._SPEEDS[self._speed_idx]
        if self._player is not None:
            self._player.setPlaybackRate(rate)
        self._speed_btn.setText(f"{rate:g}×")

    def _set_volume_pct(self, pct: int) -> None:
        """Apply the slider position as a perceptually-even volume.

        QAudioOutput.setVolume() expects a linear amplitude (0.0-1.0), but human
        loudness perception is logarithmic — mapping the slider straight onto
        amplitude crams almost all the audible change into the bottom of the
        track.  QtAudio.convertVolume() does the standard log→linear remap so
        the slider feels uniform end to end.
        """
        if self._audio is None:
            return          # applied by _ensure_player when the pipeline builds
        amp = QtAudio.convertVolume(
            pct / 100.0,
            QtAudio.VolumeScale.LogarithmicVolumeScale,
            QtAudio.VolumeScale.LinearVolumeScale)
        self._audio.setVolume(amp)

    def _toggle_play(self) -> None:
        if self._gif.is_playing():
            self._gif.toggle_pause()      # Space pauses/resumes an animated GIF
            return
        if self._player is None:
            return
        if self._player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            self._player.pause()
            self._play_btn.setText(config.ICON_PLAY)
        else:
            self._player.play()
            self._play_btn.setText(config.ICON_PAUSE)

    def _on_pos(self, pos: int) -> None:
        # A–B loop: jump back to A the moment playback reaches B.
        if (self._loop_a_ms is not None and self._loop_b_ms is not None
                and pos >= self._loop_b_ms and self._player is not None):
            self._player.setPosition(self._loop_a_ms)
            pos = self._loop_a_ms
        self._scrub.set_position(pos)
        self._time.setText(f"{fmt_time(pos)} / {fmt_time(self._dur_ms)}")

    def _on_loop_changed(self, a_frac: float, b_frac: float) -> None:
        """A–B loop points changed on the scrubber (fractions; <0 = unset)."""
        dur = self._dur_ms or (self._player.duration() if self._player else 0)
        self._loop_a_ms = int(a_frac * dur) if a_frac >= 0 and dur > 0 else None
        self._loop_b_ms = int(b_frac * dur) if b_frac >= 0 and dur > 0 else None
        if self._loop_a_ms is not None and self._loop_b_ms is not None:
            self._time.setToolTip(
                f"Looping {fmt_time(self._loop_a_ms)}–{fmt_time(self._loop_b_ms)}")
            # Snap into the span if we're already past B.
            if (self._player is not None
                    and self._player.position() >= self._loop_b_ms):
                self._player.setPosition(self._loop_a_ms)
        elif self._loop_a_ms is not None:
            self._time.setToolTip(
                f"Loop start {fmt_time(self._loop_a_ms)} — right-click to set end")
        else:
            self._time.setToolTip("")

    def _on_dur(self, dur: int) -> None:
        self._dur_ms = dur
        self._scrub.set_duration(dur)
        pos = self._player.position() if self._player is not None else 0
        self._time.setText(f"{fmt_time(pos)} / {fmt_time(dur)}")

    def _on_seek(self, frac: float) -> None:
        if self._player is None:
            return
        dur = self._dur_ms or self._player.duration()
        if dur > 0:
            self._player.setPosition(int(frac * dur))

    def _on_media_status(self, status) -> None:
        if status == QMediaPlayer.MediaStatus.EndOfMedia:
            # Only the slideshow needs to act here: it sets loops to 1 so the
            # clip plays through once and then advances.  Otherwise the player
            # loops natively, and seeking back to 0 as well raced two restarts
            # at every loop boundary — the stutter and the decoder complaints
            # short clips showed every few seconds.
            if self._slideshow_on:
                self._slide_advance()
        elif status == QMediaPlayer.MediaStatus.InvalidMedia:
            self._fail_video()

    def _on_media_error(self, error, msg: str = "") -> None:
        """Give up on a file that never played; ride out a mid-playback hiccup.

        See _Slot._on_media_error: an error after the file has produced a
        duration is a transient decode glitch, not an unplayable file, and must
        not blacklist it for thumbnails and dimensions.
        """
        if error == QMediaPlayer.Error.NoError:
            return
        if self._player is not None and (self._player.duration() > 0
                                         or self._dur_ms > 0):
            print(f"[video] transient decode error on "
                  f"{os.path.basename(self._path() or '?')}: {msg}",
                  file=sys.stderr)
            return
        self._fail_video()

    def _fail_video(self) -> None:
        if self._player is None or self._stack.currentIndex() != 1:
            return
        path = self._path()
        if path:
            media._mark_bad_video(path)
        self._player.stop()
        self._player.setSource(QUrl())
        self._transport.setVisible(False)
        self._loading_timer.stop()
        self._loading_lbl.setText(
            "⚠  This video can't be played\n(file may be truncated or use an "
            "unsupported codec)")
        self._loading_lbl.show()
        if self._slideshow_on:              # don't stall the show on a bad file
            self._slide_timer.start(self._slide_ms)

    def _seek_relative(self, delta_ms: int) -> None:
        if self._player is None:
            return
        if self._stack.currentIndex() != 1 or self._player.duration() <= 0:
            return
        new = max(0, min(self._player.duration(),
                         self._player.position() + delta_ms))
        self._player.setPosition(new)

    def closeEvent(self, e) -> None:
        # Invalidate + drain in-flight decodes BEFORE the widget (and its
        # signals object) is destroyed by WA_DeleteOnClose, so no worker thread
        # emits into freed memory.
        self._img_gen += 1
        self._loading_timer.stop()
        self._slide_timer.stop()
        self._img_pool.clear()
        self._img_pool.waitForDone(3000)
        self._strip_pool.clear()
        self._strip_pool.waitForDone(3000)
        # Release the heavy resources explicitly.
        self._gif.stop()
        if self._player is not None:
            self._player.stop()
            self._player.setSource(QUrl())
        self._img.set_pixmap(QPixmap())
        super().closeEvent(e)
