"""Full-screen lightbox: image zoom/pan/rotate + native video playback."""
from __future__ import annotations
import os

from PySide6.QtCore import Qt, QUrl, Signal, QTimer
from PySide6.QtGui import QPixmap, QImage, QKeySequence, QShortcut
from PySide6.QtWidgets import (QDialog, QGraphicsView, QGraphicsScene,
                               QGraphicsPixmapItem, QVBoxLayout, QHBoxLayout,
                               QToolButton, QLabel, QStackedWidget, QWidget)
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput
from PySide6.QtMultimediaWidgets import QVideoWidget

from . import config
from .engine import media
from .seekbar import SeekBar, fmt_time


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

    def __init__(self, model, favorites, parent=None):
        super().__init__(parent)
        self._model = model
        self._favs = favorites
        self._row = 0
        self._dur_ms = 0
        self.setWindowTitle("Viewer")
        self.setStyleSheet(f"background: #000; color: {config.FG_BRIGHT};")

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        bar = QHBoxLayout()
        bar.setContentsMargins(8, 6, 8, 6)
        self._hold = self._tb("HOLD", self._toggle_hold, checkable=True)
        bar.addWidget(self._hold)
        bar.addStretch(1)
        self._counter = QLabel("")
        self._counter.setStyleSheet(f"color: {config.FG_MID};")
        bar.addWidget(self._counter)
        bar.addStretch(1)
        self._fav_btn = self._tb(config.ICON_HEART_EMPTY, self._toggle_fav)
        self._tb(config.ICON_ROTATE_CCW, lambda: self._img.rotate_by(-90), bar)
        self._tb(config.ICON_ROTATE_CW, lambda: self._img.rotate_by(90), bar)
        self._tb(config.ICON_INFO, lambda: self.requestInfo.emit(self._path()), bar)
        self._tb(config.ICON_GRID, lambda: self.openMulti.emit(self._row), bar)
        self._tb(config.ICON_TRASH, self._trash, bar)
        self._tb(config.ICON_CLOSE, self.close, bar)
        bar.insertWidget(bar.count() - 6, self._fav_btn)
        root.addLayout(bar)

        self._stack = QStackedWidget()
        self._img = _ImageView()
        self._video_panel = QWidget()
        vlay = QVBoxLayout(self._video_panel)
        vlay.setContentsMargins(0, 0, 0, 0)
        self._video = QVideoWidget()
        vlay.addWidget(self._video, 1)
        self._player = QMediaPlayer(self)
        self._audio = QAudioOutput(self)
        self._player.setAudioOutput(self._audio)
        self._player.setVideoOutput(self._video)
        self._player.setLoops(QMediaPlayer.Loops.Infinite)
        self._player.mediaStatusChanged.connect(self._on_media_status)
        self._stack.addWidget(self._img)
        self._stack.addWidget(self._video_panel)
        root.addWidget(self._stack, 1)

        self._transport = QWidget()
        tlay = QHBoxLayout(self._transport)
        tlay.setContentsMargins(10, 4, 10, 8)
        self._play_btn = self._tb(config.ICON_PAUSE, self._toggle_play)
        tlay.addWidget(self._play_btn)
        self._scrub = SeekBar()
        tlay.addWidget(self._scrub, 1)
        self._time = QLabel("0:00 / 0:00")
        self._time.setStyleSheet(f"color: {config.FG_MID};")
        tlay.addWidget(self._time)
        root.addWidget(self._transport)

        self._player.positionChanged.connect(self._on_pos)
        self._player.durationChanged.connect(self._on_dur)
        self._scrub.seeked.connect(self._on_seek)

        self._install_shortcuts()

    def _tb(self, glyph, cb, layout=None, checkable=False) -> QToolButton:
        b = QToolButton()
        b.setText(glyph)
        b.setCheckable(checkable)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        b.setStyleSheet(f"color: {config.FG_MID}; font-size: 15px; border: none;"
                        " padding: 4px 8px;")
        b.clicked.connect(cb)
        if layout is not None:
            layout.addWidget(b)
        return b

    def _install_shortcuts(self) -> None:
        for keys, fn in [
            (QKeySequence(Qt.Key.Key_Escape), self.close),
            (QKeySequence(Qt.Key.Key_Left), self.prev),
            (QKeySequence(Qt.Key.Key_Right), self.next),
            (QKeySequence(Qt.Key.Key_Plus), lambda: self._img.zoom_by(1.25)),
            (QKeySequence(Qt.Key.Key_Equal), lambda: self._img.zoom_by(1.25)),
            (QKeySequence(Qt.Key.Key_Minus), lambda: self._img.zoom_by(0.8)),
            (QKeySequence(Qt.Key.Key_Space), self._toggle_play),
            (QKeySequence(Qt.Key.Key_Delete), self._trash),
            (QKeySequence(Qt.Key.Key_F), self._toggle_fs),
            (QKeySequence(Qt.Key.Key_Comma),  lambda: self._seek_relative(-1000)),
            (QKeySequence(Qt.Key.Key_Period), lambda: self._seek_relative(1000)),
            (QKeySequence("Shift+Left"),  lambda: self._seek_relative(-5000)),
            (QKeySequence("Shift+Right"), lambda: self._seek_relative(5000)),
            (QKeySequence("Ctrl+Left"),  lambda: self._seek_relative(-15000)),
            (QKeySequence("Ctrl+Right"), lambda: self._seek_relative(15000)),
        ]:
            QShortcut(keys, self, activated=fn)

    def show_row(self, row: int) -> None:
        self._row = row
        self._dur_ms = 0
        path = self._path()
        if not path:
            return
        n, total = row + 1, self._model.rowCount()
        self._counter.setText(f"{n} / {total}   ·   {os.path.basename(path)}")
        self._fav_btn.setText(config.ICON_HEART_FULL if self._favs.is_fav(path)
                              else config.ICON_HEART_EMPTY)
        self._fav_btn.setStyleSheet(
            f"color: {config.RED}; font-size: 15px; border: none; padding: 4px 8px;"
            if self._favs.is_fav(path) else
            f"color: {config.FG_MID}; font-size: 15px; border: none; padding: 4px 8px;")
        if media.is_video(path):
            self._stack.setCurrentIndex(1)
            self._transport.setVisible(True)
            self._player.setSource(QUrl.fromLocalFile(path))
            self._player.play()
            self._play_btn.setText(config.ICON_PAUSE)
        else:
            self._player.stop()
            self._stack.setCurrentIndex(0)
            self._transport.setVisible(False)
            qim = media.load_full_qimage(path)
            if qim is not None:
                self._img.set_pixmap(QPixmap.fromImage(qim))

    def _path(self) -> str | None:
        return self._model.path_at(self._row)

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

    def _toggle_hold(self) -> None:
        pass

    def _toggle_fav(self) -> None:
        p = self._path()
        if p:
            self.favToggled.emit(p)
            self.show_row(self._row)

    def _trash(self) -> None:
        p = self._path()
        if p:
            self._player.stop()
            self.trashed.emit(p)

    def _toggle_fs(self) -> None:
        self.showNormal() if self.isFullScreen() else self.showFullScreen()

    def _toggle_play(self) -> None:
        if self._player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            self._player.pause()
            self._play_btn.setText(config.ICON_PLAY)
        else:
            self._player.play()
            self._play_btn.setText(config.ICON_PAUSE)

    def _on_pos(self, pos: int) -> None:
        self._scrub.set_position(pos)
        self._time.setText(f"{fmt_time(pos)} / {fmt_time(self._dur_ms)}")

    def _on_dur(self, dur: int) -> None:
        self._dur_ms = dur
        self._scrub.set_duration(dur)
        self._time.setText(
            f"{fmt_time(self._player.position())} / {fmt_time(dur)}")

    def _on_seek(self, frac: float) -> None:
        dur = self._dur_ms or self._player.duration()
        if dur > 0:
            self._player.setPosition(int(frac * dur))

    def _on_media_status(self, status) -> None:
        if status == QMediaPlayer.MediaStatus.EndOfMedia:
            self._player.setPosition(0)
            self._player.play()

    def _seek_relative(self, delta_ms: int) -> None:
        if self._stack.currentIndex() != 1 or self._player.duration() <= 0:
            return
        new = max(0, min(self._player.duration(),
                         self._player.position() + delta_ms))
        self._player.setPosition(new)

    def closeEvent(self, e) -> None:
        self._player.stop()
        super().closeEvent(e)
