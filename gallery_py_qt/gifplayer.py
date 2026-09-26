"""Animate a GIF (or any QMovie-readable format) by feeding frames to a sink.

GIFs were shown as a single static frame everywhere because the viewers only
special-cased *video*; an animated GIF is neither video nor a still, so it fell
through to a one-shot image decode.  QMovie decodes and *times* the frames
(handling per-frame disposal and the file's loop count) correctly.

Rather than give GIFs their own widget, this player hands each frame's pixmap
to a callback, so the existing image widgets — the lightbox's zoomable
_ImageView and multi-view's fit/fill _AspectLabel — do the scaling and every
feature built on them keeps working.
"""
from __future__ import annotations

from PySide6.QtCore import QObject
from PySide6.QtGui import QMovie


class GifPlayer(QObject):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._movie: "QMovie | None" = None
        self._path = ""
        self._sink = None
        self._first = True

    def is_playing(self) -> bool:
        return self._movie is not None

    def path(self) -> str:
        return self._path

    def play(self, path: str, on_frame) -> bool:
        """Start animating `path`, calling on_frame(pixmap, is_first) per frame.

        Returns False if the file isn't a decodable animation (the caller
        should fall back to a static decode).  Re-playing the same path while
        already running is a no-op that keeps the animation going.
        """
        if self._movie is not None and self._path == path:
            return True
        self.stop()
        mv = QMovie(path)
        if not mv.isValid():
            return False
        self._movie = mv
        self._path = path
        self._sink = on_frame
        self._first = True
        mv.frameChanged.connect(self._emit)
        mv.start()
        self._emit()                 # push frame 0 now, no initial blank
        return True

    def _emit(self, _frame: int = 0) -> None:
        if self._movie is None or self._sink is None:
            return
        pm = self._movie.currentPixmap()
        if pm.isNull():
            return
        first, self._first = self._first, False
        self._sink(pm, first)

    def is_paused(self) -> bool:
        return (self._movie is not None
                and self._movie.state() == QMovie.MovieState.Paused)

    def set_paused(self, paused: bool) -> None:
        if self._movie is not None:
            self._movie.setPaused(paused)

    def toggle_pause(self) -> None:
        if self._movie is not None:
            self.set_paused(not self.is_paused())

    def stop(self) -> None:
        if self._movie is not None:
            self._movie.stop()
            self._movie.deleteLater()
            self._movie = None
        self._path = ""
        self._sink = None
