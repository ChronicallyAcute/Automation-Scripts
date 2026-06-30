"""Threaded thumbnail loader.

A QThreadPool runs decode jobs off the GUI thread; results arrive on the GUI
thread via a queued signal carrying the source path and a ready QImage.  Only
the GUI thread converts QImage -> QPixmap (a Qt requirement).

Fork improvements over gallery_qt.loader:
  \u2022 Priority scheduling \u2014 each new request gets a monotonically increasing
    priority so the most-recently-requested path is always decoded first.
    This means scrolling to any position in a 50,000-item gallery immediately
    shows thumbnails at that position rather than waiting for earlier requests
    in the queue to drain.
  \u2022 Thread count auto-sizes to the machine's CPU count (capped at 16) instead
    of the original hard-coded 4, making better use of modern multi-core CPUs
    for the mix of I/O and CPU work that thumbnail decoding involves.
  \u2022 cancel() lets callers mark queued-but-not-yet-started jobs as skip-on-run,
    useful when a path is deleted or scrolled so far out of view that decoding
    it is wasted work.
"""
from __future__ import annotations
import os

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal
from PySide6.QtGui import QImage

from .engine import cache


class _Signals(QObject):
    ready = Signal(str, int, QImage)   # path, max_px, image
    failed = Signal(str)


class _Job(QRunnable):
    def __init__(self, path: str, max_px: int, signals: _Signals,
                 cancelled: set):
        super().__init__()
        self._path = path
        self._max_px = max_px
        self._signals = signals
        self._cancelled = cancelled   # shared mutable set; checked at run time

    def run(self) -> None:
        key = (self._path, self._max_px)
        if key in self._cancelled:
            self._cancelled.discard(key)
            return
        try:
            qim = cache.get_thumbnail(self._path, self._max_px)
        except Exception:
            qim = None
        if qim is not None and not qim.isNull():
            self._signals.ready.emit(self._path, self._max_px, qim)
        else:
            self._signals.failed.emit(self._path)


class ThumbnailLoader(QObject):
    """Deduplicating async loader with priority scheduling.

    Connect to ``ready(path, max_px, QImage)``.

    The most recently submitted path gets the highest scheduling priority, so
    the viewport is always served before items that have scrolled out of view.
    """

    ready = Signal(str, int, QImage)
    failed = Signal(str)

    def __init__(self, parent: QObject | None = None,
                 max_threads: int | None = None):
        super().__init__(parent)
        # Cap concurrency at 8: each worker can transiently hold a decoded
        # image, so 16 threads on big-photo folders was a peak-memory hazard.
        n = (max_threads if max_threads is not None
             else min(max(4, os.cpu_count() or 4), 8))
        self._pool = QThreadPool(self)
        self._pool.setMaxThreadCount(n)
        self._inflight: set[tuple[str, int]] = set()
        self._cancelled: set[tuple[str, int]] = set()
        self._priority = 0          # monotonically increasing; higher = sooner
        self._sig = _Signals(self)
        self._sig.ready.connect(self._on_ready)
        self._sig.failed.connect(self._on_failed)

    def request(self, path: str, max_px: int) -> None:
        key = (path, max_px)
        if key in self._inflight:
            return
        self._inflight.add(key)
        self._priority += 1
        self._pool.start(_Job(path, max_px, self._sig, self._cancelled),
                         self._priority)

    def cancel(self, path: str, max_px: int) -> None:
        """Mark a queued job as cancelled; it skips its work when its turn comes."""
        key = (path, max_px)
        if key in self._inflight:
            self._cancelled.add(key)
            self._inflight.discard(key)

    def _on_ready(self, path: str, max_px: int, qim: QImage) -> None:
        self._inflight.discard((path, max_px))
        self.ready.emit(path, max_px, qim)

    def _on_failed(self, path: str) -> None:
        self._inflight = {k for k in self._inflight if k[0] != path}
        self.failed.emit(path)

    def clear(self) -> None:
        self._pool.clear()
        self._inflight.clear()
        self._cancelled.clear()
