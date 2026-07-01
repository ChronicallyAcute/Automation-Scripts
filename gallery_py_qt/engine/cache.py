"""Disk thumbnail cache keyed by (path, mtime, max_px).

Stores decoded thumbnails as PNG so re-hydration on scroll is a fast disk read
instead of a full re-decode.  QImage.save/load is thread-safe (no QPixmap), so
this can run entirely inside the loader's worker threads.

Fork improvement over gallery_qt.engine.cache:
  \u2022 A single os.stat() call replaces the original os.path.exists() +
    os.path.getmtime() pair, halving the syscall count per cache lookup.
    At thousands of thumbnails per second this is a measurable win.
"""
from __future__ import annotations
import hashlib
import os

from PySide6.QtGui import QImage

from .. import config
from . import media


def _key(path: str, mtime: float, max_px: int) -> str:
    raw = f"{path}|{mtime:.3f}|{max_px}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_path(key: str) -> str:
    return os.path.join(config.CACHE_DIR, key + ".png")


def get_thumbnail(path: str, max_px: int) -> QImage | None:
    """Return a cached thumbnail QImage, decoding + caching on a miss."""
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        mtime = 0.0
    cp = _cache_path(_key(path, mtime, max_px))

    # Single stat check -- raises FileNotFoundError on a miss, avoiding the
    # separate os.path.exists() call the original used.
    try:
        os.stat(cp)
        qim = QImage(cp)
        if not qim.isNull():
            return qim
    except FileNotFoundError:
        pass

    qim = media.load_thumbnail(path, max_px)
    if qim is None or qim.isNull():
        return None
    try:
        os.makedirs(config.CACHE_DIR, exist_ok=True)
        qim.save(cp, "PNG")
    except Exception:
        pass
    return qim


_MAX_CACHE_BYTES = 400 * 1024 * 1024   # ~400 MB on-disk thumbnail budget


def enforce_cap(max_bytes: int = _MAX_CACHE_BYTES) -> int:
    """Evict oldest cached thumbnails until the cache is under *max_bytes*.

    The disk cache keys on (path, mtime, max_px); column-count changes and
    rotations mint new keys, so without eviction it grew without bound.  Called
    on startup.  Returns the number of files removed.
    """
    try:
        entries = []
        total = 0
        for name in os.listdir(config.CACHE_DIR):
            fp = os.path.join(config.CACHE_DIR, name)
            try:
                st = os.stat(fp)
            except OSError:
                continue
            entries.append((st.st_mtime, st.st_size, fp))
            total += st.st_size
    except OSError:
        return 0
    if total <= max_bytes:
        return 0
    entries.sort()                      # oldest (smallest mtime) first
    removed = 0
    for _mtime, size, fp in entries:
        if total <= max_bytes:
            break
        try:
            os.remove(fp)
            total -= size
            removed += 1
        except OSError:
            pass
    return removed


def clear() -> int:
    """Delete all cached thumbnails. Returns number of files removed."""
    n = 0
    try:
        for name in os.listdir(config.CACHE_DIR):
            try:
                os.remove(os.path.join(config.CACHE_DIR, name))
                n += 1
            except OSError:
                pass
    except OSError:
        pass
    return n
