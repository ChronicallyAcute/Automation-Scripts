"""Persistent (width, height) cache keyed by path + mtime + size.

Probing a video's dimensions means opening it with OpenCV/FFmpeg — hundreds of
milliseconds each, serialised on the cv2 lock, and noisy on stderr for files
with damaged headers.  Nothing about that result changes unless the file does,
yet it was recomputed from scratch on every launch, so a large video library
spent minutes re-deriving numbers it already knew.

This is the dimension equivalent of the thumbnail disk cache: results survive
restarts and are invalidated automatically when a file is modified or replaced.
Images are cached too — their header read is cheap but not free at scale.

Writes are batched: callers do many lookups in a burst (a dimension pass over a
whole folder), so the JSON is flushed once at the end via ``flush()`` rather
than per entry.
"""
from __future__ import annotations
import json
import os
import sys
import threading

from .. import config

_FILE = os.path.join(config.HOME, ".gallery_py_qt_dims.json")
# path -> [w, h, mtime, size]
_store: "dict[str, list] | None" = None
_dirty = False
_load_failed = False
_LOCK = threading.Lock()          # the dimension pass runs on worker threads


def _load() -> dict:
    global _store, _load_failed
    if _store is None:
        if not os.path.exists(_FILE):
            _store, _load_failed = {}, False
            return _store
        try:
            with open(_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _store = ({k: list(v) for k, v in d.items()
                       if isinstance(v, (list, tuple)) and len(v) == 4}
                      if isinstance(d, dict) else {})
            _load_failed = False
        except Exception as exc:
            # Same rule as the tag store: an unreadable file is not "no data".
            _store, _load_failed = {}, True
            print(f"[dims] could not read {_FILE}: {exc}", file=sys.stderr)
    return _store


def _stat(path: str) -> "tuple[float, int] | None":
    try:
        st = os.stat(path)
        return st.st_mtime, st.st_size
    except OSError:
        return None


def get(path: str) -> "tuple[int, int] | None":
    """Cached (w, h) for `path`, or None when unknown or the file changed."""
    sig = _stat(path)
    if sig is None:
        return None
    with _LOCK:
        row = _load().get(path)
    if not row:
        return None
    w, h, mtime, size = row
    # Compare mtime loosely: some filesystems round it differently on write.
    if abs(float(mtime) - sig[0]) > 1e-6 or int(size) != sig[1]:
        return None
    return int(w), int(h)


def put(path: str, w: int, h: int) -> None:
    """Remember `path`'s dimensions.  A zero/unknown size is not cached, so a
    file that failed to probe is retried rather than pinned as 0x0 forever."""
    global _dirty
    if w <= 0 or h <= 0:
        return
    sig = _stat(path)
    if sig is None:
        return
    with _LOCK:
        _load()[path] = [int(w), int(h), sig[0], sig[1]]
        _dirty = True


def flush() -> None:
    """Persist pending entries (call once after a batch, not per file)."""
    global _dirty
    with _LOCK:
        if not _dirty or _load_failed:
            return
        data = dict(_load())
        _dirty = False
    try:
        tmp = _FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, _FILE)
    except OSError as exc:
        print(f"[dims] {exc}", file=sys.stderr)


def clear() -> int:
    """Forget every cached dimension.  Returns how many entries were dropped."""
    global _store, _dirty
    with _LOCK:
        n = len(_load())
        _store, _dirty = {}, True
    flush()
    return n
