"""Remember which media files are damaged, so we stop paying for them.

A truncated download keeps a valid header claiming the full duration, so the
file opens and plays — until the decoder runs past the real end of the data.
FFmpeg then reports ``partial file`` and scans BACKWARDS hunting for a readable
sample; on a 100MB+ file that is slow, blocking work, and it is repeated every
single time the file is touched (thumbnail, dimension probe, playback, loop).
That is felt as stutter in the grid and in multi-view.

:mod:`~gallery_py_qt.engine.media` already keeps an in-memory blacklist, but it
is rebuilt from scratch every launch and only learns about a file after paying
the cost at least once.  This records the verdict on disk — keyed by
path + mtime + size, like the dimension cache, so a file that is repaired or
replaced is re-checked rather than condemned forever — and can seed the
in-memory blacklist for a folder before anything touches it.
"""
from __future__ import annotations
import json
import os
import sys
import threading

from .. import config
from . import media

_FILE = os.path.join(config.HOME, ".gallery_py_qt_health.json")
# path -> [ok, reason, mtime, size]
_store: "dict[str, list] | None" = None
_dirty = False
_load_failed = False
_LOCK = threading.Lock()


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
            # Same rule as the tag store: unreadable is not "no data".
            _store, _load_failed = {}, True
            print(f"[health] could not read {_FILE}: {exc}", file=sys.stderr)
    return _store


def _stat(path: str) -> "tuple[float, int] | None":
    try:
        st = os.stat(path)
        return st.st_mtime, st.st_size
    except OSError:
        return None


def get(path: str) -> "tuple[bool, str] | None":
    """Remembered (ok, reason) for `path`, or None when unknown or changed."""
    sig = _stat(path)
    if sig is None:
        return None
    with _LOCK:
        row = _load().get(path)
    if not row:
        return None
    ok, reason, mtime, size = row
    if abs(float(mtime) - sig[0]) > 1e-6 or int(size) != sig[1]:
        return None                       # file changed — the verdict is stale
    return bool(ok), str(reason)


def put(path: str, ok: bool, reason: str = "") -> None:
    global _dirty
    sig = _stat(path)
    if sig is None:
        return
    with _LOCK:
        _load()[path] = [bool(ok), str(reason), sig[0], sig[1]]
        _dirty = True


def forget(path: str) -> None:
    """Drop a verdict so the file is checked afresh."""
    global _dirty
    with _LOCK:
        if _load().pop(path, None) is not None:
            _dirty = True
    media.forget_bad_video(path)


def flush() -> None:
    """Persist pending verdicts (call once after a batch, not per file)."""
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
        print(f"[health] {exc}", file=sys.stderr)


def clear() -> int:
    """Forget every verdict.  Returns how many were dropped."""
    global _store, _dirty
    with _LOCK:
        n = len(_load())
        _store, _dirty = {}, True
    flush()
    return n


def damaged() -> "list[tuple[str, str]]":
    """Every file currently recorded as damaged, as (path, reason)."""
    with _LOCK:
        rows = list(_load().items())
    out = []
    for path, row in rows:
        if not row[0] and get(path) is not None:      # still stale-checked
            out.append((path, str(row[1])))
    return sorted(out)


def seed_blacklist(paths: "list[str]") -> int:
    """Pre-load known-damaged files into the in-memory skip list.

    Cheap (one stat per file) and it means a folder full of truncated videos
    costs nothing on the very first thumbnail pass rather than one expensive
    FFmpeg backward scan each.  Returns how many were seeded.
    """
    n = 0
    for p in paths:
        rec = get(p)
        if rec is not None and not rec[0]:
            media._mark_bad_video(p)
            n += 1
    return n


def check(paths: "list[str]", recheck: bool = False,
          progress=None) -> dict:
    """Probe `paths` for damage, remembering each verdict.

    `progress(done, total, path)` may return False to cancel.  Returns
    ``{"checked", "cached", "ok", "damaged": [(path, reason)], "cancelled"}``.
    """
    vids = [p for p in paths if media.is_video(p)]
    report = {"checked": 0, "cached": 0, "ok": 0,
              "damaged": [], "cancelled": False}
    total = len(vids)
    for i, p in enumerate(vids):
        if progress is not None and progress(i, total, p) is False:
            report["cancelled"] = True
            break
        rec = None if recheck else get(p)
        if rec is None:
            ok, reason = media.probe_integrity(p)
            put(p, ok, reason)
            report["checked"] += 1
        else:
            ok, reason = rec
            report["cached"] += 1
        if ok:
            report["ok"] += 1
        else:
            report["damaged"].append((p, reason))
            media._mark_bad_video(p)
    flush()
    return report
