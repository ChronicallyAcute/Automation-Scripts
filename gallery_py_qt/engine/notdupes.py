"""Pairs the user has declared are NOT duplicates of each other.

Fuzzy matching finds real duplicates, but it also finds look-alikes: two
frames from the same shoot, two edits of one clip, a photo and its crop. Those
are flagged every single scan, and re-judging them is the tax that makes a
duplicate finder not worth running.

A verdict is remembered per PAIR rather than per group, because groups are not
stable: the same file turns up with different company next time, and a
group-level "keep these" would say nothing about the new member.

Paths are normalised the way the tag store normalises them (case and
separators), so the same file spelled differently is the same file here too.
"""
from __future__ import annotations
import json
import os
import sys
import threading

from .. import config

_FILE = os.path.join(config.HOME, ".gallery_py_qt_notdupes.json")
# Sorted (a, b) normalised-path pairs.
_store: "set[tuple[str, str]] | None" = None
_dirty = False
_load_failed = False
_LOCK = threading.Lock()


def _norm(path: str) -> str:
    try:
        return os.path.normcase(os.path.normpath(path))
    except Exception:
        return path


def _key(a: str, b: str) -> "tuple[str, str]":
    x, y = _norm(a), _norm(b)
    return (x, y) if x <= y else (y, x)


def _load() -> "set[tuple[str, str]]":
    global _store, _load_failed
    if _store is None:
        if not os.path.exists(_FILE):
            _store, _load_failed = set(), False
            return _store
        try:
            with open(_FILE, encoding="utf-8") as f:
                raw = json.load(f)
            _store = {(_norm(p[0]), _norm(p[1])) for p in raw
                      if isinstance(p, (list, tuple)) and len(p) == 2}
            _store = {(a, b) if a <= b else (b, a) for a, b in _store}
            _load_failed = False
        except Exception as exc:
            # Same rule as the tag store: unreadable is not "no data".
            _store, _load_failed = set(), True
            print(f"[notdupes] could not read {_FILE}: {exc}", file=sys.stderr)
    return _store


def mark(paths: "list[str]") -> int:
    """Record every pair in `paths` as distinct. Returns pairs added."""
    global _dirty
    items = list(dict.fromkeys(paths))
    added = 0
    with _LOCK:
        store = _load()
        for i, a in enumerate(items):
            for b in items[i + 1:]:
                k = _key(a, b)
                if k not in store:
                    store.add(k)
                    added += 1
        if added:
            _dirty = True
    if added:
        flush()
    return added


def unmark(paths: "list[str]") -> int:
    """Forget the verdict for every pair in `paths` (they may flag again)."""
    global _dirty
    items = list(dict.fromkeys(paths))
    removed = 0
    with _LOCK:
        store = _load()
        for i, a in enumerate(items):
            for b in items[i + 1:]:
                k = _key(a, b)
                if k in store:
                    store.remove(k)
                    removed += 1
        if removed:
            _dirty = True
    if removed:
        flush()
    return removed


def is_marked(a: str, b: str) -> bool:
    with _LOCK:
        return _key(a, b) in _load()


def count() -> int:
    with _LOCK:
        return len(_load())


def clear() -> int:
    """Forget every verdict. Returns how many pairs were dropped."""
    global _store, _dirty
    with _LOCK:
        n = len(_load())
        _store, _dirty = set(), True
    flush()
    return n


def flush() -> None:
    global _dirty
    with _LOCK:
        if not _dirty or _load_failed:
            return
        data = [list(p) for p in sorted(_load())]
        _dirty = False
    try:
        tmp = _FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, _FILE)
    except OSError as exc:
        print(f"[notdupes] {exc}", file=sys.stderr)


def split(group: "list[str]") -> "tuple[list[list[str]], list[str]]":
    """Break `group` apart so no sub-group holds a pair declared distinct.

    Returns (sub_groups, held_back). Members are seeded most-connected first,
    which keeps the largest true group intact; each then joins the first
    sub-group it is not excluded from.

    Some loss is unavoidable and worth stating plainly: if a and b are
    identical, b and c are identical, and the user has declared a and c
    distinct, no partition can hold all three. Whoever ends up alone is
    reported as `held_back` rather than disappearing from the results with no
    explanation.
    """
    members = list(group)
    allowed = {p: sum(1 for q in members if q != p and not is_marked(p, q))
               for p in members}
    ordered = sorted(members, key=lambda p: (-allowed[p], p))

    buckets: "list[list[str]]" = []
    for path in ordered:
        for bucket in buckets:
            if all(not is_marked(path, other) for other in bucket):
                bucket.append(path)
                break
        else:
            buckets.append([path])

    kept = [sorted(b) for b in buckets if len(b) > 1]
    held = sorted(p for b in buckets if len(b) == 1 for p in b)
    return kept, held
