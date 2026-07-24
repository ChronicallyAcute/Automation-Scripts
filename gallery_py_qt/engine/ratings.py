"""Per-file star rating (0–5), persisted in a JSON sidecar store.

Independent of tags/favourites; complements them for sort/search ("rating>=4",
sort by rating).  0 means unrated and is not stored.
"""
from __future__ import annotations
import json
import os
import sys

from .. import config

MAX_STARS = 5
_RATINGS_FILE = os.path.join(config.HOME, ".gallery_py_qt_ratings.json")
_store: "dict[str, int] | None" = None


def _load() -> dict:
    global _store
    if _store is None:
        try:
            with open(_RATINGS_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _store = {k: int(v) for k, v in d.items()
                      if isinstance(v, (int, float))} if isinstance(d, dict) else {}
        except Exception:
            _store = {}
    return _store


def _save() -> None:
    try:
        tmp = _RATINGS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_load(), f, indent=1)
        os.replace(tmp, _RATINGS_FILE)
    except OSError as exc:
        print(f"[ratings] {exc}", file=sys.stderr)


def rating_of(path: str) -> int:
    return int(_load().get(path, 0))


def set_rating(path: str, stars: int) -> int:
    """Clamp to 0..MAX_STARS; 0 removes the entry.  Returns the stored value."""
    stars = max(0, min(MAX_STARS, int(stars)))
    store = _load()
    if stars <= 0:
        store.pop(path, None)
    else:
        store[path] = stars
    _save()
    return stars


def cycle_rating(path: str, stars: int) -> int:
    """Clicking the Nth star sets N, or clears if it already equals N."""
    return set_rating(path, 0 if rating_of(path) == stars else stars)
