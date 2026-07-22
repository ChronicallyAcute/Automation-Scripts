"""Per-file tag store with best-effort embedded-metadata write.

Tags live authoritatively in a JSON sidecar store (fast, format-agnostic,
works for videos).  For JPEGs we additionally try to embed the tag list into
the file's EXIF XPKeywords field — the field Windows Explorer shows as
"Tags" — losslessly via piexif when that package is available.  The embed is
best-effort and runs on the favourites worker thread, never the GUI.
"""
from __future__ import annotations
import json
import os
import sys

from .. import config
from .favorites import _MIRROR_POOL

# The fixed descriptor set exposed as buttons in multi-view.
TAGS = ("T", "BT", "HT", "Az", "Bcs", "WAM", "Jz", "Ahg")

_TAGS_FILE = os.path.join(config.HOME, ".gallery_py_qt_tags.json")
_store: dict[str, list[str]] | None = None


def _load() -> dict[str, list[str]]:
    global _store
    if _store is None:
        try:
            with open(_TAGS_FILE, encoding="utf-8") as f:
                d = json.load(f)
                _store = {k: list(v) for k, v in d.items()} \
                    if isinstance(d, dict) else {}
        except Exception:
            _store = {}
    return _store


def _save() -> None:
    try:
        tmp = _TAGS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_load(), f, indent=1)
        os.replace(tmp, _TAGS_FILE)
    except OSError as exc:
        print(f"[tags] {exc}", file=sys.stderr)


def tags_for(path: str) -> list[str]:
    return list(_load().get(path, []))


def toggle_tag(path: str, tag: str) -> bool:
    """Add/remove `tag` on `path`; returns True if the tag is now present."""
    store = _load()
    cur = store.setdefault(path, [])
    if tag in cur:
        cur.remove(tag)
        present = False
    else:
        cur.append(tag)
        present = True
    if not cur:
        store.pop(path, None)
    _save()
    # Best-effort embedded write, off the GUI thread.
    _MIRROR_POOL.submit(_embed_tags, path, list(cur))
    return present


def _embed_tags(path: str, tags: list[str]) -> None:
    """Write the tag list into JPEG EXIF XPKeywords (Explorer's 'Tags').

    Lossless (piexif rewrites only the EXIF segment).  Silently skipped for
    non-JPEG files or when piexif isn't installed — the JSON store remains
    the source of truth either way.
    """
    if os.path.splitext(path.lower())[1] not in (".jpg", ".jpeg"):
        return
    try:
        import piexif
    except ImportError:
        return
    try:
        exif = piexif.load(path)
        joined = ";".join(tags)
        exif["0th"][piexif.ImageIFD.XPKeywords] = \
            tuple(joined.encode("utf-16-le"))
        piexif.insert(piexif.dump(exif), path)
    except Exception as exc:
        print(f"[tags-embed] {os.path.basename(path)}: {exc}", file=sys.stderr)
