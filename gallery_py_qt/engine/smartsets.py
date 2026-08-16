"""Saved searches ("smart collections").

A saved search is the whole filter bar captured under a name: the media-class
toggles, the favourites-only flag, the tag filter (with its any/all mode) and
the search-query string.  Recalling one restores that state, so a view you use
often ("unrated videos", "Az AND Bp favourites") is one click away instead of
several toggles plus a retyped query.

Stored as a plain JSON list so the file stays inspectable and order is the
user's own:

    [{"name": "Unrated videos",
      "images": false, "gifs": false, "videos": true,
      "favs_only": false, "query": "rating=0",
      "tags": ["Az"], "match_all": false}, …]
"""
from __future__ import annotations
import json
import os
import sys

from .. import config

_SETS_FILE = os.path.join(config.HOME, ".gallery_py_qt_smartsets.json")
_store: "list[dict] | None" = None

# Every field a saved set carries, with the default used when one is absent.
_FIELDS = {
    "images": True, "gifs": True, "videos": True,
    "favs_only": False, "query": "", "tags": [], "match_all": False,
}


def _load() -> "list[dict]":
    global _store
    if _store is None:
        try:
            with open(_SETS_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _store = [x for x in d
                      if isinstance(x, dict) and isinstance(x.get("name"), str)] \
                if isinstance(d, list) else []
        except Exception:
            _store = []
    return _store


def _save() -> None:
    try:
        tmp = _SETS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_load(), f, indent=1, ensure_ascii=False)
        os.replace(tmp, _SETS_FILE)
    except OSError as exc:
        print(f"[smartsets] {exc}", file=sys.stderr)


def normalise(state: dict) -> dict:
    """Coerce a raw state dict to the stored shape, filling in defaults."""
    out = {}
    for key, default in _FIELDS.items():
        val = state.get(key, default)
        if key == "tags":
            out[key] = sorted({str(t) for t in val}) if val else []
        elif key == "query":
            out[key] = str(val or "").strip()
        else:
            out[key] = bool(val)
    return out


def all_sets() -> "list[dict]":
    """Every saved set, in user order (each a copy — mutate via save/delete)."""
    return [dict(s) for s in _load()]


def names() -> "list[str]":
    return [s["name"] for s in _load()]


def get(name: str) -> "dict | None":
    for s in _load():
        if s["name"] == name:
            return dict(s)
    return None


def save(name: str, state: dict) -> bool:
    """Create or overwrite the set called `name`.  Returns False on a bad name."""
    name = (name or "").strip()
    if not name:
        return False
    entry = {"name": name}
    entry.update(normalise(state))
    store = _load()
    for i, s in enumerate(store):
        if s["name"] == name:
            store[i] = entry           # overwrite in place, keeping its order
            _save()
            return True
    store.append(entry)
    _save()
    return True


def delete(name: str) -> bool:
    store = _load()
    for i, s in enumerate(store):
        if s["name"] == name:
            store.pop(i)
            _save()
            return True
    return False


def rename(old: str, new: str) -> bool:
    new = (new or "").strip()
    if not new or old == new or get(new) is not None:
        return False
    for s in _load():
        if s["name"] == old:
            s["name"] = new
            _save()
            return True
    return False
