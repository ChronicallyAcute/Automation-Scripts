"""Portable tags + ratings sidecar (export / import).

Tags and ratings live in app-local JSON stores keyed by ABSOLUTE path, so they
silently detach the moment a file is moved, a library is relocated, or the app
is reinstalled on another machine.  This module writes a single portable
manifest keyed by each file's path RELATIVE to a chosen root, so the metadata
can be re-attached after the whole tree moves:

    export_metadata(root, dest)   # library at `root` → dest.json
    …move / copy the library, or reinstall…
    import_metadata(new_root, dest)   # re-attach by relative path

The manifest is plain JSON so it is inspectable and forward-compatible:

    {"schema": 1,
     "items": {"sub/photo.jpg": {"tags": ["Az", "Bp"], "rating": 4}, …}}
"""
from __future__ import annotations
import json
import os

from . import tags, ratings

SCHEMA = 1


def _rel_under(path: str, root: str) -> "str | None":
    """`path` expressed relative to `root`, or None if it isn't beneath it.

    Uses commonpath so a sibling like ``/a/rootX`` isn't mistaken for living
    under ``/a/root``.  Returns forward-slash form for cross-platform manifests.
    """
    ap = os.path.abspath(path)
    ar = os.path.abspath(root)
    try:
        if os.path.commonpath([ap, ar]) != ar:
            return None
    except ValueError:
        return None                      # different drives on Windows
    rel = os.path.relpath(ap, ar)
    if rel == os.curdir or rel.startswith(os.pardir):
        return None
    return rel.replace(os.sep, "/")


def export_metadata(root: str, dest: str) -> int:
    """Write tags + ratings for every stored file under `root` to `dest`.

    Returns the number of files written to the manifest.
    """
    tag_store = tags._load()
    rating_store = ratings._load()
    items: "dict[str, dict]" = {}
    for p in set(tag_store) | set(rating_store):
        rel = _rel_under(p, root)
        if rel is None:
            continue
        entry: "dict" = {}
        t = tag_store.get(p)
        if t:
            entry["tags"] = list(t)
        r = rating_store.get(p)
        if r:
            entry["rating"] = int(r)
        if entry:
            items[rel] = entry
    data = {"schema": SCHEMA, "items": items}
    tmp = dest + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, ensure_ascii=False)
    os.replace(tmp, dest)
    return len(items)


def import_metadata(root: str, src: str, overwrite: bool = False) -> "tuple[int, int]":
    """Re-attach a manifest's tags + ratings to files found under `root`.

    Only entries whose target file actually exists are applied.  By default
    tags are MERGED with any already present and an existing rating is kept;
    with ``overwrite=True`` the saved values replace whatever is there.

    Returns (files_updated, files_missing) where missing counts manifest
    entries whose file wasn't found under `root`.
    """
    with open(src, encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("items", {}) if isinstance(data, dict) else {}
    updated = missing = 0
    for rel, entry in items.items():
        if not isinstance(entry, dict):
            continue
        target = os.path.normpath(os.path.join(root, rel.replace("/", os.sep)))
        if not os.path.exists(target):
            missing += 1
            continue
        changed = False
        new_tags = [t for t in entry.get("tags", []) if isinstance(t, str)]
        if new_tags:
            cur = tags.tags_for(target)
            merged = (list(new_tags) if overwrite
                      else list(dict.fromkeys(cur + new_tags)))
            if merged != cur:
                tags.set_tags_for(target, merged)
                changed = True
        rating = entry.get("rating")
        if isinstance(rating, (int, float)) and int(rating) > 0:
            if overwrite or ratings.rating_of(target) == 0:
                if int(rating) != ratings.rating_of(target):
                    ratings.set_rating(target, int(rating))
                    changed = True
        if changed:
            updated += 1
    return updated, missing
