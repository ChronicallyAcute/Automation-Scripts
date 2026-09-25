"""Reorganise the tag library under FAVORITES_DIR.

FAVORITES_DIR holds two unrelated kinds of directory side by side: the per-tag
folders this app creates, and whatever standalone media folders the user keeps
there. Nothing distinguishes them on disk, so the tag-recovery pass adopted
every directory name as a tag and the chip row filled with names that were
never tags.

This module separates them:

  * the tag folders move under a single ``Tag Folders`` subfolder, so the two
    kinds stop sharing a namespace and a future recovery pass cannot confuse
    them again;
  * names the user does not recognise as tags are dropped from the tag SET
    while their directories stay exactly where they are — they are media, not
    app data, and this module never deletes user content;
  * copies already sitting in the tag and favourites folders are replaced with
    links to the original, reclaiming the space the copies occupy.

Everything is preview-first: each operation has a ``plan_*`` that reports what
would happen and changes nothing.
"""
from __future__ import annotations
import os
from typing import Callable
import shutil
import sys

from .. import config
from . import favorites, tags as _tags

# Every per-tag folder lives under this one subfolder of FAVORITES_DIR.
TAG_FOLDERS_DIRNAME = "Tag Folders"

# Directories under FAVORITES_DIR that are app structure, never tag folders.
_RESERVED = {TAG_FOLDERS_DIRNAME.lower(), "folder tags", "favorites"}

# The convention marking a directory as a real tag rather than a media folder.
TAG_PREFIX = "T - "


def tag_folders_root() -> str:
    return os.path.join(config.FAVORITES_DIR, TAG_FOLDERS_DIRNAME)


def looks_like_tag(name: str) -> bool:
    """Heuristic used to PRE-TICK the picker; the user decides, not this."""
    return name.startswith(TAG_PREFIX)


# -- 1. survey -----------------------------------------------------------------

def survey() -> "list[dict]":
    """Every candidate tag name, from the tag set and from disk.

    Returns one entry per name:
        {"name", "in_tag_set", "folder", "files", "suggested"}
    `folder` is "" when no directory exists for the name. Sorted with the
    likely tags first so the picker opens on what matters.
    """
    root = config.FAVORITES_DIR
    names: "dict[str, dict]" = {}

    def entry(name: str) -> dict:
        return names.setdefault(name, {
            "name": name, "in_tag_set": False, "folder": "", "files": 0,
            "suggested": looks_like_tag(name)})

    for t in _tags.get_tags():
        entry(t)["in_tag_set"] = True

    for base in (root, tag_folders_root()):
        try:
            listing = sorted(os.listdir(base))
        except OSError:
            continue
        for name in listing:
            path = os.path.join(base, name)
            if not os.path.isdir(path) or name.lower() in _RESERVED:
                continue
            e = entry(name)
            e["folder"] = path
            try:
                e["files"] = sum(1 for x in os.scandir(path) if x.is_file())
            except OSError:
                pass
    return sorted(names.values(),
                  key=lambda e: (not e["suggested"], e["name"].lower()))


# -- 2. move the tag folders under one parent ----------------------------------

def plan_merge(keep: "list[str]") -> "list[tuple[str, str]]":
    """(source, destination) for each tag folder that would move."""
    root = config.FAVORITES_DIR
    dest_root = tag_folders_root()
    out = []
    for name in keep:
        src = os.path.join(root, name)
        if not os.path.isdir(src):
            continue
        dst = os.path.join(dest_root, name)
        if os.path.abspath(src) == os.path.abspath(dst):
            continue
        out.append((src, dst))
    return out


def merge_tag_folders(keep: "list[str]") -> "tuple[int, list[str]]":
    """Move each kept tag's folder under ``Tag Folders``.

    Merges into an existing destination rather than replacing it, so running
    this twice cannot lose files. Returns (moved, errors).
    """
    errors: "list[str]" = []
    moved = 0
    for src, dst in plan_merge(keep):
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if not os.path.exists(dst):
                shutil.move(src, dst)
            else:
                # Destination exists: move the contents in, keeping both sides.
                for item in os.listdir(src):
                    s_item = os.path.join(src, item)
                    d_item = os.path.join(dst, item)
                    if os.path.lexists(d_item):
                        stem, ext = os.path.splitext(item)
                        i = 1
                        while os.path.lexists(
                                os.path.join(dst, f"{stem}_{i}{ext}")):
                            i += 1
                        d_item = os.path.join(dst, f"{stem}_{i}{ext}")
                    shutil.move(s_item, d_item)
                try:
                    os.rmdir(src)          # only if we emptied it
                except OSError:
                    pass
            moved += 1
        except OSError as exc:
            errors.append(f"{src}: {exc}")
    if moved:
        _retarget_tagdb()
    return moved, errors


def _retarget_tagdb() -> None:
    """Point the tag-folder manifest at the moved copies.

    The manifest records each mirrored copy's exact path; leaving it stale
    would make the app re-copy every file it can no longer find.
    """
    db = _tags._load_tagdb()
    root = os.path.abspath(config.FAVORITES_DIR)
    dest_root = os.path.abspath(tag_folders_root())
    changed = False
    for orig, entry in db.items():
        if not isinstance(entry, dict):
            continue
        for tag, copy_path in list(entry.items()):
            ap = os.path.abspath(copy_path)
            if ap.startswith(dest_root + os.sep):
                continue                    # already under the new parent
            if not ap.startswith(root + os.sep):
                continue
            rel = os.path.relpath(ap, root)
            moved = os.path.join(dest_root, rel)
            if os.path.exists(moved):
                entry[tag] = moved
                changed = True
    if changed:
        _tags._save_tagdb(db)


# -- 3. prune the tag set ------------------------------------------------------

def prune_tag_set(keep: "list[str]") -> "list[str]":
    """Keep only `keep` in the tag SET. Returns the names dropped.

    Deliberately NOT tags.remove_tag(): that also deletes the tag's folder and
    strips the tag from every file. These names correspond to real media
    folders, and the point is to stop showing them as chips — not to touch a
    single byte or lose the record of what was tagged. The store is left
    intact, so re-adding a name brings its files straight back.
    """
    keep_set = [k for k in keep if k]
    dropped = [t for t in _tags.get_tags() if t not in set(keep_set)]
    _tags.set_tags(keep_set)
    return dropped


# -- 4. turn existing copies into links ----------------------------------------

def mirror_roots(media_folders: "list[str] | None" = None,
                 tag_names: "list[str] | None" = None) -> "list[str]":
    """Every directory holding app-made copies — and ONLY those.

    Deliberately not all of FAVORITES_DIR: the whole problem is that the user's
    own standalone media folders live there too, and rewriting files inside one
    of those would be editing their library, not tidying ours. The app-made
    locations are the per-tag folders (old layout and new), the album mirrors,
    and the per-folder Favorites directories beside the media.
    """
    from . import foldertags
    roots = [tag_folders_root(), foldertags.folder_tags_root(),
             favorites.central_favorites_root()]
    for name in (tag_names if tag_names is not None else _tags.get_tags()):
        roots.append(os.path.join(config.FAVORITES_DIR, name))
    for folder in {os.path.dirname(p) for p in (media_folders or [])}:
        roots.append(os.path.join(folder, "Favorites"))
    return [r for r in dict.fromkeys(roots) if os.path.isdir(r)]


def index_originals(roots: "list[str]",
                    exclude: "list[str] | None" = None,
                    recursive: bool = True,
                    collisions: "dict[str, list[str]] | None" = None,
                    progress: "Callable[[int, str], None] | None" = None
                    ) -> "dict[str, str]":
    """{basename: path} for the media under `roots`, for filename matching.

    Several roots may be given, and each is walked to its full depth by
    default, so originals scattered across unrelated directories (or drives)
    are all found in one pass. Roots may overlap or repeat; a file is indexed
    once.

    First occurrence wins. Pass `collisions` to learn where that mattered: it
    is filled with {basename: [every path seen]} for names found in more than
    one place. With filename-only matching that ambiguity decides which
    original a copy gets linked to, so it is worth showing rather than
    resolving silently.

    `exclude` lists the app-made mirror directories: a copy must never be
    adopted as its own original, or relinking would point a file at itself. A
    standalone media folder inside FAVORITES_DIR is NOT excluded — it is a
    legitimate place for an original to live.
    """
    skip = list(exclude if exclude is not None else mirror_roots())
    out: "dict[str, str]" = {}
    seen_paths: "set[str]" = set()
    n = 0
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            if favorites.under_any(dirpath, skip):
                dirnames[:] = []
                continue
            dirnames[:] = [d for d in dirnames if d != "Favorites"]
            if not recursive:
                dirnames[:] = []
            if progress is not None:
                progress(len(out), dirpath)
            for fn in filenames:
                if os.path.splitext(fn.lower())[1] not in config.SUPPORTED:
                    continue
                path = os.path.join(dirpath, fn)
                ap = os.path.abspath(path)
                if ap in seen_paths:
                    continue          # overlapping roots: index a file once
                if os.path.islink(path):
                    continue          # a link is not an original
                seen_paths.add(ap)
                n += 1
                if fn in out:
                    if collisions is not None:
                        collisions.setdefault(fn, [out[fn]]).append(path)
                    continue
                out[fn] = path
    return out


def plan_relink(roots: "list[str]", index: "dict[str, str]") -> "list[dict]":
    """What relink_copies() would do. One entry per real copy found.

    ``action`` is "link" when an original was matched, "orphan" when no file of
    that name exists outside the mirrors (linking would mean inventing a
    target, so it is left alone).
    """
    out: "list[dict]" = []
    for root in roots:
        for dirpath, _dirnames, filenames in os.walk(root):
            for fn in filenames:
                path = os.path.join(dirpath, fn)

                if os.path.splitext(fn.lower())[1] not in config.SUPPORTED:
                    continue
                original = index.get(fn)
                if original and os.path.abspath(original) == os.path.abspath(path):
                    continue                # this IS the original
                # A hard link is already a link, whatever the name suggests:
                # more than one name for these bytes means no copy exists.
                if favorites.is_link_entry(path):
                    continue
                try:
                    size = os.stat(path).st_size
                except OSError:
                    continue
                out.append({"copy": path, "original": original or "",
                            "bytes": size,
                            "action": "link" if original else "orphan"})
    return out


def relink_copies(plan: "list[dict]") -> "tuple[int, int, list[str]]":
    """Replace each planned copy with a link to its original.

    Returns (relinked, bytes_reclaimed, errors). The link is created at a
    temporary name and only swapped in once it exists, so a failure can never
    leave the copy deleted with nothing in its place.
    """
    relinked = freed = 0
    errors: "list[str]" = []
    for item in plan:
        if item["action"] != "link" or not item["original"]:
            continue
        copy, original = item["copy"], item["original"]
        if not os.path.exists(original):
            errors.append(f"{original}: original has gone")
            continue
        try:
            if favorites.link_in_place(original, copy) == "copy":
                errors.append(f"{copy}: no link possible (would stay a copy)")
                continue
            relinked += 1
            freed += item["bytes"]
        except OSError as exc:
            errors.append(f"{copy}: {exc}")
    return relinked, freed, errors
