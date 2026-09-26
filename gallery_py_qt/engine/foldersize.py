"""Recursive folder sizes, for working out where the space actually went.

A file manager shows a folder's own size as nothing — the bytes are in its
descendants. When a drive is filling up the useful question is "which of these
twenty folders is the 200GB one", and answering it means walking each subtree
and adding up.

The walk is pure (no Qt) so it runs on a worker thread, and it is cancellable,
because pointing this at the root of a large drive would otherwise be
uninterruptible. Hard links are counted ONCE per measurement: a file with three
names inside the tree occupies one file's worth of disk, and counting it three
times would badly overstate what deleting the folder reclaims. Symlinks are
skipped entirely, for the same reason in reverse: they hold a path, not bytes.
"""
from __future__ import annotations
import os
from typing import Callable

from .. import config


class Entry:
    """One row of a size listing: a child folder or file, with its total."""

    __slots__ = ("path", "name", "bytes", "files", "is_dir", "partial")

    def __init__(self, path: str, name: str, nbytes: int, files: int,
                 is_dir: bool, partial: bool = False):
        self.path = path
        self.name = name
        self.bytes = nbytes
        self.files = files
        self.is_dir = is_dir
        self.partial = partial        # walk was cancelled before finishing

    def __repr__(self) -> str:        # pragma: no cover - debugging aid
        return f"<Entry {self.name} {self.bytes}B {self.files} files>"


def measure(folder: str,
            cancelled: "Callable[[], bool] | None" = None,
            seen: "set | None" = None) -> "tuple[int, int, bool]":
    """Total (bytes, files, complete) under `folder`, following no symlinks.

    `seen` carries the (device, inode) pairs already counted so hard links and
    repeated visits are not double counted; pass one shared set across sibling
    measurements to make a whole listing internally consistent.
    """
    if seen is None:
        seen = set()
    total = count = 0
    complete = True
    stack = [folder]
    while stack:
        if cancelled is not None and cancelled():
            return total, count, False
        cur = stack.pop()
        try:
            it = os.scandir(cur)
        except OSError:
            complete = False
            continue
        with it:
            for entry in it:
                try:
                    # Symlinks are pointers, not content: never follow one
                    # (it can aim back up the tree or at another drive, so the
                    # walk would loop or bill someone else's bytes here) and
                    # never count the link node itself, whose own size is just
                    # the length of the path it holds.
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                        continue
                    st = entry.stat(follow_symlinks=False)
                    if st.st_ino:
                        key = (st.st_dev, st.st_ino)
                        if key in seen:
                            continue      # another name for bytes already counted
                        seen.add(key)
                    total += st.st_size
                    count += 1
                except OSError:
                    complete = False
    return total, count, complete


def children_sizes(folder: str,
                   cancelled: "Callable[[], bool] | None" = None,
                   progress: "Callable[[int, int, str], None] | None" = None
                   ) -> "list[Entry]":
    """Every immediate child of `folder`, biggest first.

    Sub-folders carry their whole subtree's total; files carry their own size.
    A single `seen` set spans the listing, so a file hard-linked into two
    sibling folders is attributed to whichever is measured first rather than
    counted twice — the totals then add up to the parent's real footprint.
    """
    try:
        kids = sorted(os.scandir(folder), key=lambda e: e.name.lower())
    except OSError:
        return []
    seen: set = set()
    out: "list[Entry]" = []
    total = len(kids)
    for i, entry in enumerate(kids):
        if cancelled is not None and cancelled():
            break
        if progress is not None:
            progress(i, total, entry.name)
        try:
            if entry.is_symlink():
                continue              # a pointer, not content — see measure()
            if entry.is_dir(follow_symlinks=False):
                nbytes, files, complete = measure(entry.path, cancelled, seen)
                out.append(Entry(entry.path, entry.name, nbytes, files,
                                 True, not complete))
            else:
                st = entry.stat(follow_symlinks=False)
                if st.st_ino and (st.st_dev, st.st_ino) in seen:
                    continue
                if st.st_ino:
                    seen.add((st.st_dev, st.st_ino))
                out.append(Entry(entry.path, entry.name, st.st_size, 1, False))
        except OSError:
            continue
    out.sort(key=lambda e: e.bytes, reverse=True)
    return out


# Re-exported so callers that measure sizes here also format them here.
fmt_size = config.fmt_bytes


# -- Where the app's own storage goes ------------------------------------------
# Three app-managed locations grow without the user ever seeing them in a file
# manager, which is exactly how a drive fills up unnoticed:
#   * the thumbnail cache (capped, but the cap is 400MB),
#   * the trash (unbounded unless auto-purge is on — deleting inside the app
#     does NOT free space until this is emptied),
#   * the tag folders under FAVORITES_DIR (one placement PER TAG per file).
# The per-folder "Favorites" mirrors are counted separately, since they live
# beside the media rather than in one place.

def app_storage_report(media_folders: "list[str] | None" = None) -> dict:
    """Measure what the app itself is using. Returns a dict of labelled totals."""
    from .. import config
    from . import favorites

    out: dict = {"places": [], "total": 0, "mirrors": 0, "mirror_dirs": 0}

    def add(label: str, path: str, note: str = "") -> None:
        if not os.path.isdir(path):
            out["places"].append({"label": label, "path": path, "bytes": 0,
                                  "files": 0, "note": "not created yet"})
            return
        nbytes, files, complete = measure(path)
        out["places"].append({"label": label, "path": path, "bytes": nbytes,
                              "files": files,
                              "note": note or ("" if complete else "partial")})
        out["total"] += nbytes

    add("Thumbnail cache", config.CACHE_DIR,
        "rebuilt on demand — safe to clear")
    add("Trash", config.TRASH_DIR,
        "deleting in the app moves here; space is freed only when emptied")
    add("Tag folders", config.FAVORITES_DIR,
        "one placement per tag, per favourited file")

    # Per-folder favourites mirrors sit next to the media, so they have to be
    # found folder by folder rather than measured in one place.
    seen: set = set()
    for folder in {os.path.dirname(p) for p in (media_folders or [])}:
        mirror = favorites.mirror_dir_for(os.path.join(folder, "x"))
        if os.path.isdir(mirror) and mirror not in seen:
            seen.add(mirror)
            nbytes, files, _ = measure(mirror)
            out["mirrors"] += nbytes
            out["mirror_dirs"] += 1
    out["total"] += out["mirrors"]
    return out
