"""Filesystem operations for the album manager: move, rename, delete.

Kept Qt-free so it is unit-testable headless.  Every function is batch-oriented
and total: it never raises for a bad member, returning per-item results so the
UI can report "moved 4, 1 failed" instead of aborting the whole operation.
Deletes go to the app's recoverable trash, never an irreversible unlink.
"""
from __future__ import annotations
import os
import shutil

from . import favorites


def _unique_dest(dest_dir: str, name: str) -> str:
    """A path in dest_dir for `name`, suffixed _1/_2… if it already exists."""
    dest = os.path.join(dest_dir, name)
    if not os.path.exists(dest):
        return dest
    stem, ext = os.path.splitext(name)
    i = 1
    while os.path.exists(os.path.join(dest_dir, f"{stem}_{i}{ext}")):
        i += 1
    return os.path.join(dest_dir, f"{stem}_{i}{ext}")


def move_paths(paths: "list[str]", dest_dir: str
               ) -> "tuple[list[tuple[str, str]], list[tuple[str, str]]]":
    """Move each path into dest_dir.

    Returns (moved, errors) where moved is [(src, new_path)] and errors is
    [(src, message)].  Collisions get a numbered suffix rather than clobbering.
    """
    moved: "list[tuple[str, str]]" = []
    errors: "list[tuple[str, str]]" = []
    dest_dir = os.path.abspath(dest_dir)
    if not os.path.isdir(dest_dir):
        return moved, [(p, "destination is not a folder") for p in paths]
    for p in paths:
        ap = os.path.abspath(p)
        try:
            if not os.path.exists(ap):
                errors.append((p, "no longer exists"))
                continue
            if os.path.dirname(ap) == dest_dir:
                continue                       # already here — silent no-op
            # Never move a folder into itself or one of its own descendants.
            if os.path.isdir(ap) and (dest_dir == ap
                                      or dest_dir.startswith(ap + os.sep)):
                errors.append((p, "can't move a folder into itself"))
                continue
            dest = _unique_dest(dest_dir, os.path.basename(ap))
            shutil.move(ap, dest)
            moved.append((ap, dest))
        except OSError as exc:
            errors.append((p, str(exc)))
    return moved, errors


def rename_path(path: str, new_name: str) -> "tuple[str | None, str | None]":
    """Rename `path`'s basename to new_name (same parent).

    Returns (new_path, None) on success or (None, message) on failure.
    """
    ap = os.path.abspath(path)
    new_name = (new_name or "").strip()
    if not new_name:
        return None, "name cannot be empty"
    if os.sep in new_name or (os.altsep and os.altsep in new_name):
        return None, "name cannot contain a path separator"
    if new_name in (".", ".."):
        return None, "invalid name"
    if not os.path.exists(ap):
        return None, "no longer exists"
    dest = os.path.join(os.path.dirname(ap), new_name)
    if os.path.abspath(dest) == ap:
        return ap, None                        # unchanged
    if os.path.exists(dest):
        return None, "a file with that name already exists"
    try:
        os.rename(ap, dest)
        return dest, None
    except OSError as exc:
        return None, str(exc)


def delete_paths(paths: "list[str]"
                 ) -> "tuple[list[tuple[str, str]], list[tuple[str, str]]]":
    """Move each path to the app's recoverable trash.

    Returns (deleted, errors) where deleted is [(src, trash_path)].
    """
    deleted: "list[tuple[str, str]]" = []
    errors: "list[tuple[str, str]]" = []
    for p in paths:
        ap = os.path.abspath(p)
        if not os.path.exists(ap):
            errors.append((p, "no longer exists"))
            continue
        trashed = favorites.trash_file(ap)
        if trashed:
            deleted.append((ap, trashed))
        else:
            errors.append((p, "couldn't move to trash"))
    return deleted, errors


def undo_move(pairs: "list[tuple[str, str]]") -> "tuple[int, list[tuple[str, str]]]":
    """Reverse a move batch: send each (src, new) back from `new` to `src`.

    Only moves an item whose original location is still free, so an undo can
    never clobber a file created there since.  Returns (restored, errors).
    """
    restored = 0
    errors: "list[tuple[str, str]]" = []
    for src, new in pairs:
        try:
            if not os.path.exists(new):
                errors.append((new, "no longer exists"))
                continue
            if os.path.exists(src):
                errors.append((src, "something is already back at the original name"))
                continue
            os.makedirs(os.path.dirname(src), exist_ok=True)
            shutil.move(new, src)
            restored += 1
        except OSError as exc:
            errors.append((new, str(exc)))
    return restored, errors


def undo_delete(pairs: "list[tuple[str, str]]") -> "tuple[int, list[tuple[str, str]]]":
    """Restore a delete batch of (src, trash_path) from the trash."""
    restored = 0
    errors: "list[tuple[str, str]]" = []
    for src, trash in pairs:
        if favorites.restore_file(src, trash):
            restored += 1
        else:
            errors.append((src, "couldn't restore from trash"))
    return restored, errors


def make_folder(parent: str, name: str) -> "tuple[str | None, str | None]":
    """Create a new subfolder. Returns (path, None) or (None, message)."""
    name = (name or "").strip()
    if not name:
        return None, "name cannot be empty"
    if os.sep in name or (os.altsep and os.altsep in name):
        return None, "name cannot contain a path separator"
    if not os.path.isdir(parent):
        return None, "parent is not a folder"
    dest = os.path.join(parent, name)
    if os.path.exists(dest):
        return None, "already exists"
    try:
        os.mkdir(dest)
        return dest, None
    except OSError as exc:
        return None, str(exc)
