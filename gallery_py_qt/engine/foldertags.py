"""Tag whole albums (folders), mirroring each into a "folder tags" subfolder.

This is the folder-level analogue of :mod:`tags`.  Where a *file* tag copies an
individual image into ``<FAVORITES_DIR>/<TAG>/``, a *folder* tag mirrors an
entire album directory into ``<FAVORITES_DIR>/folder tags/<TAG>/<album>/`` — as
a real copy or as a link, following the same ``LINK_MODE`` preference the rest
of the app uses (copy / hardlink / symlink).  So a "folder tags/Az/" directory
gathers a copy-or-link of every album tagged ``Az``.

Design mirrors :mod:`tags`:
  • the tag→folder assignment lives authoritatively in a JSON sidecar store,
    keyed by the album's absolute path;
  • a manifest DB records each mirror's exact path per (album, tag) so add /
    remove is precise even when two albums share a basename;
  • the store is only ever mutated on the calling (GUI) thread; all disk work
    and DB mutation happen on the shared single mirror worker, so there are no
    races and copy-on / copy-off can't reorder.

The same customisable tag vocabulary as file tags is reused (``tags.TAGS``);
removing or renaming a tag cleans up the folder-tag mirrors too.
"""
from __future__ import annotations
import json
import os
import shutil
import sys

from .. import config
from . import favorites, tags
from .favorites import _MIRROR_POOL

# The single subfolder under FAVORITES_DIR that holds every per-tag album mirror.
FOLDER_TAGS_DIRNAME = "folder tags"

# Never descend into these when copying an album (they are our own mirrors /
# the per-file favourites mirror, not album content).
_SKIP_NAMES = {"Favorites", FOLDER_TAGS_DIRNAME}

_FOLDERTAGS_FILE = os.path.join(config.HOME, ".gallery_py_qt_foldertags.json")
_FOLDERTAGDB_FILE = os.path.join(config.HOME, ".gallery_py_qt_foldertagdb.json")
_store: "dict[str, list[str]] | None" = None


def folder_tags_root() -> str:
    return os.path.join(config.FAVORITES_DIR, FOLDER_TAGS_DIRNAME)


def _key(folder: str) -> str:
    return os.path.abspath(os.path.normpath(folder))


def _album_name(folder: str) -> str:
    return os.path.basename(os.path.normpath(folder)) or "album"


# -- store ---------------------------------------------------------------------
def _load() -> "dict[str, list[str]]":
    global _store
    if _store is None:
        try:
            with open(_FOLDERTAGS_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _store = {k: list(v) for k, v in d.items()} \
                if isinstance(d, dict) else {}
        except Exception:
            _store = {}
    return _store


def _save() -> None:
    try:
        tmp = _FOLDERTAGS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_load(), f, indent=1)
        os.replace(tmp, _FOLDERTAGS_FILE)
    except OSError as exc:
        print(f"[foldertags] {exc}", file=sys.stderr)


def tags_for(folder: str) -> "list[str]":
    return list(_load().get(_key(folder), []))


def toggle_folder_tag(folder: str, tag: str) -> bool:
    """Add/remove `tag` on the album `folder`; returns True if now present.

    Mutates the store here (caller thread) and reconciles the on-disk mirrors
    on the background worker.
    """
    store = _load()
    key = _key(folder)
    cur = store.setdefault(key, [])
    if tag in cur:
        cur.remove(tag)
        present = False
    else:
        cur.append(tag)
        present = True
    if not cur:
        store.pop(key, None)
    _save()
    sync_folder_tags(folder)
    return present


def is_mirror_path(folder: str) -> bool:
    """True if `folder` is the folder-tags mirror root or lives inside it.

    Those are the only folders unsafe to (re-)mirror — copying a mirror back
    into the mirror tree nests copies inside copies.  A folder that merely
    lives elsewhere under FAVORITES_DIR is fine to tag: place_folder already
    skips the “folder tags” and “Favorites” names, so a mirror never descends
    into its own output.  (Boundary-aware so a sibling like
    ".../folder tags_old" is not mistaken for being inside it.)
    """
    try:
        f = os.path.abspath(os.path.normpath(folder))
        root = os.path.abspath(os.path.normpath(folder_tags_root()))
        return f == root or f.startswith(root + os.sep)
    except Exception:
        return False


def sync_folder_tags(folder: str) -> None:
    """Reconcile this album's mirrors across the per-tag folder-tag subdirs."""
    # Skip only the mirror tree itself — re-mirroring a mirror nests copies.
    # Folders elsewhere (even under FAVORITES_DIR) mirror safely.
    if is_mirror_path(folder):
        return
    _MIRROR_POOL.submit(_sync_folder_tags_now, folder, tags_for(folder))


# -- manifest DB (worker thread only) ------------------------------------------
def _load_db() -> dict:
    try:
        with open(_FOLDERTAGDB_FILE, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_db(db: dict) -> None:
    try:
        tmp = _FOLDERTAGDB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(db, f)
        os.replace(tmp, _FOLDERTAGDB_FILE)
    except OSError as exc:
        print(f"[foldertag-db] {exc}", file=sys.stderr)


# -- placing / removing a mirrored album ---------------------------------------
def _ignore(_dir: str, names: "list[str]") -> "set[str]":
    return {n for n in names if n in _SKIP_NAMES}


def _copytree(src: str, dst: str) -> None:
    shutil.copytree(src, dst, ignore=_ignore, dirs_exist_ok=True)


def _hardlink_tree(src: str, dst: str) -> None:
    """Recreate `src`'s tree at `dst`, hardlinking each file (copy on failure)."""
    for root_dir, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if d not in _SKIP_NAMES]
        rel = os.path.relpath(root_dir, src)
        target_dir = dst if rel == "." else os.path.join(dst, rel)
        os.makedirs(target_dir, exist_ok=True)
        for fn in files:
            s = os.path.join(root_dir, fn)
            d = os.path.join(target_dir, fn)
            try:
                os.link(s, d)
            except OSError:
                shutil.copy2(s, d)


def place_folder(src: str, dst: str) -> None:
    """Mirror album `src` to `dst` per LINK_MODE.

    "symlink" makes a single directory symlink; "hardlink" recreates the tree
    with per-file hardlinks; "copy" (and any link fallback) duplicates the
    bytes.  Whatever is already at `dst` is cleared first.
    """
    _remove_mirror(dst)
    src_abs = os.path.abspath(src)
    mode = favorites.LINK_MODE
    if mode == "symlink":
        try:
            os.symlink(src_abs, dst, target_is_directory=True)
            return
        except OSError:
            pass
    elif mode == "hardlink":
        try:
            _hardlink_tree(src_abs, dst)
            return
        except OSError:
            pass
    _copytree(src_abs, dst)


def _remove_mirror(path: str) -> None:
    try:
        if os.path.islink(path):
            os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.lexists(path):
            os.remove(path)
    except OSError as exc:
        print(f"[foldertag-rm] {path}: {exc}", file=sys.stderr)


def _unique_dest(sub: str, name: str) -> str:
    dest = os.path.join(sub, name)
    if not os.path.lexists(dest):
        return dest
    i = 1
    while os.path.lexists(os.path.join(sub, f"{name}_{i}")):
        i += 1
    return os.path.join(sub, f"{name}_{i}")


def _sync_folder_tags_now(folder: str, tag_list: "list[str]") -> None:
    key = _key(folder)
    name = _album_name(folder)
    active = set(tag_list)
    db = _load_db()
    entry = dict(db.get(key, {}))          # {tag: absolute mirror path}
    changed = False
    for t in tags.TAGS:
        sub = os.path.join(folder_tags_root(), t)
        try:
            if t in active:
                cur = entry.get(t)
                if cur and os.path.lexists(cur):
                    continue               # already mirrored for this tag
                os.makedirs(sub, exist_ok=True)
                dest = _unique_dest(sub, name)
                place_folder(folder, dest)
                entry[t] = dest
                changed = True
            else:
                cur = entry.pop(t, None)
                if cur is not None:
                    changed = True
                    _remove_mirror(cur)
        except OSError as exc:
            print(f"[foldertag] {t}/{name}: {exc}", file=sys.stderr)
    if changed or key in db:
        if entry:
            db[key] = entry
        else:
            db.pop(key, None)
        _save_db(db)


# -- tag lifecycle hooks (called from tags.remove_tag / rename_tag) ------------
def remove_folder_tag(tag: str) -> None:
    """Drop `tag` from every album and delete its folder-tag mirror dir.

    Store mutation happens here (caller thread); disk + DB cleanup on worker.
    """
    store = _load()
    for k, lst in list(store.items()):
        if tag in lst:
            store[k] = [t for t in lst if t != tag]
            if not store[k]:
                store.pop(k, None)
    _save()
    _MIRROR_POOL.submit(_remove_folder_tag_disk, tag)


def _remove_folder_tag_disk(tag: str) -> None:
    _remove_mirror(os.path.join(folder_tags_root(), tag))
    db = _load_db()
    changed = False
    for k in list(db):
        if tag in db[k]:
            db[k].pop(tag, None)
            changed = True
            if not db[k]:
                db.pop(k, None)
    if changed:
        _save_db(db)


def rename_folder_tag(old: str, new: str) -> None:
    """Migrate `old`→`new` across the store and the folder-tag mirror dir."""
    store = _load()
    for k, lst in list(store.items()):
        if old in lst:
            store[k] = [new if t == old else t for t in lst]
    _save()
    _MIRROR_POOL.submit(_rename_folder_tag_disk, old, new)


def _rename_folder_tag_disk(old: str, new: str) -> None:
    src = os.path.join(folder_tags_root(), old)
    dst = os.path.join(folder_tags_root(), new)
    try:
        if os.path.isdir(src) or os.path.islink(src):
            os.makedirs(folder_tags_root(), exist_ok=True)
            if os.path.lexists(dst):
                for child in os.listdir(src):
                    s = os.path.join(src, child)
                    d = _unique_dest(dst, child)
                    shutil.move(s, d)
                _remove_mirror(src)
            else:
                shutil.move(src, dst)
    except OSError as exc:
        print(f"[foldertag-mv] {old}->{new}: {exc}", file=sys.stderr)
    # Repoint the manifest entries from old/ to new/.
    db = _load_db()
    changed = False
    for entry in db.values():
        if old in entry:
            mirror = entry.pop(old)
            entry[new] = os.path.join(dst, os.path.basename(mirror))
            changed = True
    if changed:
        _save_db(db)
