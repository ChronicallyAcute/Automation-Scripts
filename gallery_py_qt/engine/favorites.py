"""Favourites (mirrored into a per-folder Favorites subfolder) and trash/undo."""
from __future__ import annotations
import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from .. import config

# Mirror file operations (copying favourites into each folder's Favorites
# subfolder) run on a single background worker: copying a large video to another drive
# froze the GUI for seconds per heart-click.  One worker keeps operations in
# submission order, so toggle-on -> toggle-off can never race each other.
_MIRROR_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="favmirror")


def flush_mirror_ops(timeout: float = 10.0) -> None:
    """Block until queued mirror copies/removals finish (tests, shutdown)."""
    try:
        _MIRROR_POOL.submit(lambda: None).result(timeout=timeout)
    except Exception:
        pass


def _same_file(a: str, b: str) -> bool:
    try:
        if os.path.getsize(a) != os.path.getsize(b):
            return False
        with open(a, "rb") as fa, open(b, "rb") as fb:
            return fa.read(65536) == fb.read(65536)
    except OSError:
        return False


def mirror_dir_for(path: str) -> str:
    """Favourited media aggregates into a Favorites folder INSIDE the folder
    the file lives in (per-folder mirrors, not one catch-all location)."""
    return os.path.join(os.path.dirname(path), "Favorites")


# How favourites / tag-folder aggregation place their files.  "copy" duplicates
# the bytes (default, always works); "hardlink" / "symlink" reference the
# original to save disk space — huge for videos.  Set from prefs at startup.
LINK_MODE = "copy"


def set_link_mode(mode: str) -> None:
    global LINK_MODE
    LINK_MODE = mode if mode in ("copy", "hardlink", "symlink") else "copy"


def place_file(src: str, dst: str) -> None:
    """Put a copy-or-link of `src` at `dst` per LINK_MODE, replacing whatever
    is there.  Links fall back to a real copy when unsupported (cross-device
    hardlink, no symlink privilege, FS without link support)."""
    try:
        if os.path.lexists(dst):
            os.remove(dst)
    except OSError:
        pass
    if LINK_MODE == "hardlink":
        try:
            os.link(src, dst)
            return
        except OSError:
            pass
    elif LINK_MODE == "symlink":
        try:
            os.symlink(os.path.abspath(src), dst)
            return
        except OSError:
            pass
    shutil.copy2(src, dst)


class Favorites:
    """In-memory favourites set with disk persistence + per-folder mirrors."""

    def __init__(self) -> None:
        self._paths: set[str] = set()
        self.load()

    def load(self) -> None:
        try:
            with open(config.FAVS_FILE, encoding="utf-8") as f:
                d = json.load(f)
                self._paths = set(d) if isinstance(d, list) else set()
        except Exception:
            self._paths = set()

    def _save(self) -> None:
        try:
            with open(config.FAVS_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(self._paths), f, indent=2)
        except OSError as exc:
            print(f"[favs] {exc}", file=sys.stderr)

    def is_fav(self, path: str) -> bool:
        return path in self._paths

    def toggle(self, path: str) -> bool:
        if path in self._paths:
            self._paths.discard(path)
            _MIRROR_POOL.submit(self._unmirror, path)
            new = False
        else:
            self._paths.add(path)
            _MIRROR_POOL.submit(self._mirror, path)
            new = True
        self._save()
        return new

    def discard(self, path: str) -> None:
        if path in self._paths:
            self._paths.discard(path)
            self._save()

    def _mirror(self, path: str) -> None:
        # Runs on the mirror worker — never on the GUI thread.
        try:
            ddir = mirror_dir_for(path)
            os.makedirs(ddir, exist_ok=True)
            dest = os.path.join(ddir, os.path.basename(path))
            if os.path.lexists(dest) and not _same_file(path, dest):
                stem, ext = os.path.splitext(os.path.basename(path))
                dest = os.path.join(ddir, f"{stem}_{int(time.time())}{ext}")
            if not os.path.lexists(dest):
                place_file(path, dest)         # copy or link per LINK_MODE
        except Exception as exc:
            print(f"[favs-mirror] {exc}", file=sys.stderr)

    def resync_mirror(self, path: str) -> None:
        """Refresh the mirror after a favourited file changed on disk (rotation
        re-encodes it; a hardlink would otherwise point at the old content)."""
        if path not in self._paths:
            return
        _MIRROR_POOL.submit(self._resync_mirror_now, path)

    def _resync_mirror_now(self, path: str) -> None:
        try:
            dest = os.path.join(mirror_dir_for(path), os.path.basename(path))
            if os.path.lexists(dest):
                place_file(path, dest)
        except Exception as exc:
            print(f"[favs-resync] {exc}", file=sys.stderr)

    def _unmirror(self, path: str) -> None:
        try:
            dest = os.path.join(mirror_dir_for(path), os.path.basename(path))
            # Remove our managed entry: a link we placed, or a same-content copy
            # (guards against nuking an unrelated file with the same name).
            if os.path.islink(dest) or (os.path.exists(dest)
                                        and _same_file(path, dest)):
                os.remove(dest)
        except Exception as exc:
            print(f"[favs-unmirror] {exc}", file=sys.stderr)


# -- Trash manifest --------------------------------------------------------
# Maps each trashed filename -> {"orig": original_path, "ts": epoch} so the
# trash browser can restore files to where they came from and show their age.
_MANIFEST = os.path.join(config.TRASH_DIR, ".trash_manifest.json")


def _load_manifest() -> dict:
    try:
        with open(_MANIFEST, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_manifest(man: dict) -> None:
    try:
        os.makedirs(config.TRASH_DIR, exist_ok=True)
        with open(_MANIFEST, "w", encoding="utf-8") as f:
            json.dump(man, f, indent=2)
    except OSError as exc:
        print(f"[trash-manifest] {exc}", file=sys.stderr)


def trash_file(path: str) -> str | None:
    try:
        os.makedirs(config.TRASH_DIR, exist_ok=True)
        base = os.path.basename(path)
        dest = os.path.join(config.TRASH_DIR, base)
        if os.path.exists(dest):
            stem, ext = os.path.splitext(base)
            dest = os.path.join(config.TRASH_DIR, f"{stem}_{int(time.time())}{ext}")
        # A video that was just playing may still be locked for a moment on
        # Windows — QMediaPlayer releases its file handle shortly after the
        # source is cleared, not instantly.  Retry briefly before giving up.
        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                shutil.move(path, dest)
                last_exc = None
                break
            except (PermissionError, OSError) as exc:
                last_exc = exc
                time.sleep(0.05 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        man = _load_manifest()
        man[os.path.basename(dest)] = {"orig": path, "ts": time.time()}
        _save_manifest(man)
        return dest
    except Exception as exc:
        print(f"[trash] {exc}", file=sys.stderr)
        return None


def restore_file(orig: str, trash: str) -> bool:
    try:
        shutil.move(trash, orig)
    except Exception as exc:
        print(f"[restore] {exc}", file=sys.stderr)
        return False
    man = _load_manifest()
    if man.pop(os.path.basename(trash), None) is not None:
        _save_manifest(man)
    return True


def list_trash() -> list[dict]:
    """Return trashed items, newest first: {name, path, size, mtime, orig}."""
    man = _load_manifest()
    out: list[dict] = []
    try:
        names = os.listdir(config.TRASH_DIR)
    except OSError:
        return out
    for name in names:
        if name == os.path.basename(_MANIFEST):
            continue
        full = os.path.join(config.TRASH_DIR, name)
        if not os.path.isfile(full):
            continue
        try:
            st = os.stat(full)
        except OSError:
            continue
        out.append({"name": name, "path": full, "size": st.st_size,
                    "mtime": st.st_mtime, "orig": man.get(name, {}).get("orig")})
    out.sort(key=lambda d: d["mtime"], reverse=True)
    return out


def restore_from_trash(trashed_path: str) -> str | None:
    """Move a trashed file back to its recorded original location."""
    man = _load_manifest()
    name = os.path.basename(trashed_path)
    orig = man.get(name, {}).get("orig")
    if not orig:
        return None
    dest = orig
    if os.path.exists(dest):
        stem, ext = os.path.splitext(orig)
        dest = f"{stem}_restored_{int(time.time())}{ext}"
    try:
        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
        shutil.move(trashed_path, dest)
    except Exception as exc:
        print(f"[trash-restore] {exc}", file=sys.stderr)
        return None
    man.pop(name, None)
    _save_manifest(man)
    return dest


def purge_item(trashed_path: str) -> bool:
    """Permanently delete one trashed file."""
    try:
        os.remove(trashed_path)
    except OSError as exc:
        print(f"[trash-purge] {exc}", file=sys.stderr)
        return False
    man = _load_manifest()
    if man.pop(os.path.basename(trashed_path), None) is not None:
        _save_manifest(man)
    return True


def empty_trash() -> int:
    """Permanently delete everything in the trash. Returns the count removed."""
    n = 0
    for item in list_trash():
        if purge_item(item["path"]):
            n += 1
    return n


def purge_older_than(days: int) -> int:
    """Permanently delete trashed items older than *days* (0 disables)."""
    if days <= 0:
        return 0
    cutoff = time.time() - days * 86400
    n = 0
    for item in list_trash():
        if item["mtime"] < cutoff and purge_item(item["path"]):
            n += 1
    return n
