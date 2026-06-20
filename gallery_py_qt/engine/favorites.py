"""Favourites (with Downloads mirror) and trash/undo."""
from __future__ import annotations
import ctypes
import json
import os
import shutil
import sys
import time

from .. import config


def _same_file(a: str, b: str) -> bool:
    try:
        if os.path.getsize(a) != os.path.getsize(b):
            return False
        with open(a, "rb") as fa, open(b, "rb") as fb:
            return fa.read(65536) == fb.read(65536)
    except OSError:
        return False


def ensure_favorites_dir() -> None:
    try:
        os.makedirs(config.FAVORITES_DIR, exist_ok=True)
    except OSError as exc:
        print(f"[favs-dir] {exc}", file=sys.stderr)
        return
    if sys.platform.startswith("win"):
        try:
            ctypes.windll.kernel32.SetFileAttributesW(config.FAVORITES_DIR, 0x02)
        except Exception:
            pass


class Favorites:
    """In-memory favourites set with disk persistence + Downloads mirror."""

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
            self._unmirror(path)
            new = False
        else:
            self._paths.add(path)
            self._mirror(path)
            new = True
        self._save()
        return new

    def discard(self, path: str) -> None:
        if path in self._paths:
            self._paths.discard(path)
            self._save()

    def _mirror(self, path: str) -> None:
        try:
            ensure_favorites_dir()
            dest = os.path.join(config.FAVORITES_DIR, os.path.basename(path))
            if os.path.exists(dest) and not _same_file(path, dest):
                stem, ext = os.path.splitext(os.path.basename(path))
                dest = os.path.join(config.FAVORITES_DIR,
                                    f"{stem}_{int(time.time())}{ext}")
            if not os.path.exists(dest):
                shutil.copy2(path, dest)
        except Exception as exc:
            print(f"[favs-mirror] {exc}", file=sys.stderr)

    def _unmirror(self, path: str) -> None:
        try:
            dest = os.path.join(config.FAVORITES_DIR, os.path.basename(path))
            if os.path.exists(dest) and _same_file(path, dest):
                os.remove(dest)
        except Exception as exc:
            print(f"[favs-unmirror] {exc}", file=sys.stderr)


def trash_file(path: str) -> str | None:
    try:
        os.makedirs(config.TRASH_DIR, exist_ok=True)
        base = os.path.basename(path)
        dest = os.path.join(config.TRASH_DIR, base)
        if os.path.exists(dest):
            stem, ext = os.path.splitext(base)
            dest = os.path.join(config.TRASH_DIR, f"{stem}_{int(time.time())}{ext}")
        shutil.move(path, dest)
        return dest
    except Exception as exc:
        print(f"[trash] {exc}", file=sys.stderr)
        return None


def restore_file(orig: str, trash: str) -> bool:
    try:
        shutil.move(trash, orig)
        return True
    except Exception as exc:
        print(f"[restore] {exc}", file=sys.stderr)
        return False
