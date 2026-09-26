"""Favourites (mirrored into a per-folder Favorites subfolder) and trash/undo."""
from __future__ import annotations
import json
import os
import re
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


# Where the per-file favourites mirror goes.  Beside the media (the original
# behaviour) keeps each folder self-contained — but it also means the mirror
# lands on the MEDIA's drive, and a drive that cannot store links (exFAT,
# FAT32) forces every favourite to be a full copy no matter how the app is
# configured.  Centralising puts the mirrors under FAVORITES_DIR instead, which
# can be moved to an NTFS drive, and a link stored there may freely point at a
# file on any filesystem.
FAVORITES_BESIDE_MEDIA = True


def set_favorites_layout(beside: bool) -> None:
    global FAVORITES_BESIDE_MEDIA
    FAVORITES_BESIDE_MEDIA = bool(beside)


def central_favorites_root() -> str:
    return os.path.join(config.FAVORITES_DIR, "Favorites")


_DRIVE_RE = re.compile(r"^([A-Za-z]):[\\/]?")


def mirror_subpath(src_dir: str) -> "list[str]":
    """The path components a centralised mirror of `src_dir` lives under.

    ``G:\\X\\Trip`` -> ``["G", "X", "Trip"]``; ``/media/Trip`` ->
    ``["media", "Trip"]``; ``\\\\nas\\share\\Trip`` -> ``["nas", "share",
    "Trip"]``.  Parsed here rather than with os.path.splitdrive so the shape is
    the same whichever OS is running — a library written on Windows must map
    the same way when read anywhere else.
    """
    text = (src_dir or "").replace("\\", "/")
    head: "list[str]" = []
    m = _DRIVE_RE.match(src_dir or "")
    if m:
        head = [m.group(1).upper()]
        text = text[m.end():]
    elif text.startswith("//"):                 # UNC \\server\share
        text = text[2:]
    parts = [p for p in text.split("/") if p and p not in (".", "..")]
    return head + parts


def mirror_dir_for(path: str) -> str:
    """The Favorites directory holding `path`'s mirrored copy-or-link.

    Beside the media by default.  Centralised, the source path's shape is
    preserved under the mirror root, so two same-named folders on different
    drives cannot collide and the result stays browsable.
    """
    if FAVORITES_BESIDE_MEDIA:
        return os.path.join(os.path.dirname(os.path.abspath(path)), "Favorites")
    # Drop the filename using BOTH separators: os.path.dirname only knows the
    # host's, and a Windows-shaped path must map identically when the same
    # library is read on another OS.
    parts = mirror_subpath(path if _DRIVE_RE.match(path or "")
                           or "\\" in (path or "")
                           else os.path.abspath(path))
    return os.path.join(central_favorites_root(), *parts[:-1])


# How favourites / tag-folder aggregation place their files.  "copy" duplicates
# the bytes (default, always works); "hardlink" / "symlink" reference the
# original to save disk space — huge for videos.  Set from prefs at startup.
LINK_MODE = "copy"


# "auto" is the sensible default for a library that should not be duplicated:
# try the cheapest real link first, and only copy when the platform leaves no
# alternative.  Hard links are preferred because they survive the original
# being moved within its volume and need no privilege; symlinks are the only
# option ACROSS volumes (the tag folders under FAVORITES_DIR are routinely on
# a different drive than the media), at the cost of breaking if the original
# moves.
_MODES = ("copy", "hardlink", "symlink", "auto")


def set_link_mode(mode: str) -> None:
    global LINK_MODE
    LINK_MODE = mode if mode in _MODES else "copy"


def _ladder() -> "tuple[str, ...]":
    """The placement strategies to attempt, in order, for the current mode."""
    if LINK_MODE == "auto":
        return ("hardlink", "symlink")
    if LINK_MODE in ("hardlink", "symlink"):
        return (LINK_MODE,)
    return ()


# When a link is requested but cannot be made, we fall back to a real copy so
# the mirror still works.  That fallback is SILENT and costs full disk space,
# which is how someone can run in "hardlink" mode, believe they are saving
# space, and still fill a drive.  The two usual causes are structural rather
# than transient:
#   * hardlink across volumes — os.link cannot span drives, and the tag folders
#     live under FAVORITES_DIR, which is often on a different drive than the
#     media;
#   * symlink without privilege — Windows needs Developer Mode or an elevated
#     process to create one.
# So count the fallbacks and say so once, rather than leaving it invisible.
_link_fallbacks = 0
_link_fallback_reason = ""
_warned_fallback = False


def link_fallbacks() -> "tuple[int, str]":
    """(how many placements silently became full copies, last reason)."""
    return _link_fallbacks, _link_fallback_reason


def reset_link_fallbacks() -> None:
    global _link_fallbacks, _link_fallback_reason, _warned_fallback
    _link_fallbacks = 0
    _link_fallback_reason = ""
    _warned_fallback = False


def _note_fallback(exc: OSError) -> None:
    global _link_fallbacks, _link_fallback_reason, _warned_fallback
    _link_fallbacks += 1
    _link_fallback_reason = str(exc)
    if not _warned_fallback:
        _warned_fallback = True
        print(f"[favs] {LINK_MODE} not possible here ({exc}) — falling back to "
              "full copies, which use disk space. Check that the favourites "
              "folder is on the same drive as your media (hardlink), or "
              "enable Developer Mode (symlink).", file=sys.stderr)


def place_file(src: str, dst: str) -> str:
    """Put a copy-or-link of `src` at `dst` per LINK_MODE, replacing whatever
    is there.  Returns the mode ACTUALLY used ("hardlink"/"symlink"/"copy"),
    which is not always the one requested — links fall back to a real copy when
    unsupported (cross-device hardlink, no symlink privilege, FS without link
    support)."""
    try:
        if os.path.lexists(dst):
            os.remove(dst)
    except OSError:
        pass
    last: "OSError | None" = None
    for how in _ladder():
        try:
            if how == "hardlink":
                os.link(src, dst)
            else:
                os.symlink(os.path.abspath(src), dst)
            return how
        except OSError as exc:
            last = exc
    if last is not None:
        _note_fallback(last)
    shutil.copy2(src, dst)
    return "copy"


# -- What the volume itself can do ---------------------------------------------
# Developer Mode grants the PRIVILEGE to create a symlink; it cannot grant the
# ability to store one.  That is a property of the filesystem, and the drives
# people keep media on are very often exFAT or FAT32 — neither of which can
# hold a hard link OR a reparse point, so every placement there degrades to a
# full copy no matter how the app is configured.  Reporting the filesystem
# turns "links are not possible" from a dead end into something actionable.

# GetVolumeInformationW capability flags.
_FILE_SUPPORTS_REPARSE_POINTS = 0x00000080
_FILE_SUPPORTS_HARD_LINKS     = 0x00400000


def volume_info(path: str) -> dict:
    """{"filesystem", "hardlinks", "symlinks", "root"} for `path`'s volume.

    `hardlinks`/`symlinks` are None when the platform cannot be asked (any
    non-Windows OS, where the probe is the only authority).
    """
    root = os.path.splitdrive(os.path.abspath(path))[0]
    out = {"filesystem": "", "hardlinks": None, "symlinks": None,
           "root": root + os.sep if root else ""}
    if not sys.platform.startswith("win") or not root:
        return out
    try:
        import ctypes
        from ctypes import wintypes
        name_buf = ctypes.create_unicode_buffer(261)
        fs_buf = ctypes.create_unicode_buffer(261)
        serial = wintypes.DWORD()
        max_comp = wintypes.DWORD()
        flags = wintypes.DWORD()
        ok = ctypes.windll.kernel32.GetVolumeInformationW(
            ctypes.c_wchar_p(out["root"]), name_buf, ctypes.sizeof(name_buf),
            ctypes.byref(serial), ctypes.byref(max_comp), ctypes.byref(flags),
            fs_buf, ctypes.sizeof(fs_buf))
        if not ok:
            return out
        out["filesystem"] = fs_buf.value
        out["hardlinks"] = bool(flags.value & _FILE_SUPPORTS_HARD_LINKS)
        out["symlinks"] = bool(flags.value & _FILE_SUPPORTS_REPARSE_POINTS)
    except Exception:
        pass
    return out


def explain_link_failure(folder: str, source: "str | None" = None) -> str:
    """Why linking into `folder` is impossible, in terms the user can act on."""
    tgt = volume_info(folder)
    bits = []
    if tgt["filesystem"]:
        bits.append(f"{tgt['root']} is formatted {tgt['filesystem']}.")
    if tgt["symlinks"] is False and tgt["hardlinks"] is False:
        bits.append(
            f"{tgt['filesystem'] or 'That filesystem'} cannot store hard links "
            "or symlinks at all — this is a property of the DRIVE, not of "
            "Windows, so Developer Mode makes no difference to it. Only NTFS "
            "(and ReFS) can hold links.")
        bits.append("Options: reformat that drive as NTFS (it erases the "
                    "drive), or keep the tag folders on an NTFS drive by "
                    "pointing the favourites location there.")
        return " ".join(bits)
    if source is not None:
        src = volume_info(source)
        if (src["root"] and tgt["root"]
                and src["root"].lower() != tgt["root"].lower()):
            bits.append(
                f"The media is on {src['root']} and the tag folders are on "
                f"{tgt['root']}. A hard link cannot cross drives, so only a "
                "symlink can work here.")
    if tgt["symlinks"] is False:
        bits.append(f"{tgt['root']} cannot store symlinks (no reparse-point "
                    "support).")
    elif tgt["symlinks"] is True:
        bits.append(
            f"{tgt['root']} does support symlinks, so the refusal is a "
            "permission or path problem rather than the filesystem. Check "
            "that the folder is writable and not inside a OneDrive or "
            "network location.")
    if bits:
        return " ".join(bits)
    return ("The volume could not be queried, so the cause is unknown — the "
            "link call simply failed. Check that the folder is writable and "
            "is not a network or cloud-synced location.")


def _probe_one(how: str, folder: str, source: "str | None" = None
               ) -> "tuple[bool, str]":
    """Try one placement strategy for real; clean up after itself.

    `source` is where the ORIGINAL media lives. It matters: a hard link cannot
    cross volumes, so linking C:\\media into G:\\X fails even though linking
    within either one succeeds. Probing both ends is the only way to get a
    truthful answer.
    """
    target = os.path.abspath(folder)
    src_dir = os.path.abspath(source) if source else target
    src = os.path.join(src_dir, ".gallery_linkprobe.tmp")
    dst = os.path.join(target, ".gallery_linkprobe.link")
    try:
        os.makedirs(target, exist_ok=True)
        os.makedirs(src_dir, exist_ok=True)
        with open(src, "wb") as f:
            f.write(b"0")
    except OSError as exc:
        return False, f"Cannot write a probe file: {exc}"
    try:
        if how == "hardlink":
            os.link(src, dst)
        else:
            os.symlink(src, dst)
        where = (f"from {src_dir} into {target}" if source
                 else f"in {target}")
        return True, f"{how} works {where}."
    except OSError as exc:
        return False, f"{how} is not possible here ({exc})."
    finally:
        for q in (dst, src):
            try:
                os.remove(q)
            except OSError:
                pass


def original_for_central_mirror(path: str) -> str:
    """The media a centralised mirror entry stands for, from its own path.

    mirror_dir_for() encodes the source's shape under the mirror root
    (``G:\\X\\Trip\\a.png`` -> ``<root>/Favorites/G/X/Trip/a.png``), so the
    original can be read straight back out of it — no filename index and no
    guessing. Returns "" when `path` is not under that root.
    """
    root = os.path.abspath(central_favorites_root())
    ap = os.path.abspath(path)
    if not ap.startswith(root + os.sep):
        return ""
    parts = [p for p in os.path.relpath(ap, root).replace("\\", "/").split("/")
             if p]
    if not parts:
        return ""
    if len(parts[0]) == 1 and parts[0].isalpha():        # a drive letter
        return parts[0].upper() + ":" + os.sep + os.sep.join(parts[1:])
    return os.sep + os.sep.join(parts)


def is_link_entry(path: str) -> bool:
    """True when `path` is a name for content that also exists elsewhere.

    A symlink, or a regular file carrying more than one name. Removing either
    is lossless — the content survives under its other name — which is what
    makes it safe to prune from a mirror folder, unlike a lone file.
    """
    if os.path.islink(path):
        return True
    try:
        return os.stat(path).st_nlink > 1
    except OSError:
        return False


def link_in_place(src: str, dst: str, allow_copy: bool = False) -> str:
    """Put a link to `src` at `dst`, replacing whatever is there.

    Returns the mode used, or "copy" having changed NOTHING when linking was
    impossible and `allow_copy` is false — replacing a copy with another copy
    reclaims no space and just churns the disk.

    The link is built under a temporary name and moved into place only once it
    exists, so a failure can never leave `dst` deleted with nothing in it.
    """
    tmp = dst + ".link.tmp"
    try:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
        if os.path.lexists(tmp):
            os.remove(tmp)
        how = place_file(src, tmp)
        if how == "copy" and not allow_copy:
            os.remove(tmp)
            return "copy"
        if os.path.lexists(dst):
            os.remove(dst)
        os.rename(tmp, dst)        # rename, not replace: tmp may be a symlink
        return how
    except OSError:
        try:
            if os.path.lexists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        raise


def under_any(path: str, roots: "list[str]") -> bool:
    """Is `path` inside any of `roots` (or one of them)?"""
    ap = os.path.abspath(path)
    return any(ap == r or ap.startswith(r + os.sep)
               for r in (os.path.abspath(x) for x in roots))


def probe_link_mode(folder: str, source: "str | None" = None
                    ) -> "tuple[str, str]":
    """What placement would ACTUALLY happen for files put into `folder`?

    Returns (effective_mode, explanation). The setting is not the answer: a
    link that cannot be made silently becomes a full copy, and whether it can
    be made depends on the pair of locations, so this tests them for real.
    """
    if LINK_MODE == "copy":
        return "copy", "Copying is selected."
    for how in _ladder():
        ok, why = _probe_one(how, folder, source)
        if ok:
            return how, why
    tried = " or ".join(_ladder()) or "linking"
    return "copy", (f"{tried} is NOT possible for {os.path.abspath(folder)}"
                    + (f" from {os.path.abspath(source)}" if source else "")
                    + " — full copies are being made instead.\n\n"
                    + explain_link_failure(folder, source))


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
        try:
            st = os.stat(full)
        except OSError:
            continue
        # Directories are trashable too (folder deletes from the album manager);
        # they carry no meaningful single size.
        size = st.st_size if os.path.isfile(full) else 0
        out.append({"name": name, "path": full, "size": size,
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
    """Permanently delete one trashed file or folder."""
    try:
        if os.path.isdir(trashed_path) and not os.path.islink(trashed_path):
            shutil.rmtree(trashed_path)
        else:
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
