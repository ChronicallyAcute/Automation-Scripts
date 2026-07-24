"""Per-file tag store with best-effort embedded-metadata write.

Tags live authoritatively in a JSON sidecar store (fast, format-agnostic,
works for videos).  For JPEGs we additionally try to embed the tag list into
the file's EXIF XPKeywords field — the field Windows Explorer shows as
"Tags" — losslessly via piexif when that package is available.  The embed is
best-effort and runs on the favourites worker thread, never the GUI.
"""
from __future__ import annotations
import errno
import json
import os
import shutil
import sys
import threading
from xml.sax.saxutils import escape as _xml_escape

from .. import config
from . import media
from .favorites import _MIRROR_POOL, place_file


class _Transient(Exception):
    """A retryable embed failure (file busy / locked / sharing violation)."""

# The descriptor set exposed as buttons in multi-view / filter / batch menus.
# User-customisable (add / remove / rename) and persisted to _TAGSET_FILE; the
# shipped default is these nine.  `TAGS` mirrors the live set for the many
# `for t in tags.TAGS` call sites.
DEFAULT_TAGS = ("T", "BT", "HT", "Az", "Bcs", "WAM", "Jz", "Ahg", "Bp")

_TAGS_FILE = os.path.join(config.HOME, ".gallery_py_qt_tags.json")
_TAGSET_FILE = os.path.join(config.HOME, ".gallery_py_qt_tagset.json")
_store: dict[str, list[str]] | None = None


def _load_tagset() -> tuple[str, ...]:
    try:
        with open(_TAGSET_FILE, encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, list) and d and all(isinstance(x, str) for x in d):
            # de-dup preserving order
            return tuple(dict.fromkeys(x for x in d if x.strip()))
    except Exception:
        pass
    return DEFAULT_TAGS


TAGS = _load_tagset()


def get_tags() -> tuple[str, ...]:
    return tuple(TAGS)


def _save_tagset() -> None:
    try:
        tmp = _TAGSET_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(TAGS), f, indent=1)
        os.replace(tmp, _TAGSET_FILE)
    except OSError as exc:
        print(f"[tagset] {exc}", file=sys.stderr)


def set_tags(new: "list[str] | tuple[str, ...]") -> None:
    """Replace the whole tag set (deduped, order-preserving, non-empty)."""
    global TAGS
    TAGS = tuple(dict.fromkeys(t.strip() for t in new if t and t.strip()))
    _save_tagset()


def add_tag(name: str) -> bool:
    name = name.strip()
    if not name or name in TAGS:
        return False
    set_tags(list(TAGS) + [name])
    return True


def remove_tag(name: str) -> None:
    """Drop a tag everywhere: the set, the JSON store, and its Favorites
    subfolder / manifest entries."""
    if name not in TAGS:
        return
    set_tags([t for t in TAGS if t != name])
    store = _load()
    for p, lst in list(store.items()):
        if name in lst:
            store[p] = [t for t in lst if t != name]
            if not store[p]:
                store.pop(p, None)
    _save()
    _MIRROR_POOL.submit(_remove_tag_folder, name)


def rename_tag(old: str, new: str) -> bool:
    """Rename a tag, migrating the JSON store and the Favorites subfolder +
    manifest.  Embedded metadata (GIF/JPEG) refreshes on the file's next tag
    change; the store is the source of truth."""
    new = new.strip()
    if old not in TAGS or not new or (new in TAGS and new != old) or old == new:
        return False
    set_tags([new if t == old else t for t in TAGS])
    store = _load()
    for p, lst in list(store.items()):
        if old in lst:
            store[p] = [new if t == old else t for t in lst]
    _save()
    _MIRROR_POOL.submit(_rename_tag_folder, old, new)
    return True


def _remove_tag_folder(name: str) -> None:
    folder = os.path.join(config.FAVORITES_DIR, name)
    try:
        if os.path.isdir(folder):
            shutil.rmtree(folder)
    except OSError as exc:
        print(f"[tag-folder-rm] {name}: {exc}", file=sys.stderr)
    db = _load_tagdb()
    changed = False
    for key in list(db):
        if name in db[key]:
            db[key].pop(name, None)
            changed = True
            if not db[key]:
                db.pop(key, None)
    if changed:
        _save_tagdb(db)


def _rename_tag_folder(old: str, new: str) -> None:
    src = os.path.join(config.FAVORITES_DIR, old)
    dst = os.path.join(config.FAVORITES_DIR, new)
    try:
        if os.path.isdir(src):
            os.makedirs(dst, exist_ok=True)
            for fn in os.listdir(src):
                s = os.path.join(src, fn)
                d = os.path.join(dst, fn)
                if os.path.exists(d):          # collision on merge
                    stem, ext = os.path.splitext(fn)
                    i = 1
                    while os.path.exists(os.path.join(dst, f"{stem}_{i}{ext}")):
                        i += 1
                    d = os.path.join(dst, f"{stem}_{i}{ext}")
                shutil.move(s, d)
            os.rmdir(src)
    except OSError as exc:
        print(f"[tag-folder-mv] {old}->{new}: {exc}", file=sys.stderr)
    # Repoint manifest entries from old/ to new/.
    db = _load_tagdb()
    changed = False
    for entry in db.values():
        if old in entry:
            path = entry.pop(old)
            entry[new] = os.path.join(dst, os.path.basename(path))
            changed = True
    if changed:
        _save_tagdb(db)


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


# -- Tag folders: aggregate tagged + favourited media by tag -------------------
# Under the Gallery Favorites folder (config.FAVORITES_DIR) we keep one
# subfolder per tag name.  A file is copied into <FAVORITES_DIR>/<TAG>/ while it
# is BOTH favourited AND carries <TAG> AND is an image, GIF, or short video
# (<= 10 min).  Copies are reconciled (added/removed) whenever the file is
# tagged/untagged or (un)favourited.
_MAX_VIDEO_TAG_SECONDS = 600      # "small videos (10 min and less)"


def _tag_copy_eligible(path: str) -> bool:
    ext = os.path.splitext(path.lower())[1]
    if ext in config.VIDEO_EXT:
        try:
            dur = media.peek_duration(path)
        except Exception:
            dur = 0.0
        # Only short videos; unknown/zero duration is excluded (can't confirm).
        return 0 < dur <= _MAX_VIDEO_TAG_SECONDS
    return ext in config.IMAGE_EXT            # images + GIFs


def sync_tag_folders(path: str, favored: bool) -> None:
    """Reconcile this file's copies across the per-tag Favorites subfolders.

    Reads the current tag set now (GUI thread) and does the copying/removing on
    the mirror worker (it touches disk and may probe a video's duration).
    """
    # Never operate on a file that already lives inside the tag folders.
    try:
        if os.path.dirname(os.path.abspath(path)).startswith(
                os.path.abspath(config.FAVORITES_DIR)):
            return
    except Exception:
        pass
    _MIRROR_POOL.submit(_sync_tag_folders_now, path, bool(favored),
                        tags_for(path))


# Records each tag-folder copy's EXACT path per (source, tag).  Identifying a
# copy by content (_same_file) is unreliable — embedding tags / rotation mutate
# the source so it no longer matches its earlier copy — and basenames can
# collide across source folders.  The manifest makes add/remove exact.  Only
# the single mirror worker touches it, so no lock is needed.
_TAGDB_FILE = os.path.join(config.HOME, ".gallery_py_qt_tagfolders.json")


def _load_tagdb() -> dict:
    try:
        with open(_TAGDB_FILE, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_tagdb(db: dict) -> None:
    try:
        tmp = _TAGDB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(db, f)
        os.replace(tmp, _TAGDB_FILE)
    except OSError as exc:
        print(f"[tag-folder-db] {exc}", file=sys.stderr)


def _sync_tag_folders_now(path: str, favored: bool, tag_list: list[str]) -> None:
    key = os.path.abspath(path)
    base = os.path.basename(path)
    eligible = favored and _tag_copy_eligible(path)
    active = set(tag_list) if eligible else set()
    db = _load_tagdb()
    entry = dict(db.get(key, {}))          # {tag: absolute copy path}
    changed = False
    for t in TAGS:
        sub = os.path.join(config.FAVORITES_DIR, t)
        try:
            if t in active:
                cur = entry.get(t)
                if cur and os.path.exists(cur):
                    continue               # already mirrored for this tag
                os.makedirs(sub, exist_ok=True)
                dest = os.path.join(sub, base)
                if os.path.lexists(dest):  # basename collision across folders
                    stem, ext = os.path.splitext(base)
                    i = 1
                    while os.path.lexists(os.path.join(sub, f"{stem}_{i}{ext}")):
                        i += 1
                    dest = os.path.join(sub, f"{stem}_{i}{ext}")
                place_file(path, dest)     # copy or link per LINK_MODE
                entry[t] = dest
                changed = True
            else:
                cur = entry.pop(t, None)
                if cur is not None:
                    changed = True
                    if os.path.exists(cur):
                        os.remove(cur)
        except OSError as exc:
            print(f"[tag-folder] {t}/{base}: {exc}", file=sys.stderr)
    if changed or key in db:
        if entry:
            db[key] = entry
        else:
            db.pop(key, None)
        _save_tagdb(db)


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


# Embeds that failed for a RETRYABLE reason (the file was busy/locked, e.g. a
# video still held open by a player) wait here and retry on flush_pending() /
# the next tag toggle.  Permanent failures (unparseable file, no writable
# property handler, access denied) are dropped, NOT queued — otherwise they
# would be re-attempted forever and grow this dict without bound.
_MAX_RETRIES = 6
_PENDING_CAP = 256
_pending_lock = threading.Lock()
_PENDING: dict[str, list[str]] = {}
_retries: dict[str, int] = {}


def _retryable(exc: Exception) -> bool:
    if isinstance(exc, _Transient):
        return True
    if isinstance(exc, PermissionError):
        return True            # file open elsewhere (player) — try again later
    win = getattr(exc, "winerror", None)
    if win in (32, 33):        # ERROR_SHARING_VIOLATION / ERROR_LOCK_VIOLATION
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in (
            errno.EACCES, errno.EBUSY):
        return True
    return False               # ValueError parse fail, decode error, etc.


def flush_pending() -> None:
    """Retry embeds that previously failed for a transient reason."""
    with _pending_lock:
        items = list(_PENDING.items())
    for p, t in items:
        _MIRROR_POOL.submit(_embed_tags, p, t)


def reembed(path: str) -> None:
    """Re-write the embedded tags after the file changed on disk (rotation
    re-encodes and drops embedded metadata)."""
    _MIRROR_POOL.submit(_embed_tags, path, tags_for(path))


def _embed_tags(path: str, tag_list: list[str]) -> None:
    """Best-effort embedded write, dispatched by format (worker thread only).

    - JPEG:  EXIF XPKeywords via piexif (lossless) — Explorer's "Tags".
    - Video: Windows Property System (System.Keywords) — Explorer's "Tags";
      the OS writes its own containers, we never hand-edit video files.
    - GIF:   embedded XMP packet (dc:subject) — the standard location; note
      Explorer has no GIF tag support at all, but XMP-aware tools read it.
    The JSON store remains the source of truth in every case.
    """
    ext = os.path.splitext(path.lower())[1]
    try:
        if ext in (".jpg", ".jpeg"):
            _embed_jpeg(path, tag_list)
        elif ext == ".gif":
            _embed_gif_xmp(path, tag_list)
        elif ext in (".mp4", ".m4v", ".mov", ".wmv"):
            _embed_windows_keywords(path, tag_list)
        else:
            return
    except Exception as exc:
        if _retryable(exc):
            with _pending_lock:
                n = _retries.get(path, 0) + 1
                if n <= _MAX_RETRIES and (
                        path in _PENDING or len(_PENDING) < _PENDING_CAP):
                    _retries[path] = n
                    _PENDING[path] = list(tag_list)
                    verdict = f"queued for retry ({n}/{_MAX_RETRIES})"
                else:
                    _PENDING.pop(path, None)
                    _retries.pop(path, None)
                    verdict = "gave up (retry limit/cap reached)"
        else:
            with _pending_lock:
                _PENDING.pop(path, None)
                _retries.pop(path, None)
            verdict = "permanent, not retried"
        print(f"[tags-embed] {os.path.basename(path)}: {exc} ({verdict})",
              file=sys.stderr)
    else:
        with _pending_lock:
            _PENDING.pop(path, None)
            _retries.pop(path, None)


def _embed_jpeg(path: str, tag_list: list[str]) -> None:
    try:
        import piexif
    except ImportError:
        return
    exif = piexif.load(path)
    xp = piexif.ImageIFD.XPKeywords
    if tag_list:
        # XPKeywords is a NUL-terminated UTF-16LE string by Windows convention;
        # without the terminator Explorer reads trailing garbage or nothing.
        exif["0th"][xp] = tuple(
            (";".join(tag_list) + "\x00").encode("utf-16-le"))
    else:
        exif["0th"].pop(xp, None)          # remove, don't write empty field
    piexif.insert(piexif.dump(exif), path)


# -- GIF: embedded XMP ---------------------------------------------------------

# XMP spec (part 3) magic trailer: whatever byte of the packet a naive GIF
# sub-block parser interprets as a length, the descending run guides it to
# the terminator — this is what makes a raw XMP packet legal inside a GIF
# Application Extension.
_XMP_TRAILER = bytes([0x01]) + bytes(range(255, -1, -1)) + b"\x00"


def _xmp_packet(tag_list: list[str]) -> bytes:
    # Escape tag text: the fixed button set is XML-safe, but the JSON store is
    # externally editable, so a value with & < > must not break the packet.
    items = "".join(f"<rdf:li>{_xml_escape(t)}</rdf:li>" for t in tag_list)
    return (
        '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/">'
        f"<dc:subject><rdf:Bag>{items}</rdf:Bag></dc:subject>"
        "</rdf:Description></rdf:RDF></x:xmpmeta>"
        '<?xpacket end="w"?>').encode("utf-8")


def _gif_scan(data: bytes) -> tuple[list[tuple[int, int]], int] | None:
    """Walk the GIF block structure.

    Returns (spans of existing XMP application-extension blocks, trailer
    offset), or None if the structure doesn't parse cleanly — in which case
    we refuse to touch the file.
    """
    if data[:6] not in (b"GIF87a", b"GIF89a") or len(data) < 14:
        return None
    packed = data[10]
    pos = 13 + ((2 ** ((packed & 7) + 1)) * 3 if packed & 0x80 else 0)
    xmp_spans: list[tuple[int, int]] = []
    n = len(data)

    def skip_subblocks(p: int) -> int:
        while True:
            if p >= n:
                raise ValueError("truncated sub-blocks")
            sz = data[p]
            p += 1 + sz
            if sz == 0:
                return p

    try:
        while pos < n:
            b = data[pos]
            if b == 0x3B:                      # trailer
                return xmp_spans, pos
            if b == 0x21:                      # extension
                start = pos
                label = data[pos + 1]
                p = pos + 2
                appid = b""
                if label == 0xFF and p < n:
                    appid = data[p + 1: p + 1 + data[p]]
                pos = skip_subblocks(p)
                if label == 0xFF and appid[:11] == b"XMP DataXMP":
                    xmp_spans.append((start, pos))
            elif b == 0x2C:                    # image descriptor
                p = pos + 1 + 9
                packed = data[pos + 9]
                p += (2 ** ((packed & 7) + 1)) * 3 if packed & 0x80 else 0
                p += 1                         # LZW minimum code size
                pos = skip_subblocks(p)
            else:
                raise ValueError(f"unknown block 0x{b:02x}")
    except (ValueError, IndexError):
        return None
    return None                                # no trailer found


def _embed_gif_xmp(path: str, tag_list: list[str]) -> None:
    with open(path, "rb") as f:
        data = f.read()
    scan = _gif_scan(data)
    if scan is None:
        raise ValueError("GIF structure did not parse; not modifying file")
    spans, trailer = scan
    out = bytearray()
    last = 0
    for s, e in spans:                         # drop any existing XMP block
        out += data[last:s]
        last = e
    out += data[last:trailer]
    if tag_list:
        # An Application Extension is a GIF89a-only construct — normalise the
        # signature so the written file is spec-valid (some 87a inputs exist).
        out[0:6] = b"GIF89a"
        out += b"\x21\xff\x0bXMP DataXMP" + _xmp_packet(tag_list) + _XMP_TRAILER
    out += data[trailer:]
    tmp = path + ".tagtmp"
    with open(tmp, "wb") as f:
        f.write(out)
    os.replace(tmp, path)


def read_gif_tags(path: str) -> list[str]:
    """Read back dc:subject entries from a GIF's embedded XMP (verification)."""
    try:
        with open(path, "rb") as f:
            data = f.read()
        scan = _gif_scan(data)
        if not scan or not scan[0]:
            return []
        s, e = scan[0][0]
        blob = data[s:e].decode("utf-8", "ignore")
        import re
        from xml.sax.saxutils import unescape
        return [unescape(m) for m in re.findall(r"<rdf:li>(.*?)</rdf:li>", blob)]
    except OSError:
        return []


# -- Video: Windows Property System (System.Keywords) --------------------------

def _embed_windows_keywords(path: str, tag_list: list[str]) -> None:
    """Ask Windows to write System.Keywords — the Explorer 'Tags' field —
    through the format's own property handler (MP4/M4V/MOV/WMV).

    No-op on other platforms.  Raises on failure (sharing violation while
    the video is playing is the common case) so the caller queues a retry.
    """
    if sys.platform != "win32":
        return
    import ctypes
    from ctypes import POINTER, byref, c_void_p, c_wchar_p, c_uint, c_ushort

    class GUID(ctypes.Structure):
        _fields_ = [("d1", c_uint), ("d2", c_ushort), ("d3", c_ushort),
                    ("d4", ctypes.c_ubyte * 8)]

        @classmethod
        def make(cls, d1, d2, d3, *d4):
            return cls(d1, d2, d3, (ctypes.c_ubyte * 8)(*d4))

    class PROPERTYKEY(ctypes.Structure):
        _fields_ = [("fmtid", GUID), ("pid", ctypes.c_ulong)]

    class PROPVARIANT(ctypes.Structure):
        _fields_ = [("vt", c_ushort), ("r1", c_ushort), ("r2", c_ushort),
                    ("r3", c_ushort), ("data", ctypes.c_byte * 16)]

    IID_IPropertyStore = GUID.make(
        0x886D8EEB, 0x8CF2, 0x4446,
        0x8D, 0x02, 0xCD, 0xBA, 0x1D, 0xBD, 0xCF, 0x99)
    PKEY_Keywords = PROPERTYKEY(GUID.make(
        0xF29F85E0, 0x4FF9, 0x1068,
        0xAB, 0x91, 0x08, 0x00, 0x2B, 0x27, 0xB3, 0xD9), 5)
    GPS_READWRITE = 0x2
    # Sharing/lock violations are transient (video is playing) → retry.
    # Everything else (no writable handler, access-denied) is permanent → drop.
    _TRANSIENT_HR = (0x80070020, 0x80070021)

    def _check(where: str, hr: int) -> None:
        hr &= 0xFFFFFFFF
        if hr == 0:
            return
        if hr in _TRANSIENT_HR:
            raise _Transient(f"{where} busy (hr=0x{hr:08x})")
        raise OSError(f"{where} failed (hr=0x{hr:08x})")

    ole32 = ctypes.windll.ole32
    propsys = ctypes.windll.propsys
    # Own the COM init so we can balance it: 0/1 = we initialised (S_OK/S_FALSE),
    # anything else (already-init different mode) means don't uninit.
    co_hr = ole32.CoInitializeEx(None, 0x2) & 0xFFFFFFFF
    try:
        store = c_void_p()
        _check("SHGetPropertyStore", propsys.SHGetPropertyStoreFromParsingName(
            c_wchar_p(path), None, GPS_READWRITE,
            byref(IID_IPropertyStore), byref(store)))
        if not store.value:                    # guard: never deref NULL (crash)
            raise OSError("SHGetPropertyStore returned NULL store")
        try:
            pv = PROPVARIANT()
            if tag_list:
                arr = (c_wchar_p * len(tag_list))(*tag_list)
                _check("InitPropVariant", propsys.InitPropVariantFromStringVector(
                    arr, len(tag_list), byref(pv)))
            # vt stays VT_EMPTY for an empty list — clears the property.
            vtbl = ctypes.cast(
                ctypes.cast(store, POINTER(c_void_p)).contents,
                POINTER(c_void_p))
            proto = ctypes.WINFUNCTYPE(
                ctypes.c_long, c_void_p, c_void_p, c_void_p)
            set_value = proto(vtbl[6])         # IPropertyStore::SetValue
            commit = ctypes.WINFUNCTYPE(ctypes.c_long, c_void_p)(vtbl[7])
            hr = set_value(store, byref(PKEY_Keywords), byref(pv))
            ole32.PropVariantClear(byref(pv))
            _check("SetValue", hr)
            _check("Commit", commit(store))
        finally:
            ctypes.WINFUNCTYPE(ctypes.c_long, c_void_p)(
                ctypes.cast(ctypes.cast(store, POINTER(c_void_p)).contents,
                            POINTER(c_void_p))[2])(store)   # IUnknown::Release
    finally:
        if co_hr in (0, 1):
            ole32.CoUninitialize()
