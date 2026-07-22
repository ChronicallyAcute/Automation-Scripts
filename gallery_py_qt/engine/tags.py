"""Per-file tag store with best-effort embedded-metadata write.

Tags live authoritatively in a JSON sidecar store (fast, format-agnostic,
works for videos).  For JPEGs we additionally try to embed the tag list into
the file's EXIF XPKeywords field — the field Windows Explorer shows as
"Tags" — losslessly via piexif when that package is available.  The embed is
best-effort and runs on the favourites worker thread, never the GUI.
"""
from __future__ import annotations
import json
import os
import sys

from .. import config
from .favorites import _MIRROR_POOL

# The fixed descriptor set exposed as buttons in multi-view.
TAGS = ("T", "BT", "HT", "Az", "Bcs", "WAM", "Jz", "Ahg")

_TAGS_FILE = os.path.join(config.HOME, ".gallery_py_qt_tags.json")
_store: dict[str, list[str]] | None = None


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


# Embeds that failed (typically a video still held open by a player) wait
# here and retry on flush_pending() / the next tag toggle.
_PENDING: dict[str, list[str]] = {}


def flush_pending() -> None:
    """Retry embeds that previously failed (e.g. the file was playing)."""
    for p, t in list(_PENDING.items()):
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
        _PENDING[path] = list(tag_list)
        print(f"[tags-embed] {os.path.basename(path)}: {exc} (queued for retry)",
              file=sys.stderr)
    else:
        _PENDING.pop(path, None)


def _embed_jpeg(path: str, tag_list: list[str]) -> None:
    try:
        import piexif
    except ImportError:
        return
    exif = piexif.load(path)
    exif["0th"][piexif.ImageIFD.XPKeywords] = \
        tuple(";".join(tag_list).encode("utf-16-le"))
    piexif.insert(piexif.dump(exif), path)


# -- GIF: embedded XMP ---------------------------------------------------------

# XMP spec (part 3) magic trailer: whatever byte of the packet a naive GIF
# sub-block parser interprets as a length, the descending run guides it to
# the terminator — this is what makes a raw XMP packet legal inside a GIF
# Application Extension.
_XMP_TRAILER = bytes([0x01]) + bytes(range(255, -1, -1)) + b"\x00"


def _xmp_packet(tag_list: list[str]) -> bytes:
    items = "".join(f"<rdf:li>{t}</rdf:li>" for t in tag_list)
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
        return re.findall(r"<rdf:li>([^<]+)</rdf:li>", blob)
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

    ole32 = ctypes.windll.ole32
    propsys = ctypes.windll.propsys
    ole32.CoInitializeEx(None, 0x2)            # STA on this worker thread
    store = c_void_p()
    hr = propsys.SHGetPropertyStoreFromParsingName(
        c_wchar_p(path), None, GPS_READWRITE,
        byref(IID_IPropertyStore), byref(store))
    if hr != 0:
        raise OSError(f"SHGetPropertyStore failed (hr=0x{hr & 0xFFFFFFFF:08x})")
    try:
        pv = PROPVARIANT()
        if tag_list:
            arr = (c_wchar_p * len(tag_list))(*tag_list)
            hr = propsys.InitPropVariantFromStringVector(
                arr, len(tag_list), byref(pv))
            if hr != 0:
                raise OSError(f"InitPropVariant failed (hr=0x{hr:08x})")
        # vt stays VT_EMPTY for an empty list — clears the property.
        vtbl = ctypes.cast(
            ctypes.cast(store, POINTER(c_void_p)).contents, POINTER(c_void_p))
        proto = ctypes.WINFUNCTYPE(ctypes.c_long, c_void_p, c_void_p, c_void_p)
        set_value = proto(vtbl[6])             # IPropertyStore::SetValue
        commit = ctypes.WINFUNCTYPE(ctypes.c_long, c_void_p)(vtbl[7])
        hr = set_value(store, byref(PKEY_Keywords), byref(pv))
        ole32.PropVariantClear(byref(pv))
        if hr != 0:
            raise OSError(f"SetValue failed (hr=0x{hr & 0xFFFFFFFF:08x})")
        hr = commit(store)
        if hr != 0:
            raise OSError(f"Commit failed (hr=0x{hr & 0xFFFFFFFF:08x})")
    finally:
        release = ctypes.WINFUNCTYPE(ctypes.c_long, c_void_p)(
            ctypes.cast(ctypes.cast(store, POINTER(c_void_p)).contents,
                        POINTER(c_void_p))[2])
        release(store)
