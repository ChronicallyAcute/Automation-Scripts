"""Recover tags from every historical storage mechanism into the JSON store.

Tags have been written in several places over this project's life, and the
JSON store is only the newest of them.  When that store is lost, reset, or
overwritten, the tags themselves usually still exist somewhere else:

  1. Legacy / backup JSON stores — the app's own ``.bak``, and the older
     ``.gallery_*`` names shared with the original gallery_qt.
  2. The tag-folder manifest (``.gallery_py_qt_tagfolders.json``), which maps
     each tagged+favourited file to its mirrored copy per tag.
  3. The tag folders themselves — ``<FAVORITES_DIR>/<TAG>/<name>`` — where the
     mere presence of a copy records that the original carried <TAG>.
  4. Metadata embedded in the files: JPEG EXIF XPKeywords (what Windows
     Explorer shows as "Tags") and the XMP packet written into GIFs.

Everything here MERGES — a recovered tag is added, never removed, and an
existing tag is never dropped — so running it repeatedly is safe and can only
ever increase what you have.
"""
from __future__ import annotations
import json
import os
import re

from .. import config
from . import tags as _tags


# -- 1. legacy / backup JSON stores -------------------------------------------

def legacy_json_sources() -> "list[str]":
    """Existing JSON stores other than the live one, newest-looking first."""
    home = config.HOME
    candidates = [
        _tags._TAGS_FILE + ".bak",                       # our own backup
        os.path.join(home, ".gallery_tags.json"),        # shared gallery_qt name
        os.path.join(home, ".gallery_qt_tags.json"),
        os.path.join(home, ".gallery_py_qt_tags.json.old"),
    ]
    out = []
    for p in candidates:
        if p != _tags._TAGS_FILE and os.path.isfile(p):
            out.append(p)
    return out


def read_json_store(path: str) -> "dict[str, list[str]]":
    """{path: [tags]} from a store file, or {} if it isn't one."""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return {}
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(k, str) and isinstance(v, (list, tuple)):
            names = [str(t) for t in v if isinstance(t, str) and t.strip()]
            if names:
                out[k] = names
    return out


# -- 2/3. tag folders (manifest + what's actually on disk) --------------------

def read_tag_folders() -> "dict[str, list[str]]":
    """Reconstruct {original_path: [tags]} from the tag-folder mirrors.

    The manifest is authoritative when present; the folders on disk are also
    scanned so a file whose manifest entry was lost is still recovered by
    basename.
    """
    found: "dict[str, list[str]]" = {}

    def _add(path: str, tag: str) -> None:
        if path and tag:
            found.setdefault(path, [])
            if tag not in found[path]:
                found[path].append(tag)

    # Manifest: {original_path: {tag: mirrored_copy_path}}
    try:
        db = _tags._load_tagdb()
    except Exception:
        db = {}
    for orig, entry in db.items():
        if isinstance(entry, dict):
            for tag in entry:
                _add(orig, tag)

    # Folders on disk: <FAVORITES_DIR>/<TAG>/<basename>.  Only the basename
    # survives there, so map it back through the manifest's known originals.
    by_base: "dict[str, list[str]]" = {}
    for orig in list(db) + list(_tags._load()):
        by_base.setdefault(os.path.basename(orig), []).append(orig)
    root = config.FAVORITES_DIR
    try:
        tag_dirs = [d for d in os.listdir(root)
                    if os.path.isdir(os.path.join(root, d))]
    except OSError:
        tag_dirs = []
    for tag in tag_dirs:
        try:
            names = os.listdir(os.path.join(root, tag))
        except OSError:
            continue
        for name in names:
            for orig in by_base.get(name, ()):
                _add(orig, tag)
    return found


# -- 4. metadata embedded in the media files ----------------------------------

def read_embedded(path: str) -> "list[str]":
    """Tags embedded in the file itself (JPEG XPKeywords / GIF XMP)."""
    ext = os.path.splitext(path.lower())[1]
    try:
        if ext in (".jpg", ".jpeg"):
            return _read_jpeg_keywords(path)
        if ext == ".gif":
            return _read_gif_xmp(path)
    except Exception:
        pass
    return []


def _read_jpeg_keywords(path: str) -> "list[str]":
    try:
        import piexif
    except ImportError:
        return []
    exif = piexif.load(path)
    raw = exif.get("0th", {}).get(piexif.ImageIFD.XPKeywords)
    if not raw:
        return []
    if isinstance(raw, (tuple, list)):
        raw = bytes(raw)
    # Written as a NUL-terminated UTF-16LE string, semicolon separated.
    text = raw.decode("utf-16-le", "ignore").rstrip("\x00")
    return [t.strip() for t in text.split(";") if t.strip()]


_XMP_SUBJECT = re.compile(
    rb"<rdf:li[^>]*>([^<]+)</rdf:li>", re.I)


def _read_gif_xmp(path: str) -> "list[str]":
    with open(path, "rb") as f:
        data = f.read()
    i = data.find(b"XMP DataXMP")
    if i < 0:
        return []
    packet = data[i:]
    end = packet.find(b"</x:xmpmeta>")
    if end >= 0:
        packet = packet[:end]
    return [m.decode("utf-8", "ignore").strip()
            for m in _XMP_SUBJECT.findall(packet)
            if m.strip()]


# -- merge --------------------------------------------------------------------

def merge(found: "dict[str, list[str]]", require_exists: bool = True
          ) -> "tuple[int, int]":
    """Add `found` tags to the live store without removing anything.

    Returns (files_updated, tags_added).  Entries whose file no longer exists
    are skipped by default so a recovery pass doesn't repopulate the store with
    paths that have since been deleted or moved.
    """
    if _tags.load_failed():
        return (0, 0)                      # never write over an unreadable store
    files = added = 0
    for path, names in found.items():
        if require_exists and not os.path.exists(path):
            continue
        cur = _tags.tags_for(path)
        new = [t for t in names if t not in cur]
        if not new:
            continue
        _tags.set_tags_for(path, cur + new)
        files += 1
        added += len(new)
    if added:
        _tags.adopt_tags_in_use()          # make recovered names buttons again
    return files, added


def recover(paths: "list[str] | None" = None) -> dict:
    """Run every recovery source and merge the results.

    `paths` limits the (slower) embedded-metadata scan to files you care about
    — typically the currently loaded folder.  The JSON and tag-folder sources
    are always scanned; they're cheap and need no file reads.
    Returns a per-source summary for reporting.
    """
    report: dict = {"sources": {}, "files": 0, "tags": 0}

    def _run(label: str, found: "dict[str, list[str]]") -> None:
        f, a = merge(found)
        report["sources"][label] = {"files": f, "tags": a}
        report["files"] += f
        report["tags"] += a

    for src in legacy_json_sources():
        _run(os.path.basename(src), read_json_store(src))
    _run("tag folders", read_tag_folders())

    if paths:
        embedded: "dict[str, list[str]]" = {}
        for p in paths:
            names = read_embedded(p)
            if names:
                embedded[p] = names
        _run("embedded metadata", embedded)
    return report


# -- diagnosis ----------------------------------------------------------------

def diagnose(paths: "list[str]") -> dict:
    """Explain why loaded files may show no tags.

    Distinguishes the three real cases: the file genuinely has no tags, its
    entry is found only after path normalisation (a spelling mismatch — the
    cause of "my tags vanished" while the JSON is perfectly valid), and its
    tags exist but carry names the tag set no longer has (no button to show).
    """
    store = _tags._load()
    report = {"loaded": len(paths), "store_entries": len(store),
              "exact": 0, "normalised_only": 0, "untagged": 0,
              "tags_without_buttons": []}
    known = set(_tags.get_tags())
    missing_names: "set[str]" = set()
    for p in paths:
        if p in store:
            report["exact"] += 1
        elif _tags._resolve(p) is not None:
            report["normalised_only"] += 1
        else:
            report["untagged"] += 1
            continue
        for t in _tags.tags_for(p):
            if t not in known:
                missing_names.add(t)
    report["tags_without_buttons"] = sorted(missing_names)
    return report


# -- filename-based tag reconciliation ----------------------------------------
# The same media often exists several times across a library — re-downloaded,
# re-encoded, or copied between folders — under the SAME filename but as
# different bytes, so the duplicate finder (which compares content) never pairs
# them.  Tagging applied to one copy therefore never reaches the others, and
# successive "iterations of tag formatting" leave the library inconsistent.
#
# This reconciles by filename: every copy of a name receives the UNION of the
# tags held by any copy of that name.  Union, not "richest wins", so a copy
# that uniquely carries a tag never loses it.

def _name_key(path: str, ignore_case: bool = True) -> str:
    name = os.path.basename(path)
    return name.lower() if ignore_case else name


def group_by_filename(paths: "list[str]", ignore_case: bool = True
                      ) -> "dict[str, list[str]]":
    """{filename: [paths]} for names that occur more than once."""
    groups: "dict[str, list[str]]" = {}
    for p in paths:
        groups.setdefault(_name_key(p, ignore_case), []).append(p)
    return {k: v for k, v in groups.items() if len(v) > 1}


def preview_filename_sync(paths: "list[str]", ignore_case: bool = True
                          ) -> "list[dict]":
    """What reconcile_by_filename() would do, without changing anything.

    Returns one entry per name that would gain tags:
        {"name", "union", "paths": [(path, [tags_it_would_gain]), ...]}
    """
    out: "list[dict]" = []
    for name, members in sorted(group_by_filename(paths, ignore_case).items()):
        union: "list[str]" = []
        for p in members:
            for t in _tags.tags_for(p):
                if t not in union:
                    union.append(t)
        if not union:
            continue
        gains = []
        for p in members:
            cur = set(_tags.tags_for(p))
            missing = [t for t in union if t not in cur]
            if missing:
                gains.append((p, missing))
        if gains:
            out.append({"name": name, "union": sorted(union), "paths": gains})
    return out


def reconcile_by_filename(paths: "list[str]", ignore_case: bool = True
                          ) -> "tuple[int, int, int]":
    """Give every same-named file the union of that name's tags.

    Returns (names_touched, files_updated, tags_added).  Purely additive: no
    file ever loses a tag, so this is safe to re-run.
    """
    if _tags.load_failed():
        return (0, 0, 0)
    names = files = added = 0
    for entry in preview_filename_sync(paths, ignore_case):
        names += 1
        for path, missing in entry["paths"]:
            _tags.set_tags_for(path, _tags.tags_for(path) + missing)
            files += 1
            added += len(missing)
    if added:
        _tags.adopt_tags_in_use()
    return names, files, added
