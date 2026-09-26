"""Exact-duplicate detection across a set of media paths.

Two files are duplicates when their bytes are identical.  Comparing every pair
directly would be O(n²) reads, so the search narrows in cheapening stages:

    1. bucket by file size          — different size ⇒ never equal (a stat, no read)
    2. within a size bucket, hash    — first + last CHUNK bytes ("partial" key);
       a quick partial key            cheap and kills most accidental collisions
    3. within a partial bucket,      — full content hash confirms true equality
       hash the whole file

Only stages 2–3 read bytes, and only for files that survive the previous stage,
so a folder of mostly-unique media pays almost nothing.  The module is pure
(no Qt) so it runs on a worker thread and is unit-testable.
"""
from __future__ import annotations
import hashlib
import os
from typing import Callable

# Bytes read from each end for the cheap partial key.
_CHUNK = 65536


def _partial_key(path: str, size: int) -> str:
    """Hash the first and last _CHUNK bytes — cheap, catches most non-dupes."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        head = f.read(_CHUNK)
        h.update(head)
        if size > _CHUNK:
            f.seek(max(_CHUNK, size - _CHUNK))
            h.update(f.read(_CHUNK))
    return h.hexdigest()


def _full_hash(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _group_by(items, keyfn):
    """Bucket *items* by keyfn(item); skip items whose key raises OSError."""
    buckets: "dict[object, list]" = {}
    for it in items:
        try:
            k = keyfn(it)
        except OSError:
            continue
        buckets.setdefault(k, []).append(it)
    return buckets


def find_duplicates(
    paths: "list[str]",
    progress: "Callable[[int, int], None] | None" = None,
    cancelled: "Callable[[], bool] | None" = None,
) -> "list[list[str]]":
    """Return groups of duplicate paths (each group has ≥2 members).

    Existing, distinct files only — non-existent paths and repeats of the same
    path are dropped.  Within each returned group the paths are sorted; the
    groups themselves are ordered largest-file-first so the biggest space wins
    surface at the top.  *progress(done, total)* is called as full hashes are
    computed; *cancelled()* is polled to allow an early, empty-handed return.
    """
    # De-dup the input list and keep only real files, remembering each size.
    sizes: "dict[str, int]" = {}
    for p in dict.fromkeys(paths):          # preserve order, drop repeats
        try:
            st = os.stat(p)
        except OSError:
            continue
        if os.path.isfile(p):
            sizes[p] = st.st_size

    # Stage 1: bucket by size; only sizes shared by ≥2 files can hold dupes.
    size_buckets = _group_by(sizes, sizes.get)
    candidates = [p for grp in size_buckets.values() if len(grp) > 1 for p in grp]

    # Stage 2: within same-size groups, split by the cheap partial key.
    partial_buckets: "dict[tuple[int, str], list[str]]" = {}
    for p in candidates:
        if cancelled and cancelled():
            return []
        try:
            key = (sizes[p], _partial_key(p, sizes[p]))
        except OSError:
            continue
        partial_buckets.setdefault(key, []).append(p)

    # Stage 3: confirm with a full hash only where partial keys already collide.
    to_hash = [p for grp in partial_buckets.values() if len(grp) > 1 for p in grp]
    total = len(to_hash)
    full_buckets: "dict[str, list[str]]" = {}
    for i, p in enumerate(to_hash, 1):
        if cancelled and cancelled():
            return []
        try:
            full_buckets.setdefault(_full_hash(p), []).append(p)
        except OSError:
            continue
        if progress:
            progress(i, total)

    groups = [sorted(grp) for grp in full_buckets.values() if len(grp) > 1]
    # Biggest files first (each group's members share a size).
    groups.sort(key=lambda g: sizes.get(g[0], 0), reverse=True)
    return groups


# -- Group inspection: where copies live, and how their tags differ ------------
# A duplicate group is byte-identical media, but the COPIES are not
# interchangeable to the user: they sit in different folders and may carry
# different tag sets.  Deleting the wrong one silently discards tagging work,
# so the UI needs to show both, and the survivor should inherit the richest
# tag set rather than whatever its own happened to be.

def locations(paths: "list[str]") -> "list[str]":
    """Distinct parent folders of `paths`, in sorted order."""
    return sorted({os.path.dirname(os.path.abspath(p)) for p in paths})


def tag_sets(paths: "list[str]") -> "dict[str, set]":
    """{path: set(tags)} for each path."""
    from . import tags as _tags
    return {p: set(_tags.tags_for(p)) for p in paths}


def tags_differ(paths: "list[str]") -> bool:
    """True when the copies do NOT all carry the same tags."""
    sets = list(tag_sets(paths).values())
    return any(s != sets[0] for s in sets[1:]) if sets else False


def richest_tags(paths: "list[str]") -> "list[str]":
    """The largest tag set among `paths`.

    "Largest" is by count, as requested; ties are broken by the
    lexicographically smallest set so the result is deterministic rather than
    dependent on dict ordering.  Returns a sorted list (possibly empty).
    """
    sets = tag_sets(paths)
    if not sets:
        return []
    best = max(sets.values(), key=lambda s: (len(s), sorted(s, reverse=True)))
    return sorted(best)


def stamp_richest_tags(group: "list[str]", survivors: "list[str]") -> "list[str]":
    """Give each survivor the richest tag set found anywhere in its group.

    Called before the redundant copies are trashed, so tagging applied to a
    copy that is about to disappear is preserved on the one that is kept.
    Returns the survivors whose tags actually changed.
    """
    from . import tags as _tags
    want = richest_tags(group)
    if not want:
        return []
    changed = []
    for p in survivors:
        cur = set(_tags.tags_for(p))
        merged = sorted(cur | set(want))
        if merged != sorted(cur):
            _tags.set_tags_for(p, merged)
            changed.append(p)
    return changed


# -- Hard-link awareness -------------------------------------------------------
# With link_mode="hardlink", a favourited or tag-foldered file is a SECOND NAME
# for the same bytes, not a second copy.  Byte-comparison cannot tell the two
# apart, so the finder reports those names as duplicates and offers to reclaim
# space that was never occupied.  Deleting one name frees nothing until the
# last name is gone, so the estimate has to account for shared storage or it
# will overstate the win — badly, for anyone running in hardlink mode.

def storage_id(path: str) -> "tuple[int, int] | None":
    """(device, inode) identifying the bytes behind `path`, or None.

    Two paths sharing this pair are the same file on disk. Populated on
    Windows as well as POSIX; None when the platform cannot report it.
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    if not st.st_ino:              # some filesystems report 0 — unusable
        return None
    return (st.st_dev, st.st_ino)


def link_clusters(paths: "list[str]") -> "list[list[str]]":
    """Group `paths` by the storage they share; singletons included."""
    by_id: "dict[tuple[int, int], list[str]]" = {}
    loose: "list[list[str]]" = []
    for p in paths:
        sid = storage_id(p)
        if sid is None:
            loose.append([p])
        else:
            by_id.setdefault(sid, []).append(p)
    return [sorted(v) for v in by_id.values()] + loose


def is_all_one_file(paths: "list[str]") -> bool:
    """True when every path names the SAME bytes (hard links to one file)."""
    if len(paths) < 2:
        return False
    ids = [storage_id(p) for p in paths]
    return ids[0] is not None and all(i == ids[0] for i in ids)


def reclaimable_bytes(group: "list[str]", doomed: "list[str] | None" = None
                      ) -> int:
    """Bytes actually freed by deleting `doomed` (default: all but one).

    Space is only released when the LAST name for a given set of bytes goes,
    so a cluster of hard links that keeps any survivor frees nothing.
    """
    if not group:
        return 0
    try:
        size = os.stat(group[0]).st_size
    except OSError:
        return 0
    gone = set(doomed) if doomed is not None else set(group[1:])
    freed = 0
    for cluster in link_clusters(group):
        if cluster and all(p in gone for p in cluster):
            freed += size          # every name for these bytes is going
    return freed


# -- Choosing which copy to keep / show ---------------------------------------
# One rule, used in two places: the duplicate dialog's "select all but
# best-tagged" button and the gallery's automatic duplicate collapsing.  They
# must agree, or hiding a copy in the grid and keeping a different one in the
# cleanup dialog would contradict each other.

def best_tagged(group: "list[str]") -> str:
    """The copy worth keeping: most tags, then newest, then stable by path.

    Tag count comes first because tagging is the work that cannot be recovered
    from the bytes — the files are byte-identical, so every other difference is
    cosmetic.  Next, a real file beats a symlink: once the tag folders hold
    links rather than copies, the link and its target are the same media, and
    the one worth showing is the one at its own location — a link would break
    if the original ever moved.  mtime breaks the common tie (nothing tagged);
    the path breaks the rest so the choice never depends on dict ordering.
    """
    from . import tags as _tags

    def rank(p: str):
        try:
            mtime = os.stat(p).st_mtime
        except OSError:
            mtime = 0.0
        return (len(_tags.tags_for(p)), not os.path.islink(p), mtime, p)

    return max(group, key=rank) if group else ""


def collapse(paths: "list[str]",
             groups: "list[list[str]]") -> "tuple[list[str], dict[str, list[str]]]":
    """Drop duplicate copies from `paths`, keeping the best of each group.

    Returns (kept_paths, hidden_by_keeper) where hidden_by_keeper maps the
    surviving path to the copies it now stands in for — so the UI can say what
    was hidden and offer to show it again.  Order is preserved; paths not in
    any group pass through untouched.
    """
    hidden: "dict[str, list[str]]" = {}
    drop: "set[str]" = set()
    have = set(paths)                     # hoisted: groups can be numerous
    for g in groups:
        present = [p for p in g if p in have]
        if len(present) < 2:
            continue
        keeper = best_tagged(present)
        others = [p for p in present if p != keeper]
        if others:
            hidden[keeper] = sorted(others)
            drop.update(others)
    return [p for p in paths if p not in drop], hidden


# -- Near-duplicates: same media, different bytes -------------------------------
# Byte-comparison only ever finds exact copies, and the copies that actually
# accumulate are not exact: a re-download lands as "clip (1).mp4", a file
# manager writes "clip - Copy.mp4", a re-encode keeps the name but changes
# every byte. None of those pair with their original under a hash, so the
# finder reported a clean library while the disk filled with near-misses.

import re as _re

# Suffixes a copy picks up.  Deliberately narrow, because over-stripping
# invents duplicates out of ordinary filenames:
#   * the counter is capped at THREE digits.  Cameras name files IMG_2024.jpg,
#     DSC_0001.jpg, PXL_20240101.jpg — stripping "_2024" would reduce every
#     photo in a folder to "img.jpg" and flag the lot as copies of each other.
#     A copy counter is "_1".."_99"; a four-digit run is a sequence or a year.
#   * a bare "clip 2.mp4" is left alone for the same reason: a numbered
#     sequence is far more often two files than two copies of one.
_COPY_SUFFIX = _re.compile(
    r"(?:"
    r"\s*\(\d{1,3}\)"                   # "clip (1)", "clip(1)"
    r"|\s*\[\d{1,3}\]"                  # "clip [1]"
    r"|_\d{1,3}"                        # "clip_1"  (never "IMG_2024")
    r"|\s*-\s*copy(?:\s*\(?\d{1,3}\)?)?"   # "clip - Copy", "- Copy (2)"
    r"|\s*copy(?:\s*\(?\d{1,3}\)?)?"       # "clip copy", "clip copy 2"
    r"|\s*-\s*kopie|\s*-\s*kopia"       # localised file managers
    r")+$", _re.IGNORECASE)


def base_name(path: str) -> str:
    """`path`'s filename with any copy suffix stripped, lowercased.

    ``clip.mp4``, ``clip (1).mp4``, ``clip_1.mp4`` and ``clip - Copy.mp4`` all
    reduce to ``clip.mp4``.
    """
    stem, ext = os.path.splitext(os.path.basename(path))
    stripped = _COPY_SUFFIX.sub("", stem).strip()
    return ((stripped or stem) + ext).lower()


def _duration(path: str) -> float:
    from . import media
    if not media.is_video(path):
        return 0.0
    try:
        return float(media.peek_duration(path))
    except Exception:
        return 0.0


# Below this, files collide on size for no reason at all (icons, thumbnails,
# solid-colour placeholders), so size stops being evidence of anything.
_SIG_MIN_BYTES = 4096


def signature(path: str, duration_places: int = 0) -> "tuple | None":
    """What "the same media" looks like from outside the file.

    Size ALONE is not evidence: any two similar images weigh the same, and
    matching on it groups a folder of unrelated pictures into one blob. It
    needs corroboration, which differs by kind:

      * video — size plus duration, rounded so two encodes of one clip that
        differ by milliseconds still match;
      * image — size plus pixel dimensions.

    Returns None when the file is too small for size to mean anything, or its
    corroborating measure cannot be read.
    """
    from . import media
    try:
        size = os.path.getsize(path)
    except OSError:
        return None
    if size < _SIG_MIN_BYTES:
        return None
    if media.is_video(path):
        dur = round(_duration(path), duration_places)
        return ("v", size, dur) if dur > 0 else None
    try:
        w, h = media.peek_size(path)
    except Exception:
        return None
    return ("i", size, w, h) if w > 0 and h > 0 else None


def find_near_duplicates(
    paths: "list[str]",
    by_name: bool = True,
    by_signature: bool = True,
    progress: "Callable[[int, int], None] | None" = None,
    cancelled: "Callable[[], bool] | None" = None,
) -> "list[dict]":
    """Groups of files that are probably the same media.

    Returns ``[{"paths": [...], "reason": str}]``.

    The three signals are UNIONED rather than ranked. Ranking them left a file
    stranded: once ``a`` and ``b`` paired on bytes, a third copy sharing their
    name had nobody left to pair with and was reported as nothing at all. One
    item belongs in one group however many ways its copies are related, and
    the group is labelled with the strongest relation it contains.

    Pairs the user has declared distinct are never grouped together.
    """
    from . import notdupes

    existing = [p for p in dict.fromkeys(paths) if os.path.isfile(p)]
    parent: "dict[str, str]" = {p: p for p in existing}
    # How strongly each member is tied in; the group reports its best.
    STRENGTH = {"identical bytes": 3, "near-identical name": 2,
                "same size and duration": 1, "same size and dimensions": 1}
    reasons_of: "dict[str, set]" = {}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str, reason: str) -> None:
        ra, rb = find(a), find(b)
        merged = reasons_of.pop(ra, set()) | reasons_of.pop(rb, set())
        if ra != rb:
            parent[rb] = ra
        merged.add(reason)
        reasons_of[find(a)] = merged

    def link_all(members: "list[str]", reason: str) -> None:
        for other in members[1:]:
            union(members[0], other, reason)

    for group in find_duplicates(existing, progress=progress,
                                 cancelled=cancelled):
        link_all(group, "identical bytes")

    if cancelled and cancelled():
        return []

    if by_name:
        by_base: "dict[str, list[str]]" = {}
        for p in existing:
            by_base.setdefault(base_name(p), []).append(p)
        for members in by_base.values():
            if len(members) > 1:
                link_all(members, "near-identical name")

    if by_signature:
        by_sig: "dict[tuple, list[str]]" = {}
        for i, p in enumerate(existing):
            if cancelled and cancelled():
                return []
            sig = signature(p)
            if sig is None or sig[0] <= 0:
                continue
            by_sig.setdefault(sig, []).append(p)
            if progress:
                progress(i + 1, len(existing))
        for sig, members in by_sig.items():
            if len(members) > 1:
                link_all(members, "same size and duration" if sig[0] == "v"
                         else "same size and dimensions")

    clusters: "dict[str, list[str]]" = {}
    for p in existing:
        clusters.setdefault(find(p), []).append(p)

    out: "list[dict]" = []
    for root, members in clusters.items():
        if len(members) < 2:
            continue
        # Name the group by EVERY signal that bound it: calling a group
        # "identical bytes" when only two of its four members are would
        # misrepresent what the user is being asked to judge.
        found = sorted(reasons_of.get(root, {"same size and dimensions"}),
                       key=lambda r: -STRENGTH.get(r, 0))
        reason = " + ".join(found)
        buckets, held = notdupes.split(sorted(members))
        for bucket in buckets:
            out.append({"paths": bucket, "reason": reason,
                        "signals": found, "held_back": held})
    out.sort(key=lambda g: (-STRENGTH.get(g["signals"][0], 0), g["paths"][0]))
    return out
