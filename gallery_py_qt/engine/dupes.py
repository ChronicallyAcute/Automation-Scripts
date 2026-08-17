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
