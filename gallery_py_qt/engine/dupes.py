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
