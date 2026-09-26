"""Rebuild the tag and favourites folders as links, from the data itself.

The per-file mirroring in :mod:`tags` and :mod:`favorites` is INCREMENTAL: it
reacts to a file being tagged or favourited while the app is running. Anything
that happened otherwise — tags recovered from a legacy store, files tagged
before the mirror existed, a tag folder deleted by hand, a library moved to a
new favourites location — leaves the folders out of step with the data, and
nothing ever reconciles them.

This does the reconciliation. It reads the two sources of truth (the tag store,
and what is favourited) and makes the folders match, placing LINKS rather than
copies, so a complete mirror of a large library costs essentially nothing.

Two deliberate differences from the incremental path:

  * a tagged file is linked whether or not it is also favourited, and whatever
    its type or length. Those restrictions existed because each mirror was a
    full COPY and the disk cost had to be contained; a link does not have that
    problem, and "show me everything with this tag" is what a tag folder is
    for.
  * stale links are pruned, so removing a tag or deleting an original empties
    the folder instead of leaving it lying about.

Nothing here deletes a real file. Pruning only ever removes links.
"""
from __future__ import annotations
import os
from typing import Callable

from .. import config
from . import favorites, taglibrary, tags as _tags


# -- reading the sources of truth ----------------------------------------------

def collect_tagged(roots: "list[str] | None" = None,
                   only_tags: "set[str] | list[str] | None" = None
                   ) -> "dict[str, list[str]]":
    """{original: [tags]} from the tag store.

    Only existing files, only tags still in the tag set (a name dropped from
    the set should not resurrect a folder), and never a file that already
    lives inside the mirror tree — linking a link would compound.

    `only_tags` narrows it further: mirror just those tags. A library with
    fifty tags does not necessarily want fifty folders, and rebuilding one tag
    should not mean walking all of them.
    """
    skip = list(roots if roots is not None else taglibrary.mirror_roots())
    known = set(_tags.get_tags())
    if only_tags is not None:
        known &= {str(t) for t in only_tags}
    out: "dict[str, list[str]]" = {}
    for path, names in _tags._load().items():
        keep = [t for t in names if t in known]
        if not keep:
            continue
        if favorites.under_any(path, skip) or not os.path.isfile(path):
            continue
        out[path] = sorted(keep)
    return out


def find_favorites_dirs(roots: "list[str]") -> "list[str]":
    """Every ``Favorites`` subfolder under `roots` (the per-folder mirrors)."""
    found: "list[str]" = []
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for dirpath, dirnames, _files in os.walk(root):
            if os.path.basename(dirpath) == "Favorites":
                found.append(dirpath)
                dirnames[:] = []            # don't descend into a mirror
    return found


def read_central_favorites() -> "set[str]":
    """Originals recorded by the CENTRALISED favourites tree.

    The mirror there is the favourites equivalent of a tag folder, and it was
    the one place never read back: only the per-folder mirrors beside the
    media were scanned, so a library using the central layout could not
    rebuild its favourites from disk the way the tags could.

    Each entry names its original outright — a symlink through its target, a
    copy through the path shape the mirror root encodes.
    """
    root = favorites.central_favorites_root()
    out: "set[str]" = set()
    if not os.path.isdir(root):
        return out
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            entry = os.path.join(dirpath, name)
            original = ""
            if os.path.islink(entry):
                target = os.path.realpath(entry)
                original = target if os.path.isfile(target) else ""
            if not original:
                original = favorites.original_for_central_mirror(entry)
            if original and os.path.isfile(original):
                out.add(original)
    return out


def _original_for_mirror_entry(entry: str,
                               index: "dict[str, str] | None") -> str:
    """The original a file inside a ``Favorites`` folder stands for.

    A symlink names its target outright. Otherwise the file beside the mirror
    (``<media folder>/<name>`` for ``<media folder>/Favorites/<name>``) is the
    natural original, since that is where the mirror was made from; failing
    that, fall back to the filename index.
    """
    if os.path.islink(entry):
        target = os.path.realpath(entry)
        return target if os.path.isfile(target) else ""
    name = os.path.basename(entry)
    sibling = os.path.join(os.path.dirname(os.path.dirname(entry)), name)
    if os.path.isfile(sibling):
        return sibling
    if index:
        return index.get(name, "")
    return ""


def collect_favorites(media_roots: "list[str] | None" = None,
                      index: "dict[str, str] | None" = None
                      ) -> "set[str]":
    """Originals that are favourited, from the store AND from disk.

    The store is authoritative for this session, but a library that has moved
    between machines (or predates the store) carries the record only as the
    mirrors themselves — so the ``Favorites`` folders are read back too, both
    the centralised tree and the per-folder ones, and all of it is unioned.
    """
    out: "set[str]" = set()
    try:
        favs = favorites.Favorites()
        out.update(p for p in favs._paths if os.path.isfile(p))
    except Exception:
        pass
    out |= read_central_favorites()
    for mirror in find_favorites_dirs(list(media_roots or [])):
        try:
            entries = os.listdir(mirror)
        except OSError:
            continue
        for name in entries:
            original = _original_for_mirror_entry(
                os.path.join(mirror, name), index)
            if original and os.path.isfile(original):
                out.add(original)
    return out


# -- planning ------------------------------------------------------------------

def _is_correct_link(dst: str, src: str) -> bool:
    """True when `dst` already stands for `src` — a link, or the same inode."""
    if not os.path.lexists(dst):
        return False
    try:
        if os.path.islink(dst):
            return os.path.realpath(dst) == os.path.realpath(src)
        a, b = os.stat(dst), os.stat(src)
        return a.st_ino != 0 and (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)
    except OSError:
        return False


def _assign_names(items: "list[tuple[str, str]]") -> "dict[str, str]":
    """{src: filename} for one destination folder, disambiguating collisions.

    Two different originals can share a basename under the same tag. Suffixes
    are assigned in sorted order so the same library always produces the same
    names — otherwise every run would invent new ones and nothing would ever
    look already-done.
    """
    by_name: "dict[str, list[str]]" = {}
    for src, name in items:
        by_name.setdefault(name, []).append(src)
    out: "dict[str, str]" = {}
    for name, srcs in by_name.items():
        if len(srcs) == 1:
            out[srcs[0]] = name
            continue
        stem, ext = os.path.splitext(name)
        for i, src in enumerate(sorted(srcs)):
            out[src] = name if i == 0 else f"{stem}_{i}{ext}"
    return out


def plan(tagged: "dict[str, list[str]]", favourited: "set[str]"
         ) -> "list[dict]":
    """Every link that should exist, with what it would take to create it.

    ``action`` is "ok" (already correct), "create" (nothing there), or
    "replace" (a real file occupies the spot — normally a copy from before
    linking was possible).
    """
    wanted: "dict[str, list[tuple[str, str]]]" = {}

    for src, names in tagged.items():
        for tag in names:
            folder = os.path.join(taglibrary.tag_folders_root(), tag)
            wanted.setdefault(folder, []).append((src, os.path.basename(src)))
    for src in favourited:
        folder = favorites.mirror_dir_for(src)
        wanted.setdefault(folder, []).append((src, os.path.basename(src)))

    out: "list[dict]" = []
    for folder, items in wanted.items():
        # De-duplicate: one original can reach a folder twice (tagged and
        # favourited into the same place).
        uniq = list(dict.fromkeys(items))
        for src, name in _assign_names(uniq).items():
            dst = os.path.join(folder, name)
            if _is_correct_link(dst, src):
                action = "ok"
            elif os.path.lexists(dst):
                action = "replace"
            else:
                action = "create"
            out.append({"src": src, "dst": dst, "folder": folder,
                        "action": action})
    out.sort(key=lambda d: d["dst"])
    return out


def stale_links(plan_items: "list[dict]",
                roots: "list[str] | None" = None,
                only_tags: "set[str] | list[str] | None" = None
                ) -> "list[str]":
    """Links in the mirror tree that the plan does not call for.

    A file untagged since the last sync, or an original that has been deleted,
    leaves a link behind.

    Only LINKS are ever listed, in both senses: a symlink, or a regular file
    with more than one name (a hard link we placed). Removing either is
    provably lossless — the content survives under its other name — whereas a
    single-named file in a mirror folder might be a copy not yet converted, or
    something the user put there, so it is left alone.
    """
    wanted = {os.path.abspath(d["dst"]) for d in plan_items}
    if roots is None:
        if only_tags is None:
            roots = [taglibrary.tag_folders_root(),
                     favorites.central_favorites_root()]
        else:
            # A filtered run has no opinion about the other tags' folders, and
            # must not treat "not planned" as "stale" for them.
            roots = [os.path.join(taglibrary.tag_folders_root(), t)
                     for t in only_tags]
    out: "list[str]" = []
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                path = os.path.join(dirpath, name)
                if os.path.abspath(path) in wanted:
                    continue
                if not is_link_entry(path):
                    continue
                out.append(path)
    return sorted(out)


# Shared with the relink pass: a symlink, or a file with more than one name.
is_link_entry = favorites.is_link_entry


# -- applying ------------------------------------------------------------------

def apply(plan_items: "list[dict]", prune: "list[str] | None" = None,
          replace_copies: bool = True,
          progress: "Callable[[int, int, str], bool | None] | None" = None
          ) -> dict:
    """Create the planned links and remove the stale ones.

    Returns a report. `progress(done, total, path)` may return False to stop.
    A link is built under a temporary name and only moved into place once it
    exists, so a failure can never leave the destination empty.
    """
    report = {"created": 0, "replaced": 0, "already": 0, "pruned": 0,
              "copied": 0, "errors": [], "cancelled": False}
    todo = [d for d in plan_items if d["action"] != "ok"]
    report["already"] = len(plan_items) - len(todo)
    total = len(todo) + len(prune or [])
    done = 0

    for item in todo:
        if progress is not None and progress(done, total, item["dst"]) is False:
            report["cancelled"] = True
            return report
        done += 1
        if item["action"] == "replace" and not replace_copies:
            continue
        src, dst = item["src"], item["dst"]
        if not os.path.isfile(src):
            report["errors"].append(f"{src}: original has gone")
            continue
        existed = os.path.lexists(dst)
        try:
            if favorites.link_in_place(src, dst) == "copy":
                # A copy here would defeat the point and silently consume the
                # space this exists to save.
                report["copied"] += 1
                continue
            if existed:
                report["replaced"] += 1
            else:
                report["created"] += 1
        except OSError as exc:
            report["errors"].append(f"{dst}: {exc}")

    for path in (prune or []):
        if progress is not None and progress(done, total, path) is False:
            report["cancelled"] = True
            return report
        done += 1
        try:
            if is_link_entry(path):         # re-checked: never a lone file
                os.remove(path)
                report["pruned"] += 1
        except OSError as exc:
            report["errors"].append(f"{path}: {exc}")
    _prune_empty_dirs()
    return report


def _prune_empty_dirs() -> None:
    """Remove tag folders left with nothing in them after a prune."""
    for root in (taglibrary.tag_folders_root(),
                 favorites.central_favorites_root()):
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, _files in os.walk(root, topdown=False):
            if os.path.abspath(dirpath) == os.path.abspath(root):
                continue
            try:
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
            except OSError:
                pass


def adopt_favorites(found: "set[str]") -> int:
    """Put favourites discovered on disk back into the store.

    The tag side already heals itself this way (adopt_tags_in_use); without
    the same for favourites, a rebuild would re-create the mirrors while the
    heart icons stayed empty, because the store never learned what the folders
    already knew. Returns how many were adopted.
    """
    try:
        favs = favorites.Favorites()
    except Exception:
        return 0
    added = [p for p in found if p not in favs._paths]
    if not added:
        return 0
    favs._paths.update(added)
    favs._save()
    return len(added)


def sync(media_roots: "list[str] | None" = None,
         index: "dict[str, str] | None" = None,
         do_prune: bool = True,
         only_tags: "set[str] | list[str] | None" = None,
         include_favorites: bool = True,
         progress: "Callable[[int, int, str], bool | None] | None" = None
         ) -> dict:
    """Read the data, plan, and apply — the whole reconciliation.

    `only_tags` restricts the rebuild to those tags; `include_favorites` can
    switch the favourites mirror off, so a run can address one without
    disturbing the other.
    """
    roots = taglibrary.mirror_roots(media_roots)
    tagged = collect_tagged(roots, only_tags)
    favourited = (collect_favorites(media_roots, index)
                  if include_favorites else set())
    adopted = adopt_favorites(favourited) if include_favorites else 0
    items = plan(tagged, favourited)
    prune = (stale_links(items, only_tags=only_tags)
             if do_prune else [])
    report = apply(items, prune, progress=progress)
    report["tagged_files"] = len(tagged)
    report["favourited_files"] = len(favourited)
    report["favourites_adopted"] = adopted
    report["planned"] = len(items)
    return report
