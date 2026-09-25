"""Rebuilding the tag / favourites folders as links from the data itself."""
from __future__ import annotations
import os
import shutil

import pytest
from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine import (favorites, media, mirrorsync, scan,
                                  taglibrary, tags)


def _img(path, size=(20, 20)):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", size).save(path)
    return str(path)


def _symlinks_ok(tmp_path) -> bool:
    a = _img(str(tmp_path / "_probe" / "a.png"))
    try:
        os.symlink(a, str(tmp_path / "_probe" / "b.png"))
        return True
    except (OSError, NotImplementedError):
        return False


@pytest.fixture
def lib(tmp_path, monkeypatch):
    x = tmp_path / "X"
    x.mkdir()
    monkeypatch.setattr(config, "FAVORITES_DIR", str(x))
    tags.set_tags(["T - Face", "T - Landscape"])
    favorites.set_link_mode("auto")
    return x


# -- reading the sources of truth ---------------------------------------------

def test_collect_tagged_reads_the_store(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.toggle_tag(p, "T - Face")
    assert mirrorsync.collect_tagged([]) == {p: ["T - Face"]}


def test_collect_tagged_ignores_names_not_in_the_tag_set(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.toggle_tag(p, "T - Face")
    tags.set_tags(["T - Landscape"])          # "T - Face" dropped
    assert mirrorsync.collect_tagged([]) == {}


def test_collect_tagged_skips_missing_files(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.toggle_tag(p, "T - Face")
    os.remove(p)
    assert mirrorsync.collect_tagged([]) == {}


def test_collect_tagged_never_mirrors_a_mirror(lib, tmp_path):
    """A file already inside the tag tree must not be linked again."""
    inside = _img(str(lib / "Tag Folders" / "T - Face" / "a.png"))
    tags.toggle_tag(inside, "T - Face")
    assert mirrorsync.collect_tagged(
        [taglibrary.tag_folders_root()]) == {}


def test_find_favorites_dirs_locates_the_mirrors(lib, tmp_path):
    _img(str(tmp_path / "media" / "Favorites" / "a.png"))
    _img(str(tmp_path / "media" / "sub" / "Favorites" / "b.png"))
    found = mirrorsync.find_favorites_dirs([str(tmp_path / "media")])
    assert len(found) == 2


def test_favorites_are_read_back_from_the_mirror_folders(lib, tmp_path):
    """A library that predates the store carries the record only on disk."""
    original = _img(str(tmp_path / "media" / "a.png"))
    (tmp_path / "media" / "Favorites").mkdir()
    shutil.copy(original, str(tmp_path / "media" / "Favorites" / "a.png"))
    got = mirrorsync.collect_favorites([str(tmp_path / "media")])
    assert original in got


def test_a_symlinked_mirror_entry_resolves_to_its_target(lib, tmp_path):
    if not _symlinks_ok(tmp_path):
        pytest.skip("no symlink privilege")
    original = _img(str(tmp_path / "media" / "a.png"))
    mirror = tmp_path / "media" / "Favorites"
    mirror.mkdir()
    os.symlink(original, str(mirror / "a.png"))
    assert original in mirrorsync.collect_favorites([str(tmp_path / "media")])


def test_favourites_store_and_disk_are_unioned(lib, tmp_path):
    a = _img(str(tmp_path / "media" / "a.png"))
    b = _img(str(tmp_path / "media" / "b.png"))
    (tmp_path / "media" / "Favorites").mkdir(exist_ok=True)
    shutil.copy(b, str(tmp_path / "media" / "Favorites" / "b.png"))
    favorites.Favorites().toggle(a)
    favorites.flush_mirror_ops()
    got = mirrorsync.collect_favorites([str(tmp_path / "media")])
    assert {a, b} <= got


# -- planning ------------------------------------------------------------------

def test_plan_puts_a_tagged_file_in_each_of_its_tag_folders(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    items = mirrorsync.plan({p: ["T - Face", "T - Landscape"]}, set())
    folders = {os.path.basename(d["folder"]) for d in items}
    assert folders == {"T - Face", "T - Landscape"}
    assert all(d["action"] == "create" for d in items)


def test_plan_marks_an_existing_copy_for_replacement(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    dest = lib / "Tag Folders" / "T - Face"
    dest.mkdir(parents=True)
    shutil.copy(p, str(dest / "a.png"))
    items = mirrorsync.plan({p: ["T - Face"]}, set())
    assert [d["action"] for d in items] == ["replace"]


def test_plan_leaves_a_correct_link_alone(lib, tmp_path):
    if not _symlinks_ok(tmp_path):
        pytest.skip("no symlink privilege")
    p = _img(str(tmp_path / "media" / "a.png"))
    dest = lib / "Tag Folders" / "T - Face"
    dest.mkdir(parents=True)
    os.symlink(p, str(dest / "a.png"))
    items = mirrorsync.plan({p: ["T - Face"]}, set())
    assert [d["action"] for d in items] == ["ok"]


def test_same_named_originals_get_distinct_links(lib, tmp_path):
    a = _img(str(tmp_path / "one" / "pic.png"))
    b = _img(str(tmp_path / "two" / "pic.png"))
    items = mirrorsync.plan({a: ["T - Face"], b: ["T - Face"]}, set())
    assert len({d["dst"] for d in items}) == 2


def test_link_names_are_stable_across_runs(lib, tmp_path):
    """Unstable names would make every run look like fresh work."""
    a = _img(str(tmp_path / "one" / "pic.png"))
    b = _img(str(tmp_path / "two" / "pic.png"))
    first = {d["src"]: d["dst"] for d in
             mirrorsync.plan({a: ["T - Face"], b: ["T - Face"]}, set())}
    second = {d["src"]: d["dst"] for d in
              mirrorsync.plan({b: ["T - Face"], a: ["T - Face"]}, set())}
    assert first == second


def test_a_file_both_tagged_and_favourited_is_planned_once_per_place(lib,
                                                                     tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    items = mirrorsync.plan({p: ["T - Face"]}, {p})
    assert len(items) == len({d["dst"] for d in items})


# -- applying ------------------------------------------------------------------

def test_apply_creates_the_links(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"), (40, 30))
    rep = mirrorsync.apply(mirrorsync.plan({p: ["T - Face"]}, set()))
    dst = lib / "Tag Folders" / "T - Face" / "a.png"
    assert rep["created"] == 1 and not rep["errors"]
    assert os.path.islink(dst) or os.stat(dst).st_nlink > 1
    assert media.peek_size(str(dst)) == (40, 30)


def test_apply_is_idempotent(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    plan = mirrorsync.plan({p: ["T - Face"]}, set())
    mirrorsync.apply(plan)
    again = mirrorsync.plan({p: ["T - Face"]}, set())
    rep = mirrorsync.apply(again)
    assert rep["created"] == 0 and rep["replaced"] == 0
    assert rep["already"] == 1


def test_apply_replaces_a_copy_with_a_link(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    dest = lib / "Tag Folders" / "T - Face"
    dest.mkdir(parents=True)
    shutil.copy(p, str(dest / "a.png"))
    rep = mirrorsync.apply(mirrorsync.plan({p: ["T - Face"]}, set()))
    assert rep["replaced"] == 1
    dst = str(dest / "a.png")
    assert os.path.islink(dst) or os.stat(dst).st_nlink > 1


def test_apply_refuses_to_write_a_copy(lib, tmp_path):
    favorites.set_link_mode("copy")
    p = _img(str(tmp_path / "media" / "a.png"))
    rep = mirrorsync.apply(mirrorsync.plan({p: ["T - Face"]}, set()))
    assert rep["copied"] == 1 and rep["created"] == 0
    assert not os.path.exists(lib / "Tag Folders" / "T - Face" / "a.png")


def test_apply_can_be_cancelled(lib, tmp_path):
    paths = {_img(str(tmp_path / "media" / f"{i}.png")): ["T - Face"]
             for i in range(5)}
    rep = mirrorsync.apply(mirrorsync.plan(paths, set()),
                           progress=lambda d, t, p: d < 2)
    assert rep["cancelled"] and rep["created"] <= 2


def test_a_missing_original_is_reported_not_fatal(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    plan = mirrorsync.plan({p: ["T - Face"]}, set())
    os.remove(p)
    rep = mirrorsync.apply(plan)
    assert rep["errors"] and rep["created"] == 0


# -- pruning -------------------------------------------------------------------

def test_stale_links_are_found(lib, tmp_path):
    if not _symlinks_ok(tmp_path):
        pytest.skip("no symlink privilege")
    p = _img(str(tmp_path / "media" / "a.png"))
    dest = lib / "Tag Folders" / "T - Face"
    dest.mkdir(parents=True)
    os.symlink(p, str(dest / "gone.png"))          # nothing plans this
    assert mirrorsync.stale_links([]) == [str(dest / "gone.png")]


def test_a_hardlinked_mirror_is_prunable_too(lib, tmp_path):
    """Pruning keyed on islink() alone would never clean a same-volume
    library, where the mirrors are hard links."""
    p = _img(str(tmp_path / "media" / "a.png"))
    dest = lib / "Tag Folders" / "T - Face"
    dest.mkdir(parents=True)
    try:
        os.link(p, str(dest / "a.png"))
    except OSError:
        pytest.skip("filesystem has no hard links")
    assert mirrorsync.is_link_entry(str(dest / "a.png"))
    assert mirrorsync.stale_links([]) == [str(dest / "a.png")]


def test_pruning_removes_only_links(lib, tmp_path):
    """A real file in a tag folder may be a copy not yet converted — or the
    user's own. It must survive."""
    dest = lib / "Tag Folders" / "T - Face"
    dest.mkdir(parents=True)
    real = _img(str(dest / "real.png"))
    assert mirrorsync.stale_links([]) == []
    mirrorsync.apply([], mirrorsync.stale_links([]))
    assert os.path.isfile(real)


def test_untagging_empties_the_folder_on_the_next_sync(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.toggle_tag(p, "T - Face")
    mirrorsync.sync([str(tmp_path / "media")])
    dst = lib / "Tag Folders" / "T - Face" / "a.png"
    assert os.path.lexists(dst)
    tags.toggle_tag(p, "T - Face")                 # untagged
    mirrorsync.sync([str(tmp_path / "media")])
    assert not os.path.lexists(dst)


# -- the whole reconciliation ---------------------------------------------------

def test_sync_mirrors_every_tagged_file_regardless_of_favourite(lib, tmp_path):
    """The incremental path only mirrored favourited files; a link costs
    nothing, so a tag folder should hold everything carrying the tag."""
    a = _img(str(tmp_path / "media" / "a.png"))
    b = _img(str(tmp_path / "media" / "b.png"))
    tags.toggle_tag(a, "T - Face")
    tags.toggle_tag(b, "T - Face")
    favorites.Favorites().toggle(a)                # only a is favourited
    favorites.flush_mirror_ops()
    rep = mirrorsync.sync([str(tmp_path / "media")])
    folder = lib / "Tag Folders" / "T - Face"
    assert {"a.png", "b.png"} <= set(os.listdir(folder))
    assert rep["tagged_files"] == 2


def test_sync_mirrors_long_videos_too(lib, tmp_path):
    """The old eligibility rule excluded videos over ten minutes because a
    copy was expensive; a link is not."""
    v = str(tmp_path / "media" / "movie.mp4")
    os.makedirs(os.path.dirname(v), exist_ok=True)
    with open(v, "wb") as f:
        f.write(b"\x00" * 128)
    tags.toggle_tag(v, "T - Face")
    mirrorsync.sync([str(tmp_path / "media")])
    assert "movie.mp4" in os.listdir(lib / "Tag Folders" / "T - Face")


def test_synced_tag_folder_scans_and_loads(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"), (64, 48))
    tags.toggle_tag(p, "T - Face")
    mirrorsync.sync([str(tmp_path / "media")])
    folder = str(lib / "Tag Folders" / "T - Face")
    found = scan.scan_media(folder).paths
    assert [os.path.basename(x) for x in found] == ["a.png"]
    assert media.peek_size(found[0]) == (64, 48)


def test_sync_reports_what_it_did(lib, tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.toggle_tag(p, "T - Face")
    rep = mirrorsync.sync([str(tmp_path / "media")])
    assert rep["tagged_files"] == 1 and rep["planned"] >= 1
    assert set(rep) >= {"created", "replaced", "pruned", "already", "errors"}


def test_sync_populates_the_central_favourites_folder(lib, tmp_path):
    favorites.set_favorites_layout(False)
    p = _img(str(tmp_path / "media" / "a.png"))
    favorites.Favorites().toggle(p)
    favorites.flush_mirror_ops()
    mirrorsync.sync([str(tmp_path / "media")])
    root = favorites.central_favorites_root()
    hits = [f for _d, _s, fs in os.walk(root) for f in fs]
    assert "a.png" in hits
