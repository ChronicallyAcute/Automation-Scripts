"""Separating real tags from media folders, and copies becoming links."""
from __future__ import annotations
import os
import shutil

import pytest
from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine import favorites, media, scan, taglibrary, tags


def _img(path, size=(20, 20)):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", size).save(path)
    return str(path)


def _can_link(tmp_path) -> bool:
    a = _img(str(tmp_path / "_p" / "a.png"))
    try:
        os.link(a, str(tmp_path / "_p" / "b.png"))
        return True
    except OSError:
        return False


@pytest.fixture
def lib(tmp_path, monkeypatch):
    """A FAVORITES_DIR holding real tag folders AND standalone media folders."""
    x = tmp_path / "X"
    x.mkdir()
    monkeypatch.setattr(config, "FAVORITES_DIR", str(x))
    for name in ("T - Face", "T - Landscape", "Holiday 2024", "Screenshots"):
        (x / name).mkdir()
    tags.set_tags(["T - Face", "T - Landscape", "Holiday 2024", "Screenshots"])
    return x


# -- survey --------------------------------------------------------------------

def test_survey_lists_every_candidate(lib):
    names = {e["name"] for e in taglibrary.survey()}
    assert {"T - Face", "T - Landscape", "Holiday 2024", "Screenshots"} <= names


def test_survey_suggests_only_prefixed_names(lib):
    suggested = {e["name"] for e in taglibrary.survey() if e["suggested"]}
    assert suggested == {"T - Face", "T - Landscape"}


def test_survey_puts_suggestions_first(lib):
    rows = taglibrary.survey()
    assert rows[0]["suggested"] and not rows[-1]["suggested"]


def test_survey_counts_files(lib):
    _img(str(lib / "T - Face" / "a.png"))
    row = next(e for e in taglibrary.survey() if e["name"] == "T - Face")
    assert row["files"] == 1


def test_survey_ignores_app_structure(lib):
    (lib / taglibrary.TAG_FOLDERS_DIRNAME).mkdir()
    (lib / "folder tags").mkdir()
    names = {e["name"] for e in taglibrary.survey()}
    assert taglibrary.TAG_FOLDERS_DIRNAME not in names
    assert "folder tags" not in names


# -- merging -------------------------------------------------------------------

def test_merge_moves_only_kept_tags(lib):
    taglibrary.merge_tag_folders(["T - Face", "T - Landscape"])
    assert sorted(os.listdir(taglibrary.tag_folders_root())) == [
        "T - Face", "T - Landscape"]
    left = set(os.listdir(lib))
    assert {"Holiday 2024", "Screenshots"} <= left
    assert "T - Face" not in left


def test_merge_carries_the_files_across(lib):
    _img(str(lib / "T - Face" / "a.png"))
    taglibrary.merge_tag_folders(["T - Face"])
    assert os.path.isfile(
        os.path.join(taglibrary.tag_folders_root(), "T - Face", "a.png"))


def test_merge_is_idempotent(lib):
    _img(str(lib / "T - Face" / "a.png"))
    taglibrary.merge_tag_folders(["T - Face"])
    moved, errors = taglibrary.merge_tag_folders(["T - Face"])
    assert moved == 0 and not errors


def test_merge_into_an_existing_destination_keeps_both(lib):
    _img(str(lib / "T - Face" / "a.png"))
    dest = os.path.join(taglibrary.tag_folders_root(), "T - Face")
    _img(os.path.join(dest, "b.png"))
    taglibrary.merge_tag_folders(["T - Face"])
    assert sorted(os.listdir(dest)) == ["a.png", "b.png"]


def test_merge_does_not_clobber_a_same_named_file(lib):
    _img(str(lib / "T - Face" / "a.png"), (32, 32))
    dest = os.path.join(taglibrary.tag_folders_root(), "T - Face")
    _img(os.path.join(dest, "a.png"), (8, 8))
    taglibrary.merge_tag_folders(["T - Face"])
    assert len(os.listdir(dest)) == 2, "the incoming file must not overwrite"


# -- pruning -------------------------------------------------------------------

def test_prune_keeps_only_the_chosen_names(lib):
    dropped = taglibrary.prune_tag_set(["T - Face", "T - Landscape"])
    assert set(tags.get_tags()) == {"T - Face", "T - Landscape"}
    assert set(dropped) == {"Holiday 2024", "Screenshots"}


def test_prune_never_deletes_a_folder(lib):
    taglibrary.prune_tag_set(["T - Face"])
    assert os.path.isdir(lib / "Holiday 2024")
    assert os.path.isdir(lib / "Screenshots")


def test_prune_keeps_the_tag_records(lib, tmp_path):
    """Dropping a chip must not lose which files carried it — re-ticking the
    name has to bring them straight back."""
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.toggle_tag(p, "Holiday 2024")
    taglibrary.prune_tag_set(["T - Face"])
    assert "Holiday 2024" in tags.tags_for(p)
    tags.set_tags(list(tags.get_tags()) + ["Holiday 2024"])
    assert "Holiday 2024" in tags.tags_for(p)


# -- scoping: the user's own folders are off limits ----------------------------

def test_mirror_roots_excludes_standalone_media_folders(lib):
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    assert any("T - Face" in r for r in roots)
    assert not any("Holiday 2024" in r for r in roots)
    assert str(lib) not in [os.path.abspath(r) for r in roots]


def test_index_can_find_an_original_inside_favorites_dir(lib, tmp_path):
    """A standalone folder in G:\\X is a legitimate home for an original."""
    _img(str(lib / "Holiday 2024" / "beach.png"))
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    index = taglibrary.index_originals([str(lib)], exclude=roots)
    assert "beach.png" in index


def test_index_never_adopts_a_copy_as_its_own_original(lib):
    _img(str(lib / "T - Face" / "only-here.png"))
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    index = taglibrary.index_originals([str(lib)], exclude=roots)
    assert "only-here.png" not in index


# -- relinking -----------------------------------------------------------------

def test_plan_finds_real_copies(lib, tmp_path):
    src = _img(str(tmp_path / "media" / "pic.png"))
    shutil.copy(src, lib / "T - Face" / "pic.png")
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    plan = taglibrary.plan_relink(
        roots, taglibrary.index_originals([str(tmp_path / "media")], exclude=roots))
    assert len(plan) == 1 and plan[0]["action"] == "link"


def test_plan_marks_copies_with_no_original_as_orphans(lib):
    _img(str(lib / "T - Face" / "nowhere.png"))
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    plan = taglibrary.plan_relink(roots, {})
    assert plan and plan[0]["action"] == "orphan"


def test_plan_skips_files_that_are_already_links(lib, tmp_path):
    src = _img(str(tmp_path / "media" / "pic.png"))
    try:
        os.symlink(src, str(lib / "T - Face" / "pic.png"))
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    assert taglibrary.plan_relink(roots, {"pic.png": src}) == []


def test_plan_skips_existing_hardlinks(lib, tmp_path):
    if not _can_link(tmp_path):
        pytest.skip("filesystem has no hard links")
    src = _img(str(tmp_path / "media" / "pic.png"))
    os.link(src, str(lib / "T - Face" / "pic.png"))
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    assert taglibrary.plan_relink(roots, {"pic.png": src}) == []


def test_relink_replaces_the_copy_and_frees_space(lib, tmp_path):
    if not _can_link(tmp_path):
        pytest.skip("filesystem has no hard links")
    favorites.set_link_mode("auto")
    src = _img(str(tmp_path / "media" / "pic.png"), (60, 60))
    dst = str(lib / "T - Face" / "pic.png")
    shutil.copy(src, dst)
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    plan = taglibrary.plan_relink(
        roots, taglibrary.index_originals([str(tmp_path / "media")], exclude=roots))
    relinked, freed, errors = taglibrary.relink_copies(plan)
    assert relinked == 1 and freed > 0 and not errors
    assert os.path.islink(dst) or os.stat(dst).st_nlink > 1


def test_relinked_media_still_loads_from_its_source(lib, tmp_path):
    if not _can_link(tmp_path):
        pytest.skip("filesystem has no hard links")
    favorites.set_link_mode("auto")
    src = _img(str(tmp_path / "media" / "pic.png"), (77, 55))
    dst = str(lib / "T - Face" / "pic.png")
    shutil.copy(src, dst)
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    plan = taglibrary.plan_relink(
        roots, taglibrary.index_originals([str(tmp_path / "media")], exclude=roots))
    taglibrary.relink_copies(plan)
    assert media.peek_size(dst) == (77, 55)


def test_relink_leaves_the_copy_in_place_if_no_link_is_possible(lib, tmp_path):
    favorites.set_link_mode("copy")          # forces place_file to copy
    src = _img(str(tmp_path / "media" / "pic.png"))
    dst = str(lib / "T - Face" / "pic.png")
    shutil.copy(src, dst)
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    plan = taglibrary.plan_relink(roots, {"pic.png": src})
    relinked, _freed, errors = taglibrary.relink_copies(plan)
    assert relinked == 0 and errors
    assert os.path.isfile(dst) and not os.path.islink(dst), "must not be lost"


def test_relink_never_touches_a_standalone_media_folder(lib, tmp_path):
    favorites.set_link_mode("auto")
    src = _img(str(tmp_path / "media" / "pic.png"))
    standalone = str(lib / "Holiday 2024" / "pic.png")
    shutil.copy(src, standalone)
    roots = taglibrary.mirror_roots(tag_names=["T - Face"])
    plan = taglibrary.plan_relink(roots, {"pic.png": src})
    assert not any(p["copy"] == standalone for p in plan)
    taglibrary.relink_copies(plan)
    assert os.path.isfile(standalone) and not os.path.islink(standalone)


# -- linked media loads like any other -----------------------------------------

def test_a_tag_folder_of_links_scans_normally(lib, tmp_path):
    src = _img(str(tmp_path / "media" / "pic.png"))
    folder = str(lib / "T - Face")
    try:
        os.symlink(src, os.path.join(folder, "pic.png"))
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    found = scan.scan_media(folder).paths
    assert [os.path.basename(p) for p in found] == ["pic.png"]
    assert media.peek_size(found[0]) == (20, 20)


# -- link-first placement -------------------------------------------------------

def test_auto_mode_prefers_a_real_link(tmp_path):
    if not _can_link(tmp_path):
        pytest.skip("filesystem has no hard links")
    favorites.set_link_mode("auto")
    src = _img(str(tmp_path / "m" / "a.png"))
    assert favorites.place_file(src, str(tmp_path / "m" / "b.png")) == "hardlink"


def test_auto_mode_falls_through_to_symlink(tmp_path, monkeypatch):
    favorites.set_link_mode("auto")
    src = _img(str(tmp_path / "m" / "a.png"))

    def _no_hardlink(*_a, **_k):
        raise OSError("Invalid cross-device link")
    monkeypatch.setattr(os, "link", _no_hardlink)
    try:
        assert favorites.place_file(src, str(tmp_path / "m" / "b.png")) == "symlink"
    except OSError:
        pytest.skip("no symlink privilege")


def test_auto_mode_copies_only_as_a_last_resort(tmp_path, monkeypatch):
    favorites.set_link_mode("auto")
    src = _img(str(tmp_path / "m" / "a.png"))
    monkeypatch.setattr(os, "link", lambda *a, **k: (_ for _ in ()).throw(OSError("x")))
    monkeypatch.setattr(os, "symlink", lambda *a, **k: (_ for _ in ()).throw(OSError("y")))
    assert favorites.place_file(src, str(tmp_path / "m" / "b.png")) == "copy"


def test_probe_reports_the_mode_auto_would_use(tmp_path):
    favorites.set_link_mode("auto")
    mode, why = favorites.probe_link_mode(str(tmp_path), str(tmp_path))
    assert mode in ("hardlink", "symlink") and why


# -- a link and its target are never both shown --------------------------------

def test_the_original_is_preferred_over_a_link(tmp_path):
    from gallery_py_qt.engine import dupes
    src = _img(str(tmp_path / "m" / "pic.png"))
    link = str(tmp_path / "m" / "link.png")
    try:
        os.symlink(src, link)
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    assert dupes.best_tagged([src, link]) == src


def test_a_link_and_its_target_collapse_to_one(tmp_path):
    from gallery_py_qt.engine import dupes
    src = _img(str(tmp_path / "m" / "pic.png"))
    link = str(tmp_path / "m" / "link.png")
    try:
        os.symlink(src, link)
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    groups = dupes.find_duplicates([src, link])
    kept, _hidden = dupes.collapse([src, link], groups)
    assert kept == [src]


# -- timers must not outlive the window ----------------------------------------

def test_startup_timers_die_with_the_window(qapp):
    """A bare QTimer.singleShot outlives its target: the store warning opened a
    MODAL dialog on a half-torn-down window and aborted the process."""
    from PySide6.QtCore import QTimer
    from gallery_py_qt.main_window import MainWindow
    win = MainWindow()
    for name in ("_store_warn_timer", "_housekeeping_timer"):
        t = getattr(win, name)
        assert isinstance(t, QTimer) and t.parent() is win, f"{name} is unowned"
    win.close()
    win.deleteLater()
    qapp.processEvents()


def test_store_warning_is_silent_on_a_hidden_window(qapp, monkeypatch):
    from gallery_py_qt.main_window import MainWindow
    from gallery_py_qt.engine import tags as _tags
    win = MainWindow()                      # never shown
    monkeypatch.setattr(_tags, "load_failed", lambda: True)
    shown = []
    from PySide6.QtWidgets import QMessageBox
    monkeypatch.setattr(QMessageBox, "warning",
                        lambda *a, **k: shown.append(1))
    win._warn_if_stores_unreadable()
    assert not shown, "must not raise a modal dialog on a window with no screen"
    win.close()


def test_album_mirrors_link_under_auto(tmp_path):
    """"auto" must reach album mirroring too, or whole trees still get copied."""
    from gallery_py_qt.engine import foldertags
    favorites.set_link_mode("auto")
    album = tmp_path / "album"
    _img(str(album / "a.png"))
    dst = str(tmp_path / "mirror")
    how = foldertags.place_folder(str(album), dst)
    assert how in ("hardlink", "symlink"), f"album was {how}ed, not linked"
    assert os.path.isfile(os.path.join(dst, "a.png"))


def test_album_mirror_copy_mode_still_copies(tmp_path):
    from gallery_py_qt.engine import foldertags
    favorites.set_link_mode("copy")
    album = tmp_path / "album"
    _img(str(album / "a.png"))
    dst = str(tmp_path / "mirror")
    assert foldertags.place_folder(str(album), dst) == "copy"
    assert os.path.isfile(os.path.join(dst, "a.png"))
